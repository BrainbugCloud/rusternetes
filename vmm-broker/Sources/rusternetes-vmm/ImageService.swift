//===----------------------------------------------------------------------===//
// Image -> ext4 block device.
//
// A VM boots from an initfs block carrying vminitd, and every container needs its
// own rootfs block. Both are the *same* operation: unpack an OCI image into an
// ext4 file. Apple ships the init filesystem as an image
// (ghcr.io/apple/containerization/vminit) rather than a file, so there is nothing
// to special-case — `initfs()` is `provision()` with a different reference and a
// read-only result.
//
// This lives in the broker because it already links ContainerizationEXT4, whose
// EXT4Unpacker is what upstream uses (`InitImage.initBlock` builds an
// `EXT4Unpacker(capacityInBytes: 512.mib())`). Reimplementing an ext4 writer in
// Rust to avoid one Swift call would be a poor trade.
//===----------------------------------------------------------------------===//

import Containerization
import ContainerizationExtras
import ContainerizationOCI
import Foundation

/// Materialises images as ext4 block devices, keyed by owner id.
///
/// A plain `Sendable` class rather than an actor: every field is a `let` and the
/// only shared mutable state is the filesystem, which is already keyed by owner
/// id. Making it an actor bought no safety and forced the setup path (which is
/// synchronous, before the server exists) to hop isolation.
final class ImageService: Sendable {
    private let store: ImageStore
    private let blocksDir: URL
    /// Root of the image store on disk, for `ImageFsInfo`.
    private let storeDir: URL
    private let platform: SystemPlatform
    /// Default rootfs capacity. ext4 is sparse, so this is a ceiling the image may
    /// grow into, not space consumed up front.
    private let rootfsCapacityInBytes: UInt64

    init(
        store: ImageStore,
        blocksDir: URL,
        storeDir: URL,
        platform: SystemPlatform,
        rootfsCapacityInBytes: UInt64 = 8 * 1024 * 1024 * 1024
    ) {
        self.store = store
        self.blocksDir = blocksDir
        self.storeDir = storeDir
        self.platform = platform
        self.rootfsCapacityInBytes = rootfsCapacityInBytes
    }

    /// Path of the rootfs directory backing `ownerId`.
    private func rootfsPath(_ ownerId: String) -> URL {
        blocksDir.appendingPathComponent("\(ownerId).rootfs")
    }

    /// Unpack `reference` into a per-owner rootfs directory, pulling the image if
    /// the store does not have it.
    ///
    /// A directory, not an ext4 block, because every container reaches the pod
    /// after its VM has booted and VZ cannot attach a block device to a running
    /// VM. See DirectoryUnpacker.swift for what that costs in metadata fidelity,
    /// and VZHotplugProvider.swift for how the directory gets in.
    ///
    /// Each container gets its own copy rather than a shared, snapshotted one:
    /// a container writes to its rootfs, and two containers from the same image
    /// must not see each other's writes.
    func provision(reference: String, ownerId: String) throws -> BlockMountWire {
        let path = rootfsPath(ownerId)
        try FileManager.default.createDirectory(
            at: blocksDir, withIntermediateDirectories: true)

        return try runBlocking {
            let image = try await self.store.get(reference: reference, pull: true)
            // Unpacking is destructive on `path`, so a leftover tree from a
            // previous attempt cannot leave a mix of two images.
            let mount = try await DirectoryUnpacker().unpack(
                image, for: self.platform.ociPlatform(), at: path)
            return BlockMountWire(
                format: mount.type,
                source: mount.source,
                destination: mount.destination,
                // Attached read-write: the OCI runtime remounts read-only from
                // the spec's root.readonly, which is how the Rust pod layer
                // expresses it (and why it strips "ro" before hotplug).
                options: []
            )
        }
    }

    /// Discard the block backing `ownerId`. Idempotent.
    // MARK: - CRI image management
    //
    // The pod path runs rootfs images out of *this* store, not Apple's CLI store,
    // so CRI's image RPCs have to be answered from here too. Pointing them at the
    // CLI store instead would let `PullImage` populate one store while the pod
    // pulls into the other, and `RemoveImage` leave the image still resolvable —
    // exactly what critest's Image Consistency suite checks.

    func list() throws -> [ImageWire] {
        try runBlocking { [store, platform] in
            var out: [ImageWire] = []
            for image in try await store.list() {
                out.append(await Self.describe(image, platform: platform))
            }
            return out
        }
    }

    /// `nil` rather than an error when the image is simply not present: CRI's
    /// `ImageStatus` reports absence, it does not fail.
    func status(reference: String) throws -> ImageWire? {
        try runBlocking { [store, platform] () -> ImageWire? in
            guard let image = try? await store.get(reference: reference) else { return nil }
            return await Self.describe(image, platform: platform)
        }
    }

    func pull(reference: String) throws -> ImageWire {
        try runBlocking { [store, platform] in
            let image = try await store.pull(reference: reference, platform: platform.ociPlatform())
            return await Self.describe(image, platform: platform)
        }
    }

    /// Removing an image that is already gone is success, as CRI requires.
    func remove(reference: String) throws {
        try runBlocking { [store] in
            do {
                try await store.delete(reference: reference, performCleanup: true)
            } catch {
                if (try? await store.get(reference: reference)) != nil { throw error }
            }
        }
    }

    /// Bytes the image store occupies on disk, for `ImageFsInfo`.
    ///
    /// Measured by walking the directory rather than summing image sizes: layers
    /// shared between images would otherwise be counted twice.
    func filesystemUsage() -> (path: String, usedBytes: UInt64) {
        let root = storeDir
        var total: UInt64 = 0
        if let walker = FileManager.default.enumerator(
            at: root, includingPropertiesForKeys: [.fileSizeKey], options: [])
        {
            for case let url as URL in walker {
                let size = (try? url.resourceValues(forKeys: [.fileSizeKey]))?.fileSize ?? 0
                total += UInt64(size)
            }
        }
        return (root.path, total)
    }

    private static func describe(_ image: Containerization.Image, platform: SystemPlatform) async
        -> ImageWire
    {
        // Size is the manifest's own accounting: config plus layers. A failure
        // here must not hide the image, so it degrades to 0 rather than throwing.
        var size: UInt64 = 0
        if let manifest = try? await image.manifest(for: platform.ociPlatform()) {
            size = UInt64(manifest.config.size) + manifest.layers.reduce(0) { $0 + UInt64($1.size) }
        }
        // The image's process configuration, reported verbatim. Every CRI
        // convention over it — splitting `User` into uid vs username, merging
        // entrypoint/cmd with the container's command/args — is the Rust side's.
        let config = try? await image.config(for: platform.ociPlatform()).config
        return ImageWire(
            reference: image.reference,
            digest: image.digest,
            sizeBytes: size,
            user: (config?.user ?? nil) ?? "",
            entrypoint: (config?.entrypoint ?? nil) ?? [],
            cmd: (config?.cmd ?? nil) ?? [],
            env: (config?.env ?? nil) ?? [],
            workingDir: (config?.workingDir ?? nil) ?? ""
        )
    }

    func release(ownerId: String) throws {
        let path = rootfsPath(ownerId)
        if FileManager.default.fileExists(atPath: path.path) {
            try FileManager.default.removeItem(at: path)
        }
    }

    /// Materialise the init filesystem — the block the VM boots, containing
    /// vminitd, which then serves SandboxContext on vsock port 1024.
    ///
    /// Cached: it is identical for every VM, and unpacking it per pod would add
    /// seconds to each sandbox creation.
    func initfs(reference: String) throws -> Containerization.Mount {
        let path = blocksDir.appendingPathComponent("initfs.ext4")
        try FileManager.default.createDirectory(
            at: blocksDir, withIntermediateDirectories: true)

        if FileManager.default.fileExists(atPath: path.path) {
            return .block(format: "ext4", source: path.path, destination: "/", options: ["ro"])
        }
        return try runBlocking {
            let initImage = try await self.store.getInitImage(reference: reference)
            return try await initImage.initBlock(at: path, for: self.platform)
        }
    }
}

/// Bridge an async call into a synchronous context.
///
/// The actor's callers are already async, but `ImageStore` work is driven from
/// setup paths that are not, and Swift has no `await` outside async. A semaphore
/// on a detached task is the narrow, standard workaround; it is only used for
/// image unpacking, which is slow and infrequent, never on a request hot path.
func runBlocking<T: Sendable>(_ body: @escaping @Sendable () async throws -> T) throws -> T {
    let semaphore = DispatchSemaphore(value: 0)
    // `nonisolated(unsafe)` is sound here: the semaphore serialises the write in
    // the task with the read after `wait()`.
    nonisolated(unsafe) var result: Result<T, Error>?
    Task.detached {
        do {
            result = .success(try await body())
        } catch {
            result = .failure(error)
        }
        semaphore.signal()
    }
    semaphore.wait()
    switch result! {
    case .success(let value): return value
    case .failure(let error): throw error
    }
}
