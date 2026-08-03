//===----------------------------------------------------------------------===//
// Image -> host directory, for a virtiofs rootfs.
//
// The ext4 path (`EXT4Unpacker`) produces a block device, and on VZ a block
// device can only be attached at boot — there is no runtime `storageDevices`
// array to add one to. Containers created after `RunPodSandbox`, which is every
// container CRI ever asks for, therefore cannot use one. They get a directory
// shared into the running VM over virtiofs instead; see VZHotplugProvider.swift.
//
// Apple's own suite has a version of this (`unpackRootfsDirectory` in
// Sources/Integration/PodTests.swift) but it states its assumption plainly:
// "Assumes a single-layer image ... so no OCI whiteout processing is required."
// Kubernetes images are routinely multi-layer, so the whiteout rules are ported
// here from containerd's `pkg/archive/tar.go` (`whiteoutPrefix`,
// `whiteoutMetaPrefix`, `whiteoutOpaqueDir` — lines 122-131, and the
// `convertWhiteout` default in `applyNaive`).
//
// # Fidelity, and what is lost
//
// `EXT4Unpacker` writes inode metadata directly into a filesystem image, so it
// reproduces uid/gid, device nodes and setuid bits without being root. Extracting
// to a host directory as an ordinary user cannot:
//
//   - **Ownership is not preserved.** Every file ends up owned by the broker's
//     uid, and virtiofs shows the guest exactly that. A container running as root
//     is unaffected (root bypasses the checks), and world-readable image content
//     is unaffected. A container running as a non-root user that needs to *write*
//     to image-owned paths will see EACCES where it would not under ext4.
//   - **Device nodes, fifos and sockets are skipped.** Creating them needs
//     CAP_MKNOD. Images rarely ship them (the runtime makes /dev), and the count
//     is traced when it is not zero.
//   - **APFS is case-insensitive by default.** Two image paths differing only in
//     case collide; the later one wins. Rare, and loud enough to spot in the
//     extracted tree if it ever happens.
//
// These are the cost of attaching a rootfs after boot on this platform, not
// oversights. They are recorded in crates/apple-cri/STATUS.md.
//===----------------------------------------------------------------------===//

import Containerization
import ContainerizationArchive
import ContainerizationOCI
import Foundation

/// Unpacks an OCI image's layers into a host directory usable as a virtiofs rootfs.
struct DirectoryUnpacker: Sendable {
    /// containerd `pkg/archive/tar.go:122`.
    private static let whiteoutPrefix = ".wh."
    /// containerd `pkg/archive/tar.go:127`. Markers with this prefix carry meaning
    /// other than "delete this file" and are never extracted.
    private static let whiteoutMetaPrefix = ".wh..wh."
    /// containerd `pkg/archive/tar.go:131`.
    private static let whiteoutOpaqueDir = ".wh..wh..opq"

    func unpack(
        _ image: Containerization.Image,
        for platform: ContainerizationOCI.Platform,
        at root: URL
    ) async throws -> Containerization.Mount {
        try? FileManager.default.removeItem(at: root)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)

        // Directory modes are applied only once every layer is in. An image may
        // ship a read-only directory that a later layer writes into, and the
        // extraction has to be able to write it first. containerd defers the same
        // way (the `dirs` slice in `applyNaive`).
        var directoryModes: [(url: URL, mode: mode_t)] = []
        var skippedSpecial = 0

        let manifest = try await image.manifest(for: platform)
        for layer in manifest.layers {
            try Task.checkCancellation()
            let content = try await image.getContent(digest: layer.digest)
            let reader = try ArchiveReader(
                format: .paxRestricted,
                filter: try Self.compressionFilter(for: layer.mediaType),
                file: content.path
            )

            // Whiteouts only erase what *lower* layers put down; a marker cannot
            // remove a sibling written by its own layer.
            var writtenThisLayer: Set<String> = []

            for (entry, data) in reader {
                guard let relative = Self.sanitise(entry.path) else { continue }
                let components = relative.split(separator: "/").map(String.init)
                guard let base = components.last else { continue }
                let parentComponents = components.dropLast()
                let parent = parentComponents.reduce(root) { $0.appendingPathComponent($1) }

                if base.hasPrefix(Self.whiteoutMetaPrefix) {
                    if base == Self.whiteoutOpaqueDir {
                        Self.makeOpaque(parent, keeping: writtenThisLayer, parentComponents: Array(parentComponents))
                    }
                    // Any other meta marker (`.wh..wh..plnk`, …) is not content.
                    continue
                }
                if base.hasPrefix(Self.whiteoutPrefix) {
                    let target = parent.appendingPathComponent(String(base.dropFirst(Self.whiteoutPrefix.count)))
                    try? FileManager.default.removeItem(at: target)
                    continue
                }

                let destination = parent.appendingPathComponent(base)
                try FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)

                if let hardlink = entry.hardlink, !hardlink.isEmpty {
                    guard let linkTarget = Self.sanitise(hardlink) else { continue }
                    Self.removeExisting(destination)
                    try? FileManager.default.linkItem(
                        at: root.appendingPathComponent(linkTarget), to: destination)
                    writtenThisLayer.insert(relative)
                    continue
                }

                switch entry.fileType {
                case .directory:
                    // Merged, not replaced: layers add to directories.
                    if !FileManager.default.fileExists(atPath: destination.path) {
                        try FileManager.default.createDirectory(
                            at: destination, withIntermediateDirectories: true)
                    }
                    directoryModes.append((destination, entry.permissions))

                case .symbolicLink:
                    guard let target = entry.symlinkTarget else { break }
                    Self.removeExisting(destination)
                    try? FileManager.default.createSymbolicLink(
                        atPath: destination.path, withDestinationPath: target)

                case .regular:
                    Self.removeExisting(destination)
                    FileManager.default.createFile(atPath: destination.path, contents: data)
                    Self.setMode(entry.permissions, on: destination)

                default:
                    // Character/block devices, fifos, sockets: need CAP_MKNOD.
                    skippedSpecial += 1
                }
                writtenThisLayer.insert(relative)
            }
        }

        for (url, mode) in directoryModes {
            Self.setMode(mode, on: url)
        }
        if skippedSpecial > 0 {
            trace("directory unpack: skipped \(skippedSpecial) special file(s) (device/fifo/socket)")
        }

        return .share(source: root.path, destination: "/")
    }

    // MARK: - Helpers

    /// Normalise a tar path and refuse anything that escapes the root.
    private static func sanitise(_ path: String?) -> String? {
        guard var path else { return nil }
        while path.hasPrefix("./") { path.removeFirst(2) }
        while path.hasPrefix("/") { path.removeFirst() }
        if path.hasSuffix("/") { path.removeLast() }
        guard !path.isEmpty, path != "." else { return nil }
        // A `..` component would let a malicious layer write outside the rootfs.
        guard !path.split(separator: "/").contains("..") else {
            trace("directory unpack: refusing path escaping the rootfs: \(path)")
            return nil
        }
        return path
    }

    /// An opaque marker hides everything the lower layers put in this directory.
    private static func makeOpaque(_ directory: URL, keeping: Set<String>, parentComponents: [String]) {
        let children = (try? FileManager.default.contentsOfDirectory(atPath: directory.path)) ?? []
        for child in children {
            let childRelative = (parentComponents + [child]).joined(separator: "/")
            guard !keeping.contains(childRelative) else { continue }
            try? FileManager.default.removeItem(at: directory.appendingPathComponent(child))
        }
    }

    /// A higher layer's entry replaces a lower layer's, whatever its type was.
    private static func removeExisting(_ url: URL) {
        // `fileExists` follows symlinks, so a dangling link would report false and
        // survive to break the write.
        if (try? FileManager.default.attributesOfItem(atPath: url.path)) != nil {
            try? FileManager.default.removeItem(at: url)
        }
    }

    private static func setMode(_ mode: mode_t, on url: URL) {
        try? FileManager.default.setAttributes(
            [.posixPermissions: NSNumber(value: mode)], ofItemAtPath: url.path)
    }

    /// Ported from `EXT4Unpacker.compressionFilter`, so both unpackers accept
    /// exactly the same set of layer media types.
    private static func compressionFilter(for mediaType: String) throws -> ContainerizationArchive.Filter {
        switch mediaType {
        case MediaTypes.imageLayer, MediaTypes.dockerImageLayer:
            return .none
        case MediaTypes.imageLayerGzip, MediaTypes.dockerImageLayerGzip:
            return .gzip
        case MediaTypes.imageLayerZstd, MediaTypes.dockerImageLayerZstd:
            return .zstd
        default:
            throw BrokerError.badRequest("media type \(mediaType) not supported")
        }
    }
}
