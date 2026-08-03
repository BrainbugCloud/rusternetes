//===----------------------------------------------------------------------===//
// Adding containers to a *running* pod on Virtualization.framework.
//
// CRI's ordering is RunPodSandbox → CreateContainer → StartContainer: containers
// arrive one at a time, after the sandbox exists. `LinuxPod.addContainer` handles
// that by calling `vm.hotplug(rootfs, id:)`, and on VZ that throws
// "hotplug not supported" — `VZVirtualMachineInstance.hotplugProvider` is nil and
// nothing in Containerization ever assigns it. The only implementation Apple
// ships is `CHHotplugProvider`, for cloud-hypervisor on Linux.
//
// Block hotplug is genuinely impossible here: `VZVirtualMachine` exposes no
// runtime `storageDevices` array, so a virtio-blk device can only be attached at
// boot. (`VZUSBController.attachDevice` — macOS 15+ — could carry a disk, but the
// guest kernel Apple ships has no USB stack at all: zero xhci/usb-storage
// symbols.)
//
// virtio-fs can, though. `VZVirtioFileSystemDevice.share` is read-write at
// runtime, and Containerization already gives every VZ VM exactly one unified
// virtiofs device tagged `virtiofs` holding a `VZMultipleDirectoryShare` —
// "This device hosts all virtiofs shares and supports runtime updates"
// (VZVirtualMachineInstance.swift). The guest mounts that tag once at
// /run/virtiofs and sees each share as a subdirectory. Swapping the share under
// the mounted guest was measured before this was written (see
// ShareMutationProbe.swift): additions land in under a poll interval, removals in
// ~1.1s, and the mountpoint's st_dev never changes — the filesystem is not
// remounted and inode identity survives.
//
// So a container's rootfs is admitted by adding its host directory to the live
// share. This is Kata's `disable_block_device_use` model: pass the rootfs over
// virtio-fs instead of a block device, trading I/O throughput for the ability to
// attach it after boot. Ported from `CHHotplugProvider` — same protocol, same
// refcount-per-tag structure, same record/release split.
//
// The one shape difference from CH: CH runs `.perTag` (one virtiofs device per
// tag, mountable directly), VZ runs `.unified` (one device, shares as
// subdirectories). So where CH returns a virtiofs mount of the tag, this returns
// a *bind* of the tag's subdirectory — the rootfs is already inside a mounted
// filesystem by the time LinuxPod mounts it. See `unifiedMountPoint` for why that
// mount is not the one LinuxPod makes.
//===----------------------------------------------------------------------===//

import Containerization
import ContainerizationOCI
import Foundation
import Synchronization
import Virtualization

/// The tag Containerization gives the single unified virtiofs device on VZ.
private let unifiedTag = "virtiofs"
/// Where *this provider* mounts the unified share.
///
/// Deliberately not `/run/virtiofs`, which is LinuxPod's. LinuxPod mounts the
/// same tag there itself the first time a container brings additional virtiofs
/// mounts, and it tracks that with private state it sets at boot — state that is
/// always `false` here, because a CRI pod boots with no containers at all. Both
/// mounting the same tag on the same path wedged the guest: `addContainer` for a
/// container with a volume never returned. Two mountpoints, one owner each.
private let unifiedMountPoint = "/run/rk-virtiofs"

/// `instance.hotplugProvider = self` makes the instance own the provider, so the
/// back-reference has to be weak or the pair outlives the pod.
private final class InstanceBox: @unchecked Sendable {
    private let lock = NSLock()
    private weak var stored: VZVirtualMachineInstance?

    func set(_ instance: VZVirtualMachineInstance) {
        lock.lock()
        defer { lock.unlock() }
        stored = instance
    }

    var value: VZVirtualMachineInstance? {
        lock.lock()
        defer { lock.unlock() }
        return stored
    }
}

/// Admits container rootfs and file mounts into a running VZ pod by mutating the
/// unified virtiofs share.
final class VZVirtiofsHotplugProvider: HotplugProvider {
    /// What a container holds, so release can hand it back. Split by kind
    /// because `LinuxPod` releases the rootfs (`releaseHotplug`) and the
    /// additional shares (`releaseVirtioFS`) through separate calls.
    private enum Record: Sendable {
        case rootfs(tag: String)
        case additional(tag: String)

        var tag: String {
            switch self {
            case .rootfs(let tag), .additional(let tag): return tag
            }
        }
    }

    private struct TagState: Sendable {
        let url: URL
        let readOnly: Bool
        var refcount: Int
    }

    private let instanceBox = InstanceBox()
    /// Only tags *this provider* added. Boot-time entries are never in here, so
    /// they can never be refcounted to zero and removed out from under the pod.
    private let tags = Mutex<[String: TagState]>([:])
    private let records = Mutex<[String: [Record]]>([:])
    private let unifiedMounted = Mutex<Bool>(false)

    func attach(to instance: VZVirtualMachineInstance) {
        instanceBox.set(instance)
    }

    private func instance() throws -> VZVirtualMachineInstance {
        guard let instance = instanceBox.value else {
            throw BrokerError.badRequest("hotplug provider is not attached to a live VM")
        }
        return instance
    }

    // MARK: - Mutating the live share

    /// Read the device's current directories, apply `body`, write them back.
    ///
    /// Read-modify-write against the device rather than against a host-side
    /// mirror: boot-time entries were named by Containerization's internal
    /// `hashFilePath`, and this way they are preserved without modelling them.
    private func mutateShare(_ body: (inout [String: VZSharedDirectory]) -> Void) throws {
        let instance = try instance()
        let vm = instance.vzVirtualMachine
        try instance.vmQueue.sync {
            let devices = vm.directorySharingDevices.compactMap { $0 as? VZVirtioFileSystemDevice }
            guard let device = devices.first(where: { $0.tag == unifiedTag }) else {
                throw BrokerError.badRequest(
                    "no virtiofs device tagged '\(unifiedTag)' (found \(devices.map(\.tag)))")
            }
            var directories = (device.share as? VZMultipleDirectoryShare)?.directories ?? [:]
            body(&directories)
            device.share = VZMultipleDirectoryShare(directories: directories)
        }
    }

    /// Add `source` to the share under `tag`, or bump its refcount if another
    /// container already brought the same host directory in.
    private func retain(tag: String, source: String, readOnly: Bool) throws {
        let isNew = tags.withLock { tags -> Bool in
            if var state = tags[tag] {
                state.refcount += 1
                tags[tag] = state
                return false
            }
            tags[tag] = TagState(url: URL(filePath: source), readOnly: readOnly, refcount: 1)
            return true
        }
        guard isNew else { return }
        do {
            try mutateShare { $0[tag] = VZSharedDirectory(url: URL(filePath: source), readOnly: readOnly) }
        } catch {
            tags.withLock { $0.removeValue(forKey: tag) }
            throw error
        }
        trace("hotplug: shared \(source) as \(unifiedMountPoint)/\(tag)")
    }

    /// Drop a reference, removing the directory from the share when the last
    /// container using it goes away.
    private func release(tag: String) {
        let drop = tags.withLock { tags -> Bool in
            guard var state = tags[tag] else { return false }
            state.refcount -= 1
            if state.refcount <= 0 {
                tags.removeValue(forKey: tag)
                return true
            }
            tags[tag] = state
            return false
        }
        guard drop else { return }
        do {
            try mutateShare { $0.removeValue(forKey: tag) }
            trace("hotplug: released \(unifiedMountPoint)/\(tag)")
        } catch {
            // The VM is usually already gone by the time a release fails, and
            // the share dies with it. Losing the pod over it would be worse.
            trace("hotplug: releasing \(tag) failed: \(error)")
        }
    }

    // MARK: - The guest side of the unified mount

    /// Ensure `/run/virtiofs` is mounted before any rootfs bind resolves against it.
    ///
    /// `LinuxPod` mounts it itself, but only when the added container brings
    /// *additional* virtiofs mounts, and only after it has already mounted the
    /// rootfs — so a container with no file mounts would bind against a path
    /// that does not exist yet. Doing it here keeps the attachment this provider
    /// returns self-contained.
    private func ensureUnifiedMount() async throws {
        if unifiedMounted.withLock({ $0 }) { return }

        let agent = try await instance().dialAgent()
        defer { Task { try? await agent.close() } }

        // Already mounted (a boot-time container had virtiofs mounts) shows up as
        // a different device number than the parent directory. Mounting twice
        // would only stack an identical view, but it would also be a lie in
        // /proc/mounts.
        try await agent.mkdir(path: unifiedMountPoint, all: true, perms: 0o755)
        let parent = try? await agent.stat(path: URL(filePath: "/run"))
        let target = try? await agent.stat(path: URL(filePath: unifiedMountPoint))
        if let parent, let target, parent.dev != target.dev {
            unifiedMounted.withLock { $0 = true }
            return
        }

        try await agent.mount(
            ContainerizationOCI.Mount(
                type: "virtiofs",
                source: unifiedTag,
                destination: unifiedMountPoint,
                options: []
            ))
        unifiedMounted.withLock { $0 = true }
        trace("hotplug: mounted \(unifiedTag) at \(unifiedMountPoint)")
    }

    // MARK: - HotplugProvider

    func hotplug(_ rootfs: Containerization.Mount, id: String) async throws -> AttachedFilesystem {
        guard case .virtiofs = rootfs.runtimeOptions else {
            throw BrokerError.badRequest(
                """
                container \(id) has a \(rootfs.type) rootfs, which cannot be attached to a \
                running VM: Virtualization.framework exposes no runtime storage devices, so \
                block rootfs images can only be attached at boot. Containers added after \
                RunPodSandbox need a directory rootfs shared over virtiofs.
                """)
        }

        let tag = try rootfs.tagHash
        try retain(tag: tag, source: rootfs.source, readOnly: rootfs.options.contains("ro"))
        records.withLock { $0[id, default: []].append(.rootfs(tag: tag)) }

        do {
            try await ensureUnifiedMount()
        } catch {
            release(tag: tag)
            records.withLock { $0[id]?.removeAll { $0.tag == tag } }
            throw error
        }

        // A bind, not a virtiofs mount: under `.unified` the share is already
        // mounted at /run/virtiofs and this rootfs is a subdirectory of it.
        // `LinuxPod` mounts whatever comes back at /run/container/<id>/rootfs.
        return AttachedFilesystem(
            type: "bind",
            source: "\(unifiedMountPoint)/\(tag)",
            destination: rootfs.destination,
            options: rootfs.options + ["bind"]
        )
    }

    func hotplugVirtioFS(_ mounts: [Containerization.Mount], id: String) async throws {
        // Several mounts can point at one host directory; they share a tag and a
        // single share entry, so dedupe before refcounting.
        var byTag: [String: Containerization.Mount] = [:]
        for mount in mounts {
            guard case .virtiofs = mount.runtimeOptions else { continue }
            byTag[try mount.tagHash] = mount
        }
        guard !byTag.isEmpty else { return }

        for (tag, mount) in byTag {
            try retain(tag: tag, source: mount.source, readOnly: mount.options.contains("ro"))
            records.withLock { $0[id, default: []].append(.additional(tag: tag)) }
        }
        try await ensureUnifiedMount()
    }

    func registerMounts(id: String, rootfs: AttachedFilesystem, additionalMounts: [Containerization.Mount]) throws {
        var attached: [AttachedFilesystem] = [rootfs]
        for mount in additionalMounts {
            attached.append(try Self.attachment(for: mount))
        }
        try instance().withMountRegistry { registry in
            registry[id, default: []].append(contentsOf: attached)
        }
    }

    /// `FileMountContext.mountHoldingDirectories` looks each file mount up by
    /// `type == "virtiofs" && source == tag`, so virtiofs entries must be
    /// recorded under their tag, not their host path.
    private static func attachment(for mount: Containerization.Mount) throws -> AttachedFilesystem {
        switch mount.runtimeOptions {
        case .virtiofs:
            return AttachedFilesystem(
                type: mount.type,
                source: try mount.tagHash,
                destination: mount.destination,
                options: mount.options
            )
        case .shared, .any:
            return AttachedFilesystem(
                type: mount.type,
                source: mount.source,
                destination: mount.destination,
                options: mount.options
            )
        case .virtioblk:
            throw BrokerError.badRequest(
                "mount \(mount.source) is virtio-blk; VZ cannot attach block devices to a running VM")
        }
    }

    func releaseHotplug(id: String) async throws {
        for record in pop(id, keeping: { if case .rootfs = $0 { return false } else { return true } }) {
            release(tag: record.tag)
        }
        dropRegistryEntries(id) { $0.type == "bind" && $0.source.hasPrefix("\(unifiedMountPoint)/") }
    }

    func releaseVirtioFS(id: String) async throws {
        for record in pop(id, keeping: { if case .additional = $0 { return false } else { return true } }) {
            release(tag: record.tag)
        }
        dropRegistryEntries(id) { $0.type == "virtiofs" }
    }

    func cleanup() {
        // The VM is gone; the share went with it. Just stop holding the state.
        tags.withLock { $0.removeAll() }
        records.withLock { $0.removeAll() }
        unifiedMounted.withLock { $0 = false }
    }

    // MARK: - Bookkeeping

    /// Remove and return the records for `id` that `keeping` rejects.
    private func pop(_ id: String, keeping: (Record) -> Bool) -> [Record] {
        records.withLock { records in
            let all = records[id] ?? []
            let taken = all.filter { !keeping($0) }
            let remaining = all.filter(keeping)
            if remaining.isEmpty {
                records.removeValue(forKey: id)
            } else {
                records[id] = remaining
            }
            return taken
        }
    }

    private func dropRegistryEntries(_ id: String, matching: (AttachedFilesystem) -> Bool) {
        guard let instance = instanceBox.value else { return }
        instance.withMountRegistry { registry in
            guard var perID = registry[id] else { return }
            perID.removeAll(where: matching)
            if perID.isEmpty {
                registry.removeValue(forKey: id)
            } else {
                registry[id] = perID
            }
        }
    }
}

/// Installs the provider on the instance the moment Containerization creates it.
///
/// `VZInstanceExtension.didCreate` is the only hook that hands out the live
/// `VZVirtualMachineInstance`; `LinuxPod` forwards `Configuration.extensions`
/// into the VM config, so putting one of these in a pod's extensions is what
/// makes `addContainer`-after-`create` work.
struct VZHotplugInstaller: VZInstanceExtension {
    let provider: VZVirtiofsHotplugProvider

    func didCreate(_ instance: VZVirtualMachineInstance) throws {
        provider.attach(to: instance)
        instance.hotplugProvider = provider
    }

    func willStop(_ instance: VZVirtualMachineInstance) async throws {
        provider.cleanup()
    }
}
