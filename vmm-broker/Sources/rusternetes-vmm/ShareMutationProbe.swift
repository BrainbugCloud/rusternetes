//===----------------------------------------------------------------------===//
// A probe for the one unknown that decides whether a pod can grow containers on
// Virtualization.framework.
//
// VZ has no virtio-blk hotplug and never will: `VZVirtualMachine` exposes no
// runtime `storageDevices` array (only console/directorySharing/graphics/
// memoryBalloon/network/socket/usbControllers), and Containerization says so
// itself — "VZ has no runtime hotplug", Sources/Integration/Suite.swift.
//
// But `VZVirtioFileSystemDevice.share` is read-write at runtime (macOS 12+),
// and Containerization already gives every VZ VM exactly one unified virtiofs
// device tagged `virtiofs` carrying a `VZMultipleDirectoryShare`, with the
// comment "This device hosts all virtiofs shares and supports runtime updates"
// (VZVirtualMachineInstance.swift). The guest mounts that tag once at
// /run/virtiofs and sees each share as a subdirectory.
//
// So: if a share can be swapped under an already-*mounted* virtiofs without
// disturbing it, a container rootfs can be handed to a running pod as a new
// subdirectory — Kata's `disable_block_device_use` model — and CRI's
// create-after-boot ordering works. If the swap remounts or invalidates the
// filesystem, it does not, and the pod path needs a different design.
//
// This probe answers exactly that, with no images and no LinuxPod: boot a VM,
// mount the unified share, then add / add-again / remove directories under the
// running guest, checking visibility and — via the mountpoint's st_dev — that
// the superblock underneath never changed.
//
//   rusternetes-vmm --kernel <path> --share-mutation-probe
//===----------------------------------------------------------------------===//

import Containerization
import ContainerizationOCI
import ContainerizationOS
import Foundation
import Virtualization

/// The tag Containerization gives the single unified virtiofs device on VZ.
private let unifiedTag = "virtiofs"
/// Where this probe mounts that tag in the guest, matching LinuxPod's `.unified` layout.
private let mountPoint = "/run/virtiofs"

private func probeNote(_ message: String) {
    FileHandle.standardError.write("share-probe: \(message)\n".data(using: .utf8)!)
}

// MARK: - Host side: reading and swapping the live share

/// Every VZ object below is touched only inside `queue.sync`, and only
/// `Sendable` values cross that boundary — the framework requires its own queue
/// and the types are not `Sendable`.
private func withUnifiedDevice<T: Sendable>(
    _ vm: VZVirtualMachine,
    _ queue: DispatchQueue,
    _ body: (VZVirtioFileSystemDevice) throws -> T
) throws -> T {
    try queue.sync {
        let devices = vm.directorySharingDevices.compactMap { $0 as? VZVirtioFileSystemDevice }
        guard let device = devices.first(where: { $0.tag == unifiedTag }) else {
            throw BrokerError.badRequest(
                "no virtiofs device tagged '\(unifiedTag)' (found \(devices.map(\.tag)))")
        }
        return try body(device)
    }
}

/// The directory names the guest currently sees as subdirectories of `mountPoint`.
private func shareNames(_ vm: VZVirtualMachine, _ queue: DispatchQueue) throws -> [String] {
    try withUnifiedDevice(vm, queue) { device in
        guard let share = device.share as? VZMultipleDirectoryShare else { return [] }
        return share.directories.keys.sorted()
    }
}

/// Replace the live share with `mutate` applied to its current directories.
///
/// Reading the existing dictionary back off the device (rather than tracking it
/// host-side) is what a real `HotplugProvider` would do, and it sidesteps
/// Containerization's internal `hashFilePath` naming for the boot-time entries.
private func mutateShare(
    _ vm: VZVirtualMachine,
    _ queue: DispatchQueue,
    _ mutate: ([String: VZSharedDirectory]) -> [String: VZSharedDirectory]
) throws {
    try withUnifiedDevice(vm, queue) { device in
        let current = (device.share as? VZMultipleDirectoryShare)?.directories ?? [:]
        device.share = VZMultipleDirectoryShare(directories: mutate(current))
    }
}

// MARK: - Guest side: probing through the agent

private func guestStat(_ agent: Vminitd, _ path: String) async -> ContainerizationOS.Stat? {
    try? await agent.stat(path: URL(filePath: path))
}

private func guestExists(_ agent: Vminitd, _ path: String) async -> Bool {
    await guestStat(agent, path) != nil
}

/// Poll until `path` reaches `expected` presence, returning how long it took.
///
/// The wait matters as much as the result: virtiofs caches dentries (including
/// negative ones), so a share change could in principle be correct but land
/// seconds late. A real provider would need to know that before it returns to
/// CRI. Reported in milliseconds.
private func waitForPresence(
    _ agent: Vminitd,
    _ path: String,
    expected: Bool,
    timeout: Duration = .seconds(15)
) async -> (settled: Bool, elapsedMs: Int) {
    let clock = ContinuousClock()
    let start = clock.now
    while clock.now - start < timeout {
        if await guestExists(agent, path) == expected {
            return (true, Int((clock.now - start) / .milliseconds(1)))
        }
        try? await Task.sleep(for: .milliseconds(100))
    }
    return (false, Int(timeout / .milliseconds(1)))
}

// MARK: - The probe

func runShareMutationProbe(manager: VZVirtualMachineManager, runtimeDir: URL) async throws -> Bool {
    var failures = 0
    func check(_ name: String, _ passed: Bool, _ detail: String = "") {
        failures += passed ? 0 : 1
        let suffix = detail.isEmpty ? "" : " — \(detail)"
        probeNote("\(passed ? "PASS" : "FAIL")  \(name)\(suffix)")
    }

    // Host scratch: one directory per share entry, each with a file to stat.
    let root = runtimeDir.appendingPathComponent("share-probe")
    try? FileManager.default.removeItem(at: root)
    let seedDir = root.appendingPathComponent("seed")
    let alphaDir = root.appendingPathComponent("alpha")
    let bravoDir = root.appendingPathComponent("bravo")
    for (dir, file) in [(seedDir, "seed.txt"), (alphaDir, "a.txt"), (bravoDir, "b.txt")] {
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        try "\(file)\n".write(to: dir.appendingPathComponent(file), atomically: true, encoding: .utf8)
    }

    // Boot with one virtiofs mount so the unified device starts non-empty, which
    // is the real shape: a pod always has at least its first container's shares.
    var config = VMConfiguration(cpus: 2, memoryInBytes: 1024 * 1024 * 1024)
    config.bootLog = .file(path: runtimeDir.appendingPathComponent("share-probe-boot.log"))
    config.mountsByID = ["probe": [Mount.share(source: seedDir.path, destination: "/mnt/seed")]]

    probeNote("booting vm")
    let instance = try manager.create(config: StandardVMConfig(configuration: config))
    guard let vz = instance as? VZVirtualMachineInstance else {
        throw BrokerError.badRequest("expected a VZVirtualMachineInstance, got \(type(of: instance))")
    }
    try await vz.start()

    func teardown() async {
        try? await vz.stop()
    }

    let vm = vz.vzVirtualMachine
    let queue = vz.vmQueue

    let agent: Vminitd
    do {
        agent = try await vz.dialAgent()
        try await agent.standardSetup()
    } catch {
        await teardown()
        throw error
    }

    do {
        // Mount the unified tag exactly as LinuxPod does for `.unified`.
        try await agent.mkdir(path: mountPoint, all: true, perms: 0o755)
        try await agent.mount(
            ContainerizationOCI.Mount(
                type: "virtiofs", source: unifiedTag, destination: mountPoint, options: []))
        probeNote("mounted \(unifiedTag) at \(mountPoint)")

        let bootNames = try shareNames(vm, queue)
        probeNote("boot-time share entries: \(bootNames)")
        guard let seedName = bootNames.first else {
            check("boot share is non-empty", false, "no directories in the boot share")
            await teardown()
            return false
        }

        let seedPath = "\(mountPoint)/\(seedName)/seed.txt"
        let alphaPath = "\(mountPoint)/probe-alpha/a.txt"
        let bravoPath = "\(mountPoint)/probe-bravo/b.txt"

        // 1. Baseline — the boot-time share is visible, and record the superblock
        //    we must not lose.
        check("boot-time share visible in guest", await guestExists(agent, seedPath), seedPath)
        guard let mountStat = await guestStat(agent, mountPoint) else {
            check("mountpoint stat", false, mountPoint)
            await teardown()
            return false
        }
        let originalDev = mountStat.dev
        let originalSeedIno = await guestStat(agent, seedPath)?.ino
        probeNote("mountpoint st_dev=\(originalDev), seed file st_ino=\(originalSeedIno.map(String.init) ?? "?")")

        // 2. Add a directory under the running guest — the operation a
        //    HotplugProvider would perform to admit a new container's rootfs.
        try mutateShare(vm, queue) { current in
            var next = current
            next["probe-alpha"] = VZSharedDirectory(url: alphaDir, readOnly: false)
            return next
        }
        let alphaAppeared = await waitForPresence(agent, alphaPath, expected: true)
        check(
            "added directory appears in the running guest", alphaAppeared.settled,
            "after \(alphaAppeared.elapsedMs)ms")
        check("pre-existing share survives the swap", await guestExists(agent, seedPath), seedPath)
        check(
            "mount not replaced (st_dev stable)",
            await guestStat(agent, mountPoint)?.dev == originalDev,
            "st_dev=\(await guestStat(agent, mountPoint)?.dev.description ?? "?")")
        check(
            "pre-existing file identity stable (st_ino)",
            await guestStat(agent, seedPath)?.ino == originalSeedIno,
            "st_ino=\(await guestStat(agent, seedPath)?.ino.description ?? "?")")

        // 3. A second add — a pod grows more than once, and each swap replaces
        //    the whole share object.
        try mutateShare(vm, queue) { current in
            var next = current
            next["probe-bravo"] = VZSharedDirectory(url: bravoDir, readOnly: false)
            return next
        }
        let bravoAppeared = await waitForPresence(agent, bravoPath, expected: true)
        check(
            "second added directory appears", bravoAppeared.settled,
            "after \(bravoAppeared.elapsedMs)ms")
        check("first added directory survives second swap", await guestExists(agent, alphaPath), alphaPath)
        check("boot-time share survives second swap", await guestExists(agent, seedPath), seedPath)
        check(
            "mount still not replaced (st_dev stable)",
            await guestStat(agent, mountPoint)?.dev == originalDev)

        // 4. Removal — RemoveContainer has to give the rootfs back.
        try mutateShare(vm, queue) { current in
            var next = current
            next.removeValue(forKey: "probe-alpha")
            return next
        }
        let alphaGone = await waitForPresence(agent, alphaPath, expected: false)
        check("removed directory disappears", alphaGone.settled, "after \(alphaGone.elapsedMs)ms")
        check("unrelated share survives removal", await guestExists(agent, bravoPath), bravoPath)
        check("boot-time share survives removal", await guestExists(agent, seedPath), seedPath)
        check(
            "mount still not replaced after removal (st_dev stable)",
            await guestStat(agent, mountPoint)?.dev == originalDev)

        probeNote("final share entries: \(try shareNames(vm, queue))")
    } catch {
        await teardown()
        throw error
    }

    await teardown()
    probeNote(failures == 0 ? "ALL CHECKS PASSED" : "\(failures) CHECK(S) FAILED")
    return failures == 0
}
