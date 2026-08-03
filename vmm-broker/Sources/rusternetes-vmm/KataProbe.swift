//===----------------------------------------------------------------------===//
// Step 1 of the kata-guest spike: boot a Kata guest under VZ and expose its
// agent to the host.
//
// The whole feasibility question is one `GetGuestDetails` round-trip, so this
// does the minimum that makes that possible: boot Kata's aarch64 kernel +
// initrd, wait for `kata-agent` to listen on vsock 1024, and splice that port
// to a unix socket the Rust ttrpc client can connect to. It then parks, because
// the client runs in another process.
//
//   rusternetes-vmm --kata-probe --kata-kernel <vmlinux> --kata-initrd <img> \
//                   --runtime-dir /tmp/kata
//
// See docs/KATA_GUEST.md. Nothing here is on the production path; it exists to
// answer a yes/no question before any of the port is written.
//===----------------------------------------------------------------------===//

import Containerization
import ContainerizationExtras
import Foundation
import Virtualization

/// Boot from a kernel + initrd instead of the kernel + ext4 block Containerization
/// assumes.
///
/// `VZVirtualMachineInstance.Configuration.toVZ` hardcodes
/// `VZLinuxBootLoader(kernelURL:)` — no initial ramdisk, its own command line —
/// and refuses to build at all without a block `initialFilesystem`. None of that
/// has to be forked: `configureVZ` runs *after* `toVZ`, on the finished
/// configuration, so the boot loader can simply be replaced. The block device
/// Containerization insisted on stays attached and the Kata guest ignores it.
struct KataBootExtension: VZInstanceExtension {
    let kernel: URL
    let initrd: URL
    let commandLine: String

    func configureVZ(
        _ config: inout VZVirtualMachineConfiguration,
        allocator: any AddressAllocator<Character>,
        storageDeviceCount: Int,
        mountsByID: [String: [Containerization.Mount]]
    ) throws {
        let loader = VZLinuxBootLoader(kernelURL: kernel)
        loader.initialRamdiskURL = initrd
        loader.commandLine = commandLine
        config.bootLoader = loader
    }
}

/// The guest agent's vsock port. `DEFAULT_AGENT_VSOCK_PORT` in
/// kata-types/src/config/default.rs — and, conveniently, the same port
/// `vminitd` uses, so the relay needs no change.
private let kataAgentPort: UInt32 = 1024

/// The agent's log port. The agent *connects out* on this one, so the host
/// listens — which also makes it a liveness signal: nothing else proves the
/// agent is alive before it starts serving.
private let kataLogPort: UInt32 = 1025

/// Kata boots its initrd straight into the agent: the initrd's `/init` *is*
/// `kata-agent`, so no `init=` is needed.
///
/// `agent.log_vport` matters more than it looks. The agent logs to stdout by
/// default, and PID 1 has no stdout — the kernel's own printk reaches the
/// console but the agent's does not, so a silent boot log says nothing about
/// whether the agent is alive. Sending its log over vsock is the only way to
/// tell "dead" from "quiet".
private let kataCommandLine =
    "console=hvc0 panic=1 reboot=k agent.log=debug agent.log_vport=\(kataLogPort)"

@Sendable private func note(_ message: String) {
    FileHandle.standardError.write("kata-probe: \(message)\n".data(using: .utf8)!)
}

/// `runBlocking` for a body that returns nothing and whose failure is not fatal.
private func runBlockingIgnoringErrors(_ body: @escaping @Sendable () async -> Void) {
    _ = try? runBlocking { await body() }
}

/// Carries the dial's result off the thread it ran on.
private final class DialOutcome: @unchecked Sendable {
    private let lock = NSLock()
    private var _handle: FileHandle?
    private var _error: Error?

    func set(handle: FileHandle) { lock.withLock { _handle = handle } }
    func set(error: Error) { lock.withLock { _error = error } }
    var handle: FileHandle? { lock.withLock { _handle } }
    var error: Error? { lock.withLock { _error } }
}

func runKataProbe(
    manager: VZVirtualMachineManager,
    runtimeDir: URL,
    kernel: String,
    initrd: String,
    socketPath: String
) throws {

    for (label, path) in [("kernel", kernel), ("initrd", initrd)] {
        guard FileManager.default.fileExists(atPath: path) else {
            throw BrokerError.badRequest("\(label) not found: \(path)")
        }
    }

    var configuration = VMConfiguration(cpus: 2, memoryInBytes: 2048 * 1024 * 1024)
    configuration.bootLog = .file(path: runtimeDir.appendingPathComponent("kata-boot.log"))
    configuration.extensions = [
        KataBootExtension(
            kernel: URL(filePath: kernel),
            initrd: URL(filePath: initrd),
            commandLine: kataCommandLine
        )
    ]

    note("booting kata guest")
    let vm = try manager.create(config: StandardVMConfig(configuration: configuration))
    guard let vz = vm as? VZVirtualMachineInstance else {
        throw BrokerError.badRequest("expected a VZVirtualMachineInstance")
    }
    // Listen before starting: the agent connects its log out as soon as it runs,
    // and a listener registered late would miss it.
    let logs = try vz.listen(kataLogPort)
    Thread {
        runBlockingIgnoringErrors {
            for await connection in logs {
                note("agent log stream opened")
                while true {
                    let chunk = connection.availableData
                    if chunk.isEmpty { break }
                    FileHandle.standardError.write(chunk)
                }
            }
        }
    }.start()

    try runBlocking { try await vz.start() }
    note("started; waiting for kata-agent on vsock \(kataAgentPort)")

    // One dial, with a deadline, rather than a retry loop: `dial` takes the
    // instance's async lock, so a connect that hangs (which is what VZ does when
    // nothing is listening on the port yet) wedges every later attempt behind it.
    // Give the guest a moment to boot first, then ask once.
    sleep(8)
    let outcome = DialOutcome()
    let dialed = DispatchSemaphore(value: 0)
    Thread {
        do { outcome.set(handle: try runBlocking { try await vz.dial(kataAgentPort) }) } catch {
            outcome.set(error: error)
        }
        dialed.signal()
    }.start()

    if dialed.wait(timeout: .now() + 45) == .timedOut {
        note("dial to vsock \(kataAgentPort) hung — nothing is listening there")
        note("boot log: \(runtimeDir.appendingPathComponent("kata-boot.log").path)")
        try? runBlocking { try await vz.stop() }
        throw BrokerError.io("kata-agent never listened on vsock \(kataAgentPort)")
    }
    guard let handle = outcome.handle else {
        note("boot log: \(runtimeDir.appendingPathComponent("kata-boot.log").path)")
        try? runBlocking { try await vz.stop() }
        throw BrokerError.io(
            "dial failed: \(outcome.error.map(String.init(describing:)) ?? "unknown")")
    }

    let relay = try VsockRelay(path: socketPath, guestHandle: handle)
    relay.start()
    note("agent reachable at unix://\(socketPath)")
    note("boot log: \(runtimeDir.appendingPathComponent("kata-boot.log").path)")
    note("parked — run the ttrpc client, then ^C")
    dispatchMain()
}
