//===----------------------------------------------------------------------===//
// Pods, via Containerization's own LinuxPod.
//
// This replaces a ~2400-line Rust port of LinuxPod + Vminitd + the OCI spec
// types. That port duplicated working Swift, did not receive Apple's fixes, and
// its first live boot failed on a `LinuxPod.create()` precondition it had not
// replicated. Calling the real thing is both less code and less risk.
//
// What stays on the Rust side is the part that is genuinely ours: translating CRI
// into these calls (`apple-cri/src/pod_runtime.rs`).
//
// Note the deliberate limit: LinuxPod gives each container a *fresh* ipc and uts
// namespace, so containers in a pod do not share System V IPC. Pod-scoped
// hostname still works, because a pod-level hostname is applied to every
// container's own UTS namespace — the same string, just not the same namespace.
// Sharing IPC would mean patching LinuxPod, i.e. maintaining a fork, which is the
// thing this file exists to avoid. See crates/apple-cri/STATUS.md.
//===----------------------------------------------------------------------===//

import Containerization
import ContainerizationExtras
import ContainerizationOCI
import ContainerizationOS
import Foundation

/// An exec'd process and the output files it writes to, held together so the
/// files can be closed exactly when the process is reaped.
private struct ExecProcess {
    let process: LinuxProcess
    let writers: [any Writer]
    let stdin: SocketStdin?
}

/// Owns the `LinuxPod` instances, one per CRI pod sandbox.
final class PodService: @unchecked Sendable {
    private let manager: VZVirtualMachineManager
    private let paths: BrokerPaths
    private let network: NetworkService
    private let lock = NSLock()
    private var pods: [String: LinuxPod] = [:]
    /// One open CRI log file per container, keyed `podID/containerID`.
    private var logs: [String: ContainerLog] = [:]
    /// Live exec'd processes, keyed `podID/containerID/processID`, so they can be
    /// waited on and signalled after `exec` returns.
    private var processes: [String: ExecProcess] = [:]
    /// Each container's stdio fan-out, keyed `podID/containerID`, so `attach` can
    /// join a stream that is already running.
    private var stdio: [String: ContainerStdio] = [:]

    private static func key(_ podID: String, _ containerID: String) -> String {
        "\(podID)/\(containerID)"
    }
    private var relaySequence: UInt64 = 0

    init(manager: VZVirtualMachineManager, paths: BrokerPaths, network: NetworkService) {
        self.manager = manager
        self.paths = paths
        self.network = network
    }

    /// The address assigned to a pod, for `PodSandboxStatus.network.ip`.
    func podAddress(_ id: String) -> String? {
        network.address(podID: id)
    }

    private func pod(_ id: String) throws -> LinuxPod {
        guard let pod = lock.withLock({ pods[id] }) else {
            throw BrokerError.noSuchVm(id)
        }
        return pod
    }

    // MARK: - Pod lifecycle

    /// Build a pod, without booting it. Containers added before `create` are
    /// attached at boot; those added after are hotplugged.
    @discardableResult
    func createPod(_ config: PodConfigWire) throws -> String? {
        let dir = paths.vmDir(config.id)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)

        // The caller may bring its own addressing (a future CNI); otherwise the
        // broker allocates, because the allocator and the vmnet subnet are both
        // Apple's side of the boundary.
        let wires = config.interfaces.isEmpty
            ? [try network.allocate(podID: config.id)]
            : config.interfaces
        let interfaces: [any Interface] = try wires.map { wire in
            NATInterface(
                ipv4Address: try CIDRv4(wire.address),
                ipv4Gateway: try wire.gateway.map { try IPv4Address($0) },
                macAddress: try wire.macAddress.map { try MACAddress($0) },
                mtu: wire.mtu ?? 1500
            )
        }

        let pod = try LinuxPod(config.id, vmm: manager) { c in
            // Without this the pod is fixed at the set of containers it booted
            // with: `addContainer` after `create()` calls `vm.hotplug`, which
            // throws "hotplug not supported" on a bare VZ instance. One provider
            // per pod — it refcounts shares across that pod's containers.
            c.extensions = [VZHotplugInstaller(provider: VZVirtiofsHotplugProvider())]
            c.interfaces = interfaces
            c.cpus = Int(config.cpus)
            c.memoryInBytes = config.memoryInBytes
            c.shareProcessNamespace = config.shareProcessNamespace
            if let hostname = config.hostname {
                c.hostname = hostname
            }
            if let dns = config.dns {
                c.dns = DNS(
                    nameservers: dns.nameservers,
                    domain: dns.domain,
                    searchDomains: dns.searchDomains,
                    options: dns.options
                )
            }
            if let bootLog = config.bootLog {
                c.bootLog = .file(path: URL(filePath: bootLog))
            }
        }
        lock.withLock { pods[config.id] = pod }
        return network.address(podID: config.id)
    }

    /// Boot the pod's VM and bring the sandbox up.
    func create(_ id: String) throws {
        let pod = try pod(id)
        try runBlocking { try await pod.create() }
    }

    /// Stop every container and shut the VM down. Idempotent from the caller's
    /// point of view: a pod we no longer know about is already gone.
    func stopPod(_ id: String) throws {
        // Released even if the stop below fails: the VM is going away either way
        // and a leaked address would shrink the pool for the process lifetime.
        defer { network.release(podID: id) }
        guard let pod = lock.withLock({ pods[id] }) else { return }
        try runBlocking { try await pod.stop() }
        lock.withLock {
            pods.removeValue(forKey: id)
            for key in processes.keys where key.hasPrefix("\(id)/") {
                processes.removeValue(forKey: key)
            }
            // Close after the pod has stopped, so the last lines a container
            // wrote on the way down are still on disk for `kubectl logs`.
            let prefix = "\(id)/"
            for (key, log) in logs where key.hasPrefix(prefix) {
                log.close()
                logs.removeValue(forKey: key)
            }
        }
    }

    // MARK: - Containers

    func addContainer(podID: String, container: ContainerConfigWire) throws {
        let pod = try pod(podID)
        let rootfs = container.rootfs.toMount()

        // Opened before the container exists, so a bad log path fails the create
        // rather than silently losing every line the workload writes.
        let log = try container.logPath.map { path -> ContainerLog in
            let log = try ContainerLog(path: path)
            lock.withLock { logs["\(podID)/\(container.id)"] = log }
            return log
        }

        // Built now even when nothing is attached: Containerization takes the
        // writers once, here, so the fan-out `attach` joins later has to already
        // be in place. See ContainerStdio.
        let stdio = ContainerStdio(
            log: log, stdin: container.stdin, stdinOnce: container.stdinOnce,
            tty: container.terminal)
        lock.withLock { self.stdio[Self.key(podID, container.id)] = stdio }

        try runBlocking {
            try await pod.addContainer(container.id, rootfs: rootfs) { c in
                var process = LinuxProcessConfiguration()
                process.arguments = container.args
                process.stdout = stdio.stdout
                process.stderr = stdio.stderr
                process.stdin = stdio.stdin
                if !container.env.isEmpty {
                    process.environmentVariables = container.env
                }
                if !container.workingDirectory.isEmpty {
                    process.workingDirectory = container.workingDirectory
                }
                process.terminal = container.terminal
                process.user = ContainerizationOCI.User(
                    uid: container.uid, gid: container.gid,
                    additionalGids: container.additionalGids,
                    username: container.username
                )
                c.process = process

                // A pod-level hostname reaches each container this way; see the
                // note at the top of this file on why it is not a shared UTS ns.
                if let hostname = container.hostname {
                    c.hostname = hostname
                }
                if let cpus = container.cpus, cpus > 0 {
                    c.cpus = Int(cpus)
                }
                if let memory = container.memoryInBytes, memory > 0 {
                    c.memoryInBytes = memory
                }
                if !container.sysctl.isEmpty {
                    c.sysctl = container.sysctl
                }
                if !container.maskedPaths.isEmpty {
                    c.maskedPaths = container.maskedPaths
                }
                if !container.readonlyPaths.isEmpty {
                    c.readonlyPaths = container.readonlyPaths
                }
                // Extra mounts on top of LinuxContainer.defaultMounts(), which the
                // ContainerConfiguration initialiser already seeds.
                c.mounts.append(contentsOf: container.mounts.map { $0.toMount() })
            }
        }
    }

    func startContainer(podID: String, containerID: String) throws {
        let pod = try pod(podID)
        try runBlocking { try await pod.startContainer(containerID) }
    }

    func stopContainer(podID: String, containerID: String) throws {
        let pod = try pod(podID)
        try runBlocking { try await pod.stopContainer(containerID) }
    }

    /// Signal a container's init process. `signal` is a raw POSIX number, as CRI
    /// and the Rust side use.
    func killContainer(podID: String, containerID: String, signal: Int32) throws {
        let pod = try pod(podID)
        try runBlocking {
            try await pod.killContainer(containerID, signal: Signal(rawValue: signal))
        }
    }

    /// Wait for a container's init process and return its exit code.
    func waitContainer(podID: String, containerID: String) throws -> Int32 {
        let pod = try pod(podID)
        let status = try runBlocking { try await pod.waitContainer(containerID) }
        // The container is gone, so nothing more will be written: end every
        // attached client's streams. `LinuxProcess.wait` has already drained the
        // IO relays, so this cannot truncate output. Without it an attach client
        // waits for a stdout stream that no longer has a writer.
        lock.withLock { self.stdio[Self.key(podID, containerID)] }?.finish()
        return status.exitCode
    }

    /// CRI's `ReopenContainerLog`, after the kubelet has rotated the file away.
    func reopenLog(podID: String, containerID: String) throws {
        guard let log = lock.withLock({ logs[Self.key(podID, containerID)] }) else {
            throw BrokerError.badRequest("container \(containerID) has no log file")
        }
        try log.reopen()
    }

    func listContainers(podID: String) throws -> [String] {
        let pod = try pod(podID)
        return try runBlocking { await pod.listContainers() }
    }

    /// Exec a process in a running container. `processID` must differ from the
    /// container id — that is how the guest distinguishes an exec from the
    /// container's init process.
    ///
    /// `stdoutPath`/`stderrPath` are host files the process's output is written to
    /// verbatim. Files rather than sockets: `ExecSync` is synchronous and bounded,
    /// a file never blocks the guest when nobody is reading, and there is no
    /// connect race between `exec` returning and the caller attaching.
    func exec(
        podID: String, containerID: String, processID: String, args: [String], env: [String],
        terminal: Bool, stdinPath: String?, stdoutPath: String?, stderrPath: String?,
        sockets: Bool
    ) throws -> Int32 {
        let pod = try pod(podID)

        // Files for `ExecSync`, sockets for interactive `Exec`. Opened before the
        // process exists either way, so a bad path fails the exec rather than
        // silently discarding its output.
        func writer(_ path: String?) throws -> (any Writer)? {
            guard let path else { return nil }
            return sockets ? try SocketWriter(path: path) : try FileWriter(path: path)
        }
        let stdout = try writer(stdoutPath)
        let stderr = try writer(stderrPath)
        let stdin: SocketStdin? = (sockets && stdinPath != nil) ? SocketStdin() : nil

        let process = try runBlocking { () -> LinuxProcess in
            let process = try await pod.execInContainer(containerID, processID: processID) { c in
                c.arguments = args
                if !env.isEmpty { c.environmentVariables = env }
                c.terminal = terminal
                c.stdout = stdout
                c.stderr = stderr
                c.stdin = stdin
            }
            // `execInContainer` only *builds* the process — it is `start()` that
            // creates and runs it in the guest. Returning `pid` without this gave
            // a pid for a process that was never there.
            try await process.start()
            return process
        }
        // Stdin is fed only once the process exists, so nothing is read from the
        // client before there is somewhere to put it.
        if let stdin, let stdinPath {
            // An exec has exactly one client, so its stdin closing *is* the
            // process's stdin closing — no `stdinOnce` question to ask.
            try stdin.feed(from: stdinPath) { [stdin] in stdin.close() }
        }
        lock.withLock {
            processes[Self.key(podID, containerID, processID)] = ExecProcess(
                process: process, writers: [stdout, stderr].compactMap { $0 }, stdin: stdin)
        }
        return process.pid
    }

    /// Wait for an exec'd process and return its exit code.
    ///
    /// `ExecSync` needs this: the pid alone tells the caller nothing about how
    /// the command finished. The process is deleted once reaped, which is what
    /// releases its guest-side state.
    func waitProcess(podID: String, containerID: String, processID: String) throws -> Int32 {
        let key = Self.key(podID, containerID, processID)
        guard let entry = lock.withLock({ processes[key] }) else {
            throw BrokerError.badRequest("no such process: \(key)")
        }
        let status = try runBlocking { () -> ExitStatus in
            let status = try await entry.process.wait()
            try await entry.process.delete()
            return status
        }
        // Close only after the process is reaped, so the caller reading the
        // output files sees everything the command wrote.
        entry.stdin?.close()
        for writer in entry.writers {
            try? writer.close()
        }
        lock.withLock { _ = processes.removeValue(forKey: key) }
        return status.exitCode
    }

    /// Join a client's sockets to a running container's stdio.
    ///
    /// The caller must already be listening on each path; the broker connects.
    func attach(
        podID: String, containerID: String, stdinPath: String?, stdoutPath: String?,
        stderrPath: String?
    ) throws {
        guard let stdio = lock.withLock({ self.stdio[Self.key(podID, containerID)] }) else {
            throw BrokerError.noSuchVm("\(podID)/\(containerID)")
        }
        try stdio.attach(stdinPath: stdinPath, stdoutPath: stdoutPath, stderrPath: stderrPath)
    }

    /// Resize an exec'd process's pty.
    ///
    /// Only exec processes: `LinuxPod` exposes `resize` on the `LinuxProcess` it
    /// hands back from `execInContainer`, and a container's own init process is
    /// not reachable through its public surface. So a TTY `Attach` cannot be
    /// resized — recorded in STATUS.md rather than silently ignored.
    func resize(podID: String, containerID: String, processID: String, width: UInt16, height: UInt16)
        throws
    {
        let key = Self.key(podID, containerID, processID)
        guard let entry = lock.withLock({ processes[key] }) else {
            throw BrokerError.badRequest("no such process: \(key)")
        }
        try runBlocking {
            try await entry.process.resize(to: Terminal.Size(width: width, height: height))
        }
    }

    /// EOF a container's stdin — CRI's `stdinOnce`.
    func closeStdin(podID: String, containerID: String) throws {
        let pod = try pod(podID)
        if let stdio = lock.withLock({ self.stdio[Self.key(podID, containerID)] }) {
            stdio.stdin?.close()
        }
        try runBlocking { try await pod.closeContainerStdin(containerID) }
    }

    /// Signal an exec'd process — how `ExecSync` enforces its timeout.
    func killProcess(podID: String, containerID: String, processID: String, signal: Int32) throws {
        let key = Self.key(podID, containerID, processID)
        guard let entry = lock.withLock({ processes[key] }) else {
            throw BrokerError.badRequest("no such process: \(key)")
        }
        try runBlocking { try await entry.process.kill(Signal(rawValue: signal)) }
    }

    private static func key(_ podID: String, _ containerID: String, _ processID: String) -> String {
        "\(podID)/\(containerID)/\(processID)"
    }

    /// Per-container statistics from the guest.
    func statistics(podID: String, containerIDs: [String]) throws -> [ContainerStatsWire] {
        let pod = try pod(podID)
        let stats = try runBlocking {
            try await pod.statistics(
                containerIDs: containerIDs.isEmpty ? nil : containerIDs, categories: .all)
        }
        return stats.map { s in
            ContainerStatsWire(
                id: s.id,
                memoryUsageBytes: s.memory?.usageBytes ?? 0,
                memoryInactiveFileBytes: s.memory?.inactiveFile ?? 0,
                memoryAnonBytes: s.memory?.anon ?? 0,
                cpuUsageUsec: s.cpu?.usageUsec ?? 0
            )
        }
    }

    // MARK: - vsock

    /// Dial a guest vsock port and expose it as a unix socket path.
    ///
    /// The pod owns the VM, so the dial goes through it; the relay is the same
    /// one-shot splice the VM-level path used.
    func dial(podID: String, port: UInt32) throws -> String {
        let pod = try pod(podID)
        let handle = try runBlocking { try await pod.dialVsock(port: port) }
        let sequence = lock.withLock { () -> UInt64 in
            relaySequence += 1
            return relaySequence
        }
        let socket = paths.relaySocket(vmId: podID, port: port, sequence: sequence)
        let relay = try VsockRelay(path: socket, guestHandle: handle)
        relay.start()
        return socket
    }
}
