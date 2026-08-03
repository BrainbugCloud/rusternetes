//===----------------------------------------------------------------------===//
// rusternetes-vmm: the VMM broker daemon.
//
//   rusternetes-vmm --listen /tmp/rk/vmm.sock \
//                   --kernel ~/Library/Application\ Support/com.apple.container/kernels/default.kernel-arm64 \
//                   --initfs /path/to/initfs.ext4 \
//                   --runtime-dir /tmp/rk/vmm
//
// Requires the `com.apple.security.virtualization` entitlement; see README.md.
//===----------------------------------------------------------------------===//

import Containerization
import ContainerizationExtras
import Foundation

struct Options {
    var listen = "/tmp/rusternetes-vmm.sock"
    var kernel: String?
    /// An init filesystem block. Optional: without it the broker unpacks
    /// `--init-image` itself, which is how Apple actually distributes it.
    var initfs: String?
    var initImage = "ghcr.io/apple/containerization/vminit:0.40.1"
    var runtimeDir = "/tmp/rusternetes-vmm"
    /// Pod addressing. See NetworkService for why the range is configurable.
    /// `nil` means "discover the vmnet bridge"; see `NetworkService.discoverSubnet`.
    var podSubnet: String?
    var podRangeStart = NetworkService.defaultRangeStart
    var podRangeSize = NetworkService.defaultRangeSize
    /// Boot one VM directly and exit, bypassing the socket server. Isolates VZ
    /// problems from the server plumbing.
    var selftest = false
    /// Decide whether a live virtiofs share can be swapped under a mounted
    /// guest — the mechanism a VZ hotplug provider would rest on. See
    /// ShareMutationProbe.swift.
    var shareMutationProbe = false
    /// Step 1 of the kata-guest spike: boot Kata's kernel + initrd and relay the
    /// agent's vsock port. See KataProbe.swift.
    var kataProbe = false
    var kataKernel: String?
    var kataInitrd: String?
    var kataSocket: String?

    static func parse(_ argv: [String]) throws -> Options {
        var options = Options()
        var i = 0
        while i < argv.count {
            let flag = argv[i]
            func value() throws -> String {
                guard i + 1 < argv.count else {
                    throw BrokerError.badRequest("\(flag) requires a value")
                }
                i += 1
                return argv[i]
            }
            switch flag {
            case "--listen": options.listen = try value()
            case "--kernel": options.kernel = try value()
            case "--initfs": options.initfs = try value()
            case "--init-image": options.initImage = try value()
            case "--runtime-dir": options.runtimeDir = try value()
            case "--pod-subnet": options.podSubnet = try value()
            case "--pod-range-start":
                guard let n = UInt32(try value()) else {
                    throw BrokerError.badRequest("--pod-range-start must be a number")
                }
                options.podRangeStart = n
            case "--pod-range-size":
                guard let n = Int(try value()) else {
                    throw BrokerError.badRequest("--pod-range-size must be a number")
                }
                options.podRangeSize = n
            case "--selftest": options.selftest = true
            case "--share-mutation-probe": options.shareMutationProbe = true
            case "--kata-probe": options.kataProbe = true
            case "--kata-kernel": options.kataKernel = try value()
            case "--kata-initrd": options.kataInitrd = try value()
            case "--kata-socket": options.kataSocket = try value()
            case "-h", "--help":
                print(
                    """
                    rusternetes-vmm — VMM broker for apple-cri's pod path

                      --listen <path>       unix socket to serve (default \(options.listen))
                      --kernel <path>       guest kernel image (required)
                      --initfs <path>       prebuilt ext4 initfs (optional)
                      --init-image <ref>    init image to unpack instead
                                            (default \(options.initImage))
                      --runtime-dir <path>  scratch dir for relay sockets (default \(options.runtimeDir))
                      --pod-subnet <cidr>   subnet pods get addresses on
                                            (default: the host's vmnet bridge)
                      --pod-range-start <n> first host number to allocate (default \(options.podRangeStart))
                      --pod-range-size <n>  how many addresses to hand out (default \(options.podRangeSize))
                      --selftest            boot one VM, dial the agent, exit
                      --share-mutation-probe
                                            check whether a live virtiofs share can be
                                            swapped under a mounted guest, and exit
                      --kata-probe          boot a Kata guest and relay its agent socket
                      --kata-kernel <path>  Kata's uncompressed arm64 kernel (vmlinux-*)
                      --kata-initrd <path>  kata-containers-initrd.img
                      --kata-socket <path>  where to expose the agent
                                            (default <runtime-dir>/kata-agent.sock)

                    Protocol: newline-delimited JSON, one request per connection.
                    See crates/apple-containerization/src/broker.rs.
                    """)
                exit(0)
            default:
                throw BrokerError.badRequest("unknown flag: \(flag)")
            }
            i += 1
        }
        return options
    }
}

/// Bring macOS's vmnet bridge up for long enough to read its subnet.
///
/// The bridge exists only while some VM is attached, so on an idle host there is
/// nothing for `NetworkService.discoverSubnet()` to find at startup — and the
/// authoritative source, `/Library/Preferences/SystemConfiguration/com.apple.vmnet.plist`,
/// is root-only. Booting one throwaway VM settles it: the bridge appears when the
/// VM *starts*, long before the guest kernel is up, so this waits a fraction of a
/// second and never waits for Linux. The VM is force-stopped either way.
func probeVmnetSubnet(manager: VZVirtualMachineManager, runtimeDir: URL) -> String? {
    do {
        var configuration = VMConfiguration(cpus: 1, memoryInBytes: 512 * 1024 * 1024)
        configuration.bootLog = .file(path: runtimeDir.appendingPathComponent("vmnet-probe.log"))
        // Any address will do — the probe never talks to anything. It just has to
        // be a NAT interface, because that is what makes macOS create the bridge.
        configuration.interfaces = [
            NATInterface(ipv4Address: try CIDRv4("192.168.64.2/24"), ipv4Gateway: nil)
        ]
        let vm = try manager.create(config: StandardVMConfig(configuration: configuration))
        try runBlocking { try await vm.start() }
        defer { try? runBlocking { try await vm.stop() } }

        for _ in 0..<60 {
            if let subnet = NetworkService.discoverSubnet() { return subnet }
            usleep(50_000)
        }
    } catch {
        FileHandle.standardError.write(
            "vmnet probe failed: \(error)\n".data(using: .utf8)!)
    }
    return nil
}

/// Trace to stderr, unbuffered, for localising failures in the request path.
func trace(_ message: String) {
    if ProcessInfo.processInfo.environment["VMM_TRACE"] != nil {
        let t = Date().timeIntervalSince1970
        FileHandle.standardError.write(
            String(format: "trace %.3f: %@\n", t, message).data(using: .utf8)!)
    }
}

/// Accept loop: one request per connection, mirroring the Rust client.
func serve(socketPath: String, service: BrokerService) throws {
    unlink(socketPath)

    let listenFd = socket(AF_UNIX, SOCK_STREAM, 0)
    guard listenFd >= 0 else {
        throw BrokerError.io("socket(AF_UNIX): \(String(cString: strerror(errno)))")
    }

    var addr = sockaddr_un()
    addr.sun_family = sa_family_t(AF_UNIX)
    let maxLen = MemoryLayout.size(ofValue: addr.sun_path)
    guard socketPath.utf8.count < maxLen else {
        throw BrokerError.io(
            "listen path is \(socketPath.utf8.count) bytes, over the \(maxLen)-byte sun_path limit")
    }
    withUnsafeMutablePointer(to: &addr.sun_path) { dst in
        socketPath.withCString { src in
            _ = strncpy(
                UnsafeMutableRawPointer(dst).assumingMemoryBound(to: CChar.self), src, maxLen - 1)
        }
    }
    let size = socklen_t(MemoryLayout<sockaddr_un>.size)
    let bound = withUnsafePointer(to: &addr) { raw in
        raw.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(listenFd, $0, size) }
    }
    guard bound == 0 else {
        throw BrokerError.io("bind(\(socketPath)): \(String(cString: strerror(errno)))")
    }
    guard listen(listenFd, 64) == 0 else {
        throw BrokerError.io("listen(\(socketPath)): \(String(cString: strerror(errno)))")
    }
    FileHandle.standardError.write("vmm broker listening on \(socketPath)\n".data(using: .utf8)!)

    while true {
        trace("accept: waiting")
        let clientFd = accept(listenFd, nil, nil)
        trace("accept: fd=\(clientFd) errno=\(clientFd < 0 ? errno : 0)")
        if clientFd < 0 {
            if errno == EINTR { continue }
            throw BrokerError.io("accept: \(String(cString: strerror(errno)))")
        }
        // One thread per connection, because requests block for as long as the
        // guest work takes: `waitContainer` runs for a container's whole life,
        // `waitProcess` for an exec's. Served inline, the first such call wedged
        // the broker for good — and `ExecSync`'s timeout is the sharp case,
        // since it recovers by sending `killProcess`, which the blocked handler
        // could never get to. critest's "execSync with timeout" spec deadlocked
        // the whole suite on exactly that.
        //
        // Threads rather than Tasks to match the accept loop, and safe because
        // every piece of shared state is already `NSLock`-guarded: PodService's
        // pods/logs/processes/stdio, NetworkService's assignments, and
        // ImageService's all-`let` fields keyed by owner id.
        let connection = Thread { handleConnection(fd: clientFd, service: service) }
        connection.name = "vmm.conn"
        connection.stackSize = 1 << 20
        connection.start()
    }
}

func handleConnection(fd: Int32, service: BrokerService) {
    // Raw read(2)/write(2) rather than FileHandle. FileHandle is a *file*
    // abstraction; on a socket its buffered write reported EPIPE on every reply
    // even when the peer was still waiting in recv(), which stalled every client.
    // The relay already speaks raw syscalls, so this keeps one I/O model.
    defer { close(fd) }

    // One request per connection: read until the first newline.
    var buffer = [UInt8]()
    var chunk = [UInt8](repeating: 0, count: 4096)
    while !buffer.contains(UInt8(ascii: "\n")) {
        let n = chunk.withUnsafeMutableBytes { read(fd, $0.baseAddress, $0.count) }
        if n < 0 {
            if errno == EINTR { continue }
            trace("read failed: \(String(cString: strerror(errno)))")
            return
        }
        if n == 0 { break }
        buffer.append(contentsOf: chunk[0..<n])
    }
    trace("read \(buffer.count) bytes")
    guard let newline = buffer.firstIndex(of: UInt8(ascii: "\n")) else {
        trace("no newline in request; dropping")
        return
    }

    let response: Response
    do {
        let request = try JSONDecoder().decode(Request.self, from: Data(buffer[0..<newline]))
        trace("dispatching \(request.method)")
        response = service.handle(request)
        trace("dispatched \(request.method) ok=\(response.ok != nil) error=\(response.error ?? "-")")
    } catch {
        response = .failure("bad request: \(error)")
    }

    guard var out = try? JSONEncoder().encode(response) else {
        trace("could not encode reply")
        return
    }
    out.append(UInt8(ascii: "\n"))
    var written = 0
    out.withUnsafeBytes { raw in
        while written < raw.count {
            let w = write(fd, raw.baseAddress!.advanced(by: written), raw.count - written)
            if w <= 0 {
                if w < 0 && errno == EINTR { continue }
                trace("write failed at \(written)/\(raw.count): \(String(cString: strerror(errno)))")
                return
            }
            written += w
        }
    }
    trace("replied \(written) bytes")
}

// MARK: - Entry point

// A client that gives up mid-request leaves us writing to a closed socket, and
// an unhandled SIGPIPE *terminates the process* — one impatient caller would take
// the broker and every VM it owns down with it. Ignore it and handle the EPIPE
// from write(2) instead.
signal(SIGPIPE, SIG_IGN)

do {
    let options = try Options.parse(Array(CommandLine.arguments.dropFirst()))
    guard let kernelPath = options.kernel else {
        throw BrokerError.badRequest("--kernel is required (see --help)")
    }

    let runtimeDir = URL(filePath: options.runtimeDir)
    try FileManager.default.createDirectory(at: runtimeDir, withIntermediateDirectories: true)

    let platform = SystemPlatform.linuxArm
    let imagesDir = runtimeDir.appendingPathComponent("images")
    let images = ImageService(
        store: try ImageStore(path: imagesDir),
        blocksDir: runtimeDir.appendingPathComponent("blocks"),
        storeDir: imagesDir,
        platform: platform
    )

    // The initfs is a block device carrying vminitd; the guest boots it as PID 1
    // and serves SandboxContext on vsock port 1024, which is what Rust dials.
    // Apple distributes it as an OCI image, so unpack that unless a prebuilt
    // block was supplied.
    let initialFilesystem: Containerization.Mount
    if let initfsPath = options.initfs {
        initialFilesystem = .block(
            format: "ext4", source: initfsPath, destination: "/", options: ["ro"])
    } else {
        FileHandle.standardError.write(
            "materialising init image \(options.initImage)\n".data(using: .utf8)!)
        initialFilesystem = try images.initfs(reference: options.initImage)
    }

    let kernel = Kernel(path: URL(filePath: kernelPath), platform: platform)
    let manager = VZVirtualMachineManager(
        kernel: kernel,
        initialFilesystem: initialFilesystem
    )
    // Discovered rather than assumed: macOS chooses the vmnet subnet, and a pod
    // addressed on the wrong one is unreachable from the host in a way that looks
    // like a broken container.
    var podSubnet = options.podSubnet
    if podSubnet == nil {
        let discovered =
            NetworkService.discoverSubnet()
            ?? probeVmnetSubnet(manager: manager, runtimeDir: runtimeDir)
        podSubnet = discovered ?? NetworkService.fallbackSubnet
        let note =
            discovered == nil
            ? " (no vmnet bridge found — pods will be unreachable from the host)"
            : " (vmnet bridge)"
        FileHandle.standardError.write(
            "pod subnet: \(podSubnet ?? "")\(note)\n".data(using: .utf8)!)
    }
    let network = try NetworkService(
        subnet: podSubnet ?? NetworkService.fallbackSubnet,
        rangeStart: options.podRangeStart,
        rangeSize: options.podRangeSize
    )
    let service = BrokerService(
        manager: manager,
        paths: BrokerPaths(runtimeDir: runtimeDir),
        images: images,
        network: network
    )

    if options.kataProbe {
        guard let kataKernel = options.kataKernel, let kataInitrd = options.kataInitrd else {
            throw BrokerError.badRequest("--kata-probe requires --kata-kernel and --kata-initrd")
        }
        try runKataProbe(
            manager: manager,
            runtimeDir: runtimeDir,
            kernel: kataKernel,
            initrd: kataInitrd,
            socketPath: options.kataSocket
                ?? runtimeDir.appendingPathComponent("kata-agent.sock").path
        )
        exit(0)
    }

    if options.shareMutationProbe {
        let ok = try runBlocking {
            try await runShareMutationProbe(manager: manager, runtimeDir: runtimeDir)
        }
        exit(ok ? 0 : 1)
    }

    if options.selftest {
        func note(_ m: String) {
            FileHandle.standardError.write("selftest: \(m)\n".data(using: .utf8)!)
        }
        note("creating vm")
        let vm = try runBlocking {
            try await manager.create(
                config: StandardVMConfig(
                    configuration: {
                        var c = VMConfiguration(cpus: 2, memoryInBytes: 1024 * 1024 * 1024)
                        c.bootLog = .file(path: runtimeDir.appendingPathComponent("selftest-boot.log"))
                        return c
                    }()))
        }
        note("created; starting")
        try runBlocking { try await vm.start() }
        note("started; state=\(vm.state)")
        note("dialing agent on 1024")
        let handle = try runBlocking { try await vm.dial(1024) }
        note("dialed fd=\(handle.fileDescriptor) — the guest agent is answering")
        try runBlocking { try await vm.stop() }
        note("stopped — OK")
        exit(0)
    }

    // The accept loop runs on its own thread and the main thread pumps a run loop.
    // Virtualization.framework delivers VM state changes through its own queues and
    // expects a live process run loop, so leaving the main thread parked in
    // dispatchMain() is the conventional shape for a VZ-hosting daemon rather than
    // blocking it in accept(). (Not the fix for the EPIPE stall — see
    // handleConnection — but the right structure regardless.)
    let accepting = Thread {
        do {
            try serve(socketPath: options.listen, service: service)
        } catch {
            FileHandle.standardError.write(
                "rusternetes-vmm: serve: \(error)\n".data(using: .utf8)!)
            exit(1)
        }
    }
    accepting.name = "vmm.accept"
    accepting.stackSize = 1 << 20
    accepting.start()

    dispatchMain()
} catch {
    FileHandle.standardError.write("rusternetes-vmm: \(error)\n".data(using: .utf8)!)
    exit(1)
}
