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
import Foundation

struct Options {
    var listen = "/tmp/rusternetes-vmm.sock"
    var kernel: String?
    var initfs: String?
    var runtimeDir = "/tmp/rusternetes-vmm"

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
            case "--runtime-dir": options.runtimeDir = try value()
            case "-h", "--help":
                print(
                    """
                    rusternetes-vmm — VMM broker for apple-cri's pod path

                      --listen <path>       unix socket to serve (default \(options.listen))
                      --kernel <path>       guest kernel image (required)
                      --initfs <path>       ext4 initfs containing vminitd (required)
                      --runtime-dir <path>  scratch dir for relay sockets (default \(options.runtimeDir))

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
        let clientFd = accept(listenFd, nil, nil)
        if clientFd < 0 {
            if errno == EINTR { continue }
            throw BrokerError.io("accept: \(String(cString: strerror(errno)))")
        }
        Task.detached {
            await handleConnection(fd: clientFd, service: service)
        }
    }
}

func handleConnection(fd: Int32, service: BrokerService) async {
    let handle = FileHandle(fileDescriptor: fd, closeOnDealloc: true)
    defer { try? handle.close() }

    // One request per connection, so read until the first newline.
    var buffer = Data()
    while !buffer.contains(UInt8(ascii: "\n")) {
        guard let chunk = try? handle.read(upToCount: 4096), !chunk.isEmpty else { break }
        buffer.append(chunk)
    }
    guard let newline = buffer.firstIndex(of: UInt8(ascii: "\n")) else { return }
    let line = buffer[buffer.startIndex..<newline]

    let response: Response
    do {
        let request = try JSONDecoder().decode(Request.self, from: Data(line))
        response = await service.handle(request)
    } catch {
        response = .failure("bad request: \(error)")
    }

    if var out = try? JSONEncoder().encode(response) {
        out.append(UInt8(ascii: "\n"))
        try? handle.write(contentsOf: out)
    }
}

// MARK: - Entry point

do {
    let options = try Options.parse(Array(CommandLine.arguments.dropFirst()))
    guard let kernelPath = options.kernel, let initfsPath = options.initfs else {
        throw BrokerError.badRequest("--kernel and --initfs are required (see --help)")
    }

    let runtimeDir = URL(filePath: options.runtimeDir)
    try FileManager.default.createDirectory(at: runtimeDir, withIntermediateDirectories: true)

    // The initfs is a block device carrying vminitd; the guest boots it as PID 1
    // and serves SandboxContext on vsock port 1024, which is what Rust dials.
    let kernel = Kernel(path: URL(filePath: kernelPath), platform: .linuxArm)
    let initialFilesystem = Containerization.Mount.block(
        format: "ext4",
        source: initfsPath,
        destination: "/",
        options: ["ro"]
    )

    let manager = VZVirtualMachineManager(
        kernel: kernel,
        initialFilesystem: initialFilesystem
    )
    let service = BrokerService(
        manager: manager,
        paths: BrokerPaths(runtimeDir: runtimeDir)
    )

    try serve(socketPath: options.listen, service: service)
} catch {
    FileHandle.standardError.write("rusternetes-vmm: \(error)\n".data(using: .utf8)!)
    exit(1)
}
