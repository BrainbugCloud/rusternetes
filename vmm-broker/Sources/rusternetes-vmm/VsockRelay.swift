//===----------------------------------------------------------------------===//
// vsock <-> unix socket relay.
//
// This is the reason the broker exists at all. A guest vsock connection comes
// from `VZVirtioSocketDevice.connect(toPort:)` on the in-process VM object, so no
// other process can dial the guest. The broker dials on Rust's behalf and pumps
// the bytes over a unix socket, which is the shape the Rust `VmInstance::dial`
// contract is written in terms of — and the same shape upstream's
// `Vminitd.init(connection: FileHandle, …)` expects, an already-connected fd.
//===----------------------------------------------------------------------===//

import Foundation

/// A one-shot relay: accepts a single connection on a unix socket and splices it
/// to an already-connected guest fd.
///
/// One-shot rather than long-lived because the Rust side's agent channel consumes
/// its stream exactly once (`agent_over_stream` cannot re-dial, matching
/// upstream's `withConnectedSocket`), and a fresh `dial` is cheap.
final class VsockRelay: @unchecked Sendable {
    private let path: String
    private let listenFd: Int32
    private let guestHandle: FileHandle
    private let queue: DispatchQueue

    /// Bind a unix socket at `path` and prepare to splice it to `guestHandle`.
    init(path: String, guestHandle: FileHandle) throws {
        self.path = path
        self.guestHandle = guestHandle
        self.queue = DispatchQueue(label: "vmm.relay.\(path)")

        // A stale socket file would make bind(2) fail with EADDRINUSE.
        unlink(path)

        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else {
            throw BrokerError.io("socket(AF_UNIX): \(String(cString: strerror(errno)))")
        }

        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let maxLen = MemoryLayout.size(ofValue: addr.sun_path)
        // macOS caps sun_path at 104 bytes and counts the whole path, so a long
        // runtime directory silently breaks dialing. Fail here with something
        // actionable instead.
        guard path.utf8.count < maxLen else {
            close(fd)
            throw BrokerError.io(
                "relay socket path is \(path.utf8.count) bytes, over the \(maxLen)-byte "
                    + "sun_path limit: \(path)")
        }
        withUnsafeMutablePointer(to: &addr.sun_path) { dst in
            path.withCString { src in
                _ = strncpy(
                    UnsafeMutableRawPointer(dst).assumingMemoryBound(to: CChar.self), src, maxLen - 1
                )
            }
        }

        let size = socklen_t(MemoryLayout<sockaddr_un>.size)
        let bound = withUnsafePointer(to: &addr) { raw in
            raw.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(fd, $0, size) }
        }
        guard bound == 0 else {
            close(fd)
            throw BrokerError.io("bind(\(path)): \(String(cString: strerror(errno)))")
        }
        guard Foundation.listen(fd, 1) == 0 else {
            close(fd)
            throw BrokerError.io("listen(\(path)): \(String(cString: strerror(errno)))")
        }
        self.listenFd = fd
    }

    var socketPath: String { path }

    /// Accept one connection and splice both directions until either side closes.
    func start() {
        queue.async { [self] in
            let clientFd = accept(listenFd, nil, nil)
            close(listenFd)
            unlink(path)
            guard clientFd >= 0 else { return }

            // Closed explicitly in the completion below; see handleConnection on
            // why closeOnDealloc must not also be set.
            let client = FileHandle(fileDescriptor: clientFd, closeOnDealloc: false)
            let guestFd = guestHandle.fileDescriptor

            // Two pumps, one per direction. Closing both ends when either
            // direction finishes is what gives the guest's stdout/stderr a real
            // EOF; without it a CRI streaming session never terminates.
            let done = DispatchGroup()
            Self.pump(from: clientFd, to: guestFd, group: done, queue: queue)
            Self.pump(from: guestFd, to: clientFd, group: done, queue: queue)
            done.notify(queue: queue) {
                try? client.close()
                try? self.guestHandle.close()
            }
        }
    }

    private static func pump(
        from: Int32, to: Int32, group: DispatchGroup, queue: DispatchQueue
    ) {
        group.enter()
        DispatchQueue.global(qos: .userInitiated).async {
            defer { group.leave() }
            var buffer = [UInt8](repeating: 0, count: 64 * 1024)
            while true {
                let n = buffer.withUnsafeMutableBytes { read(from, $0.baseAddress, $0.count) }
                if n <= 0 {
                    // EINTR is not an end-of-stream; anything else is.
                    if n < 0 && errno == EINTR { continue }
                    shutdown(to, SHUT_WR)
                    return
                }
                var written = 0
                while written < n {
                    let w = buffer.withUnsafeBytes {
                        write(to, $0.baseAddress!.advanced(by: written), n - written)
                    }
                    if w <= 0 {
                        if w < 0 && errno == EINTR { continue }
                        return
                    }
                    written += w
                }
            }
        }
    }
}
