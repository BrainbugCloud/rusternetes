//===----------------------------------------------------------------------===//
// Streaming stdio: unix sockets between the broker and apple-cri.
//
// `ExecSync` captures to files (see LogWriter.swift) because it is synchronous
// and bounded. Attach and interactive exec are neither: they are bidirectional,
// unbounded, and the caller is already holding an SPDY stream open. So they get a
// socket per stream.
//
// **The caller listens; the broker connects.** That removes the race that the
// other direction has — output produced between "socket created" and "caller
// attached" would be lost. apple-cri binds its listeners before it issues the
// call, so by the time the broker connects there is always someone reading.
//===----------------------------------------------------------------------===//

import Containerization
import Foundation

enum UnixSocket {
    /// Connect to a listening unix socket and return the raw fd.
    static func connect(path: String) throws -> Int32 {
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else {
            throw BrokerError.io("socket(AF_UNIX): \(String(cString: strerror(errno)))")
        }
        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let maxLen = MemoryLayout.size(ofValue: addr.sun_path)
        guard path.utf8.count < maxLen else {
            close(fd)
            throw BrokerError.io(
                "stdio socket path is \(path.utf8.count) bytes, over the \(maxLen)-byte sun_path limit"
            )
        }
        withUnsafeMutablePointer(to: &addr.sun_path) { dst in
            path.withCString { src in
                _ = strncpy(
                    UnsafeMutableRawPointer(dst).assumingMemoryBound(to: CChar.self), src, maxLen - 1)
            }
        }
        let size = socklen_t(MemoryLayout<sockaddr_un>.size)
        let rc = withUnsafePointer(to: &addr) { raw in
            raw.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(fd, $0, size)
            }
        }
        guard rc == 0 else {
            let message = String(cString: strerror(errno))
            close(fd)
            throw BrokerError.io("connect(\(path)): \(message)")
        }
        return fd
    }
}

/// A process stream written to a connected unix socket.
final class SocketWriter: Writer, @unchecked Sendable {
    private let fd: Int32
    private let lock = NSLock()
    private var closed = false

    init(path: String) throws {
        self.fd = try UnixSocket.connect(path: path)
    }

    func write(_ data: Data) throws {
        lock.lock()
        defer { lock.unlock() }
        guard !closed else { return }
        try data.withUnsafeBytes { raw in
            var written = 0
            while written < raw.count {
                let n = Darwin.write(fd, raw.baseAddress!.advanced(by: written), raw.count - written)
                if n <= 0 {
                    if n < 0 && errno == EINTR { continue }
                    // The reader hung up. Mark closed rather than throwing on
                    // every subsequent line — a detached client must not turn
                    // into a permanent error on the container's output.
                    trace("stdio: socket write failed (\(String(cString: strerror(errno)))), dropping subscriber")
                    closed = true
                    Darwin.close(fd)
                    return
                }
                written += n
            }
        }
    }

    func close() throws {
        lock.lock()
        defer { lock.unlock() }
        guard !closed else { return }
        closed = true
        Darwin.close(fd)
    }
}

/// Fans one process stream out to several destinations.
///
/// A container's stdout goes to its CRI log file *and*, once someone attaches, to
/// that client's socket. Subscribers can be added while the stream is running,
/// which is the whole point: `attach` joins a container that is already talking.
final class TeeWriter: Writer, @unchecked Sendable {
    private let lock = NSLock()
    private var writers: [(id: UInt64, writer: any Writer)] = []
    private var nextID: UInt64 = 0

    init(_ writers: [any Writer] = []) {
        for writer in writers { _ = add(writer) }
    }

    /// Subscribe, returning a handle for [`remove`]. Attaches come and go, and a
    /// detached client's socket must stop being written to — otherwise every
    /// attach over a container's life leaves a dead subscriber behind.
    @discardableResult
    func add(_ writer: any Writer) -> UInt64 {
        lock.lock()
        defer { lock.unlock() }
        nextID += 1
        writers.append((id: nextID, writer: writer))
        return nextID
    }

    func remove(_ id: UInt64) {
        let dropped: (any Writer)? = lock.withLock {
            guard let i = writers.firstIndex(where: { $0.id == id }) else { return nil }
            return writers.remove(at: i).writer
        }
        try? dropped?.close()
    }

    func write(_ data: Data) throws {
        let current = lock.withLock { writers.map(\.writer) }
        for writer in current {
            // One failing subscriber must not stop the others, and must not
            // propagate back into the guest's stdout.
            try? writer.write(data)
        }
    }

    func close() throws {
        let current = lock.withLock { writers.map(\.writer) }
        for writer in current {
            try? writer.close()
        }
    }
}

/// A process's stdin, fed from unix sockets that clients connect over time.
///
/// `ReaderStream` hands Containerization a single `AsyncStream`, so the queue is
/// created once with the process and each attach feeds into it.
final class SocketStdin: ReaderStream, @unchecked Sendable {
    private let queue: AsyncStream<Data>
    private let continuation: AsyncStream<Data>.Continuation
    private let lock = NSLock()
    private var finished = false

    init() {
        (queue, continuation) = AsyncStream.makeStream(of: Data.self)
    }

    func stream() -> AsyncStream<Data> {
        queue
    }

    /// Read `path` on a dedicated thread, yielding everything it carries.
    ///
    /// A thread rather than a Task: the read is a blocking syscall for the life
    /// of the attach, which would pin a cooperative-pool thread anyway.
    func feed(from path: String, onEnd: @escaping @Sendable () -> Void) throws {
        let fd = try UnixSocket.connect(path: path)
        let thread = Thread { [continuation, lock] in
            var chunk = [UInt8](repeating: 0, count: 4096)
            while true {
                let n = chunk.withUnsafeMutableBytes { read(fd, $0.baseAddress, $0.count) }
                if n < 0 && errno == EINTR { continue }
                if n <= 0 { break }
                continuation.yield(Data(chunk[0..<n]))
            }
            Darwin.close(fd)
            // The queue is deliberately *not* finished here: a client detaching
            // ends that client's input, not necessarily the process's stdin.
            // Whether it does is `stdinOnce`, which the owner decides.
            _ = lock
            onEnd()
        }
        thread.name = "vmm.stdin"
        thread.stackSize = 1 << 19
        thread.start()
    }

    /// End the process's stdin for good — EOF in the guest.
    func close() {
        lock.lock()
        defer { lock.unlock() }
        guard !finished else { return }
        finished = true
        continuation.finish()
    }
}

/// A container's stdio, owned by the broker so `attach` can join it later.
///
/// Containerization takes the writers once, at `addContainer`, and there is no
/// way to re-point them afterwards — so the fan-out has to exist from the start
/// even when nothing is attached yet.
final class ContainerStdio: @unchecked Sendable {
    let stdout = TeeWriter()
    let stderr = TeeWriter()
    /// Present only when the container was created with CRI's `stdin: true`; a
    /// process with no stdin must get `nil`, not an empty stream that never EOFs.
    let stdin: SocketStdin?
    /// CRI `stdin_once`: close the container's stdin once an attached client
    /// detaches. Kubernetes sets it for `kubectl attach --stdin`, and critest's
    /// attach spec depends on it — without it the shell never sees EOF, never
    /// exits, its stdout never closes, and the client's stream never ends.
    private let stdinOnce: Bool
    private let tty: Bool
    /// Subscriptions made by `attach`, so they can be closed when the container
    /// dies. Containerization does not close a process's writers on exit, and an
    /// attach client waits on its stdout stream ending — so without this the
    /// client hangs on a container that is already gone.
    private let subscriptions = NSLock()
    private var attached: [(out: UInt64?, err: UInt64?)] = []

    init(log: ContainerLog?, stdin wantsStdin: Bool, stdinOnce: Bool, tty: Bool) {
        if let log {
            stdout.add(log.stream("stdout"))
            stderr.add(log.stream("stderr"))
        }
        self.stdin = wantsStdin ? SocketStdin() : nil
        self.stdinOnce = stdinOnce
        self.tty = tty
    }

    /// Join a client's sockets to this container's streams.
    ///
    /// When the client's stdin ends, containerd branches on `stdinOnce && !tty`
    /// (`internal/cri/io/container_io.go:182`): close the container's stdin for
    /// good, or else just unsubscribe this client's stdout/stderr. The `tty`
    /// carve-out is upstream's, for kubectl's benefit — with a terminal, stdout
    /// stays open until the container stops.
    func attach(stdinPath: String?, stdoutPath: String?, stderrPath: String?) throws {
        trace("attach: stdin=\(stdinPath ?? "-") stdout=\(stdoutPath ?? "-") stderr=\(stderrPath ?? "-")")

        func subscribe(_ path: String?, to tee: TeeWriter) throws -> UInt64? {
            guard let path else { return nil }
            return tee.add(try SocketWriter(path: path))
        }
        // `let`, so the detach callback below can capture them.
        let outSubscription = try subscribe(stdoutPath, to: stdout)
        let errSubscription = try subscribe(stderrPath, to: stderr)
        subscriptions.withLock { attached.append((out: outSubscription, err: errSubscription)) }

        guard let stdinPath else { return }
        guard let stdin else {
            // Undo the subscriptions this attach made; it is not happening.
            outSubscription.map(stdout.remove)
            errSubscription.map(stderr.remove)
            throw BrokerError.badRequest(
                "container was not created with stdin; nothing to attach to")
        }

        let stdinOnce = self.stdinOnce
        let tty = self.tty
        let stdout = self.stdout
        let stderr = self.stderr
        try stdin.feed(from: stdinPath) { [weak stdin] in
            if stdinOnce && !tty {
                trace("attach: client detached, closing container stdin (stdinOnce)")
                stdin?.close()
            } else {
                outSubscription.map(stdout.remove)
                errSubscription.map(stderr.remove)
            }
        }
    }

    /// The container has exited: end every attached client's streams.
    ///
    /// The log subscriber is left alone — `stopPod` closes it, so the last lines
    /// a container wrote are still on disk for `kubectl logs`.
    func finish() {
        let clients = subscriptions.withLock {
            let all = attached
            attached = []
            return all
        }
        for client in clients {
            client.out.map(stdout.remove)
            client.err.map(stderr.remove)
        }
    }
}
