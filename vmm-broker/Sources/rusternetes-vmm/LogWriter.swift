//===----------------------------------------------------------------------===//
// Container stdio -> the CRI log file.
//
// `listen` (host-side vsock accept) used to be the plan for getting a container's
// output back to the host. It is the wrong primitive now: `LinuxProcessConfiguration`
// carries `stdout`/`stderr` as `Writer`s, so the process's output is already in
// this process — there is nothing to accept.
//
// The runtime therefore writes the CRI log file itself, which is also what
// containerd's CRI plugin does (`pkg/cri/io/logger.go`). Format, one line per
// record:
//
//     <RFC3339Nano> <stdout|stderr> <F|P> <content>\n
//
// `F` is a complete line, `P` a partial one that hit the size cap. Both streams
// share one file and one lock, because interleaving order is part of the log.
//===----------------------------------------------------------------------===//

import Containerization
import Foundation

/// One container's log file. Hand out a `Writer` per stream via ``stream(_:)``.
final class ContainerLog: @unchecked Sendable {
    /// containerd uses 16KiB before it splits a line and tags the pieces `P`.
    static let maxLineBytes = 16 * 1024

    private var handle: FileHandle
    private let path: String
    private let lock = NSLock()
    /// Partial line carried over between writes, per stream.
    private var pending: [String: [UInt8]] = [:]

    init(path: String) throws {
        self.path = path
        self.handle = try Self.open(path)
    }

    private static func open(_ path: String) throws -> FileHandle {
        let url = URL(filePath: path)
        try FileManager.default.createDirectory(
            at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        if !FileManager.default.fileExists(atPath: path) {
            guard FileManager.default.createFile(atPath: path, contents: nil) else {
                throw BrokerError.io("could not create log file \(path)")
            }
        }
        let handle = try FileHandle(forWritingTo: url)
        try handle.seekToEnd()
        return handle
    }

    /// CRI's `ReopenContainerLog`: drop the current file and open the path
    /// afresh.
    ///
    /// The kubelet calls this after rotating the log away, and the contract is
    /// that a *new* file appears at the same path — writing on through the old
    /// handle would keep appending to the rotated-away inode, where nothing will
    /// ever read it again.
    func reopen() throws {
        lock.lock()
        defer { lock.unlock() }
        try? handle.close()
        handle = try Self.open(path)
    }

    /// A `Writer` that tags everything it receives with `tag` ("stdout"/"stderr").
    func stream(_ tag: String) -> any Writer {
        StreamWriter(log: self, tag: tag)
    }

    fileprivate func append(_ data: Data, tag: String) {
        lock.lock()
        defer { lock.unlock() }

        var buffer = pending[tag] ?? []
        buffer.append(contentsOf: data)

        var out = Data()
        while let newline = buffer.firstIndex(of: UInt8(ascii: "\n")) {
            emit(Array(buffer[..<newline]), tag: tag, full: true, into: &out)
            buffer.removeSubrange(...newline)
        }
        // A line longer than the cap is emitted in `P` pieces rather than held
        // forever; the kubelet reassembles them.
        while buffer.count >= Self.maxLineBytes {
            emit(Array(buffer[..<Self.maxLineBytes]), tag: tag, full: false, into: &out)
            buffer.removeSubrange(..<Self.maxLineBytes)
        }
        pending[tag] = buffer

        if !out.isEmpty {
            // A failing log write must not take the container down with it.
            try? handle.write(contentsOf: out)
        }
    }

    fileprivate func flush(tag: String) {
        lock.lock()
        defer { lock.unlock() }
        guard let buffer = pending[tag], !buffer.isEmpty else { return }
        var out = Data()
        // End of stream: whatever is left is a complete record, not a partial.
        emit(buffer, tag: tag, full: true, into: &out)
        pending[tag] = []
        try? handle.write(contentsOf: out)
    }

    /// Close the file. Callers must have flushed every stream first.
    func close() {
        lock.lock()
        defer { lock.unlock() }
        try? handle.close()
    }

    private func emit(_ line: [UInt8], tag: String, full: Bool, into out: inout Data) {
        out.append(contentsOf: Array("\(Self.timestamp()) \(tag) \(full ? "F" : "P") ".utf8))
        out.append(contentsOf: line)
        out.append(UInt8(ascii: "\n"))
    }

    /// RFC3339 with nanosecond precision, which is what the kubelet parses.
    /// `ISO8601DateFormatter` tops out at milliseconds, so the fraction is
    /// formatted by hand.
    static func timestamp(_ date: Date = Date()) -> String {
        let epoch = date.timeIntervalSince1970
        let seconds = epoch.rounded(.down)
        let nanos = Int(((epoch - seconds) * 1_000_000_000).rounded(.down))
        let formatter = DateFormatter()
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        let head = formatter.string(from: Date(timeIntervalSince1970: seconds))
        return String(format: "%@.%09dZ", head, nanos)
    }
}

/// One stream of a ``ContainerLog``. `Writer` is `write` + `close`, and `close`
/// here flushes the stream's partial line rather than closing the shared file —
/// the other stream may still be running.
private struct StreamWriter: Writer {
    let log: ContainerLog
    let tag: String

    func write(_ data: Data) throws {
        log.append(data, tag: tag)
    }

    func close() throws {
        log.flush(tag: tag)
    }
}

/// Raw process output to a file — no timestamps, no stream tag, no framing.
///
/// `ExecSync` must return the command's **exact bytes**, so exec stdio cannot go
/// through ``ContainerLog``. One file per stream, because CRI reports exec stdout
/// and stderr separately.
final class FileWriter: Writer, @unchecked Sendable {
    private let handle: FileHandle
    private let lock = NSLock()

    init(path: String) throws {
        let url = URL(filePath: path)
        try FileManager.default.createDirectory(
            at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        guard FileManager.default.createFile(atPath: path, contents: nil) else {
            throw BrokerError.io("could not create \(path)")
        }
        self.handle = try FileHandle(forWritingTo: url)
    }

    func write(_ data: Data) throws {
        lock.lock()
        defer { lock.unlock() }
        try handle.write(contentsOf: data)
    }

    func close() throws {
        lock.lock()
        defer { lock.unlock() }
        try? handle.close()
    }
}
