//===----------------------------------------------------------------------===//
// The broker wire protocol.
//
// Mirrors `crates/apple-containerization/src/broker.rs`. Newline-delimited JSON
// over a unix socket, one connection per request. Keep the two in step: the Rust
// side's `broker::tests` pin the exact field names asserted here.
//===----------------------------------------------------------------------===//

import Foundation

/// The methods the Rust side may call. Names are camelCase on the wire.
enum Method: String, Codable, Sendable {
    // Pod lifecycle, backed by Containerization's LinuxPod.
    case createPod
    case create
    case stopPod
    case addContainer
    case startContainer
    case stopContainer
    case killContainer
    case waitContainer
    case listContainers
    case exec
    case attach
    case resize
    case closeStdin
    case reopenContainerLog
    case waitProcess
    case killProcess
    case statistics
    // Images and transport.
    case listImages
    case imageStatus
    case pullImage
    case removeImage
    case imageFsInfo
    case provisionRootfs
    case releaseRootfs
    case dial
}

/// A block device to attach, as the Rust side describes it.
struct BlockMountWire: Codable, Sendable {
    var format: String
    var source: String
    var destination: String
    var options: [String]
}

/// A device as the *guest* sees it, which is what the pod needs back in order to
/// build a container's mount list.
struct AttachedFilesystemWire: Codable, Sendable {
    var type: String
    var source: String
    var destination: String
    var options: [String]

    enum CodingKeys: String, CodingKey {
        // Rust names the field `type_` but renames it to `type` on the wire.
        case type
        case source
        case destination
        case options
    }
}

struct InterfaceWire: Codable, Sendable {
    var address: String
    var gateway: String?
    var mtu: UInt32?
    var macAddress: String?
}

struct DnsConfigWire: Codable, Sendable {
    var nameservers: [String]
    var domain: String?
    var searchDomains: [String]
    var options: [String]
}

/// Pod-level configuration; mirrors `LinuxPod.Configuration`.
struct PodConfigWire: Codable, Sendable {
    var id: String
    var cpus: UInt32
    var memoryInBytes: UInt64
    var interfaces: [InterfaceWire]
    var shareProcessNamespace: Bool
    var hostname: String?
    var dns: DnsConfigWire?
    var bootLog: String?
}

/// Per-container configuration; mirrors `LinuxPod.ContainerConfiguration` plus
/// the rootfs block the broker provisioned.
struct ContainerConfigWire: Codable, Sendable {
    var id: String
    var rootfs: BlockMountWire
    /// Host path for the container's CRI log file. When set, the broker writes
    /// stdout/stderr there in the CRI format (see `LogWriter.swift`).
    var logPath: String?
    var args: [String]
    var env: [String]
    var workingDirectory: String
    var terminal: Bool
    /// CRI `stdin`: the container gets an attachable stdin. A process without it
    /// must get `nil`, not an empty stream that never EOFs.
    var stdin: Bool
    /// CRI `stdin_once`: close the container's stdin once an attached client
    /// detaches. See `ContainerStdio.attach`.
    var stdinOnce: Bool
    var uid: UInt32
    var gid: UInt32
    var additionalGids: [UInt32]
    var username: String
    var hostname: String?
    var cpus: UInt32?
    var memoryInBytes: UInt64?
    var sysctl: [String: String]
    var mounts: [AttachedFilesystemWire]
    var maskedPaths: [String]
    var readonlyPaths: [String]
}

/// One image in the broker's store, as CRI needs to describe it.
struct ImageWire: Codable, Sendable {
    var reference: String
    var digest: String
    var sizeBytes: UInt64
    /// The raw OCI `User` string; CRI's uid-vs-username split is the Rust side's.
    var user: String
    /// The image's own process configuration.
    ///
    /// A CRI `ContainerConfig` routinely leaves `command`, `args`, `envs` and
    /// `working_dir` empty and expects the image's values to apply — critest's
    /// nginx containers do exactly that. The merge rules are CRI's, so they live
    /// on the Rust side; these fields are what it merges against.
    var entrypoint: [String]
    var cmd: [String]
    var env: [String]
    var workingDir: String
}

struct ContainerStatsWire: Codable, Sendable {
    var id: String
    var memoryUsageBytes: UInt64
    var memoryInactiveFileBytes: UInt64
    var memoryAnonBytes: UInt64
    var cpuUsageUsec: UInt64
}

/// Flat, all-optional parameter union: each method reads only what it needs.
struct Params: Codable, Sendable {
    var podId: String?
    var config: PodConfigWire?
    var container: ContainerConfigWire?
    var containerId: String?
    var containerIds: [String]?
    var processId: String?
    var args: [String]?
    var env: [String]?
    var terminal: Bool?
    /// `exec` only: host files the process's raw output is written to.
    var stdinPath: String?
    var stdoutPath: String?
    var stderrPath: String?
    /// `exec`: stdio paths are unix sockets the caller is listening on, not files.
    var stdioSockets: Bool?
    var width: UInt16?
    var height: UInt16?
    var signal: Int32?
    var port: UInt32?
    var ownerId: String?
    var image: String?
}

struct Request: Codable, Sendable {
    var method: Method
    var params: Params
}

/// Payload of a successful reply; every field is method-specific.
struct Reply: Codable, Sendable {
    var socketPath: String?
    var block: BlockMountWire?
    var containerIds: [String]?
    var exitCode: Int32?
    var pid: Int32?
    var stats: [ContainerStatsWire]?
    /// `createPod`: the address the broker allocated for the pod.
    var ipv4: String?
    var images: [ImageWire]?
    var image: ImageWire?
    /// `imageFsInfo`: the store's path and bytes used.
    var fsPath: String?
    var fsBytes: UInt64?

    init(
        socketPath: String? = nil,
        block: BlockMountWire? = nil,
        containerIds: [String]? = nil,
        exitCode: Int32? = nil,
        pid: Int32? = nil,
        stats: [ContainerStatsWire]? = nil,
        ipv4: String? = nil,
        images: [ImageWire]? = nil,
        image: ImageWire? = nil,
        fsPath: String? = nil,
        fsBytes: UInt64? = nil
    ) {
        self.socketPath = socketPath
        self.block = block
        self.containerIds = containerIds
        self.exitCode = exitCode
        self.pid = pid
        self.stats = stats
        self.ipv4 = ipv4
        self.images = images
        self.image = image
        self.fsPath = fsPath
        self.fsBytes = fsBytes
    }
}

/// Exactly one of `ok` / `error` is set.
struct Response: Codable, Sendable {
    var ok: Reply?
    var error: String?

    static func success(_ reply: Reply = Reply()) -> Response {
        Response(ok: reply, error: nil)
    }

    static func failure(_ message: String) -> Response {
        Response(ok: nil, error: message)
    }
}
