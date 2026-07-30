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
    case createVm
    case start
    case stop
    case state
    case dial
    case listen
    case hotplug
    case releaseHotplug
    case mounts
    case registerMounts
    case provisionRootfs
    case releaseRootfs
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

/// Mirror of Rust's `VmConfig`.
struct VmConfigWire: Codable, Sendable {
    var id: String
    var cpus: UInt32
    var memoryInBytes: UInt64
    var interfaces: [InterfaceWire]
    var nestedVirtualization: Bool
    var mountsById: [String: [BlockMountWire]]
    var bootLog: String?
}

/// Flat, all-optional parameter union: each method reads only what it needs.
struct Params: Codable, Sendable {
    var vmId: String?
    var config: VmConfigWire?
    var port: UInt32?
    var ownerId: String?
    var block: BlockMountWire?
    var rootfs: AttachedFilesystemWire?
    var additional: [AttachedFilesystemWire]?
    var image: String?
}

struct Request: Codable, Sendable {
    var method: Method
    var params: Params
}

/// Payload of a successful reply; every field is method-specific.
struct Reply: Codable, Sendable {
    var socketPath: String?
    var state: String?
    var attached: AttachedFilesystemWire?
    var block: BlockMountWire?
    var mounts: [String: [AttachedFilesystemWire]]?

    init(
        socketPath: String? = nil,
        state: String? = nil,
        attached: AttachedFilesystemWire? = nil,
        block: BlockMountWire? = nil,
        mounts: [String: [AttachedFilesystemWire]]? = nil
    ) {
        self.socketPath = socketPath
        self.state = state
        self.attached = attached
        self.block = block
        self.mounts = mounts
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
