//===----------------------------------------------------------------------===//
// VM registry and method dispatch.
//
// One `VZVirtualMachineInstance` per pod, keyed by the pod id the Rust side
// passes. The method set is deliberately the four Virtualization.framework-gated
// capabilities plus rootfs materialisation — everything else a pod does is
// SandboxContext gRPC, which Rust speaks directly over the relay from `dial`.
//===----------------------------------------------------------------------===//

import Containerization
import ContainerizationOCI
import Foundation

enum BrokerError: Error, CustomStringConvertible {
    case io(String)
    case badRequest(String)
    case noSuchVm(String)
    case unsupported(String)

    var description: String {
        switch self {
        case .io(let m): return m
        case .badRequest(let m): return m
        case .noSuchVm(let id): return "no such vm: \(id)"
        case .unsupported(let m): return "unsupported: \(m)"
        }
    }
}

/// Where the broker keeps per-VM scratch state (relay sockets, boot logs).
struct BrokerPaths: Sendable {
    let runtimeDir: URL

    func vmDir(_ vmId: String) -> URL {
        runtimeDir.appendingPathComponent(vmId)
    }

    /// Relay sockets must stay short: see `VsockRelay`'s sun_path check.
    func relaySocket(vmId: String, port: UInt32, sequence: UInt64) -> String {
        vmDir(vmId).appendingPathComponent("v\(port)-\(sequence)").path
    }
}

actor BrokerService {
    private let manager: VZVirtualMachineManager
    private let paths: BrokerPaths
    private var vms: [String: any VirtualMachineInstance] = [:]
    private var relaySequence: UInt64 = 0

    init(manager: VZVirtualMachineManager, paths: BrokerPaths) {
        self.manager = manager
        self.paths = paths
    }

    func handle(_ request: Request) async -> Response {
        do {
            return .success(try await dispatch(request))
        } catch {
            return .failure("\(error)")
        }
    }

    private func dispatch(_ request: Request) async throws -> Reply {
        let params = request.params

        switch request.method {
        case .createVm:
            guard let config = params.config else {
                throw BrokerError.badRequest("createVm requires config")
            }
            try await createVm(config)
            return Reply()

        case .start:
            try await vm(params).start()
            return Reply()

        case .stop:
            let id = try vmId(params)
            try await vm(params).stop()
            vms.removeValue(forKey: id)
            return Reply()

        case .state:
            let state: String
            switch try vm(params).state {
            case .starting: state = "starting"
            case .running: state = "running"
            case .stopping: state = "stopping"
            case .stopped: state = "stopped"
            case .unknown: state = "unknown"
            }
            return Reply(state: state)

        case .dial:
            guard let port = params.port else {
                throw BrokerError.badRequest("dial requires port")
            }
            return Reply(socketPath: try await dial(params, port: port))

        case .listen:
            throw BrokerError.unsupported(
                "listen: host-side vsock listening is not wired up yet; it is only "
                    + "needed for process stdin (see STATUS.md)")

        case .hotplug:
            guard let block = params.block, let owner = params.ownerId else {
                throw BrokerError.badRequest("hotplug requires block and ownerId")
            }
            let attached = try await vm(params).hotplug(block.toMount(), id: owner)
            return Reply(attached: attached.toWire())

        case .releaseHotplug:
            guard let owner = params.ownerId else {
                throw BrokerError.badRequest("releaseHotplug requires ownerId")
            }
            try await vm(params).releaseHotplug(id: owner)
            return Reply()

        case .mounts:
            let table = try vm(params).mounts.mapValues { $0.map { $0.toWire() } }
            return Reply(mounts: table)

        case .registerMounts:
            guard let owner = params.ownerId, let rootfs = params.rootfs else {
                throw BrokerError.badRequest("registerMounts requires ownerId and rootfs")
            }
            let additional = (params.additional ?? []).map { $0.toMount() }
            try vm(params).registerMounts(
                id: owner, rootfs: rootfs.toAttached(), additionalMounts: additional)
            return Reply()

        case .provisionRootfs, .releaseRootfs:
            // Materialising an image into an ext4 block device needs the image
            // store plus ContainerizationEXT4's EXT4Unpacker. Not yet wired up —
            // the Rust side's RootfsProvider has no implementation either, so
            // nothing calls this. Tracked in crates/apple-cri/STATUS.md.
            throw BrokerError.unsupported(
                "provisionRootfs: image -> ext4 unpacking is not implemented yet")
        }
    }

    // MARK: - VM lifecycle

    private func createVm(_ config: VmConfigWire) async throws {
        let dir = paths.vmDir(config.id)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)

        var vmConfig = VMConfiguration(
            cpus: Int(config.cpus),
            memoryInBytes: config.memoryInBytes,
            interfaces: [],
            mountsByID: config.mountsById.mapValues { $0.map { $0.toMount() } },
            nestedVirtualization: config.nestedVirtualization
        )
        if let bootLog = config.bootLog {
            vmConfig.bootLog = .file(path: URL(filePath: bootLog))
        }

        let vm = try await manager.create(config: StandardVMConfig(configuration: vmConfig))
        vms[config.id] = vm
    }

    private func vmId(_ params: Params) throws -> String {
        guard let id = params.vmId else {
            throw BrokerError.badRequest("request requires vmId")
        }
        return id
    }

    private func vm(_ params: Params) throws -> any VirtualMachineInstance {
        let id = try vmId(params)
        guard let vm = vms[id] else { throw BrokerError.noSuchVm(id) }
        return vm
    }

    // MARK: - vsock

    private func dial(_ params: Params, port: UInt32) async throws -> String {
        let id = try vmId(params)
        let handle = try await vm(params).dial(port)
        relaySequence += 1
        let socket = paths.relaySocket(vmId: id, port: port, sequence: relaySequence)
        let relay = try VsockRelay(path: socket, guestHandle: handle)
        relay.start()
        return socket
    }
}

// MARK: - Wire conversions

extension BlockMountWire {
    func toMount() -> Containerization.Mount {
        .block(
            format: format,
            source: source,
            destination: destination,
            options: options
        )
    }
}

extension AttachedFilesystemWire {
    func toMount() -> Containerization.Mount {
        // `.any` rather than a specific transport: these are attachments the VM
        // already resolved, replayed back into the mount table, so nothing should
        // re-derive a virtio-blk or virtiofs device from them.
        .any(type: type, source: source, destination: destination, options: options)
    }

    func toAttached() -> AttachedFilesystem {
        .init(
            type: type,
            source: source,
            destination: destination,
            options: options
        )
    }
}

extension AttachedFilesystem {
    func toWire() -> AttachedFilesystemWire {
        .init(
            type: type,
            source: source,
            destination: destination,
            options: options
        )
    }
}

extension Containerization.Mount {
    func toWire() -> AttachedFilesystemWire {
        .init(
            type: type,
            source: source,
            destination: destination,
            options: options
        )
    }
}
