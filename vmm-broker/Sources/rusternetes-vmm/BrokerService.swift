//===----------------------------------------------------------------------===//
// Method dispatch.
//
// The broker's surface is pod-shaped, not VM-shaped: `PodService` owns one
// Containerization `LinuxPod` per CRI pod sandbox, and this file is the thin
// decoding layer in front of it. Rootfs materialisation goes to `ImageService`;
// `dial` exposes a guest vsock port as a unix socket so the Rust side can speak
// SandboxContext gRPC directly when it needs to.
//
// The wire contract is `Protocol.swift`, mirrored by
// `crates/apple-containerization/src/broker.rs`, whose tests pin the exact field
// names asserted here.
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

/// Serves one broker request **synchronously, on its caller's thread** — and
/// `serve` gives every connection a thread of its own.
///
/// Each handler blocks for as long as its guest work takes, so this must be
/// re-entrant: `waitContainer` blocks for a container's entire lifetime, and an
/// `ExecSync` that times out only recovers by sending `killProcess` *while* its
/// `waitProcess` is still blocked. Serving inline on a single accept thread
/// deadlocked on exactly that.
///
/// Re-entrancy is safe because all shared state is `NSLock`-guarded:
/// `PodService`'s pods/logs/processes/stdio maps, `NetworkService`'s address
/// assignments, and `ImageService`'s `let`-only fields keyed by owner id.
/// `LinuxPod` does its own locking on top.
///
/// (This started out as an actor driven from `Task.detached`. That was changed
/// while chasing a hang whose real cause turned out to be `FileHandle.write` on a
/// socket — see `handleConnection`. The synchronous shape is kept because it is
/// simpler, not because Swift concurrency was at fault.)
final class BrokerService: @unchecked Sendable {
    private let pods: PodService
    private let images: ImageService

    init(
        manager: VZVirtualMachineManager, paths: BrokerPaths, images: ImageService,
        network: NetworkService
    ) {
        self.pods = PodService(manager: manager, paths: paths, network: network)
        self.images = images
    }

    func handle(_ request: Request) -> Response {
        do {
            return .success(try dispatch(request))
        } catch {
            return .failure("\(error)")
        }
    }

    private func dispatch(_ request: Request) throws -> Reply {
        let params = request.params

        switch request.method {
        // MARK: Pod lifecycle

        case .createPod:
            guard let config = params.config else {
                throw BrokerError.badRequest("createPod requires config")
            }
            // The assigned address comes straight back, so the caller does not
            // need a second round trip to answer PodSandboxStatus.
            return Reply(ipv4: try pods.createPod(config))

        case .create:
            try pods.create(try podId(params))
            return Reply()

        case .stopPod:
            try pods.stopPod(try podId(params))
            return Reply()

        // MARK: Containers

        case .addContainer:
            guard let container = params.container else {
                throw BrokerError.badRequest("addContainer requires container")
            }
            try pods.addContainer(podID: try podId(params), container: container)
            return Reply()

        case .startContainer:
            let (pod, container) = try podAndContainer(params, "startContainer")
            try pods.startContainer(podID: pod, containerID: container)
            return Reply()

        case .stopContainer:
            let (pod, container) = try podAndContainer(params, "stopContainer")
            try pods.stopContainer(podID: pod, containerID: container)
            return Reply()

        case .killContainer:
            let (pod, container) = try podAndContainer(params, "killContainer")
            guard let signal = params.signal else {
                throw BrokerError.badRequest("killContainer requires signal")
            }
            try pods.killContainer(podID: pod, containerID: container, signal: signal)
            return Reply()

        case .waitContainer:
            let (pod, container) = try podAndContainer(params, "waitContainer")
            let code = try pods.waitContainer(podID: pod, containerID: container)
            return Reply(exitCode: code)

        case .listContainers:
            return Reply(containerIds: try pods.listContainers(podID: try podId(params)))

        case .exec:
            let (pod, container) = try podAndContainer(params, "exec")
            guard let processID = params.processId else {
                throw BrokerError.badRequest("exec requires processId")
            }
            let pid = try pods.exec(
                podID: pod,
                containerID: container,
                processID: processID,
                args: params.args ?? [],
                env: params.env ?? [],
                terminal: params.terminal ?? false,
                stdinPath: params.stdinPath,
                stdoutPath: params.stdoutPath,
                stderrPath: params.stderrPath,
                sockets: params.stdioSockets ?? false
            )
            return Reply(pid: pid)

        case .attach:
            let (pod, container) = try podAndContainer(params, "attach")
            try pods.attach(
                podID: pod,
                containerID: container,
                stdinPath: params.stdinPath,
                stdoutPath: params.stdoutPath,
                stderrPath: params.stderrPath
            )
            return Reply()

        case .reopenContainerLog:
            let (pod, container) = try podAndContainer(params, "reopenContainerLog")
            try pods.reopenLog(podID: pod, containerID: container)
            return Reply()

        case .resize:
            let (pod, container) = try podAndContainer(params, "resize")
            guard let processID = params.processId else {
                throw BrokerError.badRequest("resize requires processId")
            }
            guard let width = params.width, let height = params.height else {
                throw BrokerError.badRequest("resize requires width and height")
            }
            try pods.resize(
                podID: pod, containerID: container, processID: processID, width: width,
                height: height)
            return Reply()

        case .closeStdin:
            let (pod, container) = try podAndContainer(params, "closeStdin")
            try pods.closeStdin(podID: pod, containerID: container)
            return Reply()

        case .waitProcess:
            let (pod, container) = try podAndContainer(params, "waitProcess")
            guard let processID = params.processId else {
                throw BrokerError.badRequest("waitProcess requires processId")
            }
            let code = try pods.waitProcess(
                podID: pod, containerID: container, processID: processID)
            return Reply(exitCode: code)

        case .killProcess:
            let (pod, container) = try podAndContainer(params, "killProcess")
            guard let processID = params.processId else {
                throw BrokerError.badRequest("killProcess requires processId")
            }
            guard let signal = params.signal else {
                throw BrokerError.badRequest("killProcess requires signal")
            }
            try pods.killProcess(
                podID: pod, containerID: container, processID: processID, signal: signal)
            return Reply()

        case .statistics:
            let stats = try pods.statistics(
                podID: try podId(params), containerIDs: params.containerIds ?? [])
            return Reply(stats: stats)

        // MARK: Images

        case .listImages:
            return Reply(images: try images.list())

        case .imageStatus:
            guard let image = params.image else {
                throw BrokerError.badRequest("imageStatus requires image")
            }
            return Reply(image: try images.status(reference: image))

        case .pullImage:
            guard let image = params.image else {
                throw BrokerError.badRequest("pullImage requires image")
            }
            return Reply(image: try images.pull(reference: image))

        case .removeImage:
            guard let image = params.image else {
                throw BrokerError.badRequest("removeImage requires image")
            }
            try images.remove(reference: image)
            return Reply()

        case .imageFsInfo:
            let usage = images.filesystemUsage()
            return Reply(fsPath: usage.path, fsBytes: usage.usedBytes)

        case .provisionRootfs:
            guard let image = params.image, let owner = params.ownerId else {
                throw BrokerError.badRequest("provisionRootfs requires image and ownerId")
            }
            return Reply(block: try images.provision(reference: image, ownerId: owner))

        case .releaseRootfs:
            guard let owner = params.ownerId else {
                throw BrokerError.badRequest("releaseRootfs requires ownerId")
            }
            try images.release(ownerId: owner)
            return Reply()

        // MARK: Transport

        case .dial:
            guard let port = params.port else {
                throw BrokerError.badRequest("dial requires port")
            }
            return Reply(socketPath: try pods.dial(podID: try podId(params), port: port))
        }
    }

    // MARK: - Parameter extraction

    private func podId(_ params: Params) throws -> String {
        guard let id = params.podId else {
            throw BrokerError.badRequest("request requires podId")
        }
        return id
    }

    private func podAndContainer(_ params: Params, _ method: String) throws -> (String, String) {
        guard let container = params.containerId else {
            throw BrokerError.badRequest("\(method) requires containerId")
        }
        return (try podId(params), container)
    }
}

// MARK: - Wire conversions

extension BlockMountWire {
    func toMount() -> Containerization.Mount {
        // A rootfs the broker unpacked to a directory (the only kind that can
        // join a pod after its VM has booted — see VZHotplugProvider.swift).
        // `.share` carries the `.virtiofs` runtime options the hotplug provider
        // keys on; `.block` would send it down the virtio-blk path and be
        // rejected.
        if format == "virtiofs" {
            return .share(source: source, destination: destination, options: options)
        }
        return .block(
            format: format,
            source: source,
            destination: destination,
            options: options
        )
    }
}

extension AttachedFilesystemWire {
    func toMount() -> Containerization.Mount {
        // A CRI mount names a path on the *host*, and the container runs in a VM,
        // so it has to be shared in over virtiofs before anything in the guest can
        // see it. `Mount.share` carries the `.virtiofs` runtime options that
        // `FileMountContext.prepare` needs; `.any` would not, and the guest bind
        // would fail ENOENT.
        if type == "virtiofs" {
            return .share(source: source, destination: destination, options: options)
        }
        // Anything else is an attachment the VM already resolved, replayed back
        // into the mount table, so nothing should re-derive a device from it.
        return .any(type: type, source: source, destination: destination, options: options)
    }
}
