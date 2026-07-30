// swift-tools-version:6.0
//
// The VMM broker: the host-side helper that owns each pod's VZVirtualMachine.
//
// This exists because four operations are reachable only through
// Virtualization.framework, from the process holding the VM object: VM
// lifecycle, vsock dial/listen, block hotplug and virtiofs shares. Everything
// else about a pod is SandboxContext gRPC and lives in Rust
// (crates/apple-containerization). See that crate's `broker` module for the wire
// protocol this implements.
//
// containerization is pinned to the version `container` 1.2.0 itself pins, so
// the guest agent we talk to and the VZ code we drive are the pair Apple ships
// together.
import PackageDescription

let containerizationVersion = "0.40.1"

let package = Package(
    name: "rusternetes-vmm",
    platforms: [.macOS(.v15)],
    targets: [
        .executableTarget(
            name: "rusternetes-vmm",
            dependencies: [
                .product(name: "Containerization", package: "containerization"),
                .product(name: "ContainerizationOCI", package: "containerization"),
                .product(name: "ContainerizationOS", package: "containerization"),
            ]
        )
    ]
)

package.dependencies = [
    .package(
        url: "https://github.com/apple/containerization.git",
        exact: Version(stringLiteral: containerizationVersion)
    )
]
