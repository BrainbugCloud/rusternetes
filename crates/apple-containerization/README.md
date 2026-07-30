# apple-containerization

A low-level Rust client for Apple's Containerization sandbox protocol —
`SandboxContext` v3, spoken to `vminitd` — with **one microVM per pod** and many
containers inside it.

## Why this exists

Apple's `container` CLI runs one microVM per *container*. `apple-cri` drives that
CLI and passes critest, because every conformance spec critest runs is
single-container. But it is not a Kubernetes pod: containers get no shared
`localhost`, no single pod IP, and no shared IPC namespace.

The layer that makes a real pod possible is below the CLI. `vminitd` — the guest
agent that runs as PID 1 in the microVM — serves the `SandboxContext` gRPC
service on **vsock port 1024**, and every one of its process RPCs carries an
optional `containerID`:

```protobuf
message CreateProcessRequest {
  string id = 1;
  optional string containerID = 2;      // <- one VM, many containers
  ...
  optional string ociRuntimePath = 6;   // <- vmexec by default, or runc
  bytes configuration = 7;              // <- a JSON OCI runtime spec
}
```

The guest keys its container table by `containerID`, so a single VM hosts N
containers, each with its own rootfs and OCI runtime invocation. `id ==
containerID` is a container's init process; a distinct `id` is an exec into it.

## What is ported from where

Everything here is a port of Apple's Containerization Swift package at
`ff44a5b` (v0.40.1) — the version `container` 1.2.0 pins. Ports, not
reinventions; each module names its source:

| module | ported from |
|---|---|
| `agent` | `Sources/Containerization/Vminitd.swift`, `VirtualMachineAgent.swift` |
| `oci` | `Sources/ContainerizationOCI/Spec.swift` |
| `pod` | `Sources/Containerization/LinuxPod.swift` |
| `vmm` | `VirtualMachineManager.swift`, `VirtualMachineInstance.swift` |
| `proto` | `SandboxContext/SandboxContext.proto` (vendored verbatim) |

## `LinuxPod` is not a Kubernetes pod

Upstream's `LinuxPod` is the right mechanism but the wrong policy. It gives every
container a **fresh** `ipc` and `uts` namespace, and shares `pid` only when
`shareProcessNamespace` is set. A Kubernetes pod shares IPC and UTS across all
its containers unconditionally. Three deliberate divergences follow:

1. **The infra ("pause") process always exists.** Upstream creates it only for
   `shareProcessNamespace`. Here it is always created, because it is the anchor
   whose `/proc/<pid>/ns/*` paths member containers join, and because a CRI
   sandbox must outlive every container in it. It runs `/sbin/vminitd pause` out
   of a rootfs that is just a bind mount of the guest's `/sbin` — no image needed.
2. **Members join the infra `ipc` and `uts` namespaces**, giving pod-scoped SysV
   IPC and one shared hostname.
3. **A member container carries no `hostname`.** An OCI runtime can only set a
   hostname in a UTS namespace it created; runc errors out if `hostname` is set
   while the namespace is inherited. So the pod hostname goes on the infra spec
   and member specs clear it. Upstream could set per-container hostnames only
   because it wasn't sharing UTS.

Networking needs no namespace entry at all: the VM *is* the pod's network, so
omitting the `network` namespace leaves every container in the VM's root netns —
one pod IP, shared `localhost`. `NamespaceMode::Node` means the VM's root
namespace, since there is no macOS host namespace a guest process could join.

## The VMM boundary

Everything inside the guest is gRPC, so it is all Rust. Four host-side operations
are not, because macOS exposes them only through Virtualization.framework, and
only to the process that owns the `VZVirtualMachine`:

1. **VM lifecycle** — `VZVirtualMachine.start()/stop()`.
2. **vsock** — a guest connection comes from `VZVirtioSocketDevice.connect(toPort:)`
   on the in-process VM object. No other process can dial that guest.
3. **Block hotplug** — attaching a container's rootfs to a *running* VM. This is
   what makes CRI's "create a container in a live sandbox" possible at all;
   `LinuxPod.addContainer` calls `vm.hotplug(rootfs, id:)` for exactly this.
4. **virtiofs shares** — host directories into the guest.

Those sit behind the `vmm::Vmm` / `vmm::VmInstance` traits, so pod logic, OCI
specs, namespace wiring and process lifecycle all stay in Rust. Each vsock port
is surfaced as a host unix socket, which is a faithful shape — upstream's own
`Vminitd.init(connection: FileHandle, …)` wraps an already-connected fd rather
than dialing anything itself.

This is also why driving Apple's shipped `container-runtime-linux` helper is not
enough. Its XPC surface has `bootstrap` and `dial` (so the agent *is* reachable),
but no hotplug route — so containers could never be added to a live sandbox.

**A VMM broker is not implemented yet.** Without one this crate cannot boot a
real VM; see `crates/apple-cri/STATUS.md`.

## Testing

`testing` (behind the `testing` feature) provides a **real** in-process
`SandboxContext` gRPC server plus a mock VMM. Pod tests drive the generated tonic
client, the spec is JSON-encoded and decoded exactly as `vminitd` would, and a
malformed spec fails the test. Assertions are about the call sequence and the
exact spec bytes, which is where pod semantics actually live.

```bash
cargo test -p apple-containerization --all-features
```

## Layering

This crate must not depend on any `rusternetes-*` crate, nor on `cri-proto` /
`cri-server`. It knows Apple's guest protocol and nothing about CRI; `apple-cri`
joins the two.
