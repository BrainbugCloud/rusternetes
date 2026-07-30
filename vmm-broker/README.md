# rusternetes-vmm — the VMM broker

The host-side helper that owns each pod's `VZVirtualMachine`, so `apple-cri` can
run **one microVM per pod** with several containers inside it.

```bash
bash scripts/build-vmm-broker.sh          # builds and signs (entitlement required)
./vmm-broker/.build/debug/rusternetes-vmm \
    --listen /tmp/rk/vmm.sock \
    --kernel "$HOME/Library/Application Support/com.apple.container/kernels/default.kernel-arm64" \
    --initfs /path/to/initfs.ext4 \
    --runtime-dir /tmp/rk/vmm
```

## Why this is Swift

Everything a pod does *inside* the guest is `SandboxContext` gRPC, and all of that
is Rust ([`crates/apple-containerization`](../crates/apple-containerization)).
Four host-side operations are not, because macOS exposes them only through
Virtualization.framework and only to the process holding the VM object:

1. **VM lifecycle** — `VZVirtualMachine.start()/stop()`
2. **vsock** — a guest connection comes from `VZVirtioSocketDevice.connect(toPort:)`
   on the in-process VM object, so **no other process can dial the guest**. This
   alone forces a helper to exist.
3. **Block hotplug** — attaching a container's rootfs to a *running* VM, which is
   what makes CRI's "create a container in a live sandbox" possible.
4. **virtiofs shares** — host directories into the guest.

Driving Apple's shipped `container-runtime-linux` instead is not sufficient: its
XPC surface has `bootstrap` and `dial`, so the agent *is* reachable, but there is
no hotplug route — containers could never be added to a live sandbox.

The broker is therefore deliberately thin. It exposes those capabilities and
nothing else; pod semantics, OCI specs, namespace wiring and process lifecycle all
stay in Rust. It links Apple's `Containerization` package (pinned to `0.40.1`, the
version `container` 1.2.0 itself pins) rather than reimplementing VZ setup.

## Protocol

Newline-delimited JSON over a unix socket, **one connection per request**. The
authoritative definition is [`broker.rs`](../crates/apple-containerization/src/broker.rs);
[`Protocol.swift`](Sources/rusternetes-vmm/Protocol.swift) mirrors it.

```text
-> {"method":"createVm","params":{"vmId":"pod-1","config":{...}}}
<- {"ok":{}}
-> {"method":"dial","params":{"vmId":"pod-1","port":1024}}
<- {"ok":{"socketPath":"/tmp/rk/vmm/pod-1/v1024-1"}}
```

Bulk data never crosses the control socket. `dial` stands up a one-shot relay
socket and splices it to the guest vsock port; Rust connects to that path and
speaks gRPC over it. That mirrors upstream's own
`Vminitd.init(connection: FileHandle, …)`, which takes an already-connected fd.

`crates/apple-containerization/tests/broker_pod.rs` drives a full multi-container
pod against a Rust implementation of this same protocol, so the wire contract is
covered independently of this package.

## The entitlement

Creating a `VZVirtualMachine` requires `com.apple.security.virtualization`. An
**ad-hoc signature carries it** — no paid developer identity needed (verified on
macOS 26.5.1):

```bash
codesign --force --sign - --entitlements vmm-broker/entitlements.plist "$BIN"
```

`scripts/build-vmm-broker.sh` does this on every build and then verifies the
entitlement stuck, because without it the failure surfaces late — at the first
`createVm`, not at launch.

## Status

Implemented: VM create/start/stop/state, vsock `dial` with relay, block
`hotplug`/`releaseHotplug`, the `mounts` table and `registerMounts`.

Not implemented, and the remaining blockers for a pod to boot:

- **`provisionRootfs` — image → ext4.** Needs the image store plus
  `ContainerizationEXT4`'s `EXT4Unpacker` to write a per-container `rootfs.ext4`.
  The **initfs is the same problem**: Apple ships it as an OCI image
  (`ghcr.io/apple/containerization/vminit`), not a file, so `--initfs` currently
  has to be produced out of band. One unpacker unblocks both.
- **`listen`** — host-side vsock listening, needed only for process stdin.
- **Network interfaces.** `VMConfiguration.interfaces` is passed empty; a pod needs
  a vmnet interface and an address before `configure_network` has anything to
  configure. Who allocates the IP is still open (drive Apple's network service, or
  run our own IPAM).
- **virtiofs hotplug**, for CRI mounts that name host paths.

See [`crates/apple-cri/STATUS.md`](../crates/apple-cri/STATUS.md) for how this
fits the rest of the pod path.
