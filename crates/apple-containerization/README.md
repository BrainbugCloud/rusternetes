# apple-containerization

The host-side client for the **rusternetes VMM broker's pod protocol** — one
microVM per pod, many containers inside it.

## Why this exists

Apple's `container` CLI runs one microVM per *container*. `apple-cri` drives that
CLI and passes critest, because every conformance spec critest runs is
single-container. But it is not a Kubernetes pod: containers get no shared
`localhost`, no single pod IP.

Apple's `Containerization` package has the primitive that makes a real pod
possible — `LinuxPod`, one VM hosting N containers, each with its own rootfs.
It is Swift, and the host-side operations it needs are only reachable through
Virtualization.framework from the process that owns the `VZVirtualMachine`:

1. **VM lifecycle** — `VZVirtualMachine.start()/stop()`
2. **vsock** — a guest connection comes from `VZVirtioSocketDevice.connect(toPort:)`
   on the in-process VM object, so no other process can dial the guest
3. **Block hotplug** — attaching a container's rootfs to a *running* VM, which is
   what makes CRI's "create a container in a live sandbox" possible
4. **Image → ext4** — `ContainerizationEXT4`'s `EXT4Unpacker`
5. **Container stdio** — the process's `stdout`/`stderr` are `Writer`s held by
   whoever created it

So [`vmm-broker/`](../../vmm-broker) owns all of that and calls the real
`LinuxPod`, and **this crate is the client half**: [`PodBroker`](src/broker.rs) is
the entire host-side pod surface, spoken as newline-delimited JSON over a unix
socket.

## What this crate used to be

A Rust port of `LinuxPod` + `Vminitd` + the OCI spec types — about 2400 lines
that spoke `SandboxContext` gRPC to the guest directly and drove a dumb VM
underneath. That port duplicated working Swift, did not receive Apple's fixes,
and its first live boot failed on a `LinuxPod.create()` precondition it had not
replicated. It is gone; the broker calls the real thing.

What that leaves here is deliberately thin: **the wire, and nothing else.** Pod
semantics are Apple's. The CRI translation is `apple-cri`'s
(`pod_runtime.rs`).

## The protocol

`Protocol.swift` is authoritative; this crate mirrors it. Two rules follow from
Swift's `Codable`, and both are enforced by tests rather than by review:

1. A field that is non-optional in Swift **must always be serialised** — `Codable`
   fails to decode when it is absent.
2. Swift's `JSONEncoder` omits `nil`, so every struct we decode needs
   `#[serde(default)]`.

```text
-> {"method":"createPod","params":{"config":{"id":"pod-1",…}}}
<- {"ok":{}}
-> {"method":"waitContainer","params":{"podId":"pod-1","containerId":"init"}}
<- {"ok":{"exitCode":0}}
```

Getting this wrong is not loud. Upgrading Apple's runtime 0.7.1 → 1.2.0 broke five
things and **three failed silently**, parsing into empty values rather than
erroring — which is why `broker.rs`'s tests assert against verbatim JSON fixtures
instead of hand-checked field names.

## What Apple's `LinuxPod` does not give us

Recorded here because each one is a real divergence from the Kubernetes pod
model, not an oversight:

| CRI wants | `LinuxPod` |
|---|---|
| shared IPC across a pod's containers | a **fresh** `ipc` namespace per container |
| one shared UTS namespace | a fresh `uts` per container — a pod hostname is the same *string*, not the same namespace |
| a per-container OCI runtime | `ociRuntimePath: nil` hardcoded at all three `createProcess` call sites; `ContainerConfiguration` has no field for it. This is what a mixed wasm + classic pod would need |
| `RemoveContainer` | no remove; a container's guest state lives until the pod stops |

**All four are accepted** (2026-08-02). Each is fixable only by forking Apple's
package or dropping below `LinuxPod` to the raw `SandboxContext` protocol — the
maintenance burden that retiring the Rust port removed. Mixed-runtime pods were
the one with real upside and were **dropped** on that basis; the selector is still
there in the raw protocol if the trade ever changes. `RemoveContainer`'s residue
is tracked as a follow-up in [`apple-cri/STATUS.md`](../apple-cri/STATUS.md).

## Testing

`testing` (behind the `testing` feature) provides [`FakeBroker`](src/testing.rs):
a real server for this wire format on a unix socket that records every call and
keeps enough state to make the interesting errors real — an unknown pod or
container is reported rather than silently accepted, and it tracks whether a
container was added before the sandbox booted (attached at boot) or after
(hotplugged into a live VM).

`tests/broker_pod.rs` uses it to pin the sequences that matter, including the CRI
translations of an **init container** (wait for the exit code before creating the
next) and a **sidecar** (no wait, two containers running at once). Neither exists
in CRI — they are kubelet ordering concepts — and critest cannot cover them: its
multi-container specs live in `pkg/validate/multi_container_linux.go`, and the
`_linux.go` suffix is an implicit Go build constraint, so they are not in a
darwin critest binary at all.

```bash
cargo test -p apple-containerization --all-features
```

The ignored `a_two_container_pod_boots_against_a_live_broker` runs the same
sequence against a real VM; see its doc comment for the invocation.

## Layering

This crate must not depend on any `rusternetes-*` crate, nor on `cri-proto` /
`cri-server`. It knows the broker protocol and nothing about CRI; `apple-cri`
joins the two.
