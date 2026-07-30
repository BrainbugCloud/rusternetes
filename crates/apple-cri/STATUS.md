# apple-cri status

Host: macOS 26.5.1, Apple silicon (arm64), `container` CLI **1.2.0**
(containerization 0.40.1),
critest/crictl **1.36.0** (native darwin/arm64 builds).

## critest — 2026-07-30

```bash
INCLUDE_NETWORK=1 bash scripts/apple-cri-critest.sh
```

```
Ran 49 of 59 Specs in 216.557 seconds
SUCCESS! -- 49 Passed | 0 Failed | 0 Pending | 10 Skipped
```

**49/49 of the supported set pass.** Of critest's 59 specs, 5 self-skip on
darwin (the Linux-only suites: hostNetwork, sysctls, seccomp/AppArmor/SELinux,
capabilities, OOM, `NamespaceOption`, devices) and the harness skips 5 more —
each justified in `README.md`:

| Skipped | Reason | Kind |
|---|---|---|
| `runtime should support set hostname` | `container create` has no `--hostname` | runtime |
| `…image identifier when pulled from different registries` | needs one image in two registries | fixture |
| `removing image from one registry should remove all tags from other registries` | same | fixture |
| `port mapping with host port and container port` | Apple's publish proxy cannot be authorized (below) | **environment** |
| `runtime should support execSync with timeout` | signals never reach an exec'd process on 1.2.0 (below) | **upstream bug** |

`INCLUDE_NETWORK=1` adds `runtime should support portforward` and `port mapping
with only container port`, which need the shim's own process to hold **macOS
Local Network access** — without it every host-originated packet to a container
is dropped before egress. Plain `bash scripts/apple-cri-critest.sh` skips those
two and reports **47/47**, so an unauthorized host still gets a green run.

The gate is macOS policy, not a limitation of this shim or of Apple's runtime, and
it is per-application. Verified both ways: before the grant a shell got `No route
to host` with zero packets on the bridge while an authorized app fetched HTTP 200
from the same container; after the grant the shell got HTTP 200 and both specs
above went green with no other change. `--publish` is *not* a workaround — its
proxy is a launchd-started helper that cannot present an authorization prompt, so
it accepts on loopback, fails its own dial and resets. Note that host-originated
connections to pod IPs also cover the kubelet's HTTP/TCP **liveness and readiness
probes**, so this grant is a prerequisite for running a kubelet here, not just for
port-forward. See README "Host↔container connectivity" for the measurements.

### Upgrading the runtime 0.7.1 → 1.2.0 broke five things

Worth recording, because three of them failed **silently** — the JSON parsed
into empty values rather than erroring, so the symptom appeared far from the cause:

| Change | Symptom |
|---|---|
| `image list`/`image inspect` moved `reference`+`descriptor` under a `configuration` object as `name`+`descriptor` | every Image Manager spec: "pulled X but it is not in the image store" (silent) |
| container `status` became an object `{state, startedDate, networks}` instead of a bare string | every inspect: `invalid type: map, expected a string` (loud) |
| network attachments split into `ipv4Address`/`ipv4Gateway` | pod IP resolved to `None` (silent) |
| `networks[].options` mixes value types (`{"hostname": "…", "mtu": 1280}`) | every inspect: `invalid type: integer 1280, expected a string` (loud) |
| `network ls` renamed `config`→`configuration`, `creationDate` float→ISO string | nothing yet — only `id` is read (silent) |

`testdata/container-inspect-1.2.0.json` is now a verbatim capture of the runtime's
own output, asserted against in `model.rs`. A hand-written fixture would have gone
on agreeing with the old model, which is precisely how these got through.

1.2.0 also *adds* `status.startedDate` and `configuration.creationDate` — real
timestamps, which 0.7.x had none of. They are parsed but not yet used as a source
of truth; the checkpoint store remains authoritative. Worth revisiting, since it
was the absence of timestamps that forced that store to exist.

### An upstream 1.2.0 bug: signals never reach an exec'd process

`runtime should support execSync with timeout` cannot pass on 1.2.0. A timed-out
`ExecSync` sends SIGTERM to the `container exec` child, which the CLI then fails to
forward:

```
failed to send signal: [error: invalidArgument: "missing signal in xpc message", "signal": 15]
```

Its own XPC client and server disagree on the field's type:

```swift
// Sources/Services/ContainerAPIService/Client/ClientProcess.swift:83
request.set(key: .signal, value: Int64(signal))   // writes Int64
// Sources/Services/ContainerAPIService/Server/Containers/ContainersService.swift:1154
guard let signal = self.string(key: .signal)      // reads String
```

Container-level signals go through `ContainerClient.swift:168`, which writes a
`String` — which is why `StopContainer` and `container kill` still work and only
the exec path is affected. Verified by hand: the guest `sleep` survives SIGTERM to
the CLI and is still there as pid 2. This spec passed on 0.7.1 and nothing in the
shim changed; there is no workaround available to us, because the exec'd process's
guest pid is never exposed. Worth filing upstream.

Suites fully green: Runtime info (2), PodSandbox (4), Container runtime (18,
incl. volumes, logs, `ReopenContainerLog`, execSync, stats),
Streaming (exec tty/non-tty, attach, port-forward), Networking (DNS config,
container-port mapping), Image Manager
(11), Image Consistency (3), Image Identifier Consistency (1), Idempotence (7).

## Unit tests

```
cargo test -p apple-cri --all-features
test result: ok. 87 passed; 0 failed
```

`cargo clippy -p apple-cri -p apple-containerization --all-targets --all-features
-- -D warnings` and `cargo fmt --all -- --check` are clean.

## Bugs this shim had to solve (each found by a failing spec)

| Symptom | Cause |
|---|---|
| every container reported EXITED right after start | container ids over ~64 chars make `start --attach` fail with `EINVAL`; the cri-dockerd-style name is ~230 chars |
| `StartContainer` failed for critest's idempotence specs | an empty CRI `log_path` was treated as a fatal open error |
| a timed-out `ExecSync` left its process running in the guest | SIGKILL to `container exec` orphans the guest process; only SIGTERM is proxied — and on 1.2.0 not even that, see the upstream bug above |
| exec with `tty=true, stdin=true` failed | the CLI requires a real PTY on stdin for that combination |
| `Attach` hung for the full suite timeout | attach output came from a second `container logs --follow` that never EOF'd; and the fan-out `Sender` held in the relay handle kept the channel open, so an explicit `Eof` was needed |
| attach saw nothing for `echo -n hello` | the stdio pump was line-oriented and blocked on a newline that never came |
| container create failed for port-mapped sandboxes | `host_port = 0` means "expose only"; Apple rejects `--publish 0:80` |
| one image with 3 tags reported as 3 images | CRI reports one `Image` per id with all its tags; Apple lists one row per reference |
| `RemoveImage` left the image resolvable | only tags were untagged, not a real `name@digest` store entry |
| `Username` reported as `www-data:group` | the group must be split off the OCI `User` field |
| every Image Manager spec failed after upgrading the runtime to 1.2.0 with "pulled X but it is not in the image store" | `image list`/`image inspect` moved `reference`+`descriptor` under a `configuration` object (`name`, `descriptor`); the 0.7.x shape parsed as an empty reference instead of erroring, so every lookup missed |
| concurrent `RemoveImage` calls failed for all but one caller | Apple's `image delete` is not atomic against itself; the losers exit non-zero with `failed to delete one or more images`, which reads the same as a real failure. `remove_image` now re-reads the store and treats "the reference is gone" as success, whoever removed it |

## Real pod semantics — 2026-07-30

The CLI path above gives **one microVM per container**. Real pods live below the
CLI, in `vminitd`'s `SandboxContext` gRPC service: its process RPCs carry an
optional `containerID`, so one VM hosts N containers, each with its own rootfs and
OCI runtime invocation.

That protocol is now implemented in Rust as
[`apple-containerization`](../apple-containerization/README.md) — a port of Apple's
Containerization Swift package at `ff44a5b` (v0.40.1), the version `container`
1.2.0 pins. `pod.rs` ports `LinuxPod.swift`; `crate::pod_runtime` here translates
CRI onto it.

```
cargo test -p apple-containerization -p apple-cri --all-features
16 passed   (apple-containerization unit)
36 passed   (pod semantics, over real gRPC)
87 passed   (apple-cri, incl. 30 pod_runtime)
```

The 36 pod-semantics tests run against a **real** in-process `SandboxContext`
server: the generated tonic client, the proto encoding and the OCI-spec JSON are
all the ones `vminitd` would see, and a malformed spec fails the test. They assert
the call sequence and the exact spec bytes — where pod semantics actually live.

Verified pod behaviour, and how it differs from upstream `LinuxPod`, which is the
right mechanism but the wrong policy (it gives each container a *fresh* ipc/uts
namespace):

| Behaviour | How |
|---|---|
| one VM per pod, N containers | `containerID` on every process RPC; rootfs hotplugged per container |
| shared pod network + `localhost` | no `network` namespace declared → the VM's root netns is the pod network |
| shared IPC | members join `/proc/<infra>/ns/ipc` — **divergence**, upstream gives each a fresh one |
| one pod hostname | members join `/proc/<infra>/ns/uts`; hostname set on the infra spec only |
| `shareProcessNamespace` | members join `/proc/<infra>/ns/pid`; private otherwise |
| containers added to a live sandbox | `vm.hotplug(rootfs, id:)`, the CRI ordering |
| per-container rootfs / cgroup / limits | `/run/container/<id>/rootfs`, `/container/pod/<pod>/<id>` |
| exec | same `containerID`, distinct process `id`, inheriting the container's spec |
| guest OCI runtime | `vmexec` (`ociRuntimePath: nil`), as upstream's `LinuxPod` uses at all three call sites — it is `vmexec/RunCommand.swift` `setupNamespaces()` that does the `setns`/`unshare`. runc is opt-in and absent from Apple's init image |

A member container deliberately carries **no** `hostname`: an OCI runtime can only
set one in a UTS namespace it created, and runc errors out if `hostname` is set
while the namespace is inherited.

### The VMM broker

Everything inside the guest is gRPC, so it is Rust. Four host-side operations are
not, because macOS exposes them only through Virtualization.framework and only to
the process owning the `VZVirtualMachine`: VM lifecycle, vsock dial/listen, block
hotplug, and virtiofs shares. They sit behind `apple_containerization::Vmm`.

Driving Apple's shipped `container-runtime-linux` is not sufficient — its XPC
surface has `bootstrap` and `dial`, so the agent is reachable, but no hotplug
route, so containers could never be added to a live sandbox. The broker therefore
owns the VM itself: [`vmm-broker/`](../../vmm-broker/README.md), a small Swift
package linking Apple's `Containerization` (pinned to the `0.40.1` that
`container` 1.2.0 pins) rather than reimplementing VZ setup.

```bash
bash scripts/build-vmm-broker.sh     # builds and signs; verifies the entitlement stuck
```

Two unknowns are now settled:

- **The entitlement is not a barrier.** `com.apple.security.virtualization` is
  carried by an **ad-hoc signature** (`codesign -s -`) — no paid developer
  identity. Verified on macOS 26.5.1.
- **The wire contract is covered without a hypervisor.**
  `apple-containerization/tests/broker_pod.rs` drives a full two-container pod over
  the real broker protocol *and* the real `SandboxContext` gRPC, asserting that both
  containers join the same infra ipc/uts/pid namespaces, that one VM is created and
  each rootfs hotplugged, and that teardown signals the guest before stopping the
  VM. If the Swift side answers those methods with those shapes, the semantics
  above it already work.

Implemented in the broker: VM create/start/stop/state, `dial` with a vsock↔unix
relay, `hotplug`/`releaseHotplug`, `mounts`, `registerMounts`.

### What still blocks a pod from booting

- **image → ext4 (`provisionRootfs`).** Needs the image store plus
  `ContainerizationEXT4`'s `EXT4Unpacker` to write a per-container `rootfs.ext4`.
  The **initfs is the same problem**: Apple ships it as an OCI image
  (`ghcr.io/apple/containerization/vminit`, present in the local store), not a
  file. One unpacker unblocks both — it is now the single largest remaining piece.
- **Network.** `VMConfiguration.interfaces` is passed empty, so the pod has no
  address for `configure_network` to apply. Whether to drive Apple's network
  service or run our own IPAM is undecided.
- **Stdio relay.** `Pod::dial` and the vsock port allocator exist; the pump does
  not, and the broker's `listen` is unimplemented. Until then: no logs, no exec
  output, no attach, no port-forward, and container `stdin` is wired to `None`.
- **virtiofs hotplug**, for CRI mounts naming host paths. `pod_runtime` emits the
  right mount shape; the attach half is missing, so those binds would fail ENOENT.
- **Wiring.** `pod_runtime` is not hooked into `AppleBackend` — no flag selects pod
  vs CLI mode, and pod state is not checkpointed across a shim restart the way the
  CLI path's `state.rs` does.
- **Pod VM sizing** is a flat 4 cpu / 1 GiB default. The VM is created at
  `RunPodSandbox`, before any container config is known, so this needs the
  sandbox's pod-level resources or a resize on `CreateContainer`. Unlike a Linux
  runtime, guessing wrong costs real RAM per pod.

## Not yet done

- End-to-end rusternetes-on-macOS bring-up: this crate is the CRI half; the
  kubelet also needs a macOS story for kube-proxy (iptables) and for
  `Memory`-medium `emptyDir` (`mount -t tmpfs`).
- `apple-cri` is not yet wired into CI; the harness requires a macOS runner with
  Apple's runtime installed.
