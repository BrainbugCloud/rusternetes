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

## Real pod semantics — 2026-08-02

The CLI path above gives **one microVM per container**. A real pod needs one VM
hosting N containers, and Apple's `Containerization` package already has that
primitive: `LinuxPod`.

The layering settled here after one false start. It was first ported into Rust —
`LinuxPod` + `Vminitd` + the OCI spec types, ~2400 lines speaking `SandboxContext`
gRPC to the guest directly. That port duplicated working Swift, did not receive
Apple's fixes, and its first live boot failed on a `LinuxPod.create()` precondition
it had not replicated. **It has been retired.** The broker links Apple's package
and calls the real `LinuxPod`; the protocol between them is pod-shaped
(`createPod`, `addContainer`, `startContainer`, `waitContainer`).

So:

| layer | what it owns |
|---|---|
| `apple-cri::pod_runtime` | the **CRI translation** — `command`+`args`, `KeyValue` env bytes, quota/period → whole cores, log-path joining, mount propagation |
| [`apple-containerization`](../apple-containerization/README.md) | the **wire** — `PodBroker`, the JSON protocol, nothing else |
| [`vmm-broker/`](../../vmm-broker/README.md) | **pod semantics**, via Apple's `LinuxPod`, plus VM lifecycle, vsock, image→ext4 and container stdio |

```
cargo test -p apple-containerization -p apple-cri --all-features
 7 passed   (apple-containerization unit — the wire fixtures)
14 passed   (broker protocol contract, incl. init-container and sidecar sequences)
82 passed   (apple-cri, incl. 21 pod_runtime)
```

`swift build` in `vmm-broker/` is green. Note it was **not** before this work: the
Swift side had been half-migrated, with `Protocol.swift` pod-shaped and
`BrokerService.swift` still dispatching `createVm`/`vmId`. The pod path could not
have run at all.

### What the wire tests are, now that semantics moved to Swift

`tests/broker_pod.rs` drives a fake broker over the real wire format and pins the
call sequence and payloads. That includes the CRI translations of an **init
container** (create → start → `waitContainer` for the exit code → only then create
the next) and a **sidecar** (the same without the wait, two containers running at
once in one VM).

Neither exists in CRI — they are kubelet ordering concepts — and **critest cannot
cover them**: its multi-container specs live in
`pkg/validate/multi_container_linux.go`, and the `_linux.go` suffix is an implicit
Go build constraint, so they are absent from a darwin critest binary. That is why
these tests are written here rather than deferred to conformance.

What was lost with the Rust port: the 36 pod-semantics tests asserted the exact
OCI spec bytes. Those bytes are Apple's to build now, so the equivalent assertions
belong in Swift.

### What Apple's `LinuxPod` does not give us

Each of these is a real divergence from the Kubernetes pod model:

| CRI wants | `LinuxPod` | consequence |
|---|---|---|
| shared IPC across the pod | a fresh `ipc` per container | System V IPC is not pod-scoped |
| one shared UTS namespace | a fresh `uts` per container | a pod hostname is the same *string*, not the same namespace |
| a per-container OCI runtime | `ociRuntimePath: nil` hardcoded at all three `createProcess` call sites; `ContainerConfiguration` has no field for it | **a mixed wasm + classic pod is not expressible through `LinuxPod`** |
| `RemoveContainer` | no remove | guest-side state lives until the pod stops; `remove_container` stops it and reclaims the rootfs, which is all CRI observes |

**All four are accepted, 2026-08-02.** Every one of them is fixable only by
forking Apple's `Containerization` package or dropping below `LinuxPod` to the raw
`SandboxContext` protocol, which is precisely the maintenance burden that retiring
the Rust port removed. Two follow-ups fall out:

1. **`RemoveContainer` leaks guest-side state until the pod stops.**
   `remove_container` kills the container, drops our bookkeeping and releases the
   rootfs image, so `ListContainers` and `ContainerStatus` answer correctly and no
   ext4 file is leaked — everything CRI can observe. What remains is an entry in
   `LinuxPod`'s own container table and its `/run/container/<id>` mount.
   *Impact:* a pod that creates and removes many containers over its lifetime
   (`restartPolicy: Always` with a crash-looping container is the realistic case)
   accumulates dead entries for as long as the sandbox lives. Bounded by pod
   lifetime, unbounded within it.
   *Watch for:* rising guest memory or a container-id collision on a long-lived
   pod; critest's idempotence suite passing but a soak test not.
   *Fix when it bites:* a `removeContainer` on the broker protocol, implemented
   either by an upstream contribution to `LinuxPod` or by calling
   `SandboxContext.DeleteProcess` + unmounting the rootfs directly from the
   broker, which does not require a fork.

2. **The 36 retired pod-semantics assertions have no Swift equivalent.** Nothing
   currently checks the OCI specs `LinuxPod` builds. That is Apple's code, so the
   bar is lower than it was for our port — but "no test at all" is not the same as
   "tested upstream".

The raw `SandboxContext` protocol *does* carry `ociRuntimePath` per process, so
mixed-runtime pods stay reachable — via a `LinuxPod` fork or a drop to that
protocol for that case.

### Adding containers to a running pod — 2026-08-03

The first live pod boot reached `addContainer` and failed with
`unsupported: "hotplug not supported"`. That is not a bug to fix but a platform
fact, and it invalidated a premise recorded throughout these docs ("containers
added to a live sandbox — `vm.hotplug(rootfs, id:)`, the CRI ordering").

**Virtualization.framework cannot attach a block device to a running VM.**
`VZVirtualMachine` exposes runtime arrays for console, directory-sharing,
graphics, memory-balloon, network, socket and USB devices — and no
`storageDevices`. Apple states it themselves, twice, in
`Sources/Integration/Suite.swift`: *"Hotplug into a running pod VM is CH-only (VZ
has no runtime hotplug)"*. `VZVirtualMachineInstance.hotplugProvider` is `nil` and
nothing in the package ever assigns it; the only implementation is
`CHHotplugProvider`, for cloud-hypervisor on Linux.

`VZUSBController.attachDevice` (macOS 15+) *is* real runtime storage — the header
calls `VZUSBMassStorageDevice` "hot-pluggable". It is unusable here: the guest
kernel Apple ships (`vmlinux-6.12.28-153`) has no USB stack at all — zero
`xhci_hcd`, zero `usb-storage` symbols. That route costs a custom kernel build,
and is the fallback if virtiofs rootfs throughput disappoints.

**What works instead: mutate the live virtiofs share.**
`VZVirtioFileSystemDevice.share` is read-write at runtime (macOS 12+), and
Containerization already gives every VZ VM one unified virtiofs device tagged
`virtiofs` holding a `VZMultipleDirectoryShare` — commented *"This device hosts
all virtiofs shares and supports runtime updates"*. The guest mounts that tag once
at `/run/virtiofs`; each share is a subdirectory. Adding a container's rootfs is
adding a directory to that share.

Measured before any of it was written, by `--share-mutation-probe`
(`vmm-broker/Sources/rusternetes-vmm/ShareMutationProbe.swift`, kept as a
regression test): across three successive whole-share swaps under a mounted
guest, additions became visible in under one 100ms poll, removal settled in
~1.1s (negative dentry caching), and the mountpoint's `st_dev` and a pre-existing
file's `st_ino` never changed — the filesystem is not remounted.

This is Kata's `disable_block_device_use` model: pass the rootfs over virtio-fs
instead of a block device, trading I/O throughput for the ability to attach it
after boot. `VZHotplugProvider.swift` implements it, ported from
`CHHotplugProvider` — same protocol, same refcount-per-tag, same record/release
split. **No fork of Apple's package**: `HotplugProvider`, `hotplugProvider`'s
setter, `vzVirtualMachine`, `vmQueue`, `withMountRegistry`, `Mount.tagHash` and
`AttachedFilesystem`'s memberwise init are all public.

Two shape differences from CH, both handled in the provider:

- CH runs `.perTag` (one device per tag, mountable directly); VZ runs `.unified`.
  So the provider returns a **bind** of `/run/virtiofs/<tag>`, not a virtiofs
  mount of the tag — the rootfs is already inside a mounted filesystem.
- `LinuxPod` only mounts `/run/virtiofs` when the added container brings
  *additional* virtiofs mounts, and only *after* it has mounted the rootfs. A pod
  that boots empty — which is every CRI pod, since `RunPodSandbox` precedes
  `CreateContainer` — would bind against a path that does not exist. The provider
  mounts it itself, once, before returning.

**New divergence: rootfs metadata fidelity.** A rootfs is now an extracted host
directory, not an ext4 image, and `EXT4Unpacker`'s trick of writing inode metadata
directly is no longer available:

| lost | consequence |
|---|---|
| uid/gid ownership | every file is owned by the broker's uid, and virtiofs shows the guest exactly that. Containers running as root are unaffected; a non-root container writing to image-owned paths sees EACCES where ext4 would not |
| device nodes, fifos, sockets | need `CAP_MKNOD`, so they are skipped and counted. Images rarely ship them — the runtime creates `/dev` |
| case-sensitive paths | APFS is case-insensitive by default; two image paths differing only in case collide |

Multi-layer whiteouts *are* handled — `DirectoryUnpacker.swift` ports
`whiteoutPrefix` / `whiteoutMetaPrefix` / `whiteoutOpaqueDir` and the
`convertWhiteout` default from containerd `pkg/archive/tar.go:122-131`. Apple's own
`unpackRootfsDirectory` does not; it declares a single-layer assumption.

Verified end to end on 2026-08-03: `a_two_container_pod_boots_against_a_live_broker`
passes against a real VM — an init container runs to completion (exit 0), then a
second container is added to the already-running sandbox and started. That is the
CRI ordering, and it was the blocker.

Not covered by the probe: an open file descriptor held across a share swap. Stable
`st_dev`/`st_ino` is strong evidence it survives, but it is inference.

### Stdio

`listen` (host-side vsock accept) was the plan and is the wrong primitive:
`LinuxProcessConfiguration` carries `stdout`/`stderr` as `Writer`s, so the output
is already in the broker's process — there is nothing to accept.

The broker therefore writes the **CRI log file** itself, which is also what
containerd's CRI plugin does (`pkg/cri/io/logger.go`): `<RFC3339Nano> <stdout|stderr>
<F|P> <line>`. `ContainerConfigWire.logPath` carries the destination, joined from
CRI's `log_directory` + `log_path` as containerd joins them.

Also fixed here: `PodService.exec` returned `process.pid` without calling
`start()`. `execInContainer` only *builds* the process, so that was a pid for a
process that was never created. `exec` now starts it, and `waitProcess` /
`killProcess` were added so `ExecSync` can report an exit code and enforce a
timeout.

**Streaming stdio** (attach, interactive exec) takes the opposite shape from
`ExecSync`, and the direction matters: **the caller listens and the broker
connects**. With sockets the other way round, everything the process wrote between
"socket created" and "client attached" would be lost; `apple-cri` binds its
listeners before it issues the call, so there is always a reader. `Streaming.swift`
carries the transport — `SocketWriter`, a `SocketStdin` queue feeding
`ReaderStream`, and a `TeeWriter` so a container's stdout reaches its CRI log file
*and* any client that attaches later. The tee has to exist from `addContainer`:
Containerization takes the writers once and there is no re-pointing them, so a
container that might ever be attached to needs the fan-out in place before it
starts. `stdin` is created only when CRI asked for it — a process given an empty
stream that never EOFs would hang anything reading it in the guest.

**Exec output** takes the same shape for the same reason — the process's
`Writer`s live in the broker — but to plain files rather than the CRI log format,
because `ExecSync` must return the command's exact bytes. `ExecOptions` carries
`stdoutPath`/`stderrPath`; `PodRuntime::exec_sync` allocates a scratch dir per
exec, reads both streams once the process is reaped, and removes the dir on every
path including a panic. Files rather than sockets: `ExecSync` is synchronous and
bounded, a file cannot block the guest when nothing is reading it, and there is no
connect race between `exec` returning and the caller attaching. A timeout kills
**and reaps** the process, then reports `DeadlineExceeded` rather than the signal's
exit code — a command that was shot did not finish with that status.

### What still blocks a pod from running under a kubelet

### Pod addressing

Every pod now gets an IP, so `PodSandboxStatus.network.ip` is answerable and
port-forward and kubelet probes have something to dial. It is one address for the
whole pod, shared by every container — the VM *is* the pod's network — which is
the property the CLI-backed path structurally cannot provide.

IPAM lives in the broker (`NetworkService.swift`) and uses Apple's own
`IPv4Address.allocator`, because both the allocator and the vmnet subnet are on
that side of the boundary. The rest was already Apple's: `NATInterface` is a value
type that `VZVirtualMachineInstance` turns into a
`VZVirtioNetworkDeviceConfiguration` with a `VZNATNetworkDeviceAttachment`. The
address comes back on `createPod`'s reply, so CRI needs no second round trip.

⚠️ **One risk this does not solve.** `VZNATNetworkDeviceAttachment` puts guests on
macOS's *shared* vmnet subnet, where `bootpd` also hands out addresses by DHCP.
Our guests never ask — `vminitd` configures statically — so we cannot collide with
ourselves, but we can collide with another VM that DHCP gave the same address: an
`apple/container` container, another VZ app, Docker Desktop. Mitigated, not fixed,
by `--pod-subnet` / `--pod-range-start` / `--pod-range-size`, defaulting to a
50-address range high in the subnet where bootpd is least likely to have reached.
A real fix needs a dedicated subnet, which Virtualization.framework does not
expose to us.
- **TTY resize on `Attach`.** `LinuxPod` exposes `resize` on the `LinuxProcess` it
  returns from `execInContainer`; a container's own init process is not reachable
  through its public surface. Interactive `Exec` can be resized, a TTY `Attach`
  cannot. Accepted, same reasoning as the other `LinuxPod` divergences.
- ~~Two image stores~~ — resolved. The pod path answers CRI's image RPCs from
  the **broker's** `ImageStore`, which is the store it actually runs rootfs images
  out of. Serving them from Apple's CLI store instead would have let `PullImage`
  populate one while the pod pulled into the other, and `RemoveImage` leave the
  image still resolvable — the failure critest's Image Consistency suite exists to
  catch. `listImages`/`imageStatus`/`pullImage`/`removeImage`/`imageFsInfo` are on
  the broker; the CRI shape (digest as id, tag-stripped repo digest, the OCI
  `User` string split into uid-or-username) stays in `pod_runtime`.
- ~~virtiofs mounts~~ — done. CRI mounts now translate to `Mount.share`
  (virtiofs) rather than a guest bind, which is what `FileMountContext.prepare`
  acts on. CRI's propagation modes are dropped rather than faked: they describe
  how a *bind* relates to its parent mount and a share has no such notion.
- **Wiring.** `pod_runtime` is still not hooked into `AppleBackend` — no flag
  selects pod vs CLI mode, and pod state is not checkpointed across a shim restart
  the way the CLI path's `state.rs` does. This is the next step.
- **Pod VM sizing** is a flat 4 cpu / 1 GiB. CRI already hands `RunPodSandbox` the
  pod's summed container resources in `LinuxPodSandboxConfig.resources` plus
  `overhead`; porting upstream's `calculateSandboxResources` is what fixes it.
  Unlike a Linux runtime, guessing wrong costs real RAM per pod.

### critest against the pod path — 2026-08-03

`critest` 1.36.0, `--backend pod`, against a live broker on this Mac.

**The darwin suite is 54 of 59 specs, and the multi-container tests are not in
it.** cri-tools puts them in `pkg/validate/multi_container_linux.go`, and Go's
implicit `_linux.go` build constraint compiles the file out on darwin — along
with the security-context, seccomp, apparmor, selinux and user-namespace specs.
So critest on macOS cannot exercise the thing the hotplug provider unblocked;
`a_two_container_pod_boots_against_a_live_broker` is what covers that, and the
darwin suite is a floor, not a conformance claim.

Three real bugs, each found by a failing spec:

1. **The image's entrypoint was ignored.** A CRI `ContainerConfig` routinely
   leaves `command`/`args` empty and expects the image's `Entrypoint`/`Cmd` —
   every critest container built from the nginx image does. The pod path passed
   CRI's fields straight through, so the guest rejected them with "process args
   cannot be empty" and *every* spec that starts a container failed.
   Fixed by carrying the image's process config on `ImageWire` and porting
   containerd's merge (`WithProcessArgs`, `internal/cri/opts/spec_opts.go:59`),
   including its guard for an entrypoint that is a single empty string. Env and
   working dir merge the same way — image first, CRI overriding per key.

2. **Container state was hardcoded to RUNNING.** `list_containers` and
   `container_status` both returned `ContainerRunning` unconditionally, so a
   created container looked started and a stopped one never exited. critest's
   "stopping container" spec waits 60s for the state to leave RUNNING.
   `LinuxPod` keeps its per-container state private and `listContainers` returns
   every container it knows regardless of state, so there is nothing to ask.
   Tracked here instead — `started_at`/`finished_at`/`exit_code` on the entry,
   stamped by an exit monitor spawned at `StartContainer`, which is how
   containerd's CRI plugin does it. CRI has no `WaitContainer`, so that monitor
   is the only waiter and cannot race one.

3. **The broker served one request at a time, and deadlocked.** `handleConnection`
   ran inline on the accept thread. Handlers block for as long as their guest
   work takes, so the first `waitContainer` on a long-lived container wedged the
   broker permanently — and bug 2's exit monitor issues one per started
   container, so the fix for 2 would have made this fire on every pod. critest's
   "execSync with timeout" hit it first and hung the whole suite: `ExecSync`
   times out correctly and recovers by sending `killProcess`, but that is a
   *second* request, which a broker blocked in `waitProcess` can never serve.
   Now one thread per connection. Safe because every piece of shared state was
   already `NSLock`-guarded; the one check-then-act gap (`NetworkService.allocate`)
   is now under a single lock.

The main.swift comment justifying the inline loop cited "why VZ work must stay
off Swift concurrency" — but the note it pointed at says the opposite: the
synchronous shape was kept "because it is simpler, not because Swift concurrency
was at fault". Nothing was protecting anything.

#### Where it stands

```
Ran 54 of 59 Specs in 90.794 seconds
SUCCESS! -- 54 Passed | 0 Failed | 0 Pending | 5 Skipped
```

**54/54 of the darwin suite pass**, with no `--ginkgo.skip`. The 5 skips are
critest's own (the Linux-only files), so every spec that can run here runs.

Before the fixes below, nothing that started a container passed and the suite
deadlocked partway through. The sequence was 31/22 → 46/7 → 47/7 → 54/0, and the
wall-clock fell from 270s to 91s once the specs stopped timing out.

The clusters, and where they stand:

| n | cluster | cause |
|---|---|---|
| ~~11~~ 0 | Image Manager / Image Consistency / Image Identifier Consistency | **fixed** — see below |
| ~~4~~ 0 | Idempotence (`StopContainer`, `RemoveContainer`, `StopPodSandbox` "if not found") | **fixed** — see below |
| ~~1~~ 0 | `runtime should support attach` (hung the suite) | **fixed** — see below |
| ~~3~~ 0 | port mapping (×2), portforward | **fixed** — see below |
| ~~2~~ 0 | starting container with a volume, and with a symlinked host path | **fixed** — see below |
| ~~1~~ 0 | listing stats filtered by labels | **fixed**: `ListContainerStats` ignored `label_selector`, which `ListContainers` had always honoured |
| ~~1~~ 0 | reopening container log | **fixed** — see below |

#### Image identity and idempotence — fixed

**Images are now keyed by digest, not reference.** The broker's store holds one
entry per reference, so `tags:1`, `tags:2` and `tags:3` of one image were three
images. `aggregate_images` groups them by digest into CRI's one-image-per-id
shape, with every tag in `repo_tags` and one `repo_digests` entry per repository.
`find_image` then resolves the three interchangeable ways a CRI caller names an
image — the id from `PullImage`, a `repo@digest`, or any tag, short or fully
qualified (reusing `crate::images::normalize_reference`, which the CLI path
already had). `RemoveImage` resolves the same way and then removes *every*
reference on that digest, because CRI removes an image rather than a name.

Two consequences that were not obvious from the spec names:

- The `Uid|Username` spec was not a config-reading bug. It looks the image up as
  `…/test-image-user-uid` with no tag; that reference did not resolve, so the
  uid was absent rather than wrong. Normalisation fixed it — verified 1002.
- `ContainerStatus.image_ref` must be the image **id**, not the reference, and
  `CreateContainer` must accept an id in `ImageSpec.image` — critest creates a
  container straight from the id `PullImage` returned, which the broker rejected
  with "invalid domain for image reference". `create_container` now resolves to a
  reference the store knows before provisioning, as containerd resolves against
  its image store before calling the snapshotter.
- `Container.image_id` / `ContainerStatus.image_id` are a *separate field* from
  `image_ref` and were left empty. **critest skips rather than fails when it is
  empty** ("runtime does not seem to implement CRI API image_id correctly yet"),
  so the two Image Identifier Consistency specs silently stopped running and the
  pass count looked better than it was. Worth remembering when reading a critest
  tally: a rising "Skipped" is not neutral. api.proto is explicit that `image_id`
  "MUST always match `PullImageResponse.image_ref`", which is the digest.

**The idempotent RPCs no longer report `NotFound`.** `StopPodSandbox`,
`StopContainer` and `RemoveContainer` are all documented idempotent in CRI's
api.proto, and containerd no-ops each on a missing id
(`internal/cri/server/sandbox_stop.go`, `container_remove.go`). `StopContainer`
also short-circuits on a container the exit monitor has already reaped, so
stopping an exited container needs no guest round trip. `RemoveImage` on an
absent image was already success and stays so.

#### The last four clusters

**Volumes: two owners, one mountpoint.** A container with a CRI volume made
`addContainer` hang for ever. The provider mounts the unified virtiofs share so a
rootfs bind can resolve against it, and `LinuxPod` *also* mounts the same tag at
`/run/virtiofs` — guarded by private state it sets at boot, which is always false
here because a CRI pod boots with no containers. Two mounts of one tag on one path
wedged the guest. Only volume specs hit it, because LinuxPod only takes that branch
for a container with *additional* virtiofs mounts. The provider now uses its own
`/run/rk-virtiofs` and leaves `/run/virtiofs` to LinuxPod: one mountpoint, one
owner.

**`ReopenContainerLog` was a no-op returning success.** CRI requires a *new* file
at the path after the kubelet rotates the old one away; we kept writing through
the original handle, into an inode nothing would read again. The broker owns the
file, so this is a new RPC — `ContainerLog.reopen()` closes and reopens the path.

**Pods were addressed on the wrong subnet.** All three networking specs failed
identically, and none of it was the network code: `NetworkService` hardcoded
`192.168.64.0/24` while this host's vmnet bridge is on `192.168.65.0/24`. The
container was perfect — nginx listening, self-curl 200 — and simply had an address
the host had no interface, route or ARP for. **macOS chooses the vmnet subnet, not
us**, and it varies by machine.

`discoverSubnet()` now reads it from the live interface list. The wrinkle is
timing: the bridge exists only while a VM is attached, so at broker startup on an
idle host there is nothing to find, and the authoritative
`com.apple.vmnet.plist` is root-only. So when discovery comes up empty the broker
boots one throwaway VM — the bridge appears when the VM *starts*, well before the
guest kernel, so it costs a fraction of a second and never waits for Linux.
`--pod-subnet` still overrides. That fixed port-forward and container-port mapping
outright.

**Host port mapping had no implementation.** vmnet NAT forwards outbound only and
exposes no way to publish an inbound port; containerd gets this from CNI portmap
writing iptables DNAT, which has no equivalent here. The shim now carries the
traffic itself — an accept loop per mapping, `copy_bidirectional` per connection,
aborted when the sandbox stops. `host_port == 0` means "expose only" and binds
nothing. **UDP mappings are logged and skipped**, which is a real gap: critest only
covers TCP, and a pod publishing a UDP host port will silently not receive it.

#### The attach hang — `stdin_once` was never implemented

`runtime should support attach` used to hang the whole suite. The misleading part
was that attach *worked* when driven by hand: `crictl attach -i` delivered output
fine, immediately and after a 10s delay. What critest does differently is **end
the attach**, and that is where it wedged.

Its `checkAttach` runs `StreamWithContext` on the test's own goroutine and waits
for the stream to finish; a helper goroutine writes `echo hello`, asserts the
output, then closes its stdin pipe. Closing attach-stdin is supposed to end the
container, and the chain that makes it end had two breaks:

1. **`stdin_once` was not on the wire at all.** critest's shell container sets
   `StdinOnce: true` (`createShellContainer`, cri-tools `pkg/validate/container.go:508`),
   which CRI defines as "close stdin once all attached clients detach". We ignored
   the field, so the shell never saw EOF, never exited, and its stdout never
   closed. `SocketStdin.feed` even carried a comment asserting the opposite — that
   only an explicit `CloseStdin` should end a container's stdin. containerd's rule
   is `if opts.StdinOnce && !opts.Tty { close container stdin } else { drop this
   client's stdout/stderr subscribers }` (`internal/cri/io/container_io.go:182`);
   both halves are now ported, and the `else` branch also fixes a leak where every
   attach over a container's life left a dead subscriber in the fan-out.
2. **A container's exit did not close its attach sockets.** With (1) fixed the
   shell exited — `waitContainer` returned 5ms after the stdin close — but
   Containerization does not close a process's writers on exit, so the client's
   stdout socket stayed open and `pump_output` never saw EOF. `PodService.waitContainer`
   now calls `ContainerStdio.finish()`, which ends every attached client's streams.
   Safe against truncation because `LinuxProcess.wait` has already drained the IO
   relays before returning. The log subscriber is deliberately left for `stopPod`.

The spec now passes in ~7s. Worth keeping the shape of this in mind: a streaming
bug that only appears at *teardown* looks like a data-path bug, and manual testing
that never closes the stream will not find it.

## Not yet done

- End-to-end rusternetes-on-macOS bring-up: this crate is the CRI half; the
  kubelet also needs a macOS story for kube-proxy (iptables) and for
  `Memory`-medium `emptyDir` (`mount -t tmpfs`).
- `apple-cri` is not yet wired into CI; the harness requires a macOS runner with
  Apple's runtime installed.
