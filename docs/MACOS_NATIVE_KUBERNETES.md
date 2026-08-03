# Kubernetes natively on macOS — requirements, related work, project plan

Status: kickoff draft, 2026-08-02. Host baseline: macOS 26.5.1, Apple silicon,
`container` 1.2.0 / containerization 0.40.1.

The goal: run a Kubernetes node on macOS with **no Linux VM acting as the node** —
pods are the VMs, the kubelet is a native macOS process.

---

## 1. Direct answers to the questions asked

### 1.1 The upstream script is `hack/local-up-cluster.sh`

In the `kubernetes/kubernetes` repo. It is explicitly macOS-aware and already does
exactly the split the hypothesis assumes:

```bash
# hack/local-up-cluster.sh:143-149
case "$(uname -s)" in
Darwin)
  START_MODE=nokubelet,nokubeproxy
  ;;
Linux)
  START_MODE=all
  ;;
```

On Darwin it builds and starts **etcd + kube-apiserver + kube-controller-manager
+ kube-scheduler** natively and skips the node components. Forcing the node
components produces:

```
kubelet is not supported on macOS. Starting anyway.
kube-proxy is not supported on macOS. Please use https://sigs.k8s.io/kind.
```

Both call sites carry the same comment — this is an acknowledged upstream gap,
not an accident:

```
## TODO remove this check if/when kubelet is supported on darwin
```

(`hack/local-up-cluster.sh:1540` and `:1561`.)

So the control plane half of the hypothesis is not just possible, it is a
supported upstream configuration. `STORAGE_BACKEND` defaults to `etcd3`; etcd
runs fine on darwin/arm64.

### 1.2 "The kubelet is Linux-only because of cAdvisor" — true but understated

cAdvisor is one of roughly nineteen subsystems upstream stubs out for non-Linux.
In the pinned checkout (`v1.26.5` tree, `752b8875`):

```
pkg/kubelet/cadvisor/cadvisor_unsupported.go
pkg/kubelet/cm/container_manager_unsupported.go
pkg/kubelet/cm/cgroup_manager_unsupported.go
pkg/kubelet/cm/util/cgroups_unsupported.go
pkg/kubelet/cm/helpers_unsupported.go
pkg/kubelet/cm/internal_container_lifecycle_unsupported.go
pkg/kubelet/oom/oom_watcher_unsupported.go
pkg/kubelet/eviction/threshold_notifier_unsupported.go
pkg/kubelet/stats/pidlimit/pidlimit_unsupported.go
pkg/kubelet/kuberuntime/kuberuntime_sandbox_unsupported.go
pkg/kubelet/kuberuntime/kuberuntime_container_unsupported.go
pkg/kubelet/kuberuntime/helpers_unsupported.go
pkg/kubelet/watchdog/watchdog_unsupported.go
pkg/kubelet/lifecycle/features_unsupported.go
pkg/kubelet/allocation/features_unsupported.go
pkg/kubelet/config/file_unsupported.go
pkg/kubelet/util/util_unsupported.go
```

Upstream kubelet *compiles* on darwin; it is inert. The real dependency is the
cgroup/namespace model, not the metrics library. Porting it is a rewrite of the
container manager, not a cAdvisor swap — which is precisely the argument for the
rusternetes kubelet.

**The rusternetes kubelet is already partly portable.** It has real non-Linux
paths, not stubs — e.g. `crates/kubelet/src/eviction.rs:1053` implements
`get_pid_stats()` via `sysinfo` when `#[cfg(not(target_os = "linux"))]`, against
a `/proc`-based implementation on Linux. That is the right pattern and it is
already established.

### 1.3 "The rusternetes kubelet has no CRI" — out of date in this checkout

This working tree already contains:

| crate | what it is |
|---|---|
| `cri-proto` | generated CRI v1 types |
| `cri` | the kubelet's CRI **client** |
| `cri-server` | a reusable CRI **server harness** — backend traits, tonic plumbing, SPDY streaming (exec/attach/portforward), CRI log format, checkpoint store |
| `apple-cri` | a CRI server backed by Apple's `container` runtime |
| `bollard-cri` | a CRI server backed by the Docker API |
| `apple-containerization` | Rust client for Apple's `SandboxContext` guest protocol |
| `vmm-broker/` | Swift helper owning `VZVirtualMachine` (Virtualization.framework) |

Remotes present: `origin` = `calfonso/rusternetes`, `bbc` = `BrainbugCloud`,
`indy` = `indyjonesnl`. The CRI work is in the working tree on
`apple-cri-pod-semantics`, not upstreamed to `origin/main`.

### 1.4 containerd on macOS — this is the weak link in the hypothesis

**Recommendation: drop containerd from the design.**

- containerd's darwin runtime PR ([containerd#4526](https://github.com/containerd/containerd/pull/4526))
  was **closed as stale in July 2025**. Some plumbing merged separately
  (#5936), so parts of containerd build on darwin.
- The PR **explicitly excluded the CRI plugin**: *"grpc.v1.cri for darwin is not
  ported yet."* No CRI plugin means containerd cannot be a kubelet's runtime on
  macOS regardless of anything a shim does.
- The blocker that killed it is structural: **darwin has no bind mounts.** The
  darwin snapshotter faked them with symlinks (the author's own word: "bandaid").
  Consensus could not be reached on depending on macFUSE, and FSKit was judged
  unreliable across releases.
- The [macOS Containers](https://macoscontainers.org/) project (slonopotamus) does
  maintain containerd/buildkit forks — but for **native macOS** containers, not
  Linux ones. Wrong target.

So "containerd + a Kata-shaped shim" requires first porting the CRI plugin to
darwin *and* solving snapshotter bind mounts — two projects upstream already
abandoned — before any of our work starts.

**What replaces it:** implement CRI directly. That is what `cri-server` +
`apple-cri` already do, and `crictl`/`critest` talk to it with no containerd in
the picture. The kubelet does not care whether its CRI endpoint is containerd.

### 1.5 Can runwasi be reused? — Partly, and probably not worth it

Checked `crates/containerd-shim-wasm/Cargo.toml` on `main`:

```toml
[target.'cfg(unix)'.dependencies]
libcontainer = { workspace = true, features = ["libseccomp", "systemd", "v1", "v2"] }
nix         = { workspace = true, features = ["sched", "mount"] }
caps        = "0.5"
dbus        = { version = "0", features = ["vendored"] }
containerd-client = "0.8.0"
```

Three findings:

1. The Linux-only dependencies are gated on **`cfg(unix)`, which includes
   macOS.** `libcontainer` is youki's container runtime (cgroups v1/v2, seccomp,
   namespaces); `nix` with `sched`+`mount`; `caps`; `dbus`/systemd. These will be
   selected on darwin and fail to build. This is not a small patch — it is the
   part of runwasi that actually runs a container.
2. What is genuinely reusable is one layer down: **`containerd-shim` /
   `containerd-shimkit`** from [containerd/rust-extensions](https://github.com/containerd/rust-extensions)
   — the ttrpc task-service scaffolding. runwasi is a *consumer* of that, not the
   thing to fork.
3. Even fully ported, a shim v2 binary is only meaningful if containerd is there
   to exec it and route CRI to it. Per §1.4, it is not. **The reuse target is a
   layer that has no host.**

**We already have runwasi's structural equivalent, one layer up.**
`crates/cri-server` is the same idea — a reusable harness where a runtime author
implements three traits (`RuntimeBackend`, `ImageBackend`,
`streaming::StreamingBackend`) and gets ~40 RPCs, error conventions, streaming
and a checkpoint store for free. `apple-cri` and `bollard-cri` are two backends
on it. That is the investment to continue, not runwasi.

Worth borrowing from runwasi conceptually: its **"shared" mode** (one manager
process hosting many sandboxes) versus "normal" mode (process per pod) is the
same decision we face for the VMM broker's process model.

> **Final verdict: not reused.** There was a case for reusing its
> workload-detection logic *inside the guest* — the Linux-only dependencies are
> irrelevant there, because the guest is Linux. That case died with the
> mixed-runtime feature it existed to serve; see **§7**.

---

## 2. Where this project actually stands

The hypothesis reads as greenfield. It is not — most of it is built. Reframing
the kickoff around the true baseline:

### Done and verified

| | evidence |
|---|---|
| CRI server harness, runtime-agnostic | `cri-server`, two backends on it |
| CRI over Apple's `container` CLI | **critest 49/49 of the supported set**, 0 failed (`crates/apple-cri/STATUS.md`) |
| One-VM-per-pod is possible at all | `SandboxContext` process RPCs carry `containerID`; 36 pod-semantics tests against a real in-process gRPC server |
| The VZ entitlement is not a barrier | `com.apple.security.virtualization` carried by an **ad-hoc signature** (`codesign -s -`), no paid identity. Verified on macOS 26.5.1 |
| Host↔container reachability is a macOS *policy* gate, not a runtime limit | measured both ways; the Local Network grant flips it. **This also gates kubelet HTTP/TCP probes** |
| Broker VM lifecycle, vsock relay, hotplug, virtiofs registration | implemented in `vmm-broker/` |
| Image → ext4 rootfs and initfs | `ImageService.swift` (uncommitted) — was the largest open blocker in STATUS.md, now closed via `ContainerizationEXT4.EXT4Unpacker` |

### In flight, uncommitted — and it changed the architecture

`vmm-broker/Sources/rusternetes-vmm/PodService.swift` (untracked) records a
significant reversal from what `crates/apple-containerization/README.md` still
describes:

> This replaces a ~2400-line Rust port of LinuxPod + Vminitd + the OCI spec
> types. That port duplicated working Swift, did not receive Apple's fixes, and
> its first live boot failed on a `LinuxPod.create()` precondition it had not
> replicated.

Two consequences that need an explicit decision, not a silent drift:

1. **More logic moved into Swift.** The Rust/Swift seam is no longer "four VZ
   operations" — it is now pods, images and VM lifecycle. `apple-containerization`'s
   `pod.rs` port is superseded. The READMEs are stale and should be reconciled.
2. **Shared IPC was dropped.** Apple's `LinuxPod` gives every container a *fresh*
   `ipc` and `uts` namespace. The earlier Rust port deliberately diverged to make
   pod members join the infra namespaces. Calling upstream `LinuxPod` gives that
   up: pod-scoped hostname still works (same string, different namespaces), but
   **containers in a pod do not share System V IPC.** That is a real deviation
   from pod semantics whose conformance cost is unmeasured.

### Not started

- No pod has booted live. Everything above is tested against mocks or an
  in-process gRPC server.
- Stdio relay on the pod path: no logs, exec output, attach or port-forward.
- Pod networking / IPAM: `VMConfiguration.interfaces` is passed empty.
- virtiofs hotplug attach half (CRI mounts naming host paths would ENOENT).
- `pod_runtime` is not wired into `AppleBackend`; no flag selects pod vs CLI mode.
- kubelet-on-macOS bring-up; kube-proxy story; `Memory`-medium `emptyDir`.

---

## 3. Related work

### 3.1 The Linux-VM-as-node approaches (the incumbents to beat)

| project | model | note |
|---|---|---|
| [kiac](https://github.com/saiyam1814/kiac) | **one Apple VM per k8s node**, real Linux kubelet + cAdvisor inside | Closest competitor. Works *today*. metrics-server and MetalLB work precisely because there are real cgroups. On macOS 26 each node gets a routable IP |
| Lima / Colima | one Linux VM, k3s inside | `scripts/lima-conformance.sh` in this repo uses exactly this for conformance |
| Rancher Desktop, Docker Desktop, OrbStack | one Linux VM, k3s/k8s inside | |
| kind / k3d | Linux containers as nodes, needs a Linux VM underneath on macOS | upstream literally points macOS users here |
| minikube (vfkit/qemu) | VM per node | |

**kiac is the honest benchmark.** It reaches a working cluster with far less
novel engineering. This project must beat it on something specific — pod-level
VM granularity, memory footprint at N pods, boot latency, or being a real
Kubernetes *implementation* rather than a packaging of one. That claim should be
stated and then measured, not assumed.

That got sharper, not softer: the one capability kiac provably could not match —
mixed-runtime pods — was investigated and **dropped** as too expensive (§7).
**§7.5 states what is left, and none of it is self-evident.** The Phase 6
benchmark is now the only thing that settles the question.

### 3.2 The architectural template

| project | why relevant |
|---|---|
| [Kata Containers `runtime-rs`](https://github.com/kata-containers/kata-containers) | The exact shape being copied: shim v2 in **Rust**, one VM per **pod**, guest agent (`kata-agent`, Rust) over **ttrpc/vsock**, hypervisor abstraction layer. Apple's `vminitd` is the structural analogue of `kata-agent` — first process in the guest, gRPC over vsock. **The pattern ports; the plumbing (containerd + shim v2 on a Linux host) does not** |
| Kata sandbox resource accounting | Already solved "the VM overhead must be charged to the pod for scheduling" — we have the same problem (§4.6) |
| [runwasi](https://github.com/containerd/runwasi) | See §1.5. Reusable idea (backend-trait harness, shared vs per-pod process model); not reusable code |
| [containerd/rust-extensions](https://github.com/containerd/rust-extensions) | `containerd-shim`, `containerd-client`, ttrpc bindings — the layer runwasi sits on |

### 3.3 The macOS substrate

| project | note |
|---|---|
| [apple/containerization](https://github.com/apple/containerization) | Swift package. `vminitd` = Swift PID 1 in the guest, gRPC over vsock. `LinuxPod` is the multi-container-per-VM primitive — **experimental**, and its namespace policy is not Kubernetes' (§2) |
| [apple/container](https://github.com/apple/container) | The CLI. One microVM **per container**. macOS 26 + Apple silicon. This is the mismatch `apple-cri` documents: *Kubernetes' sandbox is a pod; Apple's sandbox is a container* |
| [containerd#4526](https://github.com/containerd/containerd/pull/4526) | darwin runtime, closed stale. No CRI plugin for darwin |
| [macOS Containers](https://macoscontainers.org/) | containerd/buildkit forks for *native macOS* containers. Not Linux workloads |

External commentary confirms nobody else is doing this: Apple Container "does not
currently offer a CRI endpoint, so it cannot serve as a backend for Kubernetes" —
and notes that if it did, k3s/kind could run on macOS without Lima. **That is the
gap this project fills.**

---

## 4. Unchecked assumptions and potential blockers

Ordered by how likely each is to end the project.

### 4.1 No pod has ever booted — everything is mock-verified 🔴

The single biggest unknown. `broker_pod.rs` drives the real protocol against a
mock VMM; the pod-semantics tests use an in-process `SandboxContext` server. The
one recorded attempt at a live boot **failed** on a `LinuxPod.create()`
precondition. Until a two-container pod runs with a shared pod IP and reachable
`localhost`, every downstream estimate is speculative.

**Retire this first. Nothing else on this list matters if it fails.**

### 4.2 Networking, and therefore Services 🔴

Three unsolved layers stacked:

- **Pod IP / IPAM** — `VMConfiguration.interfaces` is empty today. Undecided
  whether to drive Apple's network service or run our own IPAM. No CNI plugin
  binary executes on macOS, so `crates/kubelet/src/cni/` has no meaning here.
- **kube-proxy** — no iptables, no nftables, no netfilter on macOS. Services
  (ClusterIP, NodePort) have **no implementation path identified**. Options: a
  userspace proxy on the host; per-pod in-guest redirection; a proxy VM. All
  unexplored. `STATUS.md` lists this only as "not yet done", which understates it.
- **Local Network privacy grant** — already known to gate host→pod traffic, and
  therefore kubelet liveness/readiness probes. Fine interactively; a blocker for
  headless CI (§4.8).

Full Kubernetes conformance is unreachable without Services and DNS. **Node
conformance is the only realistic near-term target.**

### 4.3 VM density and pod sizing 🟡 (downgraded from 🔴 — see §8)

Investigated. The feared hard OS cap **does not apply to us**, and pod sizing has
an upstream mechanism we simply have not ported yet. Both are now measurement and
porting tasks rather than existential risks. Full detail in **§8**; the summary:

- **The famous 2-VM limit is macOS-*guest* only**, enforced in the kernel for
  `VZMacOSVirtualMachineConfiguration`. Linux guests are bounded by RAM and CPU,
  not policy — as `apple/container` (a VM per container) and kiac (a VM per node)
  both demonstrate in production. Still worth measuring the curve; no longer a
  kill criterion.
- **The flat 4 CPU / 1 GiB is fixable today, no protocol change.** CRI's
  `LinuxPodSandboxConfig` already carries `resources` ("the sum of container
  resources for this sandbox") and `overhead` at `RunPodSandbox`. The VM *can*
  be sized from the pod spec at create time.
- **Overcommit is partially available**: `VZMemoryBalloonDevice` exists, plus
  macOS host-side memory compression. Whether VZ backs guest RAM eagerly or
  lazily is the open empirical question and decides everything.

Residual risk is now concentrated in one unmeasured fact — eager vs lazy guest
memory backing (§8.3) — and in in-place pod resize (§8.4).

### 4.4 Pod semantics divergence — shared IPC dropped 🟡

See §2. Apple's `LinuxPod` gives each container fresh `ipc`/`uts`. Patching it
means maintaining a fork of Apple's Swift package — which `PodService.swift`
explicitly exists to avoid. The trade is real; it should be a recorded decision
with a measured conformance cost, not a comment in a source file.

### 4.5 Apple API stability 🟡

`STATUS.md` documents the 0.7.1 → 1.2.0 upgrade breaking **five** things, three
of them **silently** (JSON parsed into empty values rather than erroring). Plus a
live upstream bug: `container exec` signal forwarding is broken by an
`Int64`-written / `String`-read XPC field mismatch, unfixable from our side and
**not yet filed upstream**. And `LinuxPod` is explicitly experimental.

We are pinned to containerization `0.40.1`. Assumption to test: **that Apple's
guest protocol and pod primitive are stable enough to build a Kubernetes node
on.** Mitigation already in place — capture verbatim runtime output as fixtures
(`testdata/container-inspect-1.2.0.json`) rather than hand-writing them.

### 4.6 Node resource accounting and eviction 🟡

The kubelet must report node capacity/allocatable and run eviction. With a VM per
pod, memory is charged at **VM granularity** and reclaim does not behave like
cgroups. Kata solved this by charging VM overhead to the pod for scheduling —
port that mechanism rather than deriving one. Related: `Memory`-medium `emptyDir`
needs a tmpfs, which now means *in the guest*, not on the host.

### 4.7 Which control plane? — an unmade decision that changes what "passing" means 🟡

The hypothesis says "a k8s cp" (upstream Go, via `local-up-cluster.sh`) but the
repo is a Kubernetes *reimplementation* with its own control plane. Three
configurations, three meanings:

| config | what a green run proves |
|---|---|
| upstream CP + rusternetes kubelet + apple-cri | the **node stack** is correct — isolates the new work |
| rusternetes CP + rusternetes kubelet + apple-cri | the product; failures are ambiguous between CP and node |
| upstream CP + upstream kubelet | impossible (§1.2) |

**Recommend starting with upstream CP** precisely because it makes node failures
unambiguous — and because `local-up-cluster.sh` on Darwin already delivers it
with one command.

### 4.8 CI 🟡

`apple-cri` is not wired into CI; the harness needs a macOS 26 Apple-silicon
runner with Apple's runtime installed *and* the Local Network grant (§4.2), which
is an interactive TCC prompt. Assume manual verification for now and say so.

### 4.9 Smaller open items

- **Stdio relay** — the vsock port allocator exists; the pump does not, and the
  broker's `listen` is unimplemented. Blocks logs, exec, attach, port-forward, and
  container `stdin`. The kubelet needs all of these.
- **virtiofs hotplug attach** — `pod_runtime` emits the right mount shape; the
  attach half is missing, so host-path binds fail ENOENT.
- **Checkpointing pod state across a shim restart** — the CLI path has `state.rs`;
  the pod path has nothing.
- **Distribution** — ad-hoc signing verified *locally*. Whether the virtualization
  entitlement survives notarized distribution to other machines is unverified.
- **Hardware floor** — macOS 26 + Apple silicon. No Intel, no macOS 15.
- **Stale docs** — `apple-containerization/README.md` and `apple-cri/STATUS.md`
  describe the superseded Rust `LinuxPod` port and list `provisionRootfs` as the
  top blocker after `ImageService.swift` closed it.

---

## 5. Recommended architecture

Revised from the working hypothesis. One component removed, one boundary moved.

```
  macOS host                                    │  guest microVM (one per pod)
  ─────────────────────────────────────────────┼──────────────────────────────
  kube-apiserver / scheduler / controller-mgr  │
    (upstream Go via local-up-cluster.sh,      │
     or rusternetes — see §4.7)                │
              ▲                                 │
              │ kube API                        │
  rusternetes-kubelet   (native macOS process)  │
              │ CRI v1 (gRPC over unix socket)  │
  apple-cri  = cri-server harness + backend     │
              │ broker protocol (NDJSON/unix)   │
  vmm-broker (Swift, VZ entitlement)            │
    ├─ LinuxPod  (Apple's own) ──────────────── │ ──▶ vminitd (PID 1, Swift)
    │    createPod/addContainer/startContainer  │       SandboxContext gRPC :1024
    │    waitContainer/exec/waitProcess         │       │
    ├─ VZVirtualMachine lifecycle               │       ├─ ctr A → vmexec
    ├─ vsock dial + virtiofs share mutation     │       ├─ ctr B → vmexec
    ├─ ImageService: OCI image → rootfs dir     │       └─ ctr C → vmexec
    │    (initfs stays ext4: boot-time)         │
    ├─ ContainerLog: stdio → CRI log file       │       shared netns = one pod IP
    └─ VZMemoryBalloonDevice  (§8.3)            │       uts:  per-container (§7)
                                                │       ipc:  per-container (§7)
  ✗ containerd     — no CRI plugin on darwin    │
  ✗ shim v2        — no host to exec it         │
  ✗ runwasi        — libcontainer under cfg(unix)
```

Deltas from the hypothesis:

1. **containerd and shim v2 are removed.** Implement CRI directly (§1.4). The
   kubelet cannot tell the difference; it removes a dependency on two upstream
   projects that were abandoned.
2. **The Kata *pattern* is kept, the Kata *plumbing* is not.** One VM per pod, a
   guest agent over vsock, a thin host-side hypervisor owner — all preserved.
3. **The Rust/Swift seam moved up, and kept moving.** Not "four VZ operations" but
   pods, images, container stdio and VM lifecycle in Swift, calling Apple's own
   `LinuxPod`. Ratified and implemented; §7 is the price that came with it.

---

## 6. Project plan

Each phase ends in a **verifiable** artifact. Phases 0 and 1 are gates: if either
fails, the design changes rather than the schedule.

### Phase 0 — Retire the two project-ending unknowns (days, not weeks) 🔴

Do these before any further engineering.

1. **Boot one live pod (§4.1).** Two containers, one VM, via `ImageService` +
   `PodService` + broker. Assert by hand: one pod IP, containers reach each other
   on `localhost`, both survive the infra process.
2. **Run the density/overcommit experiment (§8.5)** — all five steps. The
   critical one is #2, configured memory vs actual host RSS for one idle VM: it
   decides eager-vs-lazy backing, and therefore pods-per-node, the sizing
   strategy, and whether in-place resize is possible at all.
3. **Decide and record**: the Rust/Swift seam (§2), shared-IPC divergence (§4.4),
   control plane choice (§4.7), and boot-at-sum vs boot-at-ceiling-and-balloon
   (§8.3/§8.4). Reconcile the stale READMEs.

**Kill criteria** — a live pod that cannot boot on `LinuxPod` without forking
Apple's package. (The old "the OS caps VMs in the single digits" criterion is
**retired**: that cap is macOS-guest-only and does not apply to Linux guests —
§8.3.) Fallback remains a VM-per-node model, i.e. kiac's (§3.1).

### Phase 1 — The pod path becomes real

Partly done (2026-08-02). The broker protocol is pod-shaped and both sides speak
it; `pod_runtime` is repointed onto it; the Rust `LinuxPod` port is retired.
Container stdio lands in the CRI log file, written by the broker as containerd
does it. Remaining:

- **Exec stdio, then attach and port-forward.** `ExecSync` reports a faithful exit
  code but empty stdout/stderr: container stdio has a destination, exec's does
  not. This is what the retired `listen` idea was for; the shape that replaced it
  (broker-owned `Writer`s) needs the same treatment for exec.
- virtiofs share for CRI mounts naming host paths.
- Wire `pod_runtime` into `AppleBackend` behind a flag; checkpoint pod state across restart.
- **Port `calculateSandboxResources` (§8.1)** and size the VM from
  `LinuxPodSandboxConfig.resources` + `overhead`, replacing the flat 4 CPU / 1 GiB.
  Add a floor for BestEffort pods. Apply the §8.3 boot strategy chosen in Phase 0.
- **Exit:** `critest` passes against the **pod** path, not just the CLI path.
  That is a directly comparable number against today's 49/49.

### Phase 1b — Mixed-runtime pods — **cancelled**

Was the headline differentiator; **dropped 2026-08-02** because it needs a fork of
Apple's `Containerization` package. See §7 for the full reasoning and for the
route if it is ever revisited. Consequence: Phase 6's kiac benchmark is now the
only place the project's remaining claims get tested.

### Phase 2 — Networking

- Pod IP + IPAM: drive Apple's network service or own it. Decide with a spike.
- DNS resolution into the pod.
- **Exit:** two pods on one host resolve and reach each other; `crictl` reports a
  stable pod IP.

### Phase 3 — Kubelet on macOS

- Run `hack/local-up-cluster.sh` on the Mac for the control plane (§1.1).
- Bring up `rusternetes-kubelet` natively against `apple-cri`; register the Node.
- Fill the `#[cfg(not(target_os = "linux"))]` gaps as they surface — the pattern
  already exists in `eviction.rs`.
- Node capacity/allocatable with VM-granular accounting. Declare `overhead` on the
  Apple-VM `RuntimeClass` and port upstream **Pod Overhead** (§8.2) — the same
  mechanism Kata uses — so the scheduler accounts for per-pod VM cost.
- **Exit:** `kubectl run` → Running pod → `kubectl logs` / `exec`, no Linux VM as node.

### Phase 4 — Services (the hard one)

- Design review first: kube-proxy has no macOS implementation path today (§4.2).
  Evaluate userspace proxy vs in-guest redirection vs proxy VM before coding.
- CoreDNS.
- **Exit:** a ClusterIP Service routes to two backend pods.

### Phase 5 — Conformance

- **Node conformance first** — the realistic target; it does not require Services.
- Then the conformance subset that Phases 2–4 make reachable. Do not target
  certified conformance until Services work.
- Wire a macOS runner into CI, or document manual verification honestly (§4.8).

### Phase 6 — Position and measure

- Benchmark against **kiac** (§3.1): memory at N pods, pod start latency, density.
- File the upstream `container exec` signal bug (§4.5).
- Upstream the CRI crates toward `origin/main`.

### Cross-cutting

- **Upstream-first** (CLAUDE.md): port from `../kubernetes`, Kata, and Apple's
  Swift rather than deriving. `PodService.swift` is the cautionary tale — a
  2400-line reinvention discarded for a call into the real thing.
- **Capture runtime output verbatim as fixtures.** Three of five 1.2.0 breakages
  failed silently; hand-written fixtures would have kept agreeing with the old
  model (§4.5).
- Keep `cri-server` free of `rusternetes-*` deps — it is the publishable artifact
  and the reason `bollard-cri` and `apple-cri` cost so little each.

---

## 7. Amendment A — mixed-runtime pods: investigated, **dropped**

**Status: closed 2026-08-02. Not pursued. The capability is real but it costs a
fork of Apple's `Containerization` package, and that price is not worth paying.**

This section originally argued mixed-runtime pods were the project's
differentiator and that "the plumbing already exists". **That was wrong**, and the
error is worth recording because it is the same trap twice.

### 7.1 What was actually true

The per-container runtime selector is real, and it is in the guest protocol:

```protobuf
// SandboxContext v3, CreateProcessRequest
optional string ociRuntimePath = 6;
```

It is per *process*, therefore per container. One VM really can run container A on
`vmexec` and container B on a wasm-capable runtime, sharing one `localhost` — and
a Linux node genuinely cannot match that, because there `RuntimeClass` is
pod-scoped precisely to avoid a loopback interface spanning a VM boundary.

### 7.2 Why it is unreachable at our layer

The claim rested on `apple-containerization`'s `pod.rs` carrying
`oci_runtime_path` per container. That was the **Rust port** of `LinuxPod`, which
talked to `SandboxContext` directly. The port has since been retired in favour of
calling Apple's real `LinuxPod` from the broker — and Apple's `LinuxPod`:

- has **no `ociRuntimePath`** on its `ContainerConfiguration`, and
- hardcodes `ociRuntimePath: nil` at **all three** of its `createProcess` call
  sites (`LinuxPod.swift:717`, `:965`, `:1212`).

So the selector exists one layer below what we now build on. Reaching it means
forking `Containerization` or dropping out of `LinuxPod` for the mixed case —
which reintroduces exactly the maintenance burden that retiring the Rust port
removed. **Decision: not worth it.**

### 7.3 The lesson

Both times, a capability was declared available on the strength of code *we* had
written against a lower-level protocol, without checking the API the design had
since moved to. The Rust `LinuxPod` port died the same way — it looked complete
until a live boot hit a precondition it had never replicated.

The check that would have caught both is cheap: **verify the capability against
the layer the design actually calls**, not against the protocol underneath it.

### 7.4 If it is ever revisited

Nothing here is lost, only deferred. The route is documented rather than
forgotten:

- `crun` or `youki`, built with WasmEdge, select per container from image
  annotations (`run.oci.handler: wasm`, `module.wasm.image/variant=compat`), and
  the `compat-smart` / `wasm-smart` variants exist for exactly the
  wasm-plus-sidecar case. That is upstream's mechanism; do not invent one.
- runwasi's `can_handle()` (Wasm header sniffing) is the detection logic to port.
- The runtime binary must live in the guest, so a **custom initfs** is needed.
  `ImageService` already unpacks an OCI image to ext4 and the initfs *is* an OCI
  image, so that part is cheap.
- The blocker is only the `LinuxPod` fork.

### 7.5 So what *is* the answer to "why not just use kiac"?

This was the differentiator, and dropping it leaves the question open — better
stated plainly than papered over. What remains:

- **Pod-granular isolation.** kiac's blast radius is a node; here it is a pod.
  Whether that matters depends on the audience.
- **Density and footprint**, if §8.3 measures favourably — a pod VM against a
  whole node VM.
- **It is a Kubernetes implementation, not a packaging of one.** kiac runs
  upstream's kubelet inside Linux VMs; this runs a Rust kubelet natively on macOS.
  For rusternetes that is the point, but it is a project goal rather than a user
  benefit.

None of these is as sharp as "a pod a Linux node cannot express". **Phase 6's
benchmark against kiac is now load-bearing** — it is the only place the remaining
claims get tested.

---

## 8. Amendment B — pod sizing, VM count, overcommit

### 9.1 Aligning VM size with pod size — upstream already computes this ✅

The concern was that the VM is created at `RunPodSandbox` before container
configs are known. **CRI already solves this.** `LinuxPodSandboxConfig` carries
both numbers at sandbox-create time:

```protobuf
// staging/src/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto:546-559
message LinuxPodSandboxConfig {
    …
    // Optional overhead represents the overheads associated with this sandbox
    LinuxContainerResources overhead   = 4;
    // Optional resources represents the sum of container resources for this sandbox
    LinuxContainerResources resources  = 5;
}
```

and upstream's kubelet fills them in (`kuberuntime_sandbox.go:157` →
`kuberuntime_sandbox_linux.go:48-78`):

```go
config.Linux.Resources = m.calculateSandboxResources(ctx, pod)   // PodRequests / PodLimits
config.Linux.Overhead  = m.convertOverheadToLinuxResources(pod)  // RuntimeClass overhead
```

`calculateSandboxResources` uses `resourcehelper.PodRequests` / `PodLimits` with
`ExcludeOverhead: true`, honouring the `PodLevelResources` feature gate.

**So the VM can be sized from the pod spec at creation. The flat 4 CPU / 1 GiB is
a missing port, not a protocol limitation.**

⚠️ **The catch, and it is ours to fix:** `applySandboxResources` lives in
`kuberuntime_sandbox_linux.go`. The non-Linux build
(`kuberuntime_sandbox_unsupported.go:28`) returns `nil` — another entry for the
§1.2 list. Our kubelet must **deliberately port** `calculateSandboxResources`;
it will not arrive for free just because we speak CRI.

Two caveats: BestEffort pods sum to zero, so a floor is still needed; and
`overhead` is only populated when a RuntimeClass declares it.

### 9.2 Pod Overhead is the upstream mechanism for "the VM itself costs RAM"

[Pod Overhead](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-overhead/)
exists for exactly this — it is what Kata uses. Declare `overhead` on the Apple-VM
`RuntimeClass` and the **scheduler** accounts for per-pod VM cost cluster-wide,
and the kubelet charges it to the pod cgroup. Port it; do not invent node-level
fudging. This closes §4.6 with a named upstream mechanism.

### 9.3 VM count and overcommit — the actual findings

**The 2-VM limit does not apply.** It is enforced in the kernel for **macOS
guests** (`VZMacOSVirtualMachineConfiguration`), to avoid contention on P-cores
and the interconnect. Linux guests are bounded by available RAM/CPU, not policy.
Empirical corroboration: `apple/container` runs **one VM per container** and
users run many; kiac runs a VM per node. Confidence: high. Measure the curve
anyway, but this is no longer a kill criterion.

**Overcommit — partially available, one fact still unmeasured.**

| lever | status |
|---|---|
| `VZMemoryBalloonDevice` / `memoryBalloonDevices` on `VZVirtualMachineConfiguration` | Exists in Virtualization.framework. Lets the guest **return** pages to the host. Not yet used by the broker |
| macOS host-side memory compression | Applies to VM-backing memory; free upside |
| **Eager vs lazy guest-RAM backing** | ⚠️ **UNMEASURED, and it decides everything.** If host RSS grows only as the guest touches pages, N pods × 1 GiB costs far less than N GiB and overcommit is effectively free. If VZ wires the full allocation at boot, it is not |

That last row is the single highest-value measurement in the project: create a VM
configured with 8 GiB, boot it idle, read host RSS. An afternoon's work that sets
the pods-per-node number.

**Design consequence if lazy:** invert the sizing strategy — **boot each pod VM at
a generous ceiling and balloon down**, rather than boot at the computed sum.
Costs nothing when lazy, and it is the only way to make §8.4 work.

### 9.4 In-place pod resize — the real constraint 🟡

`VZVirtualMachine` memory is **fixed at boot**; a balloon can only reclaim within
that ceiling, never exceed it. So in-place pod vertical scaling (KEP-1287,
`UpdateContainerResources`) can only ever grow a pod *up to* its boot ceiling.
This is the strongest argument for boot-high-and-balloon-down, and it should be
decided before the sizing code is written, not after.

### 9.5 Revised Phase 0 experiment

Replaces the single "loop until it fails" test:

1. Boot N idle Linux pod VMs, N = 1…64. Record host RSS, wired memory, boot
   latency, and where throughput degrades. → the pods-per-node number.
2. **Configured memory vs actual host RSS for one idle VM** (§8.3). → eager or
   lazy; decides the whole sizing strategy.
3. Attach a `VZMemoryBalloonDevice`; inflate/deflate; measure how promptly host
   RSS responds. → is balloon-down viable.
4. Compare boot-at-sum vs boot-at-ceiling-and-balloon-down on (1).
5. Port `calculateSandboxResources` and wire `LinuxPodSandboxConfig.resources`
   into VM creation, replacing the flat 4 CPU / 1 GiB.

---

## 9. The one-paragraph version

Running the Kubernetes control plane on macOS is solved — `hack/local-up-cluster.sh`
does it, and defaults to skipping the node components on Darwin with a standing
`TODO remove this check if/when kubelet is supported on darwin`. The node half is
the project. The kubelet's Linux dependency is the cgroup/namespace model, not
cAdvisor, so porting upstream's is a rewrite; the rusternetes kubelet already has
non-Linux code paths. containerd should be dropped from the design: its darwin
port is closed, its CRI plugin was never ported, and it foundered on darwin's lack
of bind mounts — so implementing CRI directly (already done, `cri-server` +
`apple-cri`, critest 49/49) is both shorter and lower risk. runwasi is not
reusable *on the host* — `containerd-shim-wasm` pulls `libcontainer`/`nix`/`caps`/`dbus`
under `cfg(unix)`, which selects on macOS and will not build, and a shim v2 binary
needs a containerd that isn't there; its backend-trait harness is the reusable
idea, and `cri-server` already is one. Keep Kata's *pattern* (VM per pod, guest
agent over vsock, thin hypervisor owner) and discard its plumbing. Mixed-runtime
pods — a wasm container beside a classic one, sharing one `localhost`, which a
Linux node structurally cannot express — looked like the differentiator and
**turned out to cost a fork of Apple's `Containerization`**, so it is dropped
(§7); that leaves the kiac benchmark carrying the whole "why not just use kiac"
argument. Pod sizing, by contrast, is a missing port rather than a limitation —
CRI already hands `RunPodSandbox` the summed pod resources and RuntimeClass
overhead, so the flat 4 CPU / 1 GiB is fixable today. The feared 2-VM cap turns
out to be macOS-*guest*-only and does not apply. The work is far along — the pod
protocol is implemented on both sides, the Rust `LinuxPod` port retired in favour
of Apple's own, container logs land in the CRI log file, and the VZ entitlement
works with ad-hoc signing — but **no pod has booted live**, exec/attach/port-forward
have no stdio yet, Services have no macOS implementation path at all, and one
number nobody has measured (does VZ back guest RAM eagerly or lazily?) decides
pods-per-node, the sizing strategy and whether in-place resize is possible. That
measurement is an afternoon and comes first; kiac already ships a VM-per-*node*
cluster today, which is the fallback if this fails and the benchmark if it
doesn't.

---

## Sources

- [hack/local-up-cluster.sh](https://github.com/kubernetes/kubernetes/blob/master/hack/local-up-cluster.sh) (read locally at `../kubernetes`)
- [containerd#4526 — darwin runtime support](https://github.com/containerd/containerd/pull/4526)
- [containerd/runwasi](https://github.com/containerd/runwasi) · [containerd-shim-wasm](https://crates.io/crates/containerd-shim-wasm) · [runwasi.dev](https://runwasi.dev/)
- [containerd/rust-extensions](https://github.com/containerd/rust-extensions)
- [kata-containers](https://github.com/kata-containers/kata-containers) · [architecture docs](https://kata-containers.github.io/kata-containers/design/architecture/) · [runtime-rs](https://deepwiki.com/kata-containers/kata-containers/2.3-runtime-rs-(rust-implementation))
- [saiyam1814/kiac](https://github.com/saiyam1814/kiac) · [introducing kiac](https://blog.kubesimplify.com/introducing-kiac-kubernetes-in-apple-containers)
- [apple/containerization](https://github.com/apple/containerization) · [apple/container](https://github.com/apple/container)
- [macOS Containers](https://macoscontainers.org/) · [The Rise of Native Containerization](https://earthly.dev/blog/macOS-native-containers/)
- [Apple container, Docker, and Kubernetes: what actually starts a container](https://walliz.cc/en/articles/apple-container-docker-kubernetes-runtime)
- Amendment A: [crun — wasm-wasi on Kubernetes](https://github.com/containers/crun/blob/main/docs/wasm-wasi-on-kubernetes.md) · [WasmEdge: deploy with crun](https://wasmedge.org/docs/develop/deploy/oci-runtime/crun/) / [with youki](https://wasmedge.org/docs/develop/deploy/oci-runtime/youki/) · [runwasi architecture](https://runwasi.dev/developer/architecture.html) · [Kubernetes RuntimeClass](https://kubernetes.io/docs/concepts/containers/runtime-class/) · [WebAssembly on Kubernetes, part 02](https://www.cncf.io/blog/2024/03/28/webassembly-on-kubernetes-the-practice-guide-part-02/)
- Amendment B: [Pod Overhead](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-overhead/) · [VZMemoryBalloonDevice](https://developer.apple.com/documentation/virtualization/vzmemoryballoondevice) · [memoryBalloonDevices](https://developer.apple.com/documentation/virtualization/vzvirtualmachineconfiguration/memoryballoondevices) · [How Apple limits VMs](https://eclecticlight.co/2022/08/04/virtualisation-on-apple-silicon-macs-8-how-apple-limits-vms/) · [Beating the 2 VM limit](https://khronokernel.com/macos/2023/08/08/AS-VM.html) · upstream `kuberuntime_sandbox_linux.go`, `cri-api/.../v1/api.proto`
- In-tree: `crates/apple-cri/STATUS.md`, `crates/apple-cri/README.md`, `crates/apple-containerization/README.md`, `crates/cri-server/README.md`, `vmm-broker/README.md`, `vmm-broker/Sources/rusternetes-vmm/PodService.swift`
