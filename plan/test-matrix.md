# Test matrix — CRI redesign end-to-end verification

Companion to [02-K7](02-kubelet-cri-only.md) and [03-B6](03-bollard-cri.md).
Defines the configurations under which the CRI-only kubelet + CRI shims are
exercised, at two test levels, and picks the execution order.

## Two test levels

- **L1 — CRI conformance (`critest` v1.36.0).** Exercises a single CRI
  runtime/shim in isolation over its UDS. No cluster, no control plane. Fast.
  This is the gate the `cri-server`/`bollard-cri` plans already use.
- **L2 — Kubernetes E2E conformance (`sonobuoy` certified-conformance).**
  Exercises the *whole* rusternetes cluster — CRI-only kubelet + api-server +
  scheduler + controller-manager + kube-proxy — against a real runtime. This is
  the **≥90 % pass-rate gate** from K7 (prior baseline 63.6 %).

L1 proves a runtime backend is correct; L2 proves the kubelet's CRI *client* +
control plane are correct against that backend. A config is "done" only when
both are green.

## Configurations

| # | OS / host | Kubelet → runtime path | Engine | L1 critest | L2 sonobuoy (≥90 %) | Bringup prerequisite |
|---|-----------|------------------------|--------|-----------|---------------------|----------------------|
| **1** | Linux (lima `default`, aarch64) | native CRI | **containerd v2.1.3** | golden baseline (reference) | **K7 target — FIRST RUN** | all-in-one binary, containerd sock |
| **2** | Linux (lima) | **bollard-cri** shim | Docker 28.2.2 | ✅ 96 pass / 0 fail / 26 skip (B5) | pending | bollard-cri process + docker sock |
| **3** | Linux (lima) | **bollard-cri** shim | **Podman** | pending → `PODMAN.md` | pending | install podman + bollard-cri |
| **4** | macOS | **bollard-cri** shim | **Podman Desktop** (podman machine) | pending | pending | **B6** compose sidecars |
| **5** | macOS *(bonus)* | **bollard-cri** shim | **Docker Desktop** | pending | pending | **B6** + `docker.sock` |

Note: "docker cri" in the request = the `bollard-cri` shim backed by the Docker
Engine (config 2 on Linux, config 5 on macOS). There is no separate cri-dockerd
in this project — `bollard-cri` *is* our Docker/Podman CRI shim, engine-agnostic
over the Docker Engine API.

## Execution order & rationale

1. **Config 1 (Linux/containerd)** first. It is the K7 target and the cleanest
   isolation of "is the kubelet's CRI client correct?" — containerd is a mature
   runtime, so any failure is the kubelet's or control plane's, not a shim's.
   The kubelet's K2–K6 code is compile-complete but has **never been run against
   a live runtime**, so this run is also the first true smoke test of the CRI
   cutover. → **We stop here to check the 90 % baseline (per request).**
2. **Config 2 (Linux/bollard-cri+Docker)**. bollard-cri is already L1-green, so
   this validates the *same* kubelet against our shim; divergences vs config 1
   isolate shim bugs from kubelet bugs (the plan-06 "which side owns the bug?"
   principle).
3. **Config 3 (Linux/bollard-cri+Podman)**. Podman is Docker-API-compatible but
   diverges (stats fields, exec inspect, log framing). Every divergence lands in
   `crates/bollard-cri/PODMAN.md` (still TODO) rather than being papered over.
4. **Config 4 (macOS/Podman Desktop)**. Restores the pre-cutover macOS compose
   workflow — **blocked on B6** (compose must supervise a bollard-cri sidecar
   inside each kubelet node container; the kubelet then dials
   `--container-runtime-endpoint unix:///run/bollard-cri.sock`).
5. **Config 5 (macOS/Docker Desktop, bonus).** See below.

## Bonus: can "docker cri" work on macOS?

**Yes, in principle — with the same architecture as Podman-on-mac, gated on B6.**

- `bollard-cri` speaks the Docker Engine API and is engine-agnostic; Docker
  Desktop exposes `/var/run/docker.sock` just like the podman machine does.
- The hard constraint on macOS: containers run inside a Linux VM (Docker
  Desktop's LinuxKit VM, or the podman machine VM). `bollard-cri` **must run
  inside that VM** (or a privileged container with the socket), not on the mac
  host, because it needs to (a) `setns` into the sandbox netns for
  `PortForward` `dial_in_sandbox`, and (b) share the CRI log directory
  (`/var/log/pods/...`) with the kubelet. A host-side bollard-cri talking a
  forwarded socket can do neither. This is exactly what B6's sidecar model
  provides.
- Pod IP comes from the Docker bridge network (same as today's compose), so
  kube-proxy semantics are unchanged.
- Caveats: Docker Desktop org licensing; socket path/permissions; and it shares
  B6's "no host iptables in the all-in-one container → kube-proxy disabled →
  Service tests degrade" limitation unless run in the compose node-container
  topology.

Bottom line: configs 4 and 5 are the *same* work — land B6 once and both the
podman and docker engines on macOS are reachable through `bollard-cri`.

## Config 1 first-run results (2026-07-19, containerd v2.1.3, lima `default`)

All-in-one `rusternetes` binary (native VM build) → containerd. **The CRI cutover
core works; two blockers stop it short of the 90 % gate — so a full sonobuoy run
was not started (it would predictably land well below 90 %).**

| Check | Result |
|-------|--------|
| Node registers `Ready` | ✅ |
| Pod scheduled → `RunPodSandbox` → image pull → `Running` | ✅ (real CNI pod IP 10.88.0.3) |
| `kubectl logs` / `logs -f` (K4, ~53-spec class) | ✅ real stdout; on-disk CRI log format correct |
| `kubectl exec` (K5, ~17-spec class) | ✅ **FIXED** — stdout/stderr/exit-code/stdin all work; 20/20 loop, zero `close 1005` (rewrote api-server SPDY client onto `cri-server` codec) |
| `kubectl port-forward` (K5) | ⚠️ ported to the same codec, not yet runtime-verified |
| kube-proxy — node egress | ✅ **FIXED** — removed the over-broad `--src-type LOCAL` MASQUERADE; DNS/egress survive with kube-proxy on |
| kube-proxy — ClusterIP DNAT | ✅ wget through a Service ClusterIP reaches the backend |

**Verdict: the two big failure classes and the networking blocker are cleared.**
K4 (logs, ~53 specs), K5 (exec, ~17 specs), and the kube-proxy blackhole are all
fixed and runtime-verified on containerd. A full sonobuoy conformance run is now
unblocked — that is the next step to actually measure the ≥90 % gate. (Known
lower-priority follow-ups before/after: ClusterIP allocator handing `10.96.0.1`
to normal Services, and port-forward verification — see cleanup-tasks.md.)

Environment prep needed on a bare VM (not code bugs): install CNI plugins +
`/etc/cni/net.d` (containerd `RunPodSandbox` fails "cni plugin not initialized"
without it); upgrade VM `protoc` to ≥ 22 (CRI proto uses `debug_redact`).

## L2 pass/fail gate (all configs)

- **Gate:** sonobuoy certified-conformance pass rate **≥ 90 %** (baseline 63.6 %).
- **Hard requirement:** **zero** failures attributable to synthetic pod logs or
  websocket `close 1005` (the two classes K4/K5 fix by construction).
- Archive each run's JUnit/e2e.log to `plan/results/<config>/<date>-<sha>` and
  diff against the prior run (`diff-junit.sh` per plan 06 — script still TODO).
