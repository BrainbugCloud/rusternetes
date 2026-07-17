# Plan 06 — Testing strategy, critest harness, and environments

Answers the three cross-cutting questions — how each piece is tested, whether
containerd is the reference implementation, containerized vs Linux-VM
development — and defines the shared harness the per-CRI plans reference.

## The critest harness

`critest` (kubernetes-sigs/cri-tools) is the conformance oracle for **every**
CRI server we build, and `crictl` is the manual debugging tool. Version is
pinned to **v1.36.0** — the same release train as the vendored proto
([01](01-cri-crates.md), decision D4). Bump proto + cri-tools together, never
separately (aurae's Makefile documents the same lockstep rule).

Adapt aurae's `hack/` scripts (its `cri-conformance.sh` /
`build-and-critest-loop.sh` pattern: version-pinned download, JUnit output,
pass/fail/skip parsing, `GINKGO_FOCUS` passthrough, **no hardcoded skips**)
into this repo:

```
scripts/critest/
  fetch-cri-tools.sh      # pinned download, linux-{amd64,arm64} + darwin-arm64
  run.sh                  # generic: --runtime-endpoint <sock> [--focus <expr>] → JUnit
  run-containerd.sh       # reference baseline (lima VM)
  run-bollard-cri.sh      # starts bollard-cri against the local docker/podman socket, runs critest
  run-apple-cri.sh        # macOS native
  diff-junit.sh           # compare two JUnit files: newly-failing / newly-passing / still-failing
```

Rules:

- Full suite by default; `--ginkgo.focus` only as an explicit argument
  (stage-gating in plans 01/03/04). Never encode skips in the scripts — skips
  live in per-runtime matrices (`crates/apple-cri/CRITEST-MATRIX.md`,
  `crates/bollard-cri/PODMAN.md`) as *documentation*, and `diff-junit.sh`
  compares against them.
- Every run archives JUnit to `plan/results/<runtime>/<date>-<git-sha>.xml`
  (git-lfs or plain — they're small). Regression = `diff-junit.sh` against the
  previous archived run; CI fails on newly-failing tests.

## Is containerd a good reference implementation? Yes — twofold

1. **Behavioral oracle.** Before any of our shims exist, run critest against
   containerd (lima VM below) and archive its JUnit as the golden baseline:
   what a mature runtime passes/skips is the ground truth for "what does this
   test actually require". When one of our shims fails a test ambiguously,
   interrogate containerd with `crictl` ("what does `ContainerStatus` return
   for a just-created container?", "what does `PodSandboxStatus.network` look
   like for host-network?") instead of guessing from the spec — the spec is
   the proto comments; containerd is the reference reading of them.
2. **First target for the kubelet's CRI client.** Plan
   [02](02-kubelet-cri-only.md) stages K2–K7 run against containerd before
   bollard-cri exists. This decouples "is the kubelet's CRI client correct?"
   from "is our shim correct?" — when the kubelet later misbehaves against
   bollard-cri, containerd tells you which side owns the bug.

Do **not** use containerd as a code reference for the shims (it's Go and
architecturally a full runtime); cri-dockerd is the code blueprint for
bollard-cri ([03](03-bollard-cri.md)).

## Containerized or Linux (lima) VM? Both — by component

| Component | Environment | Why |
|---|---|---|
| `cri-proto`, `cri-server` unit tests | anywhere (pure Rust) | UDS + in-process fakes, no runtime needed |
| containerd reference + kubelet inner loop (K2–K6) | **lima VM** | Real systemd/cgroups/netns/containerd; the kubelet needs a real Linux node. Config checked in as `lima/rusternetes-dev.yaml` |
| bollard-cri dev + critest | **containerized is fine** | It only needs a Docker socket. Linux CI: GitHub Actions runner has dockerd → this is the **CI conformance gate** (critest in a job, no VM). Locally on macOS: run inside the podman machine VM (`podman machine ssh`) or the lima VM |
| Cluster conformance on macOS (post-cutover) | podman compose + bollard-cri sidecars (plan 03 B6) | Restores today's workflow. Alternative — kind-style node images with nested containerd in privileged containers — is viable (kind proves it) but a bigger compose/Dockerfile rework; adopt only if bollard-cri's Podman divergences block conformance |
| aurae (as a kubelet target) | **lima VM only** | Root, Linux, musl build; its own repo's harness applies |
| apple-cri | **native macOS only** | Virtualization.framework; critest darwin-arm64 exists since cri-tools v1.29. Manual/self-hosted CI job |

### `lima/rusternetes-dev.yaml` (to be checked in during 01-S1)

Ubuntu LTS aarch64/amd64; provisions containerd + CNI plugins +
`crictl`/`critest` v1.36.0 + Rust toolchain + protoc deps; mounts the repo
(writable) so the inner loop is
`limactl shell rusternetes-dev -- cargo test -p rusternetes-kubelet` /
`cargo run --bin kubelet -- --container-runtime-endpoint unix:///run/containerd/containerd.sock`.
Smoke check after `limactl start`: `sudo crictl info` reports ready, and
`scripts/critest/run-containerd.sh` produces the golden baseline JUnit.

## Test pyramid per deliverable

| Layer | cri-proto/cri-server | kubelet | bollard-cri | apple-cri |
|---|---|---|---|---|
| Unit (`cargo test`, fakes) | lifecycle vs `MemoryBackend`, logfmt, SPDY framing, checkpoint | runtime layer vs a `cri-server` `MemoryBackend`-served socket (in-process UDS — the fake becomes the kubelet's test double, replacing bollard-era mocks) | mapping/naming/label converters | CLI JSON fixtures, `FakeBackend` contract |
| Runtime integration | crictl vs `memory-cri` example | pod lifecycle vs containerd in lima | crictl vs docker | crictl on macOS |
| Conformance | critest focus subsets (01-S4) | sonobuoy e2e (02-K7) | **full critest = 0 fail** (03-B5) | critest + tracked matrix (04-A4) |
| Regression | JUnit diff in CI | conformance diff per run | JUnit diff in CI | JUnit diff, manual job |

## Traceability: conformance failure analysis → fix

From the analyzed podman/mac run (157 passed / 90 failed at ~247/441 specs):

| Failure class | ~Specs | Root cause | Fixed by |
|---|---|---|---|
| Synthetic pod logs | ~53 | api-server falls back to `generate_pod_logs` stub when its direct-bollard log fetch fails | [02](02-kubelet-cri-only.md) K4: CRI log files + kubelet `/containerLogs` + api-server proxy; stub deleted |
| Exec/attach websocket `close 1005` | ~17 | ad-hoc exec path, no close handshake | [02](02-kubelet-cri-only.md) K5: CRI streaming + websocket⇄SPDY proxy with proper close frames |
| Node-proxy / apiservices (`nodes "node-1:10250" not found`, `/configz`, missing `AuthContext`, sonobuoy retrieve) | cross-cutting | node proxy routes unwired | [02](02-kubelet-cri-only.md) K4 |
| CRD strict-decoding false positives | 4 | re-serialize-and-diff validation fragility | **not this redesign** — tracked separately (api-server validation work) |
| CRD OpenAPI publishing | 5 | incomplete discovery/publishing | not this redesign |
| Scheduling (preemption/predicates) | 4 | scheduler gaps | not this redesign |
| Networking long tail | ~5 | kube-proxy/CNI-fallback interactions | partially: CRI runtimes own sandbox networking; re-baseline after K7 before deciding |
| Misc singles | 4 | various | re-baseline after K7 |

Expected outcome of [02](02-kubelet-cri-only.md) K7 alone: ~64% → ≥90%. The
"not this redesign" rows are the follow-up backlog and must be re-confirmed
from the K7 JUnit diff (some may pass incidentally, some may be
Podman-environmental).

## CI summary (target state)

- **Every PR (Linux):** fmt, clippy, workspace tests, dependency-rule check
  (`cargo tree` on `cri-proto`/`cri-server`), `cargo check -p apple-cri`
  (trait/fixtures compile), critest focus subsets vs `memory-cri`.
- **Merge to main (Linux):** full critest vs bollard-cri (Docker), JUnit diff
  vs last archive.
- **Nightly:** sonobuoy conformance (containerd cluster), archived +
  diffed — builds on the existing `scripts/run-conformance.sh` /
  `scripts/conformance-progress.sh` flow.
- **Manual/self-hosted (macOS):** apple-cri critest matrix + backend contract
  suite (mandatory for PRs touching `crates/apple-cri/src/backend/`).
