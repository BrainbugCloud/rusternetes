# Plan 02 — Kubelet: hard cutover to CRI-only

Rewrite the kubelet's runtime layer onto the CRI client from
[`cri-proto`](01-cri-crates.md) and **delete bollard from the kubelet crate**.
No fallback period (decision D3): until [bollard-cri](03-bollard-cri.md) is
ready, the kubelet is developed and tested against **containerd in a lima VM**
([06](06-testing-and-environments.md)).

## Status

- [ ] K1 — inventory + scaffolding (no behavior change)
- [ ] K2 — sandbox + container lifecycle on CRI
- [ ] K3 — statuses, init/ephemeral containers, GC
- [ ] K4 — logs pipeline end to end
- [ ] K5 — exec / attach / portforward streaming
- [ ] K6 — stats, eviction, bollard removal
- [ ] K7 — conformance re-baseline

## Current state (from coupling analysis)

- `crates/kubelet/src/runtime.rs` (~11.6k lines, 59% of the crate) holds a
  concrete `ContainerRuntime` struct with a `docker: bollard::Docker` field;
  bollard types are used directly, no trait exists.
- Two call sites bypass `ContainerRuntime` with their own Docker connections:
  the HTTP exec endpoint in `crates/kubelet/src/main.rs` (~line 269,
  `handle_exec`) and stats in `crates/kubelet/src/eviction.rs` (~line 599,
  `get_container_stats`). **Both must be routed through the new layer.**
- `kubelet.rs`, `config.rs`, and the whole `cni/` subtree are bollard-free and
  consume `ContainerRuntime` through ~20 high-level methods (`start_pod`,
  `stop_pod_for`, `get_container_statuses`, `get_init_container_statuses`,
  `get_pod_ip`, `check_liveness`, volume ops, `collect_node_metrics`,
  `update_container_resources`, `garbage_collect_containers`, …).
- Pods are already sandbox-modeled: a `{pod}_pause` container owns the pod's
  namespaces; app containers join via `network_mode/ipc_mode/pid_mode/uts_mode:
  "container:{pod}_pause"`; pod IP comes from inspecting the pause container
  (or CNI). Restart/backoff is kubelet-owned (never delegated to Docker) —
  already CRI-aligned.

## Design

### The seam stays; the internals change

Keep the ~20-method public surface of `ContainerRuntime` that `kubelet.rs`
consumes (semantics unchanged as far as callers observe), and rewrite the
internals onto two clients from `cri-proto`:

```rust
pub struct ContainerRuntime {
    runtime: RuntimeServiceClient<Channel>,   // cloneable; tonic channels are cheap to clone
    image:   ImageServiceClient<Channel>,
    // pod-name → sandbox-id and (pod, container-name) → container-id caches,
    // rebuilt on startup from ListPodSandbox/ListContainers metadata
    state: RuntimeState,
    ...
}
```

Config: new kubelet flags/env `--container-runtime-endpoint` and
`--image-service-endpoint` (default: same endpoint), parsed in
`crates/kubelet/src/config.rs`. Standard kubelet naming — keeps a future
switch to containerd/CRI-O/aurae a config change.

### Mapping table: bollard call → CRI call

| Today (bollard) | CRI-only |
|---|---|
| `start_pause_container` (busybox `sleep infinity`, `{pod}_pause`) | `RunPodSandbox(PodSandboxConfig)` — pause is the runtime's business |
| `create_container` + `network_mode/ipc_mode/pid_mode/uts_mode: "container:{pod}_pause"` | `CreateContainer(sandbox_id, ContainerConfig, sandbox_config)` — namespace joining is implied by the sandbox |
| `network_mode: "ns:{netns}"` (CNI mode) | Sandbox netns is runtime-owned; kubelet-driven CNI is retired on the CRI path (see "CNI" below) |
| `start_container` / `stop_container` / `remove_container` | `StartContainer` / `StopContainer(timeout)` / `RemoveContainer` |
| Name-prefix bookkeeping: `{pod}_pause`, `{pod}_{container}`, `list_containers(name="{pod}_")` | CRI IDs + `LabelSelector`/`PodSandboxMetadata`/`ContainerMetadata` filters (`ListPodSandbox`, `ListContainers` with filter) |
| `inspect_container` (~22 call sites) for status | `ContainerStatus` / `PodSandboxStatus`; bollard `ContainerStateStatusEnum` comparisons → CRI `ContainerState::{Created,Running,Exited,Unknown}` |
| Pod IP via pause-container `network_settings.networks` | `PodSandboxStatus.network.ip` (+ `additional_ips`) |
| `inspect_image` + `create_image` (pull, `docker.io/library` normalization) | `ImageStatus` + `PullImage` (keep image-ref normalization — CRI expects fully-qualified refs too) |
| `docker.logs` / `LogsOptions` | Read CRI log files via `cri-server::logfmt::CriLogReader` (see Logs below) |
| `create_exec`/`start_exec`/`inspect_exec` (probes, lifecycle hooks) | `ExecSync(container_id, cmd, timeout)` |
| exec endpoint in `main.rs` | `Exec` RPC → proxy the runtime streaming URL (see Streaming below) |
| `docker.stats` in `runtime.rs` + `eviction.rs` | `ContainerStats` / `ListContainerStats`; eviction reads the same client (no second connection) |
| `update_container` (in-place resize) | `UpdateContainerResources` |
| `download_from_container` | No CRI equivalent. Replace call sites: if used for termination-message/logs, read from the CRI log file or the kubelet-owned host path; enumerate remaining uses in K1 and design per-site |
| `create_volume`/`list_volumes`/`remove_volume` | Delete — CRI has no volume API; volumes are kubelet-side bind mounts (already mostly true) |
| `docker://{id}` in reported `ContainerStatus.containerID` | `{runtime_name}://{id}` with `runtime_name` from `Version()` (containerd → `containerd://…`) |

### Logs pipeline (kills ~53 conformance failures)

Root cause today: the api-server fetches logs via bollard against the runtime
socket and **falls back to a synthetic generator on any error**
(`crates/api-server/src/handlers/pod_subresources.rs`: `get_logs` →
`get_container_logs` → fallback `generate_pod_logs`). The fallback is
effectively always taken.

Target (upstream model):

1. Kubelet passes `log_directory` in `PodSandboxConfig`
   (`/var/log/pods/{ns}_{pod}_{uid}/`) and `log_path` per container
   (`{container}/{restart_count}.log`) in `ContainerConfig`. The runtime writes
   CRI-format log files there.
2. Kubelet HTTP API serves
   `GET /containerLogs/{namespace}/{pod}/{container}?follow&tailLines&sinceTime&timestamps&previous`
   by reading those files with `CriLogReader` (follow = file watch; `previous`
   = the `restart_count - 1` file).
3. The api-server pod-`log` subresource becomes a **pure proxy** to the
   kubelet (it already knows the node's kubelet address for status traffic).
   **Delete `generate_pod_logs` and the fallback entirely** — an error must
   surface as an error, never as success-shaped fake data.
4. Wire the node-proxy routes `/api/v1/nodes/{name}/proxy/...` to the same
   kubelet client (fixes the `nodes "node-1:10250" not found` and `/configz`
   failures, and un-breaks `sonobuoy retrieve`). Add the missing `AuthContext`
   layer on the `apiregistration.k8s.io` list route while in there (documented
   in the failure analysis as a one-line middleware omission).

### Exec / attach / portforward streaming (kills ~17 failures)

Root cause today: the kubelet's ad-hoc HTTP exec endpoint plus api-server
websocket handling closes streams without a close handshake
(`websocket: close 1005`).

Target:

1. Kubelet calls CRI `Exec`/`Attach`/`PortForward` → gets the runtime's
   streaming-server URL (single-use token).
2. Kubelet serves `POST /exec/{ns}/{pod}/{container}` (+ attach/portforward)
   by dialing that URL and proxying — kubelet⇄runtime is SPDY
   (`v4.channel.k8s.io`), reusing the client side of the protocol
   implementation from `cri-server::streaming` (export the SPDY client pieces
   from the crate for this purpose).
3. The api-server pod-`exec`/`attach`/`portforward` subresources proxy
   kubectl's websocket to the kubelet, translating websocket channels ⇄ SPDY
   frames with **correct close semantics** (send a proper close frame with
   status; propagate the exec exit code on the error channel per
   `v4.channel.k8s.io`).
4. `main.rs`'s direct-bollard `handle_exec` is deleted.

### Probes, lifecycle hooks

`check_liveness`/readiness/startup exec probes and `postStart`/`preStop` exec
hooks go through `ExecSync` with the probe timeout. HTTP/TCP probes are
unaffected (kubelet-side, use the pod IP).

### CNI

Today the kubelet drives CNI itself (`crates/kubelet/src/cni/`). Under CRI the
**runtime** owns sandbox networking (containerd runs CNI itself; our shims do
their own thing per their plans). On the CRI path the kubelet stops invoking
CNI for pod sandboxes and trusts `PodSandboxStatus.network.ip`.
`KUBELET_VOLUMES_PATH`-style host conventions stay. The `cni/` subtree is
**not deleted** in this plan — it is unused on the CRI path and its removal is
a follow-up once no consumer remains (kube-proxy interactions verified).

### Eviction / node metrics

`collect_node_metrics` (`runtime.rs`) and `eviction.rs` stats switch to
`ListContainerStats` + `ImageFsInfo`. Note CRI stats are narrower than Docker's
(CPU nanos, working-set, rootfs usage) — the eviction thresholds that read
Docker-specific `blkio`/detailed memory stats must be re-based on the CRI
fields; document any weakened signal in code comments.

## Stages

Every stage ends compilable and green: `cargo fmt --check`,
`cargo clippy --all-targets -- -D warnings`,
`cargo test -p rusternetes-kubelet`. Runtime verification happens against
containerd in the lima VM (`limactl shell rusternetes-dev`, see
[06](06-testing-and-environments.md)) until bollard-cri exists.

### K1 — inventory + scaffolding (no behavior change)
- Add `cri-proto` dependency; add runtime-endpoint config plumbing.
- Produce `plan/k1-inventory.md` (generated, checked in): every bollard call
  site in `runtime.rs`/`main.rs`/`eviction.rs` mapped to its CRI replacement
  per the table above, including the `download_from_container` per-site design.
- **Acceptance:** crate green; inventory reviewed (each of the ~47 bollard
  reference lines has a mapped disposition).

### K2 — sandbox + container lifecycle on CRI
- Rewrite `start_pod`, `stop_pod_*`, `stop_and_remove_pod`, container
  create/start/stop/remove, ID bookkeeping (`RuntimeState` caches rebuilt from
  `ListPodSandbox`/`ListContainers` on startup), `get_pod_ip` from
  `PodSandboxStatus`.
- **Acceptance:** in the lima VM, a single-node rusternetes (api-server +
  scheduler + kubelet pointing at containerd) runs: `kubectl run nginx`,
  pod reaches `Running` with a real IP, `kubectl delete pod` removes sandbox
  and containers (verified with `crictl ps -a && crictl pods` = empty).
  Restart-count/backoff behavior preserved (kill the container; kubelet
  restarts it with incremented `restartCount`).

### K3 — statuses, init/ephemeral containers, GC
- `get_container_statuses` / `get_init_container_statuses` /
  `get_ephemeral_container_statuses` / `compute_init_container_actions` /
  `has_terminated_containers` / `get_container_exit_code` /
  `garbage_collect_containers` / `list_all_pods` on CRI types; containerID
  prefixes from `Version()`.
- **Acceptance:** crate green; in-VM: init-container pod runs init-then-app in
  order; `kubectl get pod -o yaml` shows correct `containerStatuses` (state
  transitions, exit codes, `containerd://` IDs, restartCount).

### K4 — logs pipeline end to end
- `log_directory`/`log_path` wiring; kubelet `/containerLogs/...` endpoint
  with follow/tail/since/timestamps/previous via `CriLogReader`; api-server
  `log` subresource → kubelet proxy; **delete `generate_pod_logs` + fallback**;
  node-proxy routes + `AuthContext` fix.
- **Acceptance:** in-VM: `kubectl logs` returns real container stdout
  (`kubectl run echo --image=busybox -- echo hello` → `kubectl logs echo` ==
  `hello`); `kubectl logs -f` streams; `--previous` works after a restart;
  `kubectl get --raw /api/v1/nodes/{node}/proxy/configz` returns kubelet
  config. Grep proves the synthetic generator is gone.

### K5 — exec / attach / portforward streaming
- CRI `Exec`/`Attach`/`PortForward` + kubelet proxy + api-server
  websocket⇄SPDY translation with proper close frames; delete `main.rs`
  `handle_exec`; probes/hooks via `ExecSync`.
- **Acceptance:** in-VM: `kubectl exec pod -- sh -c 'echo hi; exit 3'` prints
  `hi` and `kubectl` exits 3 (exit-code propagation); `kubectl exec -it`
  interactive works; `kubectl port-forward` serves a local curl; zero
  `close 1005` in api-server logs across 50 looped execs
  (`for i in $(seq 50); do kubectl exec pod -- true; done`).

### K6 — stats, eviction, bollard removal
- `collect_node_metrics` + `eviction.rs` on `ListContainerStats`/`ImageFsInfo`;
  remove `bollard` from `crates/kubelet/Cargo.toml`; remove dead
  Docker-volume code paths.
- **Acceptance:** `grep -r bollard crates/kubelet/` → empty; crate green;
  in-VM: `kubectl top`-equivalent node status fields populated; eviction test
  (memory-pressure simulation from existing tests) passes on CRI stats.

### K7 — conformance re-baseline
- Full sonobuoy certified-conformance run on a containerd-backed cluster
  (lima VM, or kind-style nodes per [06](06-testing-and-environments.md)).
- **Acceptance:** pass rate ≥ 90% (baseline 63.6%); **zero** failures caused by
  synthetic logs or websocket 1005; failure diff against
  the prior run's analysis checked into `plan/results/` for the next
  iteration. (The remaining long tail — CRD strict decoding, OpenAPI
  publishing, scheduling — is out of scope here and stays tracked in that
  diff.)

## Risks / open items

- **Compose workflow gap:** between K6 and bollard-cri completion, the
  podman-compose cluster on macOS cannot run the kubelet. Mitigation:
  [03](03-bollard-cri.md) stages B1–B4 can proceed in parallel from
  01-S2; sequence releases so main never ships a kubelet no runtime can serve
  (land K6 and B-final within the same release window, or gate K6 behind a
  branch until B4).
- CRI stats are poorer than Docker stats — eviction fidelity must be
  re-validated (K6 acceptance).
- `previous` logs require restart-count-versioned log paths from day one (K4)
  or `--previous` silently breaks.
- The websocket⇄SPDY translation in the api-server is the trickiest new code;
  the 50-exec loop in K5 is the regression gate for the 1005 class.
