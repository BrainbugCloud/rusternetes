# Plan 03 — `bollard-cri`: a critest-compliant CRI shim for Docker/Podman

A new binary crate `crates/bollard-cri`: a CRI server (via
[`cri-server`](01-cri-crates.md)) backed by the Docker Engine API through
bollard. It is the successor to the kubelet's in-process bollard code and what
restores the Docker/Podman (and macOS podman-machine compose) workflow after
the kubelet's hard cutover ([02](02-kubelet-cri-only.md)).

Blueprint: **cri-dockerd** (local checkout `../cri-dockerd`, the maintained
dockershim fork, Go). Its `core/` package (~6k lines) encodes every hard-won
CRI-on-Docker decision; port the decisions, not the code.

## Design

### Process shape

```
bollard-cri --cri-listen unix:///var/run/bollard-cri.sock \
            --docker-host unix:///var/run/docker.sock \
            --pod-infra-container-image registry.k8s.io/pause:3.10 \
            --root-dir /var/lib/bollard-cri \
            --streaming-bind 127.0.0.1:0
```

One `BollardBackend` implements `RuntimeBackend`, `ImageBackend`, and
`StreamingBackend` from `cri-server`; `CriService<BollardBackend>` serves both
CRI services on the UDS. Single shared bollard `Docker` client.

### Sandbox mapping (port of cri-dockerd decisions)

- **Sandbox = pause container** running the pod-infra image (default
  `registry.k8s.io/pause:3.10`, flag-overridable). Created with
  `IpcMode=shareable`, minimal CPU shares, OOM-score protected, 10s stop grace.
  Pull the pause image only if absent.
- **App containers join the sandbox** via
  `network_mode/ipc_mode/pid_mode: "container:<sandbox-docker-id>"` per the
  CRI namespace options in `ContainerConfig`.
- **Labels are the source of truth** (from `cri-server::labels`):
  - `io.kubernetes.docker.type` = `podsandbox` | `container`
  - `io.kubernetes.sandbox.id` = sandbox container id (on app containers)
  - `io.kubernetes.container.logpath` = kubelet-desired log path
  - CRI labels + annotations flattened into Docker labels (annotation prefix so
    they split back apart in list/status conversions).
- **Names encode CRI metadata for idempotency** (Docker rejects duplicates):
  `k8s_<container>_<pod>_<namespace>_<uid>_<attempt>` (sandbox uses `POD` as
  the container field). Parse back on recovery; on a create-name conflict,
  remove the stale container or randomize-suffix (port
  `recoverFromCreationConflictIfNeeded`).
- **Checkpoint store** (`cri-server::checkpoint`, root `--root-dir/sandbox/`):
  persist per-sandbox `{port_mappings, host_network}` at RunPodSandbox step
  order create → **checkpoint** → start; read it for teardown; delete on
  RemovePodSandbox; auto-drop corrupt entries.
- **Networking:** MVP uses the **Docker/Podman network** for sandbox
  connectivity (pod IP = pause container's `NetworkSettings` IP on the
  configured network) — this matches how the rusternetes compose cluster works
  today and keeps kube-proxy semantics unchanged. CNI-driven sandbox netns is
  a documented follow-up, not MVP (cri-dockerd is CNI-only; we deviate
  deliberately and record it). Host-network pods: `network_mode: host` on the
  sandbox, flag in the checkpoint.
- **resolv.conf:** rewrite the sandbox's resolv.conf from `DnsConfig` after
  start (Docker owns the file; port `rewriteResolvFile`).
- **Forced settings on app containers:** `Healthcheck: NONE` (never surface
  Docker healthchecks to CRI) and `RestartPolicy: no` (the kubelet owns
  restarts) — both explicit cri-dockerd lessons.

### Logs — the main open design risk

CRI requires the runtime to write CRI-format log files at
`{log_directory}/{log_path}`. Docker writes its own json-file logs. Options:

- **(a) Log relay (recommended MVP):** per started container, spawn a tokio
  task attached to `docker.logs(follow=true, stdout, stderr, timestamps)`
  writing through `cri-server::logfmt::CriLogWriter` to the CRI path. Survives
  because bollard-cri owns container lifecycle; on bollard-cri restart, resume
  relays for running containers (track file offsets via docker log `since`).
  `ReopenContainerLog` = rotate the file and reopen.
- **(b) Symlink** CRI path → Docker's json log (cri-dockerd's approach) — only
  works when the log *consumer* parses Docker json format; our kubelet reads
  CRI format, so (b) would push format detection into the kubelet. Rejected
  unless (a)'s relay proves lossy.

Decide by measuring (a) against critest's log assertions in B3; document the
outcome in the crate README.

### Streaming

Reuse the `cri-server` SPDY streaming server. `StreamingBackend` impl:

- `exec_stream`: `create_exec` (with stdin/tty flags) + `start_exec` →
  demux `LogOutput` frames to the exec channels; exit code by polling
  `inspect_exec` (bounded retries — port the NativeExecHandler pattern,
  including "resize after start" ordering for tty).
- `attach_stream`: `attach_container`.
- `dial_in_sandbox`: Linux — `setns` into `/proc/<pause-pid>/ns/net` on a
  dedicated thread, then `TcpStream::connect(127.0.0.1:port)` (pause PID from
  `inspect_container`). This gives correct `PortForward` even for
  localhost-only server processes.
- `exec_sync`: same exec path with buffered, size-capped output (16 MiB cap,
  cri-dockerd convention) and timeout → gRPC `DeadlineExceeded`.

### Images

`ImageBackend` over bollard: `inspect_image`/`list_images` → CRI `Image`
(id = image ID digest, repo tags/digests mapped), `create_image` (streaming
pull with progress consumed/discarded, auth from CRI `AuthConfig`),
`remove_image` idempotent, `image_fs_info` from `docker.df()` or the graph
root's filesystem usage (best effort; critest tolerance to be verified).

### Stats

`container_stats`/`list_container_stats` via one-shot `docker.stats`: map CPU
`UsageCoreNanoSeconds`, memory `WorkingSetBytes` (usage − inactive_file, v1/v2
aware — port the existing logic from kubelet `runtime.rs` before it's
deleted), rootfs bytes via size-inspect **cached in a background task with
backoff** (Docker's size inspection is slow; cri-dockerd caches for this
reason). Fan out list-stats with bounded concurrency.

### Explicitly unimplemented (Status::unimplemented via `CriService` defaults)

`GetContainerEvents` (kubelet polls), `CheckpointContainer`, the v1.36
`Stream*` list variants, pod-sandbox-level stats/metrics beyond what critest
requires. Documented in the README with rationale.

## Stages

Green = crate passes fmt/clippy/tests; critest evidence = JUnit archived per
[06](06-testing-and-environments.md). critest runs use
`critest --runtime-endpoint unix:///var/run/bollard-cri.sock --image-endpoint <same>`
with `--ginkgo.focus` per stage, full suite at the end. Primary environment:
Linux (CI runner with Docker; locally the podman machine VM or lima). Podman
divergences are recorded per test, not papered over.

### B1 — skeleton + runtime info + images
- Crate scaffold, flags, UDS serving, `version`/`status`/`update_runtime_config`;
  full `ImageBackend`.
- **Acceptance:** `crictl info`, `crictl pull busybox`, `crictl images`,
  `crictl rmi` work; critest focus `Image` (image manager suite) green on
  Docker/Linux.

### B2 — sandbox lifecycle
- Pause container, naming, labels, checkpoint store, resolv.conf rewrite,
  host-network, pod IP reporting, idempotent stop/remove, conflict recovery.
- **Acceptance:** critest focus `PodSandbox` green; `crictl runp && crictl
  pods && crictl stopp && crictl rmp` round-trips; kill -9 bollard-cri
  mid-lifecycle and restart → `crictl pods` still consistent (checkpoint
  recovery test).

### B3 — container lifecycle + logs
- Create/start/stop/remove/list/status with metadata filters, env/mounts/
  devices/security-context mapping (start from the kubelet's existing
  `HostConfig` mapping in `runtime.rs` before K6 deletes it), log relay (a),
  `ReopenContainerLog`.
- **Acceptance:** critest focus `Container` green including its log-content
  assertions (this validates design decision (a)); `crictl logs` output
  matches `docker logs` byte-for-byte for a multi-line stdout+stderr workload.

### B4 — streaming
- `StreamingBackend` impl (exec/attach/portforward/exec_sync) as designed.
- **Acceptance:** critest focus `Streaming` green; manual `crictl exec -it`
  interactive session works; portforward to a localhost-bound server in the
  pod works (proves the setns dial).

### B5 — stats + full suite
- Stats/list-stats with the size cache; `ImageFsInfo` verified against
  critest; sweep remaining failures.
- **Acceptance:** **full critest v1.36.0: 0 failures on Docker/Linux CI**
  (skips allowed only where critest itself skips). On Podman: full run
  executed, every divergence gets a line in `crates/bollard-cri/PODMAN.md`
  (test name, root cause, upstream issue if any).

### B6 — cluster integration (restores the macOS workflow)
- Compose changes: each kubelet node container gains a bollard-cri process
  (same container, supervised, socket shared via an emptydir-style volume)
  talking to the podman socket; kubelet gets
  `--container-runtime-endpoint unix:///run/bollard-cri.sock`. The all-in-one
  binary spawns bollard-cri as another tokio task when configured for Docker.
- **Acceptance:** `podman compose up -d && bash scripts/bootstrap-cluster.sh`
  yields a working cluster on macOS again (CoreDNS Running, `kubectl run`,
  `kubectl logs`, `kubectl exec` all work); `scripts/run-conformance.sh`
  completes with pass rate ≥ the containerd baseline from
  [02-K7](02-kubelet-cri-only.md) minus documented Podman divergences.

## Risks / open items

- **Log relay fidelity** (loss on restart, partial-line handling, backpressure)
  — gated by B3's byte-for-byte acceptance; fallback design (b) documented.
- **Podman Docker-compat gaps** (stats fields, exec inspect, log follow
  framing — the old kubelet already hit podman log-stream issues per the
  conformance analysis): treat Docker/Linux as the conformance target and
  Podman as best-effort with a tracked divergence list (B5).
- Exec exit-code polling races (inspect_exec after stream close) — port
  cri-dockerd's bounded retry exactly; covered by critest Streaming.
- Security-context mapping breadth (seccomp/apparmor/privileged/sysctls) —
  start from what the kubelet ships today (feature parity), extend only as
  critest demands; do not aim for full dockershim parity in MVP.
