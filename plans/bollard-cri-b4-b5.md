# Task: Implement B4 (Streaming) and B5 (Stats + Full Suite) for bollard-cri

## Context

You are working on `crates/bollard-cri` in the rusternetes repo. This is a CRI server
that wraps Docker/Podman via the bollard crate. B1-B3 are complete:
- B1: skeleton + runtime info + images ✅
- B2: sandbox lifecycle ✅  
- B3: container lifecycle + logs ✅

You need to implement **B4 (streaming)** and **B5 (stats + full suite)**.
Skip B6 entirely.

## B4 — Streaming

Implement `StreamingBackend` for bollard-cri:

1. **exec_stream**: `create_exec` (with stdin/tty flags) + `start_exec` → demux `LogOutput` frames to exec channels; exit code by polling `inspect_exec` (bounded retries — port the NativeExecHandler pattern from cri-dockerd, including "resize after start" ordering for tty)
2. **attach_stream**: `attach_container` via bollard
3. **dial_in_sandbox**: Linux — `setns` into `/proc/<pause-pid>/ns/net` on a dedicated thread, then `TcpStream::connect(127.0.0.1:port)` (pause PID from `inspect_container`). This gives correct `PortForward` even for localhost-only server processes.
4. **exec_sync**: Already landed in B3 — verify it works with the streaming server

The SPDY streaming server is in `cri-server` crate — reuse it. The `StreamingBackend` trait is defined there.

Reference: The aurae CRI implementation at `~/git/aurae` on the `cri` branch has streaming.rs with SPDY/3.1 server and nsenter-based container I/O splicing. Look at it for patterns.

## B5 — Stats + Full Suite

1. **rootfs/writable-layer size cache** for container_stats — Docker's size inspection is slow, cache with background task + backoff (cri-dockerd pattern)
2. **container_stats / list_container_stats** via one-shot `docker.stats`: map CPU `UsageCoreNanoSeconds`, memory `WorkingSetBytes` (usage − inactive_file, v1/v2 aware)
3. **ImageFsInfo** verified against critest
4. Sweep remaining failures to get full critest passing

## Test Environment

We are in a non-privileged container. Tests need a privileged pod on the cluster.
The aurae project solved this with `hack/cri-conformance-cluster.sh` — it creates a
privileged pod, streams the binary + critest tools in, and runs tests.

For bollard-cri, the test pod needs:
- Docker daemon running inside the pod (DinD) OR access to a Docker socket
- bollard-cri binary built for aarch64-musl
- critest + crictl v1.36.0
- Privileged securityContext (for setns in dial_in_sandbox)

### Build
```bash
cd ~/git/rusternetes
export LIBCLANG_PATH=$HOME/.local/libclang/usr/lib/llvm-19/lib
export PATH=$HOME/.local/bin:$HOME/.npm-global/bin:$PATH
cargo build --target aarch64-unknown-linux-musl -p bollard-cri
```

### Test Pod Setup (adapt from aurae's pattern)
Create a privileged pod on bb-k8s-rk1b-01 with Docker-in-Docker:
- Namespace: agent-sandbox-system
- Image: docker:28-dind (or ubuntu:24.04 + install docker)
- Mount docker socket or run dockerd inside
- Stream bollard-cri binary in
- Install critest/crictl v1.36.0
- Run: `critest --runtime-endpoint unix:///var/run/bollard-cri.sock --image-endpoint unix:///var/run/bollard-cri.sock`

### Acceptance Criteria
- [x] B4: critest `Streaming` focus green (5/5 specs pass)
- [x] B4: interactive `crictl exec -it` works (verified in the test pod: real PTY `/dev/pts/0`, stdin/stdout round-trip, exit-code propagation)
- [x] B4: portforward to localhost-bound server works (`dial_in_sandbox` setns-dials `127.0.0.1` inside the sandbox netns — the only dial path — and the critest PortForward spec passes through it)
- [x] B5: full critest v1.36.0 zero failures on Docker/Linux (96 passed, 0 failed, 26 skipped — two consecutive full runs via `scripts/cri-conformance-bollard.sh`)

## Key Files
- `crates/bollard-cri/src/main.rs` — entry point, flag parsing, server startup
- `crates/bollard-cri/src/backend.rs` — BollardBackend struct
- `crates/bollard-cri/src/container.rs` — container lifecycle (has exec_sync already)
- `crates/bollard-cri/src/sandbox.rs` — sandbox/pause container management
- `crates/bollard-cri/src/images.rs` — image operations
- `crates/bollard-cri/src/logs.rs` — CRI log relay
- `crates/bollard-cri/src/naming.rs` — container naming scheme

## Reference Implementations
- cri-dockerd (Go): `~/git/rusternetes/../cri-dockerd` if available, otherwise the plan references its patterns
- aurae CRI streaming: `~/git/aurae` branch `cri` — `auraed/src/cri/streaming.rs` has SPDY server + nsenter
- Plan document: `~/git/rusternetes/plan/03-bollard-cri.md`

## Working Approach
1. First read all existing bollard-cri source to understand the current architecture
2. Read the cri-server crate to understand StreamingBackend trait
3. Look at aurae's streaming.rs for SPDY/nsenter patterns
4. Implement B4 streaming
5. Build and test iteratively using the privileged pod approach
6. Implement B5 stats
7. Run full critest suite, fix failures

## Important Notes
- The cri-server crate provides the SPDY streaming server — you just implement StreamingBackend
- exec_sync already exists in container.rs from B3 — don't duplicate it
- For setns in dial_in_sandbox: use a dedicated OS thread (not tokio), setns is blocking
- Docker stats API: bollard has `docker.stats()` — map the fields to CRI stats format
- The size cache should use a background tokio task with exponential backoff
- Commit frequently with clear messages
