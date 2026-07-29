# Plan 01 — Reusable CRI crates: `cri-proto` and `cri-server`

Two new workspace crates with **zero dependencies on other rusternetes
crates**, publishable to crates.io. They are the foundation for the kubelet's
CRI client ([02](02-kubelet-cri-only.md)) and for every CRI server we build
([03](03-bollard-cri.md), [04](04-apple-containers-cri.md)) — and are designed
so aurae could adopt them from crates.io later (never as a rusternetes dep).

## Status

- [x] **S1 — `cri-proto` builds and talks to containerd** *(2026-07-17)*
  - [x] Vendored `proto/release-1.36.proto` (from aurae checkout, identical to upstream) + `scripts/vendor-cri-proto.sh`
  - [x] tonic-prost-build codegen (client + server, feature-gated via `CARGO_FEATURE_*`); system `protoc` documented in crate README (decision: no `protobuf-src`)
  - [x] `connect_uds` + `socket_path` helpers (accepts `unix://`, `unix:`, bare paths)
  - [x] Crate metadata (Apache-2.0, description, repository)
  - [x] `#[ignore]` containerd smoke test: 3/3 pass in the lima VM (`default`, containerd v2.1.3) — cross-compiled aarch64-musl, run via `limactl copy` + `limactl shell`
  - [x] Green: fmt + clippy + `cargo test -p cri-proto`; `cargo tree -p cri-proto | grep -c rusternetes-` = 0
  - [x] `lima/rusternetes-dev.yaml` checked in (per plan 06)
- [x] **S2 — `cri-server` traits + in-memory fake** *(2026-07-17)*
  - [x] `backend.rs` (RuntimeBackend + ImageBackend traits, `Error` → CRI status codes)
  - [x] `service.rs` (`CriService<B>`, all 42 RPCs; `Stream*`/events/metrics/checkpoint → unimplemented)
  - [x] `uds.rs` (stale-socket cleanup, 0o660, graceful shutdown; `serve()` helper)
  - [x] `labels.rs` (io.kubernetes.* keys, flatten/split, internal-label stripping), `logfmt.rs` (writer + reader w/ since/tail + partial-line reassembly), `checkpoint.rs` (crc32 checksum, atomic write, corrupt auto-delete)
  - [x] `MemoryBackend` fake behind `testing` feature + `examples/memory_cri.rs`
  - [x] Tests: 9 unit + 3 integration (lifecycle over real UDS via cri-proto client, idempotency/NotFound/InvalidArgument semantics, logfmt round-trip incl. partials/tail/since, checkpoint corruption recovery) — all green; fmt/clippy clean; 0 rusternetes deps
  - [x] Acceptance: crictl v1.36.0 (brew) `version/info/pull/images/runp/create/start/ps/pods/logs/exec -s/stopp/rmp` all work against `memory-cri` on macOS
- [x] **S3 — streaming server extracted** *(2026-07-17)*
  - [x] `streaming/` fork of aurae streaming.rs (`mod.rs` registry+upgrade, `spdy.rs` framing, `channels.rs` sessions) behind `StreamingBackend` trait; provenance headers kept
  - [x] Exec/Attach/PortForward wired in `CriService` (with state validation + TTL'd token registry); ExecSync was wired in S2
  - [x] `MemoryBackend` implements `StreamingBackend` (scripted echo/stderr/cat/false; portforward dials host localhost)
  - [x] SPDY framing/dictionary unit tests (header block round-trip over shared zlib contexts, frame encode/decode, truncation)
  - [x] Acceptance vs crictl v1.36.0 on macOS: `exec` non-tty (stdout, stderr demux, exit code 1 propagates), `exec -i` (stdin piped through cat), `exec -it` (via pty; clean close, exit 0), `attach -i` (greeting + stdin echo), `port-forward` (HTTP 200 through the tunnel)
- [x] **S4 — critest subsets against the fake** *(2026-07-17)*
  - [x] Focus set documented in `crates/cri-server/README.md` (focus `runtime info|PodSandbox|Container|Streaming`; skips justified per test)
  - [x] Focus set green: 33/33 on Linux (lima VM `default`, critest v1.36.0 linux-arm64) and 31/31 on macOS (darwin-arm64); JUnit reports produced (`critest-memory-cri-{linux,macos}.xml`, session scratchpad — CI archiving lands with the CI job per plan 06)
  - [x] MemoryBackend upgraded along the way: scripted workloads (`echo` one-shots, `echo; sleep` stay-running, `while true; do echo` log loops), CRI-correct forced remove of running containers, execSync `-n` echo semantics + timeout → `DeadlineExceeded`, log rotation on `ReopenContainerLog`

## `crates/cri-proto`

Bindings for the Kubernetes CRI v1 API, pinned to **cri-api release-1.36**
(lockstep with critest v1.36.0 and aurae's `cri` branch).

### Design

- Vendor the proto at `crates/cri-proto/proto/release-1.36.proto`, fetched by a
  script/make target (mirror aurae's `proto-vendor-cri`):
  `curl https://raw.githubusercontent.com/kubernetes/cri-api/release-1.36/pkg/apis/runtime/v1/api.proto`.
  The vendored file is checked in (builds must not hit the network).
- `build.rs` uses `tonic-build` (+ `protoc` via `protobuf-src` or a documented
  system dependency — decide at implementation time; prefer vendored
  `protobuf-src` so `cargo build` works out of the box) generating:
  - all `runtime.v1` messages (prost),
  - `runtime_service_client::RuntimeServiceClient`, `image_service_client::ImageServiceClient`,
  - `runtime_service_server::{RuntimeService, RuntimeServiceServer}`, `image_service_server::{ImageService, ImageServiceServer}`.
- Cargo features: `client` (default), `server` (default), optionally `serde`
  (via `pbjson`-style serde impls) later — not needed for the MVP.
- **UDS helpers** module (client side):
  ```rust
  pub async fn connect_uds(path: impl AsRef<Path>) -> Result<Channel, tonic::transport::Error>
  ```
  implemented with the standard tower `Endpoint::connect_with_connector`
  pattern over `tokio::net::UnixStream`. Accept both `unix:///run/x.sock` URIs
  and bare paths (kubelet flag compatibility).
- Crate metadata: `license = "Apache-2.0"`, description, repository; the
  vendored proto keeps the upstream Kubernetes Authors Apache-2.0 header.

### Non-goals

- No CRI v1alpha2 (dead upstream). No serde on messages for MVP. No hand-written
  wrappers around generated types — consumers use the tonic types directly.

## `crates/cri-server`

The reusable server harness: everything a CRI server needs that is *not*
backend-specific. Forked in part from aurae's `cri` branch (Apache-2.0 — keep
`SPDX-License-Identifier: Apache-2.0` and add provenance comments referencing
`aurae auraed/src/cri/streaming.rs`).

### Module layout

```
crates/cri-server/src/
  lib.rs
  backend.rs      # RuntimeBackend + ImageBackend traits
  service.rs      # CriService<B>: implements tonic RuntimeService/ImageService
  streaming/      # SPDY/3.1 streaming server (fork of aurae streaming.rs)
    mod.rs        # Streaming registry, serve(), StreamingBackend trait
    spdy.rs       # framing, zlib dictionary, upgrade handshake
    channels.rs   # v4.channel.k8s.io (exec/attach) + portforward.k8s.io
  logfmt.rs       # CRI log format writer/reader (`RFC3339Nano stream tag msg`)
  checkpoint.rs   # JSON file store with checksum (cri-dockerd store/ pattern)
  labels.rs       # kubernetes label/annotation conventions + helpers
  uds.rs          # unix socket listener bootstrap (perms, stale-socket cleanup)
```

### `backend.rs` — the seam every runtime implements

Rather than forcing each backend to implement ~30 tonic RPCs, backends
implement two narrower async traits; `CriService<B>` supplies the gRPC
plumbing, request validation, and `Status::unimplemented` defaults for the
v1.36 streaming/metrics RPCs (`StreamContainers`, `StreamImages`,
`ListMetricDescriptors`, `CheckpointContainer`, `GetContainerEvents`, …) that
neither the kubelet nor critest require.

```rust
#[async_trait]
pub trait RuntimeBackend: Send + Sync + 'static {
    // sandbox lifecycle
    async fn run_pod_sandbox(&self, config: PodSandboxConfig, runtime_handler: &str) -> Result<String>;
    async fn stop_pod_sandbox(&self, id: &str) -> Result<()>;      // idempotent: unknown id => Ok
    async fn remove_pod_sandbox(&self, id: &str) -> Result<()>;    // idempotent
    async fn pod_sandbox_status(&self, id: &str) -> Result<PodSandboxStatus>;
    async fn list_pod_sandbox(&self, filter: Option<PodSandboxFilter>) -> Result<Vec<PodSandbox>>;
    // container lifecycle
    async fn create_container(&self, sandbox_id: &str, config: ContainerConfig, sandbox_config: PodSandboxConfig) -> Result<String>;
    async fn start_container(&self, id: &str) -> Result<()>;
    async fn stop_container(&self, id: &str, timeout_secs: i64) -> Result<()>; // idempotent
    async fn remove_container(&self, id: &str) -> Result<()>;                  // idempotent
    async fn list_containers(&self, filter: Option<ContainerFilter>) -> Result<Vec<Container>>;
    async fn container_status(&self, id: &str) -> Result<ContainerStatus>;
    async fn update_container_resources(&self, id: &str, resources: LinuxContainerResources) -> Result<()>;
    // exec / attach / portforward primitives consumed by the streaming server
    async fn exec_sync(&self, id: &str, cmd: &[String], timeout_secs: i64) -> Result<ExecSyncResult>;
    // stats & info
    async fn container_stats(&self, id: &str) -> Result<ContainerStats>;
    async fn list_container_stats(&self, filter: Option<ContainerStatsFilter>) -> Result<Vec<ContainerStats>>;
    async fn status(&self) -> Result<RuntimeStatus>;               // RuntimeReady / NetworkReady
    async fn version(&self) -> Result<VersionResponse>;
    async fn update_runtime_config(&self, pod_cidr: Option<String>) -> Result<()>;
    async fn reopen_container_log(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait ImageBackend: Send + Sync + 'static {
    async fn list_images(&self, filter: Option<ImageFilter>) -> Result<Vec<Image>>;
    async fn image_status(&self, image: &ImageSpec) -> Result<Option<Image>>;
    async fn pull_image(&self, image: &ImageSpec, auth: Option<AuthConfig>, sandbox_config: Option<PodSandboxConfig>) -> Result<String>;
    async fn remove_image(&self, image: &ImageSpec) -> Result<()>; // idempotent
    async fn image_fs_info(&self) -> Result<Vec<FilesystemUsage>>;
}
```

Notes:
- Types are the `cri-proto` generated types — no parallel type universe.
- The `Result` error type maps to `tonic::Status` with CRI-conventional codes
  (`NotFound` for unknown ids on *status* calls; `Ok` on idempotent
  stop/remove — critest asserts this, see aurae commit
  "StopContainer idempotence — unknown container returns OK per CRI spec").

### `streaming/` — the SPDY server (fork of aurae `streaming.rs`)

The single most valuable lift: a pure-Rust CRI streaming server exists nowhere
else. Fork aurae's implementation (SPDY/3.1 framing, the moby/spdystream zlib
dictionary via `flate2` with `zlib-rs`, hand-parsed HTTP/1.1 Upgrade,
`v4.channel.k8s.io` and `portforward.k8s.io` sub-protocols) and cut its two
aurae-specific call sites behind a trait:

```rust
#[async_trait]
pub trait StreamingBackend: Send + Sync + 'static {
    async fn exec_stream(&self, container_id: &str, cmd: Vec<String>, tty: bool, stdin: bool) -> Result<ExecStreams>;
    async fn attach_stream(&self, container_id: &str, tty: bool, stdin: bool) -> Result<AttachHandles>;
    /// Open a TCP connection to `port` as seen from inside the sandbox
    /// (Linux: setns into the sandbox netns then connect; macOS: connect to the sandbox VM IP).
    async fn dial_in_sandbox(&self, sandbox_id: &str, port: i32) -> Result<Box<dyn AsyncReadWrite>>;
}
```

- `Streaming` registry: single-use token → `StreamSpec`, TTL'd; `register()`
  returns the URL handed back by the `Exec`/`Attach`/`PortForward` RPCs.
- Transport: `tokio::net::TcpListener` on a configurable address (default
  `127.0.0.1:0`, base URL derived from the bound port). Runtimes and kubelet
  are co-located on the node, matching aurae/containerd practice.
- Dependencies stay minimal: `tokio`, `flate2` (zlib-rs), `uuid`, `tracing`.
  `nix` (setns) is Linux-only, behind `#[cfg(target_os = "linux")]` — the
  macOS backend implements `dial_in_sandbox` without it.
- WebSocket support is explicitly out of scope for now (kubelet/critest speak
  SPDY by default); leave a module seam for a future `websocket.rs`.

### `logfmt.rs` — CRI log format

CRI runtimes write per-container log files that the kubelet reads/serves:

```
2026-07-17T10:00:00.123456789Z stdout F full line
2026-07-17T10:00:00.123456789Z stdout P partial…
```

Provide `CriLogWriter` (wraps an `AsyncWrite`, splits lines, stamps
`RFC3339Nano stream {F|P}`) and `CriLogReader` (parse, filter by since/until,
tail-n, follow via file-watch). Used by: `bollard-cri` and `apple-cri` (relay
runtime-native logs into CRI files) and by the kubelet (read/serve). This
module is the contract that kills the synthetic-logs conformance failure class.

### `checkpoint.rs`, `labels.rs`, `uds.rs`

- `checkpoint.rs`: versioned JSON records + fnv/crc checksum, atomic writes,
  corrupt-entry auto-delete — port of cri-dockerd `store/` (~580 lines Go).
  Used by shims that can't store CRI metadata in their runtime (port mappings,
  host-network flag).
- `labels.rs`: `io.kubernetes.*` label/annotation constants, sandbox/container
  metadata encode/decode helpers, internal-label stripping.
- `uds.rs`: remove stale socket, bind, set `0o660`, serve tonic router with
  graceful shutdown. Filesystem permissions are the trust boundary (containerd
  convention; no TLS on the CRI socket).

## Stages

Each stage is a self-contained PR-sized unit for a coding agent. "Green" means
`cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`,
`cargo test -p <crate>` pass, and the internal-deps rule holds
(`cargo tree -p cri-proto -p cri-server | grep -c rusternetes-` → 0).

### S1 — `cri-proto` builds and talks to containerd
- Vendor proto, tonic-build codegen, UDS connect helper, crate metadata.
- Integration smoke test (ignored by default, `#[ignore]`, run in the lima VM
  per [06](06-testing-and-environments.md)): connect to
  `/run/containerd/containerd.sock`, call `Version`, assert
  `runtime_name == "containerd"`; call `ListPodSandbox` without error.
- **Acceptance:** crate green; smoke test passes inside the lima VM
  (`limactl shell rusternetes-dev -- cargo test -p cri-proto -- --ignored`).

### S2 — `cri-server` traits + in-memory fake
- `backend.rs`, `service.rs`, `uds.rs`, `labels.rs`, `logfmt.rs` (writer +
  reader), `checkpoint.rs`.
- `MemoryBackend` test fake (containers as state-machine records, images as a
  set) in `cri-server/src/testing.rs` behind a `testing` feature — this fake
  is also the contract-test vehicle for [05](05-apple-cri-xpc-migration.md).
- Unit tests: full sandbox+container lifecycle against `CriService<MemoryBackend>`
  over an in-process UDS; idempotency semantics (stop/remove unknown → Ok,
  status unknown → NotFound); logfmt round-trip incl. partial lines, tail,
  since; checkpoint corrupt-file recovery.
- **Acceptance:** crate green; `crictl --runtime-endpoint unix:///tmp/fake.sock ps`,
  `crictl runp` + `crictl create` + `crictl start` succeed against a
  `memory-cri` example binary (`examples/memory_cri.rs`).

### S3 — streaming server extracted
- Fork aurae `streaming.rs` into `streaming/` with the `StreamingBackend`
  trait; wire `Exec`/`Attach`/`PortForward`/`ExecSync` RPCs in `CriService`.
- `MemoryBackend` implements `StreamingBackend` with a scripted echo process.
- Tests: `crictl exec` (tty and non-tty), `crictl attach`, and
  `crictl port-forward` against the example binary; assert clean close
  semantics (no hangs, exit codes propagate).
- **Acceptance:** crate green; a recorded `crictl exec -s` session against
  `memory-cri` shows stdout+stderr demux and correct exit code; SPDY unit
  tests for framing/dictionary pass.

### S4 — critest subsets against the fake
- Run critest v1.36.0 with `--ginkgo.focus` on backend-agnostic groups
  (Runtime info, PodSandbox lifecycle, Container lifecycle basics, Streaming)
  against `memory-cri`. The fake won't run real workloads, so scope focus
  expressions to what a state-machine fake can honestly pass; document the
  covered focus list in `crates/cri-server/README.md`.
- **Acceptance:** the documented focus set is green in CI (Linux runner);
  JUnit archived per [06](06-testing-and-environments.md). This proves the
  harness plumbing (RPC surface, streaming URLs, log files) before any real
  backend exists.

## Risks / open items

- `protoc` availability: prefer `protobuf-src` vendored build; if build times
  hurt, fall back to checked-in generated code with a regen script (decide in S1).
- SPDY fork drift: we take a snapshot of aurae's implementation; upstream fixes
  don't flow automatically. Mitigate with the S3 protocol unit tests and the
  critest Streaming focus in CI.
- aurae adoption is *not* a work item here — publishing to crates.io happens
  once API churn settles (post [03](03-bollard-cri.md) at the earliest).
