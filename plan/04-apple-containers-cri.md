# Plan 04 — `apple-cri`: a CRI shim for apple/container (CLI backend)

A new binary crate `crates/apple-cri` (macOS-only, Apple silicon): a CRI
server (via [`cri-server`](01-cri-crates.md)) backed by
[apple/container](https://github.com/apple/container) — Linux containers in
lightweight per-container VMs on Virtualization.framework. Backend transport is
the **`container` CLI** (stable since 1.0.0); migration to the XPC API is
planned separately with regression tests ([05](05-apple-cri-xpc-migration.md)).

Starting material: PR #15 (`feat: Container Runtime Abstraction Layer`,
closed, branch `feature/container-runtime-abstraction`) — its
`crates/kubelet/src/container_runtime/apple.rs` is a working catalog of CLI
mappings (create/start/stop/rm/logs/exec/inspect with JSON output). Reuse the
command mappings; discard the architecture (its 17-method trait mirrors
bollard, not CRI).

## Status

- [ ] A1 — CLI probe + backend trait
- [ ] A2 — CRI surface: images + sandboxes
- [ ] A3 — containers + logs
- [ ] A4 — streaming + stats + full matrix
- [ ] A5 — rusternetes smoke on macOS

## The hard design problem, stated up front

**apple/container runs one VM per container.** Kubernetes pods assume
containers share network (localhost), IPC, and optionally PID namespaces.
Across separate VMs there is no shared netns — full pod semantics are
impossible with the CLI's one-container-per-VM model.

### MVP mapping (documented deviation)

- **Sandbox = a metadata record + a dedicated vmnet network** (`container
  network create pod-<uid>` — vmnet networks are a first-class container CLI
  concept). No pause VM in MVP: a sandbox is materialized lazily as network +
  checkpoint entry; `PodSandboxStatus.network.ip` reports the first app
  container's IP (or the reserved IP once one exists).
- **One-container-per-pod is fully correct** — which covers the kubelet's
  bring-up needs and most smoke tests.
- **Multi-container pods degrade** to same-vmnet-network-but-not-localhost:
  containers reach each other by IP on the pod network, but not via
  `127.0.0.1`, and don't share IPC/PID. This is a **documented deviation**
  (crate README + a warning log per multi-container sandbox).
- **True pods-in-one-VM** require the Containerization framework level
  (vminitd can run multiple processes per VM) — that is exactly the XPC/native
  path and is deferred to [05](05-apple-cri-xpc-migration.md) as its endgame
  justification.

## Design

### Backend transport trait (the seam doc 05 depends on)

All CLI invocation goes through one trait so XPC can land as a second impl:

```rust
#[async_trait]
pub trait AppleBackend: Send + Sync + 'static {
    async fn create(&self, req: CreateRequest) -> Result<ContainerId>;
    async fn start(&self, id: &str) -> Result<()>;
    async fn stop(&self, id: &str, timeout: Duration) -> Result<()>;
    async fn delete(&self, id: &str) -> Result<()>;
    async fn inspect(&self, id: &str) -> Result<Option<AppleContainerInfo>>;
    async fn list(&self) -> Result<Vec<AppleContainerInfo>>;
    async fn exec(&self, id: &str, cmd: Vec<String>, opts: ExecOpts) -> Result<ExecChild>; // streaming handles
    async fn logs(&self, id: &str, follow: bool) -> Result<LogStream>;
    async fn image_pull(&self, reference: &str, auth: Option<Auth>) -> Result<()>;
    async fn image_list(&self) -> Result<Vec<AppleImageInfo>>;
    async fn image_delete(&self, reference: &str) -> Result<()>;
    async fn network_create(&self, name: &str) -> Result<()>;
    async fn network_delete(&self, name: &str) -> Result<()>;
}
```

`CliBackend` implements it with `tokio::process::Command` + `--format json`
parsing (serde structs per command output; pin minimum `container` version,
check at startup via `container --version`). Every call logs the argv at
debug level — the contract-test fixtures in doc 05 are recorded from here.

### CRI bookkeeping

The CLI has no labels; identity and metadata live in **apple-cri's own state**:

- Container/sandbox naming: `k8s-<short-uid>-<container>-<attempt>` (CLI names
  are the join key), full CRI metadata in the `cri-server::checkpoint` store
  under `--root-dir` (sandbox record: metadata, network name, port mappings;
  container record: metadata, sandbox id, log path, created/started/finished
  timestamps, exit code when reaped).
- On startup, reconcile: `container ls --all` ⋈ checkpoint store; orphans on
  either side are adopted-or-removed per CRI expectations (a sandbox/container
  the store doesn't know is deleted; a store entry with no CLI object is
  reported `NotFound`/exited).

### Logs

`container logs` output is relayed through `cri-server::logfmt::CriLogWriter`
into the kubelet-specified CRI log path — same relay architecture as
[bollard-cri](03-bollard-cri.md) design (a), one tokio task per running
container, resumed on daemon restart.

### Streaming

Reuse the `cri-server` SPDY server:

- `exec_stream`: `container exec [-it]` child process; wire child
  stdin/stdout/stderr to the exec channels; exit code from child status. TTY
  resize: `container exec` has no resize API — accept fixed-size TTY in MVP
  (documented; XPC path fixes this).
- `attach_stream`: no CLI attach → serve attach as a follow-logs stream
  (stdout/stderr only, no stdin) and document the deviation; critest attach
  cases go on the expected-fail matrix.
- `dial_in_sandbox`: no netns on macOS — TCP-connect to the container VM's IP
  on the pod network. Works for any port the workload binds on its interface;
  localhost-only processes inside the VM are unreachable (matrix entry).

### Stats / fs info

`container stats` (if/where available in the pinned CLI version) else
best-effort zeros with `RuntimeReady` honesty — check exact CLI surface during
A1 and record what maps. `ImageFsInfo` from the image store directory size.

## critest on macOS

critest ships **darwin-arm64 binaries since cri-tools v1.29** — the harness
runs natively (`scripts/critest/run-apple.sh`, per
[06](06-testing-and-environments.md)). A chunk of the suite asserts
Linux-kernel semantics the macOS shim cannot honor. The acceptance artifact is
therefore an **explicit tracked matrix** checked in at
`crates/apple-cri/CRITEST-MATRIX.md`:

| Category | Expectation |
|---|---|
| Runtime info, images, sandbox+container lifecycle, logs | pass |
| Exec (non-tty, tty basic), exec_sync, portforward (VM-IP reachable) | pass |
| Attach, tty resize | expected-fail (documented above) |
| Host network, seccomp, SELinux, apparmor, sysctls, cgroups-specific, privileged | expected-fail / skip (no Linux host kernel) |
| Multi-container-pod localhost assumptions | expected-fail (MVP deviation) |

Every expected-fail row carries a reason and (where applicable) the doc-05
stage that lifts it. CI: a self-hosted/manual macOS runner job (Apple silicon
+ `container` installed) — not in the default Linux CI path.

## Stages

Green = fmt/clippy/`cargo test -p apple-cri` (unit tests use a scripted
`FakeBackend` impl of `AppleBackend`; CLI integration tests are `#[ignore]`
and run on macOS only).

### A1 — CLI probe + backend trait
- Crate scaffold (cfg-gated `target_os = "macos"` for the real backend; trait
  and fakes compile everywhere so Linux CI still type-checks the crate).
- `CliBackend` for: version probe, image pull/list/delete, container
  create/start/stop/delete/inspect/list, network create/delete — mappings
  seeded from PR #15's `apple.rs`. Written against the pinned CLI version;
  record each command's actual JSON schema as serde structs + checked-in
  sample fixtures.
- **Acceptance:** on a Mac with `container` ≥ pinned version:
  `cargo test -p apple-cri -- --ignored` green (round-trip pull→create→start→
  inspect→stop→delete); fixtures checked in; Linux `cargo check -p apple-cri`
  passes.

### A2 — CRI surface: images + sandboxes
- `ImageBackend`; sandbox lifecycle (network + checkpoint record, lazy
  materialization, startup reconciliation).
- **Acceptance:** `crictl --runtime-endpoint unix://$HOME/.apple-cri/cri.sock
  runp/pods/stopp/rmp` round-trips; critest focus `Image` and `PodSandbox`
  results recorded into `CRITEST-MATRIX.md` (target: pass rows green).

### A3 — containers + logs
- Container lifecycle bound to sandboxes (join pod network), status/exit-code
  reaping, log relay to CRI files.
- **Acceptance:** critest focus `Container` matrix rows green;
  one-container pod via crictl runs a real workload (nginx reachable on its
  VM IP from the host); `crictl logs` matches `container logs`.

### A4 — streaming + stats + full matrix
- Exec/exec_sync/portforward streaming; stats best-effort; full critest run.
- **Acceptance:** full critest executed; `CRITEST-MATRIX.md` complete — every
  test is pass / expected-fail-with-reason / upstream-skip, **zero
  unexplained failures**.

### A5 — rusternetes smoke on macOS
- Single-node rusternetes natively on macOS: api-server + scheduler +
  controller-manager + kubelet (`--container-runtime-endpoint` → apple-cri).
- **Acceptance:** `kubectl run nginx` → Running with IP; `kubectl logs` and
  `kubectl exec` work end-to-end through the doc-02 pipeline; teardown clean
  (`container ls --all` empty after namespace delete).

## Risks / open items

- CLI surface drift between `container` releases — pinned version + fixtures
  make breakage loud; the trait keeps the blast radius in `CliBackend`.
- Sandbox-without-pause means sandbox "Ready" is synthetic until a container
  exists — verify critest tolerates it in A2; if not, fall back to a minimal
  pause VM per sandbox (cost: VM per pod, slower).
- Per-exec CLI process overhead for probes (ExecSync every few seconds per
  pod) — measure in A4; if it hurts, that's another XPC motivation, not an
  MVP blocker.
