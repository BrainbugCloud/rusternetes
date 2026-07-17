# cri-server

Reusable Kubernetes CRI server harness (see `plan/01-cri-crates.md`):
backend traits + tonic gRPC plumbing, the SPDY/3.1 streaming server
(exec/attach/portforward), the CRI container log format, a checksummed JSON
checkpoint store, label conventions, and unix-socket bootstrap.

A CRI server is a backend implementing three traits:

- `RuntimeBackend` — sandbox/container lifecycle, execSync, stats, info
- `ImageBackend` — list/status/pull/remove/fs-info
- `streaming::StreamingBackend` — exec/attach stream handles + in-sandbox dialing

`CriService<B>` supplies all ~40 tonic RPCs, request validation,
CRI-conventional error codes (idempotent stop/remove, `NotFound` on status),
and `Unimplemented` answers for the RPCs neither the kubelet nor critest
require (`Stream*`, `GetContainerEvents`, `CheckpointContainer`, metrics,
pod-sandbox-level stats).

```rust,ignore
let backend = Arc::new(MyBackend::new());
let streaming = cri_server::streaming::start("127.0.0.1:0", backend.clone()).await?;
let service = CriService::new(backend).with_streaming(streaming);
cri_server::uds::serve("unix:///run/my-cri.sock", service, shutdown).await?;
```

This crate must not depend on any other rusternetes crate; it is intended for
eventual crates.io publication. The streaming server is forked from the aurae
project's `cri` branch (`auraed/src/cri/streaming.rs`, Apache-2.0) —
provenance headers are kept in `src/streaming/`.

## The `memory-cri` example and `MemoryBackend`

`MemoryBackend` (feature `testing`) is an in-memory state-machine backend: no
real workloads run, but the CRI state machine, log files (via a tiny scripted
command language: `echo`, `sh -c 'echo …; sleep …'`, `while true; do echo …`
loops), scripted exec/attach, and localhost port-forward dialing are honest.
It doubles as the contract-test vehicle for other backends (plan 05).

```bash
cargo run -p cri-server --features testing --example memory_cri -- \
    --listen unix:///tmp/memory-cri.sock --streaming-bind 127.0.0.1:0
crictl -r unix:///tmp/memory-cri.sock info
```

## critest coverage (plan 01-S4)

critest **v1.36.0** runs against `memory-cri` with this focus set — this
proves the harness plumbing (RPC surface, streaming URLs + SPDY protocol, log
files) before any real backend exists:

```bash
critest --runtime-endpoint unix:///tmp/memory-cri.sock \
        --image-endpoint   unix:///tmp/memory-cri.sock \
        --ginkgo.focus 'runtime info|PodSandbox|Container|Streaming' \
        --ginkgo.skip  'volume and device|portforward|Mount Propagation|Mount Readonly|OOM|sysctls|should support network|should support container log'
```

Status (2026-07-17): **33/33 specs pass on Linux** (lima VM, arm64),
**31/31 on macOS** (darwin-arm64). Covered: runtime info/conditions, the full
PodSandbox lifecycle, container lifecycle incl. filters and forced remove,
execSync (output, exit codes, timeout → `DeadlineExceeded`), container log
files incl. `ReopenContainerLog` rotation (critest's CRI log parser validates
`logfmt`), and Streaming exec (tty + non-tty) and attach over SPDY.

Skipped groups need real workloads or a real kernel/network and are the
responsibility of real backends (bollard-cri plan 03 runs the **full** suite):

| Skip | Why a state-machine fake cannot honestly pass it |
|---|---|
| `volume and device` | asserts on files inside real mounts |
| `portforward`, `should support network` | curls the pod IP / a real web server |
| `Mount Propagation`, `Mount Readonly` | kernel mount namespaces |
| `OOM` | real cgroup OOM kill |
| `sysctls` | reads sysctls inside a real namespace |
| `should support container log` (multi-container) | asserts on the real httpd image's own log output |
| Image Manager suite | pulls from a real registry and asserts image metadata |

The Exec/Attach/PortForward RPCs themselves are covered by unit tests and by
manual crictl acceptance (see plan 01 Status), including port-forward
end-to-end.
