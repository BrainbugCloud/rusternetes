# Plan 05 — `apple-cri`: CLI → XPC migration (with regression tests)

Replace `apple-cri`'s CLI transport with the native API of apple/container,
behind the `AppleBackend` trait introduced in
[04-apple-containers-cri.md](04-apple-containers-cri.md). The trait is the
prerequisite: XPC lands as a **second impl**, proven equivalent by a shared
regression harness before it becomes the default.

## Why migrate off the CLI

- **Performance:** every probe/exec/status today is a spawned `container`
  process; a persistent XPC connection removes per-call process overhead.
- **Fidelity:** events, TTY resize, real attach, and streaming I/O exist at
  the API layer but not in the CLI.
- **The pod endgame:** true multi-container pods (shared localhost) require
  driving the Containerization layer — multiple vminitd-managed processes in
  one VM — which no CLI invocation exposes. The migration doesn't deliver
  pods-in-one-VM by itself, but it is the only path that can.

## Alternatives analysis (researched)

| Option | Description | Verdict |
|---|---|---|
| **(a) XPC direct from Rust** | Speak `container-apiserver`'s XPC protocol from Rust (`xpc-sys`/objc2 for transport). Since container 1.0.0 the **XPC interfaces are stable, versioned commitments**. Payloads are Swift-Codable-encoded — a small decoding layer must be reverse-engineered from the open-source `ContainerAPIClient`/`ContainerClient` sources. Precedent: the Orchard macOS app drives container purely via this XPC API. | **Chosen.** Stable contract, no extra daemon, pure Rust deliverable. Risk concentrated in payload encode/decode — de-risked by stage X1 spike + fixtures. |
| **(b) Swift shim daemon** | Small Swift binary linking `ContainerClient`, exposing gRPC over UDS; Rust talks tonic to it. | **Fallback** if (a)'s Codable decoding proves brittle across container releases. Costs: Swift toolchain in the build, a second daemon to supervise, an API of our own to version. Kept as the documented plan-B; the `AppleBackend` trait makes switching to it cheap. |
| **(c) Full-Rust host stack** | Virtualization.framework via objc2 bindings + gRPC-over-vsock to vminitd (protos in apple/containerization), reimplementing the host runtime. | **Rejected.** Reimplements container-apiserver/Containerization wholesale (VM lifecycle, image→block-device, vmnet). Revisit only if pods-in-one-VM becomes a hard requirement that Apple's stack won't serve. |

## Regression harness (the heart of this plan)

Equivalence is proven, not assumed. Two layers:

1. **Backend contract tests** — a test suite written once against
   `dyn AppleBackend`, run against both `CliBackend` and `XpcBackend` on the
   same machine:
   `crates/apple-cri/tests/backend_contract.rs`, parameterized by backend;
   covers every trait method with real containers (pull, lifecycle, inspect
   consistency, exec stdout/stderr/exit codes, log streams, network
   create/delete, error cases: not-found, double-stop, delete-running).
   Assertions compare **normalized results between backends**, not just
   success — for each scenario the harness runs both backends and diffs the
   normalized outcome (a golden-master between implementations).
2. **critest matrix diff** — run the full critest suite (per
   [06](06-testing-and-environments.md)) against apple-cri with
   `--backend cli` and `--backend xpc`; diff the JUnit results.
   **Gate: the XPC pass-set must be a superset of the CLI pass-set** (from
   `CRITEST-MATRIX.md`), and any new expected-fail is a spec regression that
   blocks the default flip.

CI reality: both layers need macOS + Apple silicon + `container`; they run as
a manual/self-hosted job, and are mandatory on any PR touching
`crates/apple-cri/src/backend/`.

## Stages

### X1 — XPC spike: list containers
- Minimal Rust XPC client: connect to `container-apiserver`'s mach service,
  perform the list-containers request, decode the Swift-Codable reply into the
  `AppleContainerInfo` struct from doc 04.
- Deliverables: `xpc.rs` transport module; an `XPC-PROTOCOL.md` documenting
  the observed message envelope/encoding (with references to the
  `ContainerClient` Swift sources and the versioned interface names); recorded
  raw-message fixtures for decoder unit tests.
- **Acceptance:** `cargo test -p apple-cri xpc_spike -- --ignored` on macOS
  lists the same containers as `container ls --format json` (field-level
  compare); decoder unit tests run on Linux CI from fixtures.

### X2 — read-only ops on XPC
- `inspect`, `list`, `image_list` on `XpcBackend`; contract tests for
  read-only scenarios pass against both backends.
- **Acceptance:** contract-test read-only subset green for `cli` and `xpc`;
  fixture-based decoder tests green in Linux CI.

### X3 — lifecycle ops
- `create`, `start`, `stop`, `delete`, `image_pull`, `image_delete`,
  `network_create`/`network_delete` over XPC.
- **Acceptance:** full backend contract suite green on both backends;
  cross-backend interop sanity (container created via CLI is
  visible/stoppable via XPC and vice versa — same daemon state).

### X4 — exec and streams
- XPC exec/attach with real streaming I/O and TTY resize; log streaming.
  This is where XPC surpasses the CLI: wire resize through, implement real
  attach (lifting two doc-04 expected-fail matrix rows).
- **Acceptance:** contract exec/stream scenarios green on both; critest
  `Streaming` focus on `--backend xpc` ⊇ CLI results, with previously
  expected-fail attach/resize rows flipped to pass in `CRITEST-MATRIX.md`.

### X5 — default flip
- `--backend` flag defaults to `xpc`; CLI kept as `--backend cli` escape
  hatch for one release, then removal is a separate decision.
- Full critest diff (regression harness layer 2) attached to the PR.
- **Acceptance:** XPC critest pass-set ⊇ CLI pass-set; doc-04 A5 smoke test
  (single-node rusternetes on macOS) green on the new default; README updated.

## Post-migration follow-up (out of scope, recorded)

- **Pods-in-one-VM:** with native API access, evaluate driving Containerization
  to run all of a pod's containers as separate vminitd-managed processes in
  one VM — restoring true shared-localhost pod semantics and clearing the
  multi-container deviation from doc 04. Requires investigation of whether
  container-apiserver's XPC surface exposes multi-process VMs or whether this
  needs option (b)/(c) machinery. Write a findings doc before committing.

## Risks

- Swift-Codable payload encoding may change between container majors despite
  interface versioning — fixtures + decoder tests make this loud; plan-B (b)
  documented above.
- macOS entitlements/sandbox restrictions on connecting to the XPC service
  from a non-Apple-signed binary — verify in X1 first thing; if signing is
  required, document the dev-setup (ad-hoc signing) in the crate README.
