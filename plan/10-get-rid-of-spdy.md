# Plan 10 — Get rid of SPDY (eventually)

Kubernetes is migrating exec/attach/port-forward streaming from **SPDY/3.1** to
**WebSocket** (KEP-4006). SPDY has been deprecated ~8 years and was never
standardized. We want rusternetes to follow — but the runtimes we target are
**not ready to drop SPDY yet**, so this plan makes SPDY a **default-on,
disable-able crate feature** we can deprecate and remove once mainstream runtimes
support WebSocket streaming end to end.

This is the long-tail follow-up to [02-K5](02-kubelet-cri-only.md) (exec/attach/
portforward streaming) and depends on the `cri-server` SPDY codec from
[01](01-cri-crates.md).

## Status

- [ ] G1 — SPDY behind a `spdy` cargo feature in `cri-server` (default on)
- [ ] G2 — api-server + kubelet consume the gated codec; build with `--no-default-features` still compiles (SPDY-less skeleton)
- [ ] G3 — WebSocket streaming *client* path (behind a `websocket` feature) + per-runtime transport selection
- [ ] G4 — our own shims (`bollard-cri`, `apple-cri`) serve WebSocket streaming natively (conmon-rs model)
- [ ] G5 — deprecation gate: flip `spdy` to opt-in once containerd ships working WS; remove when no target needs it

## What we learned (2026-07-19, evidence-backed)

The streaming path has three legs; the migration does **not** treat them alike:

```
kubectl ──WS──▶ api-server ──?──▶ kubelet ──?──▶ CRI runtime (containerd / CRI-O / our shim)
        (v5, done)            (leg A)      (leg B, the SPDY problem)
```

- **Client legs (kubectl↔apiserver↔kubelet) → WebSocket.** kubectl defaults to
  WebSocket since Kubernetes **1.31**; the `ExtendWebSocketsToKubelet` gate is
  **beta/default-on in 1.36** (our pinned line). These legs are ours to control
  and should be WebSocket.
- **Runtime leg (kubelet↔runtime) → deliberately stays SPDY.** KEP-4006 states
  the migration **won't** transition the on-node kubelet↔runtime leg — SPDY's
  proxy-incompatibility problem doesn't apply node-locally, so there's little
  upstream pressure to move it.

### Runtime readiness — none are WebSocket-ready out of the box

| Runtime | CRI streaming transport | Notes / evidence |
|---|---|---|
| **containerd v2.1.3** (our config #1) | **SPDY only** | Empirically tested: `crictl exec --transport spdy` ✅; `--transport websocket` → `403 Forbidden / websocket: bad handshake`. WS code path is present but non-functional across 1.7/2.0/2.1 — containerd issue [#11136](https://github.com/containerd/containerd/issues/11136) (Stale). `SupportedRemoteCommandProtocols` config is **SPDY-only** (selects SPDY sub-protocols). No near-term fix. |
| **CRI-O ≥ v1.34.0** | **WebSocket for exec/attach** (opt-in) | `stream_websockets = true` (default `false`) + **conmon-rs** (`runtime_type = "pod"`). **port-forward still not WS** ("will be supported in future releases"). New (Sept 2025); has v5-compat rough edges (k8s [PR #133448](https://github.com/kubernetes/kubernetes/pull/133448)). |
| **`bollard-cri`** (ours) | SPDY today (via `cri-server`) | We control it → can serve WebSocket natively later (G4). |
| **`apple-cri`** (ours) | SPDY (planned, via `cri-server`) | Same — WebSocket-native is a choice we own. |

**Conclusion:** SPDY is unavoidable *today* for a general client — containerd
forces it, and even CRI-O needs SPDY for port-forward. So we keep a SPDY client,
but isolate it so it can be switched off and later removed.

## Strategy — SPDY as a deprecate-able feature

1. **Finish the SPDY client now** (the immediate K5 work): reuse
   `cri-server::streaming::spdy` (correct SPDY/3.1 zlib dictionary + `SpdyWriter`
   + `read_frame`) instead of the incomplete hand-rolled copy in
   `crates/api-server/src/streaming.rs`. This unblocks conformance on containerd.
2. **Gate SPDY behind a cargo feature** `spdy` in `cri-server`, **enabled by
   default**. All SPDY framing, the streaming server's SPDY upgrade handling, and
   the client codec compile only with `spdy`. Consumers (`api-server`, `kubelet`,
   `bollard-cri`, `apple-cri`) inherit it by default but can opt out.
3. **Add a WebSocket streaming path** behind a `websocket` feature (client for
   the kubelet↔runtime leg; server for our shims). Use a mature stack
   (`tokio-tungstenite`) — no hand-rolled protocol.
4. **Per-runtime transport negotiation.** The kubelet picks the transport from
   what the runtime offers: send a WebSocket upgrade first, fall back to SPDY on
   `403`/handshake failure (mirrors kubectl's own SPDY fallback). Default order:
   WebSocket → SPDY while `spdy` is enabled.
5. **Deprecation ladder** (G5): once containerd ships a working WS streaming
   server and CRI-O covers port-forward, flip `spdy` to **opt-in** (off by
   default), emit a deprecation warning when it's used, then **remove** the
   feature and the `cri-server` SPDY module when no supported target needs it.

## Design notes

- **Feature location:** SPDY lives entirely in `cri-server` (`streaming/spdy.rs`,
  the SPDY branch of `streaming/mod.rs`, and the SPDY client helpers). The
  `spdy` feature gates that module and any `pub` re-exports. `cri-server` with
  `--no-default-features` must still build the WebSocket + non-streaming surface.
- **No SPDY in `api-server`/`kubelet` source of its own** — they must not
  re-hand-roll SPDY (the current `api-server/src/streaming.rs` codec is deleted
  in favour of the `cri-server` one). This keeps the one unavoidable protocol in
  a single, tested place, so deprecating it is a feature-flag flip, not a
  cross-crate hunt.
- **CI:** add a `--no-default-features` build of `cri-server`/`api-server`/
  `kubelet` (SPDY off) to guarantee the code stays cleanly separable, even before
  a WebSocket runtime path exists.
- **conmon-rs is the proof point** for G4: a Rust WebSocket CRI streaming server
  already exists (CRI-O), so `bollard-cri`/`apple-cri` serving WebSocket is a
  known-good pattern, not research.

## Stages

### G1 — `spdy` feature in `cri-server` (default on)
- Move all SPDY code under `#[cfg(feature = "spdy")]`; add `spdy` to
  `cri-server`'s default features. `cargo build -p cri-server --no-default-features`
  compiles (no SPDY symbols).
- **Acceptance:** both builds green; `spdy`-on is behaviourally identical to today.

### G2 — consumers use the gated codec
- Delete `api-server/src/streaming.rs`'s hand-rolled SPDY; depend on
  `cri-server` (`spdy` feature) for the client codec. kubelet likewise.
- **Acceptance:** exec/attach/portforward work on containerd (config #1) via the
  `cri-server` codec; `--no-default-features` skeleton compiles.

### G3 — WebSocket client path + negotiation
- `websocket` feature: `tokio-tungstenite` client for the kubelet↔runtime leg;
  transport selection (WS first, SPDY fallback while enabled).
- **Acceptance:** against CRI-O ≥1.34 (`stream_websockets=true`), exec/attach run
  over WebSocket with **zero** SPDY frames; containerd still works via fallback.

### G4 — our shims serve WebSocket
- `bollard-cri` (and later `apple-cri`) expose a WebSocket streaming server
  (conmon-rs model), selectable via `cri-server`.
- **Acceptance:** kubelet→shim exec/attach/portforward over WebSocket; SPDY path
  still available behind the feature.

### G5 — deprecate + remove
- Flip `spdy` to opt-in once containerd WS works and CRI-O covers port-forward;
  warn on use; remove the module when no supported target needs it.
- **Acceptance:** default build has no SPDY; all matrix runtimes pass conformance
  over WebSocket.

## Decision log

| # | Decision | Rationale |
|---|---|---|
| G-D1 | Keep a SPDY client now; don't wait for runtimes | containerd is SPDY-only with no near-term fix; conformance can't wait |
| G-D2 | SPDY is a `cri-server` cargo feature, **default on** | One tested implementation; deprecation becomes a flag flip, not a rewrite |
| G-D3 | No hand-rolled SPDY in api-server/kubelet | Killed the duplicate incomplete codec; single source of truth |
| G-D4 | WebSocket-first with SPDY fallback per runtime | Matches kubectl's own fallback; lets CRI-O/our shims skip SPDY while containerd keeps working |
| G-D5 | Our shims go WebSocket-native (conmon-rs model) | Shrinks the SPDY blast radius to containerd only |

## Risks / open items

- **v5-over-WebSocket compatibility** is still settling upstream (CRI-O PR
  churn); pin protocol versions per leg and test against the specific runtime.
- **port-forward over WebSocket** lags exec/attach on CRI-O — SPDY stays required
  for portforward until that lands.
- **Don't strand containerd users** — SPDY must remain default-on until
  containerd's WS streaming server is real (track [#11136](https://github.com/containerd/containerd/issues/11136)).
