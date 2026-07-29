# Task: K5 — Complete Bollard Removal from API-Server / Exec-Attach-PortForward via Kubelet

## Context

The kubelet CRI migration removes bollard (Docker API) from the exec/attach/portforward path and replaces it with proper kubelet proxying matching upstream Kubernetes. The kubelet streaming server is done. This task completes the api-server side.

**What's done:**
- `crates/kubelet/src/streaming_server.rs` — TCP-based SPDY proxy server that receives requests from api-server, calls CRI gRPC, and relays to CRI runtime streaming URLs
- `crates/api-server/src/kubelet_proxy.rs` — `kubelet_streaming_endpoint()`, `spdy_proxy_response()` for SPDY relay to kubelet
- `crates/api-server/src/handlers/pod_subresources.rs` — exec handler rewritten to use kubelet proxy (SPDY path + HTTP proxy fallback)
- `crates/api-server/src/streaming.rs` — added `handle_exec_websocket_via_kubelet()` stub (simple HTTP proxy, non-interactive only)

**What remains:**

1. **Rewrite attach handler** (`pod_subresources.rs`) — same pattern as exec: resolve kubelet endpoint, proxy SPDY, HTTP fallback
2. **Rewrite portforward handler** (`pod_subresources.rs`) — same pattern
3. **Remove bollard from `spdy_handlers.rs`** — `handle_spdy_exec()` currently calls bollard/Docker directly, should proxy to kubelet
4. **Remove bollard from `streaming.rs`** — `handle_exec_websocket()` and `handle_exec_websocket_with_protocol()` call bollard. The stub `handle_exec_websocket_via_kubelet()` exists but is non-interactive. Need proper WS↔SPDY translation.
5. **Remove bollard dependency** from `crates/api-server/Cargo.toml`
6. **Clean up unused imports** after bollard removal

## Acceptance Criteria

- [x] The `attach` handler proxies to the kubelet streaming server (SPDY relay, WebSocket via `handle_attach_websocket_via_kubelet`, and plain-HTTP fallback) with no bollard calls.
- [x] The `portforward` handler proxies to the kubelet streaming server (SPDY relay, WebSocket via `handle_portforward_websocket_via_kubelet`, and plain-HTTP fallback) with no bollard calls.
- [x] The bollard-based `spdy_handlers` module is removed from the api-server (module file deleted and its `mod` declarations removed from `lib.rs` and `main.rs`).
- [x] The WebSocket exec/attach/port-forward handlers in `streaming.rs` translate between WS `channel.k8s.io` frames and kubelet SPDY streams instead of calling bollard.
- [x] The `bollard` dependency is removed from `crates/api-server/Cargo.toml` and no `bollard` references remain under `crates/api-server/src/`.
- [x] The `rusternetes-api-server` crate compiles cleanly (`cargo check`) with no errors and no unused-import/dead-code warnings.

## Reference: Upstream Kubernetes Architecture

```
kubectl ──WS/SPDY──▶ api-server ──SPDY──▶ kubelet:10250 ──SPDY──▶ CRI runtime
```

- **Upstream apiserver proxy**: `staging/src/k8s.io/apiserver/pkg/util/proxy/upgradeaware.go`
  - `NewUpgradeAwareHandler()` creates a reverse proxy that handles HTTP Upgrade (SPDY)
  - `tryUpgrade()` detects upgrade headers, hijacks both connections, relays bytes bidirectionally
- **Upstream streaming translator**: `staging/src/k8s.io/apiserver/pkg/endpoints/handlers/streamtranslator.go`
  - For plain HTTP exec: sends POST to kubelet, streams response as WebSocket channel frames

**Key insight from upstream**: The api-server is a **transparent proxy** for SPDY connections. It doesn't parse SPDY frames — it just relays raw bytes between kubectl and kubelet. No WebSocket↔SPDY translation needed when kubectl uses SPDY (default).

## Implementation Steps

### Step 1: Rewrite attach handler

File: `crates/api-server/src/handlers/pod_subresources.rs`

Copy the pattern from the exec handler (lines ~500-550):
- Resolve kubelet endpoint with `kubelet_proxy::kubelet_streaming_endpoint()`
- Build path: `/attach/{ns}/{name}/{container}?stdin=...&stdout=...&stderr=...&tty=...`
- SPDY check: `spdy::is_spdy_upgrade()` → `hyper::upgrade::on()` → `spdy_proxy_response()`
- WebSocket: `ws.on_upgrade()` → `handle_attach_websocket_via_kubelet()`
- HTTP fallback: `reqwest` GET to kubelet
- Remove the bollard block (~lines 771-836)

### Step 2: Rewrite portforward handler

Same pattern:
- Path: `/portForward/{ns}/{name}/{container}?ports=8080`
- Same SPDY/WS/HTTP proxy logic
- Remove bollard block (~lines 876-905)

### Step 3: Remove bollard from spdy_handlers.rs

File: `crates/api-server/src/spdy_handlers.rs`

The `handle_spdy_exec()` function (lines 33-140) calls bollard/Docker directly. Replace with:
- Resolve kubelet endpoint
- Open TCP to kubelet streaming port
- Send SPDY upgrade request with exec params
- Relay SPDY frames bidirectionally

OR: remove this module entirely if the pod_subresources handlers now handle all SPDY paths.

### Step 4: Fix WebSocket handler

File: `crates/api-server/src/streaming.rs`

The `handle_exec_websocket_via_kubelet()` stub only does non-interactive HTTP proxy. For interactive exec (`-it`), we need:

1. WS → SPDY translation:
   - Accept WebSocket connection from kubectl
   - Open SPDY connection to kubelet streaming port
   - Translate WS v5.channel.k8s.io frames → SPDY DATA frames:
     - WS channel 0 (stdin) → SPDY stream 1
     - WS channel 4 (resize) → SPDY stream 4
   - Translate SPDY → WS frames:
     - SPDY stream 2 → WS channel 1 (stdout)
     - SPDY stream 3 → WS channel 2 (stderr)
     - SPDY error stream → WS channel 3 (status)

2. Use `cri-server/src/streaming/spdy.rs` SPDY frame types (already `pub`):
   - `SpdyReader` for reading frames from kubelet
   - `SpdyWriter` for writing frames to kubelet

### Step 5: Remove bollard from Cargo.toml

File: `crates/api-server/Cargo.toml`

Remove `bollard` from dependencies. Check that nothing else references it:
```bash
grep -r "bollard" crates/api-server/src/
```

### Step 6: Build & verify

```bash
cd ~/git/rusternetes
export PROTOC="$HOME/.local/bin/protoc"
export PROTOC_INCLUDE="$HOME/.local/include"
cargo check 2>&1 | grep -E "error|warning: unused"
```

## Key Files Reference

| File | Purpose |
|------|---------|
| `crates/api-server/src/handlers/pod_subresources.rs` | attach/portforward handlers (lines 553-905) |
| `crates/api-server/src/spdy_handlers.rs` | bollard-based SPDY handlers (289 lines) |
| `crates/api-server/src/streaming.rs` | bollard-based WS handlers + new stub |
| `crates/api-server/src/kubelet_proxy.rs` | SPDY relay to kubelet (new) |
| `crates/api-server/src/spdy.rs` | SPDY frame types, `is_spdy_upgrade()` |
| `crates/api-server/src/spdy_upgrade.rs` | Axum SPDY upgrade extractor |
| `crates/kubelet/src/streaming_server.rs` | Kubelet SPDY streaming server (done) |
| `crates/cri-server/src/streaming/spdy.rs` | SPDY frame reader/writer (reference for WS↔SPDY) |

## Upstream Go Reference

```
/tmp/apiserver-ref/staging/src/k8s.io/apiserver/pkg/util/proxy/upgradeaware.go
/tmp/apiserver-ref/staging/src/k8s.io/apiserver/pkg/endpoints/handlers/proxy.go
/tmp/apiserver-ref/staging/src/k8s.io/apiserver/pkg/endpoints/handlers/websocket.go
```
