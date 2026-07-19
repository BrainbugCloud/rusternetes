# Task: K5 — Exec/Attach/PortForward Streaming

## Context

The kubelet CRI migration is nearly complete. K1-K4 and K6 are done. K5 streaming is the final piece.

**Current state:**
- ✅ K1: Inventory + scaffolding
- ✅ K2: Sandbox + container lifecycle on CRI
- ✅ K3: Statuses, init/ephemeral containers, GC
- ✅ K4: Logs pipeline
- ⚠️ K5: Partial — ExecSync for probes/hooks done, but streaming proxy missing
- ✅ K6: Stats, eviction, bollard removal complete

**What's missing in K5:**
1. **Kubelet streaming server** — kubelet needs to proxy exec/attach/portforward requests to the CRI runtime's streaming URLs
2. **API-server websocket⇄SPDY translation** — api-server receives websocket from kubectl, needs to translate to SPDY for kubelet

## Acceptance Criteria

- `kubectl exec pod -- sh -c 'echo hi; exit 3'` prints `hi` and exits 3
- `kubectl exec -it pod -- sh` works (interactive terminal)
- `kubectl port-forward pod 8080:80` works
- Zero `close 1005` errors in a 50-exec loop test
- All existing tests pass

## Implementation Plan

### Phase 1: Kubelet Streaming Server

The kubelet needs a streaming server that:
1. Receives exec/attach/portforward requests from api-server
2. Calls CRI runtime's `Exec()` / `Attach()` / `PortForward()` RPCs to get streaming URLs
3. Proxies the connection between api-server and runtime

**Key files:**
- `crates/kubelet/src/server.rs` — add streaming server (SPDY endpoints)
- `crates/kubelet/src/cri.rs` — add methods to call CRI streaming RPCs
- `crates/kubelet/src/runtime.rs` — wire streaming server to ContainerRuntime

**Reference:**
- `crates/cri-server/src/streaming.rs` — has SPDY server implementation
- `crates/bollard-cri/src/streaming.rs` — shows how CRI runtime implements streaming
- Look at how kubelet currently handles exec in `crates/kubelet/src/runtime.rs` (around line 5428 for ExecSync)

### Phase 2: API-Server Websocket⇄SPDY Translation

The api-server already has SPDY infrastructure (`spdy.rs`, `streaming.rs`, `spdy_handlers.rs`).

**What's needed:**
1. **Pod exec/attach/portforward endpoints** in api-server
   - `/api/v1/namespaces/{ns}/pods/{name}/exec`
   - `/api/v1/namespaces/{ns}/pods/{name}/attach`
   - `/api/v1/namespaces/{ns}/pods/{name}/portforward`
2. **Websocket receiver** — kubectl sends websocket
3. **SPDY sender** — translate to SPDY and forward to kubelet
4. **Bidirectional proxy** — shuttle data between websocket and SPDY

**Key files:**
- `crates/api-server/src/handlers/pod_subresources.rs` — add exec/attach/portforward handlers
- `crates/api-server/src/spdy.rs` — SPDY client to connect to kubelet
- `crates/api-server/src/streaming.rs` — websocket handling

**Reference:**
- Look at existing SPDY code in api-server
- Kubernetes api-server exec/attach implementation (reference architecture)

### Phase 3: E2E Testing with Privileged Pod

**CRITICAL: Use a privileged pod for testing**

The test environment needs:
1. **Privileged pod on bb-k8s cluster**
   - Namespace: `agent-sandbox-system`
   - Node: any Ready arm64 node
   - SecurityContext: `privileged: true`
   - Containerd socket mounted or Docker-in-Docker

2. **Deploy rusternetes binaries**
   - Build all binaries: `cargo build --release`
   - Copy binaries into the pod
   - Start api-server, scheduler, controller-manager, kubelet

3. **Test scenarios:**
   ```bash
   # Basic exec
   kubectl exec test-pod -- sh -c 'echo hi; exit 3'
   # Expected: prints "hi", exits 3
   
   # Interactive exec
   kubectl exec -it test-pod -- sh
   # Expected: interactive shell
   
   # Port forward
   kubectl port-forward test-pod 8080:80
   # Expected: forwards to container's port 80
   
   # Stress test
   for i in {1..50}; do kubectl exec test-pod -- echo $i; done
   # Expected: no "close 1005" errors
   ```

**How to create privileged pod:**
```bash
# Create privileged pod with containerd
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: rusternetes-test
  namespace: agent-sandbox-system
spec:
  nodeName: bb-k8s-rk1b-01  # or any Ready arm64 node
  containers:
  - name: test
    image: docker:28-dind
    securityContext:
      privileged: true
    volumeMounts:
    - name: dind-storage
      mountPath: /var/lib/docker
  volumes:
  - name: dind-storage
    emptyDir: {}
EOF

# Copy binaries into pod
kubectl cp target/release/rusternetes agent-sandbox-system/rusternetes-test:/usr/local/bin/
kubectl cp target/release/rusternetes-kubelet agent-sandbox-system/rusternetes-test:/usr/local/bin/
# ... etc

# Start services inside pod
kubectl exec -it rusternetes-test -- /bin/sh
# Inside pod: start containerd, then rusternetes components
```

## Build Environment

```bash
export PATH="$HOME/.npm-global/bin:$HOME/.local/bin:$PATH"
export LIBCLANG_PATH="$HOME/.local/libclang/usr/lib/llvm-19/lib"
export PROTOC="$HOME/.local/bin/protoc"
export PROTOC_INCLUDE="$HOME/.local/include"
```

## Working Approach

1. **Read existing code first**
   - Understand current kubelet exec flow (ExecSync for probes)
   - Read api-server SPDY infrastructure
   - Read CRI streaming RPCs in cri-proto

2. **Implement kubelet streaming server**
   - Add SPDY endpoints for exec/attach/portforward
   - Call CRI runtime to get streaming URLs
   - Proxy connections

3. **Implement api-server websocket⇄SPDY**
   - Add exec/attach/portforward endpoints
   - Receive websocket from kubectl
   - Translate to SPDY for kubelet
   - Bidirectional proxy

4. **Build and iterate**
   - `cargo build -p rusternetes-kubelet`
   - `cargo build -p rusternetes-api-server`
   - Fix compilation errors

5. **E2E test with privileged pod**
   - Create privileged pod
   - Deploy binaries
   - Run test scenarios
   - Fix issues

6. **Commit after each phase**

## Key Design Decisions

1. **Kubelet streaming server uses SPDY** — same protocol as upstream kubelet
2. **CRI runtime provides streaming URLs** — kubelet proxies, doesn't implement streaming itself
3. **API-server translates websocket⇄SPDY** — kubectl uses websocket, kubelet uses SPDY
4. **Reuse existing SPDY infrastructure** — api-server already has spdy.rs

## Important Notes

- **Use privileged pod for testing** — this is critical for e2e validation
- **ExecSync is already done** — don't duplicate it, focus on streaming
- **SPDY 3.1 protocol** — check cri-server for implementation details
- **Close frames matter** — ensure proper websocket/SPDY close to avoid "close 1005"
- **Interactive terminal needs TTY** — pass `tty: true` in CRI Exec/Attach requests
- **PortForward uses setns** — for localhost-only servers, need to enter pod network namespace

## Verification

After implementation:
```bash
# Build check
cargo build -p rusternetes-kubelet
cargo build -p rusternetes-api-server
cargo test -p rusternetes-kubelet
cargo test -p rusternetes-api-server

# E2E in privileged pod
kubectl exec test-pod -- sh -c 'echo hi; exit 3'  # Should print "hi", exit 3
kubectl exec -it test-pod -- sh                     # Interactive shell
kubectl port-forward test-pod 8080:80               # Port forward works
for i in {1..50}; do kubectl exec test-pod -- echo $i; done  # No close 1005
```

## Commit Strategy

- Commit after Phase 1 (kubelet streaming server)
- Commit after Phase 2 (api-server websocket⇄SPDY)
- Final commit after Phase 3 (e2e validation passes)
