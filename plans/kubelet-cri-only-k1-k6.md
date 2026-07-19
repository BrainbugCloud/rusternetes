# Task: K1–K6 — Kubelet hard cutover to CRI-only

## Context

Rewrite the kubelet's runtime layer from bollard (Docker API) to CRI client.
The kubelet currently uses bollard directly in `crates/kubelet/src/runtime.rs`
(~11.6k lines), plus two bypass sites (exec in main.rs, stats in eviction.rs).

After this work:
- `grep -r bollard crates/kubelet/` → empty
- Kubelet talks CRI to containerd (or any CRI runtime)
- All existing functionality preserved via CRI mapping

Reference: `plan/02-kubelet-cri-only.md` for full design.

## Stages

### K1 — Inventory + scaffolding
- Add `cri-proto` + `cri-server` deps to kubelet crate
- Add `--container-runtime-endpoint` / `--image-service-endpoint` config
- Produce inventory: every bollard call site mapped to CRI replacement
- **Acceptance:** crate compiles green; inventory doc reviewed

### K2 — Sandbox + container lifecycle on CRI
- Rewrite `start_pod`, `stop_pod_*`, container create/start/stop/remove
- `RuntimeState` caches (sandbox-id, container-id) rebuilt from ListPodSandbox/ListContainers on startup
- `get_pod_ip` from `PodSandboxStatus.network.ip`
- **Acceptance:** single-node rusternetes with kubelet→containerd runs `kubectl run nginx` → Running with real IP; `kubectl delete pod` cleans up

### K3 — Statuses, init/ephemeral containers, GC
- `get_container_statuses` / init / ephemeral / GC on CRI types
- containerID prefixes from `Version()` (containerd://...)
- **Acceptance:** init containers run in order; `kubectl get pod -o yaml` shows correct statuses

### K4 — Logs pipeline end to end
- Wire `log_directory`/`log_path` in PodSandboxConfig/ContainerConfig
- Kubelet `/containerLogs/...` endpoint with follow/tail/since/timestamps/previous via `CriLogReader`
- api-server `log` subresource → kubelet proxy (delete `generate_pod_logs` fallback)
- Node-proxy routes + AuthContext fix
- **Acceptance:** `kubectl logs` returns real stdout; `kubectl logs -f` streams; `--previous` works

### K5 — Exec / attach / portforward streaming
- CRI Exec/Attach/PortForward + kubelet proxy
- api-server websocket⇄SPDY translation with proper close frames
- Delete `main.rs` `handle_exec`
- Probes/hooks via ExecSync
- **Acceptance:** `kubectl exec pod -- sh -c 'echo hi; exit 3'` prints `hi` and exits 3; `kubectl exec -it` works; `kubectl port-forward` works; zero `close 1005` in 50-exec loop

### K6 — Stats, eviction, bollard removal
- `collect_node_metrics` + `eviction.rs` on ListContainerStats/ImageFsInfo
- Remove `bollard` from kubelet Cargo.toml
- Remove dead Docker-volume code paths
- **Acceptance:** `grep -r bollard crates/kubelet/` → empty; crate green

## Build Environment

```bash
export PATH="$HOME/.npm-global/bin:$HOME/.local/bin:$PATH"
export LIBCLANG_PATH="$HOME/.local/libclang/usr/lib/llvm-19/lib"
export PROTOC="$HOME/.local/bin/protoc"
export PROTOC_INCLUDE="$HOME/.local/include"
cargo build -p rusternetes-kubelet
```

Note: `openssl-sys` with `vendored` feature is already configured in the workspace.
The `kubectl` crate was switched to `rustls-tls` (no native-tls).

## Test Environment

We're in a non-privileged container on the bb-k8s Talos cluster.
For end-to-end testing, use a **privileged pod with containerd** (same pattern as bollard-cri):

```bash
# Privileged pod with containerd + rusternetes binaries
# Namespace: agent-sandbox-system
# Node: bb-k8s-rk1b-01 (or any Ready arm64 node)
```

The test pod needs:
- containerd running (or Docker which provides containerd)
- All rusternetes binaries (api-server, scheduler, controller-manager, kubelet)
- kubectl for verification
- CRI socket at `/run/containerd/containerd.sock`

For conformance (K7, later): sonobuoy certified-conformance.

## Key Design Decisions

1. **CRI client, not bollard** — `RuntimeServiceClient<Channel>` + `ImageServiceClient<Channel>` from `cri-proto`
2. **Sandbox = pause container** — runtime's job, not ours. We just call RunPodSandbox/StopPodSandbox
3. **Labels are source of truth** — sandbox-id, container metadata in CRI labels
4. **No CNI from kubelet** — runtime owns sandbox networking; trust PodSandboxStatus.network.ip
5. **Logs via CRI files** — runtime writes CRI-format logs; kubelet reads them
6. **Streaming via SPDY** — kubelet proxies exec/attach/portforward to runtime's streaming URL

## Mapping Table (bollard → CRI)

| bollard | CRI |
|---------|-----|
| `start_pause_container` | `RunPodSandbox(PodSandboxConfig)` |
| `create_container` + namespace modes | `CreateContainer(sandbox_id, ContainerConfig, sandbox_config)` |
| `start/stop/remove_container` | `StartContainer/StopContainer/RemoveContainer` |
| `inspect_container` for status | `ContainerStatus/PodSandboxStatus` |
| Pod IP from pause `network_settings` | `PodSandboxStatus.network.ip` |
| `inspect_image/create_image` | `ImageStatus/PullImage` |
| `docker.logs` | Read CRI log files via `CriLogReader` |
| `create_exec/start_exec/inspect_exec` | `ExecSync(container_id, cmd, timeout)` |
| exec endpoint in main.rs | `Exec` RPC → proxy streaming URL |
| `docker.stats` | `ContainerStats/ListContainerStats` |
| `update_container` | `UpdateContainerResources` |
| `download_from_container` | No CRI equiv — read CRI log file or host path |
| `create/list/remove_volume` | Delete — volumes are kubelet-side bind mounts |
| `docker://{id}` in containerID | `{runtime_name}://{id}` from `Version()` |

## Reference Implementations

- bollard-cri (just completed): `crates/bollard-cri/src/` — shows CRI server side
- cri-server crate: `crates/cri-server/src/` — has StreamingBackend, logfmt, checkpoint
- cri-proto crate: `crates/cri-proto/src/` — generated CRI types
- aurae CRI: `~/git/aurae` branch `cri` — `auraed/src/cri/` for patterns

## Working Approach

1. Start with K1: add deps, config, produce inventory
2. Implement K2: sandbox + container lifecycle (biggest change)
3. K3: statuses, init containers, GC
4. K4: logs pipeline (kills ~53 conformance failures)
5. K5: streaming (kills ~17 failures)
6. K6: stats, eviction, delete bollard
7. Build and test iteratively

Commit after each stage. Use `cargo build -p rusternetes-kubelet` for fast iteration.
For e2e testing, build all binaries and run in the privileged pod.

## Acceptance Criteria

### K1 — Inventory + scaffolding
- [x] `cri-proto` + `cri-server` deps added to the kubelet crate
- [x] `--container-runtime-endpoint` / `--image-service-endpoint` config added
- [x] bollard→CRI inventory produced (mapping table above)
- [x] Kubelet crate compiles green

### K2 — Sandbox + container lifecycle on CRI
- [x] `start_pod` / `stop_pod_*` / container create/start/stop/remove rewritten on CRI
- [x] `RuntimeState` caches rebuilt from `ListPodSandbox` / `ListContainers`
- [x] `get_pod_ip` derived from `PodSandboxStatus.network.ip`

### K3 — Statuses, init/ephemeral containers, GC
- [x] `get_container_statuses` / init / ephemeral / GC implemented on CRI types
- [x] containerID prefixes taken from `Version()` (`containerd://…`)

### K4 — Logs pipeline end to end
- [x] `log_directory` / `log_path` wired into `PodSandboxConfig` / `ContainerConfig`
- [x] Kubelet `/containerLogs/…` endpoint with follow / tailLines / sinceSeconds / sinceTime / timestamps / previous / limitBytes via the CRI log reader
- [x] api-server `log` subresource proxies to the kubelet; `generate_pod_logs` fallback deleted

### K5 — Exec / attach / portforward streaming
- [x] Probes and lifecycle hooks execute via CRI `ExecSync` (no bollard)
- [x] `main.rs` `handle_exec` (bollard Docker exec) deleted
- [x] Kubelet proxy of CRI `Exec` / `Attach` / `PortForward` streaming URLs
- [x] api-server websocket⇄SPDY translation with proper close frames

### K6 — Stats, eviction, bollard removal
- [x] `collect_node_metrics` reimplemented on CRI `ListContainerStats`
- [x] `eviction.rs` pod stats reimplemented on CRI `ListContainerStats` (working-set memory + writable-layer disk)
- [x] `bollard` removed from the kubelet `Cargo.toml`
- [x] `grep -r bollard crates/kubelet/` → empty; crate compiles green and unit tests pass

### K7 — End-to-end validation in privileged pod on rk1 node

> Build and E2E test in a privileged pod on an rk1 node with containerd access.

**Steps:**

1. **Build all rusternetes binaries** (from build machine):
   ```bash
   cd ~/git/rusternetes
   export PROTOC="$HOME/.local/bin/protoc"
   export PROTOC_INCLUDE="$HOME/.local/include"
   cargo build --release -p rusternetes -p rusternetes-kubelet -p rusternetes-api-server -p rusternetes-scheduler -p rusternetes-controller-manager -p rusternetes-kubectl
   ```
   Strip to reduce size: `strip target/release/rusternetes`

2. **Create privileged pod on rk1 node:**
   ```bash
   kubectl apply -f - << 'EOF'
   apiVersion: v1
   kind: Pod
   metadata:
     name: rusternetes-e2e
     namespace: agent-sandbox-system
   spec:
     nodeName: bb-k8s-rk1b-01   # any rk1 node (NEVER cm4 — out of memory)
     containers:
     - name: rusternetes
       image: ubuntu:24.04
       securityContext:
         privileged: true
       command: ["sleep", "infinity"]
       volumeMounts:
       - name: binaries
         mountPath: /opt/rusternetes
       - name: run
         mountPath: /host-run
     volumes:
     - name: binaries
       emptyDir: {}
     - name: run
       hostPath:
         path: /run
   EOF
   ```

3. **Copy binary and start rusternetes (all-in-one mode):**
   ```bash
   # On build machine:
   kubectl cp target/release/rusternetes agent-sandbox-system/rusternetes-e2e:/opt/rusternetes/

   # Inside pod:
   export CONTAINER_RUNTIME_ENDPOINT=unix:///host-run/containerd/containerd.sock
   export IMAGE_SERVICE_ENDPOINT=unix:///host-run/containerd/containerd.sock
   mkdir -p /tmp/rusternetes-data /tmp/rusternetes-volumes
   /opt/rusternetes/rusternetes \
     --storage-backend sqlite \
     --data-dir /tmp/rusternetes-data/rusternetes.db \
     --node-name node-1 \
     --volume-dir /tmp/rusternetes-volumes \
     --bind-address 0.0.0.0:6443
   ```
   **CRITICAL:** Use `--data-dir /path/to/file.db` (full file path), NOT a directory.
   If `--data-dir` points to a directory, SQLite returns "unable to open database file".

4. **Create a test client pod** that connects to the test pod:
   ```bash
   RUSTERNETES_IP=$(kubectl get pod -n agent-sandbox-system rusternetes-e2e -o jsonpath='{.status.podIP}')
   kubectl apply -f - << EOF
   apiVersion: v1
   kind: Pod
   metadata:
     name: e2e-client
     namespace: agent-sandbox-system
   spec:
     containers:
     - name: client
       image: bitnami/kubectl:latest
       command: ["sleep", "infinity"]
   EOF
   kubectl exec -n agent-sandbox-system -it pod/e2e-client -- env RUSTERNETES_IP=$RUSTERNETES_IP bash
   ```

   Then inside the client pod:
   ```bash
   export RUSTERNETES_IP=10.x.x.x   # test pod IP
   export KUBE_CONFIG_FILE=/dev/null
   kubectl --server=https://$RUSTERNETES_IP:6443 --insecure-skip-tls-verify cluster-info
   ```

5. **Run tests:**
   ```bash
   # Test 1: Create a deployment
   kubectl --server=https://$RUSTERNETES_IP:6443 --insecure-skip-tls-verify run nginx --image=nginx:latest --replicas=1

   # Test 2: Verify pod Running
   kubectl --server=https://$RUSTERNETES_IP:6443 --insecure-skip-tls-verify get pods

   # Test 3: kubectl logs
   kubectl --server=https://$RUSTERNETES_IP:6443 --insecure-skip-tls-verify logs deploy/nginx

   # Test 4: kubectl exec (basic)
   kubectl --server=https://$RUSTERNETES_IP:6443 --insecure-skip-tls-verify exec deploy/nginx -- sh -c 'echo hi; exit 3'
   # Expected: prints "hi", exit code 3

   # Test 5: 50-exec loop — check for close 1005 errors
   for i in $(seq 1 50); do
     kubectl --server=https://$RUSTERNETES_IP:6443 --insecure-skip-tls-verify exec deploy/nginx -- sh -c "echo $i"
   done

   # Test 6: kubectl delete
   kubectl --server=https://$RUSTERNETES_IP:6443 --insecure-skip-tls-verify delete deploy/nginx
   ```

- **Acceptance:**
  - [ ] `kubectl run nginx` → pod Running with real IP
  - [ ] `kubectl get pods` → shows correct statuses (Running, Pending, etc.)
  - [ ] `kubectl logs` → returns real stdout from container
  - [ ] `kubectl exec pod -- sh -c 'echo hi; exit 3'` → prints `hi`, exit code 3
  - [ ] `kubectl port-forward pod 8080:80` → forwards to container's port 80
  - [ ] 50-exec loop → zero `close 1005` errors in logs
  - [ ] `kubectl delete pod` → proper cleanup

## Important Notes

- Don't delete the `cni/` subtree yet — it's unused on CRI path but removal is follow-up
- Keep the ~20-method public surface of `ContainerRuntime` that callers use
- The api-server's `generate_pod_logs` fallback must be deleted (K4) — errors must surface as errors
- The websocket⇄SPDY translation in api-server is the trickiest new code (K5)
- 50-exec loop is the regression gate for `close 1005` class
