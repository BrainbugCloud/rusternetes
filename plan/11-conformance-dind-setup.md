# Plan 11 — Sonobuoy conformance via DinD + bollard-cri on remote nodes

How to run `sonobuoy run --mode quick` against the all-in-one rusternetes binary
inside a privileged Docker-in-Docker pod on a remote K8s node, using bollard-cri
as the CRI shim.

## Why DinD?

The all-in-one binary needs:

1. **containerd socket** for CRI. Mounting the host `/run/containerd/containerd.sock`
   from Talos triggers OOM/interference — the host containerd kills the container
   when the kubelet inside lists pod sandboxes.
2. **iptables** for kube-proxy. Pod needs `privileged: true`.
3. **musl static binary** (Alpine DinD image has musl libc, no glibc).

Solution: Docker-in-Docker (`docker:28-dind`) + bollard-cri shim. The kubelet
connects to bollard-cri's Unix socket (linked as `/run/containerd/containerd.sock`),
bollard-cri translates CRI gRPC → Docker API, Docker daemon runs inside the DinD
container. Fully self-contained — no host interaction.

## Pod manifest

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: bollard-cri-conformance
  namespace: agent-sandbox-system
spec:
  nodeName: bb-k8s-rk1b-01       # rockchip node; has resources
  restartPolicy: Never
  containers:
  - name: dind
    image: docker:28-dind
    env:
    - name: DOCKER_TLS_CERTDIR
      value: ""                   # disable TLS for simplicity
    securityContext:
      privileged: true
    resources:
      requests:
        cpu: "2"
        memory: 12Gi
      limits:
        memory: 16Gi              # Docker + rusternetes + CoreDNS + sonobuoy
```

The pod must run on a node with sufficient memory. The hermès pod itself is
resource-restricted (weight 1), so builds happen locally but execution targets
rk1 nodes.

## Build: musl static binaries

```bash
export PROTOC=$HOME/.local/bin/protoc
export PROTOC_INCLUDE=$HOME/.local/include
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld
export RUSTFLAGS="-C link-self-contained=yes"
export CC_aarch64_unknown_linux_musl=$HOME/.local/musl-native/bin/aarch64-linux-musl-gcc

cargo build --release --target aarch64-unknown-linux-musl -p rusternetes
cargo build --release --target aarch64-unknown-linux-musl -p bollard-cri
cargo build --release --target aarch64-unknown-linux-musl -p rusternetes-kubectl
```

Build time on low-CPU hermès pod: ~42 min for `rusternetes`, ~3 min for
`bollard-cri` (incremental).

## bollard-cri mount path fix (critical)

**Bug:** The kubelet provides volume mount paths relative to its working
directory (e.g., `./data/volumes/coredns/config-volume`). bollard-cri passes
these directly to Docker's `HostConfig.Mounts` field, which **requires absolute
paths**. Docker returns:

```
invalid mount config for type "bind": invalid mount path:
'./data/volumes/coredns/config-volume' mount path must be absolute
```

This error was silently swallowed — bollard-cri had no error logging in the
CreateContainer handler, so the kubelet only saw "CRI CreateContainer" with no
detail.

**Fix** (committed at `1f0130d9`):

```rust
// crates/bollard-cri/src/container.rs — mount_bindings()
let host_path = std::path::absolute(std::path::Path::new(&m.host_path))
    .unwrap_or_else(|_| std::path::PathBuf::from(&m.host_path))
    .to_string_lossy()
    .into_owned();
// Use host_path instead of m.host_path in source:
```

## Startup sequence inside the pod

```bash
# 1. Start bollard-cri CRI shim
nohup /tmp/bollard-cri > /tmp/bollard-cri.log 2>&1 &

# 2. Link socket for kubelet (hardcoded path)
ln -sf /var/run/bollard-cri.sock /run/containerd/containerd.sock

# 3. Start all-in-one binary
nohup /tmp/rusternetes --log-level info --skip-auth \
  --kubernetes-service-host 10.96.0.1 \
  > /tmp/rusternetes.log 2>&1 &

# 4. Bootstrap cluster
kubectl --server http://127.0.0.1:6443 --insecure-skip-tls-verify \
  apply -f bootstrap-cluster.yaml

# 5. Create kube-root-ca.crt ConfigMap (needed for projected SA volumes)
kubectl create cm kube-root-ca.crt -n kube-system --from-literal=ca.crt=dummy

# 6. Fix CoreDNS Corefile (semicolons break CoreDNS parsing — use newlines)
kubectl create cm coredns -n kube-system --from-file=Corefile=/tmp/corefile.txt
```

## CoreDNS Corefile

Must use newlines, NOT semicolons. Semicolons are treated as part of the
directive name (`errors;` → unknown directive). Minimal working config:

```
.:53 {
    errors
    health { lameduck 5s }
    ready
    forward . 8.8.8.8 { max_concurrent 1000 }
    cache 30
    loop
    reload
    loadbalance
}
```

Omit the `kubernetes` plugin — it requires `ca.crt` from the projected service
account volume, which the kubelet doesn't provide when auth is disabled.

## Known issues

### 1. TLS on musl is broken

`axum_server::bind_rustls` logs "HTTPS server listening" but the lazy bind in
`.serve()` never actually opens the port — no error, no crash, just silently
non-functional. Works fine on glibc. Sonobuoy's aggregator requires HTTPS for
in-cluster config, so this blocks `sonobuoy run`.

**Workaround options:**
- Use a glibc-based DinD image (e.g., Ubuntu + manual Docker install)
- Fix the `axum-server` + musl lazy-bind interaction
- Inject an HTTP kubeconfig into the sonobuoy aggregator pod

### 2. `sleep infinity` broken on Alpine busybox

Alpine's busybox `sleep` doesn't support `"infinity"` — use `sleep 86400`
instead. Pods using `command: ["sleep", "infinity"]` with Alpine images
will crash at ContainerCreating with exit code 137.

### 3. Memory pressure

DinD + rusternetes (all components) + bollard-cri + CoreDNS + sonobuoy
aggregator needs ~12-16 GiB. Below 8 GiB: OOMKilled.

### 4. Kubernetes service port

Without kube-proxy, service IPs aren't routed. With `--kubernetes-service-host
127.0.0.1`, pods default to localhost which works for the aggregator but
bypasses service discovery. With `--kubernetes-service-host 10.96.0.1` and
kube-proxy enabled, service IP routing works correctly (iptables DNAT).

### 5. Sonobuoy CLI kubeconfig

Use `127.0.0.1`, NOT `localhost` — Alpine/musl DNS resolver returns IPv6
`::1` which the API server doesn't bind.

## Sonobuoy invocation

```bash
# kubeconfig must use 127.0.0.1 (not localhost) and HTTP URL
export KUBECONFIG=/root/.kube/config
cat > /root/.kube/config << KCFG
apiVersion: v1
kind: Config
clusters:
- cluster:
    server: http://127.0.0.1:6443
  name: rusternetes
contexts:
- context:
    cluster: rusternetes
  name: rusternetes
current-context: rusternetes
KCFG

sonobuoy run --mode quick --wait
sonobuoy status
sonobuoy results /tmp/results.tar.gz
```

## Results

| Item | Status |
|------|--------|
| bollard-cri mount path fix | ✅ Committed (`1f0130d9`) |
| CoreDNS running | ✅ (simple Corefile, no kube-API plugin) |
| kube-proxy (iptables) | ✅ Working, service IP routing correct |
| Pod scheduling | ✅ Sonobuoy aggregator scheduled to node-1 immediately |
| Sonobuoy full run | ❌ Blocked by TLS-on-musl (aggregator requires HTTPS) |
| Scheduler bug (pods stuck in Creating) | ❓ Did NOT reproduce — may have been fixed by kubelet status-update commits |

After upgrading to a glibc-based image or fixing the musl TLS issue, the
conformance run should proceed. The mount path fix unblocks container creation
(was the immediate blocker for CoreDNS and sonobuoy pods).
