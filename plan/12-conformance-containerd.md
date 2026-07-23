# Plan 12 — Sonobuoy conformance via containerd CRI in a privileged Ubuntu pod

> ## Lima path (Mac) — `scripts/lima-conformance.sh` (added 2026-07-23)
>
> The body of this plan targets a **privileged Ubuntu pod on the Talos cluster**.
> On a Mac, the same all-in-one-binary → native-containerd conformance runs in the
> **lima `default` VM**, and several workarounds below are Talos-only and are
> **NOT needed on lima**:
>
> | Talos workaround (below) | lima status |
> |---|---|
> | containerd `native` snapshotter + explicit `unpack_config` (Pitfalls A/B) | **Unneeded** — no `/etc/containerd/config.toml` exists; default overlayfs on the ext4 root works (containerd is not nested-in-overlay). |
> | iptables nft→legacy shims (§4) | **Unneeded** — lima's Ubuntu kernel has `iptables-legacy`; kube-proxy works as-is. |
> | Re-extract live cert + recreate every `kube-root-ca.crt` after each restart (CA-rotation pitfall, Gotcha E) | **Replaced** by a **persistent** self-signed cert passed via `--tls-cert-file`/`--tls-key-file` (api-server loads it with `from_pem_files`). The CA is then stable across restarts and the namespace controller (reads `/etc/kubernetes/pki/ca.crt`) auto-mints a correct, stable `kube-root-ca.crt` in every namespace — verified: kube-system/default/sonobuoy serials all match the persistent CA. |
> | "delete leftover same-name pods before a run" (Gotcha G workaround) | **Fixed at the source** in `f8ab90ab` (kubelet keys pods by `namespace/name`; scheduler treats empty `schedulerName` as default; empty `terminationMessagePath` defaults to `/dev/termination-log`). |
>
> `bash scripts/lima-conformance.sh [mode]` does the whole lima bringup:
> rsync→build (glibc, native, TLS works), generate the persistent cert once,
> wipe the bloat-prone sqlite DB, start rusternetes, bootstrap, pre-create the
> sonobuoy ns, and launch sonobuoy (default `certified-conformance`, v1.35.0).
> DB note: the all-in-one's startup VACUUM stalls badly once `rusternetes.db`
> bloats (110 MB seen after days of a crash-looping pod) — the script wipes it.

Follow-up to plan 11. Replaces the DinD + bollard-cri stack with **real containerd
as the CRI** inside a privileged `ubuntu:24.04` pod. This sidesteps the two big
problems of plan 11 in one move:

1. **TLS-on-musl** — glibc binary in Ubuntu, so `axum_server::bind_rustls` works.
2. **bollard-cri shim fidelity** — the kubelet talks to genuine containerd 2.x
   over CRI gRPC, the same runtime real clusters use. No Docker API translation
   layer, no shim bugs masking (or causing) kubelet bugs.

Verified working 2026-07-20: CoreDNS Running, sonobuoy aggregator + e2e plugin
scheduled and executing in `--mode quick` against the all-in-one binary with TLS.

## Architecture

```
┌─ privileged ubuntu:24.04 pod (bb-k8s-rk1b-01) ──────────────────┐
│  containerd 2.2 (apt) ← CRI gRPC ← kubelet (in rusternetes)     │
│      │ native snapshotter              │                         │
│      │ runc                            │ API :6443 (HTTPS, glibc)│
│      ↓                                 ↓                         │
│  pause / coredns / sonobuoy pods   sonobuoy CLI (kubectl)        │
│  (bridge CNI 10.88.0.0/16, ipMasq)                               │
└──────────────────────────────────────────────────────────────────┘
```

## Pod manifest

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: rusternetes-tls
  namespace: agent-sandbox-system
spec:
  nodeName: bb-k8s-rk1b-01
  restartPolicy: Never
  containers:
  - name: main
    image: ubuntu:24.04
    command: ["sleep", "infinity"]     # works on Ubuntu (not Alpine busybox)
    securityContext:
      privileged: true
    resources:
      requests: { memory: 4Gi }
      limits:   { memory: 8Gi }        # containerd needs far less than DinD
    volumeMounts:
    - { name: binaries, mountPath: /opt/rusternetes }
  volumes:
  - { name: binaries, emptyDir: {} }
```

Memory footprint is much lower than plan 11 (no dockerd): 8 Gi limit suffices
where DinD needed 12–16 Gi.

## Build: glibc (host-native), not musl

```bash
export PROTOC=$HOME/.local/bin/protoc
export PROTOC_INCLUDE=$HOME/.local/include
cd ~/git/rusternetes
cargo build --release -p rusternetes            # glibc aarch64 — TLS works
cargo build --release --target aarch64-unknown-linux-musl -p rusternetes-kubectl
```

The kubectl binary can stay musl (static, no TLS server). Copy in:

```bash
kubectl cp target/release/rusternetes agent-sandbox-system/rusternetes-tls:/opt/rusternetes/rusternetes
kubectl cp target/aarch64-unknown-linux-musl/release/kubectl agent-sandbox-system/rusternetes-tls:/usr/local/bin/kubectl
kubectl cp bootstrap-cluster.yaml agent-sandbox-system/rusternetes-tls:/tmp/
```

## Runtime setup inside the pod

### 1. Packages

```bash
apt-get update && apt-get install -y containerd runc iptables curl ca-certificates
```

Gets containerd 2.2.x + runc 1.3 + iptables 1.8.10 (nft).

### 2. CNI plugins + bridge config

```bash
mkdir -p /opt/cni/bin /etc/cni/net.d
curl -sSL https://github.com/containernetworking/plugins/releases/download/v1.6.2/cni-plugins-linux-arm64-v1.6.2.tgz \
  | tar xz -C /opt/cni/bin

cat > /etc/cni/net.d/10-bridge.conflist << 'EOF'
{
  "cniVersion": "1.0.0",
  "name": "rusternetes",
  "plugins": [
    {"type": "bridge", "bridge": "cni0", "isGateway": true, "ipMasq": true,
     "ipam": {"type": "host-local", "ranges": [[{"subnet": "10.88.0.0/16"}]],
              "routes": [{"dst": "0.0.0.0/0"}]}},
    {"type": "portmap", "capabilities": {"portMappings": true}},
    {"type": "loopback"}
  ]
}
EOF
```

### 3. containerd config — TWO critical edits

```bash
mkdir -p /etc/containerd
containerd config default > /etc/containerd/config.toml
```

**Pitfall A — nested overlayfs is rejected by the kernel.** The pod's rootfs is
already overlayfs; containerd's default overlayfs snapshotter then tries
overlay-on-overlay and every `RunPodSandbox` fails with:

```
failed to mount rootfs component: mount source: "overlay" ... err: invalid argument
```

Fix: switch the CRI snapshotter to `native`:

```bash
sed -i "s/snapshotter = 'overlayfs'/snapshotter = 'native'/" /etc/containerd/config.toml
```

**Pitfall B — containerd 2.x transfer service has its own unpack config.**
With only pitfall A fixed, sandboxes start but `PullImage` fails with:

```
unable to initialize unpacker: no unpack platforms defined: invalid argument
```

The transfer plugin derives unpack platforms from the (now non-default)
snapshotter and ends up with none. Fix — append an explicit unpack config:

```toml
[[plugins.'io.containerd.transfer.v1.local'.unpack_config]]
  platform = 'linux/arm64'
  snapshotter = 'native'
```

Then start it:

```bash
nohup containerd > /tmp/containerd.log 2>&1 &
# socket appears at /run/containerd/containerd.sock — exactly where the
# kubelet's hardcoded CRI endpoint expects it. No symlink needed.
```

### 4. iptables: Ubuntu ships nft, rusternetes calls legacy

The kernel on Talos nodes has no legacy xtables modules, so
`iptables-legacy` (which rusternetes' kube-proxy resolves to) fails with
`can't initialize iptables table 'nat'`. Shim all legacy entry points to the
nft backend:

```bash
update-alternatives --set iptables /usr/sbin/iptables-nft
for tool in iptables ip6tables; do
  printf '#!/bin/sh\nexec /usr/sbin/xtables-nft-multi %s "$@"\n' "$tool" > /usr/sbin/${tool}-legacy
  chmod +x /usr/sbin/${tool}-legacy
done
printf '#!/bin/sh\nexec /usr/sbin/xtables-nft-multi iptables-restore "$@"\n' > /usr/sbin/iptables-legacy-restore
printf '#!/bin/sh\nexec /usr/sbin/xtables-nft-multi iptables-save "$@"\n'    > /usr/sbin/iptables-legacy-save
chmod +x /usr/sbin/iptables-legacy-restore /usr/sbin/iptables-legacy-save
```

After this, kube-proxy's atomic `iptables-restore --noflush` path works and
service DNAT rules (RUSTERNETES-SERVICES) are installed cleanly.

### 5. Start rusternetes (TLS, glibc — works)

```bash
cd /opt/rusternetes
nohup ./rusternetes --log-level info --skip-auth --tls \
  --tls-san "localhost,127.0.0.1,10.96.0.1,kubernetes.default.svc,kubernetes.default,kubernetes" \
  --bind-address 0.0.0.0:6443 \
  --kubernetes-service-host 10.96.0.1 \
  --volume-dir /opt/rusternetes/data/volumes \
  --data-dir /opt/rusternetes/data/rusternetes.db \
  > /tmp/rusternetes.log 2>&1 &
```

Notes:
- `--tls-san` takes ONE comma-separated value, not repeated flags.
- **SAN list must include the pod's own IP** if you ever pass it via
  `--kubernetes-service-host`. The kubelet env-injects that host as
  `KUBERNETES_SERVICE_HOST` into every pod, so in-cluster clients dial it
  directly. Keep it on `10.96.0.1` (ClusterIP) — that path exercises the
  `kubernetes` Service + kube-proxy DNAT, matches `kubernetes.default.svc`
  SAN entries, and avoids per-pod-IP SAN churn.
- **`--volume-dir` must be absolute.** The kubelet defaults to
  `./data/volumes`, and runc resolves bind-mount sources relative to *its own*
  cwd, so relative paths break with
  `failed to fulfil mount request: open .../data/volumes/...: no such file or directory`.
  (Plan 11 never hit this because Docker's API canonicalizes differently.)
- `curl -sk https://127.0.0.1:6443/healthz` → 200. TLS actually binds — the
  musl silent-bind failure is gone.

### 5a. Verify clean iptables before bootstrap

Earlier sessions (DinD, manual debugging) can leave stale rules behind that
break in-cluster traffic. The rusternetes kube-proxy itself never installs a
REDIRECT-to-6443 rule — if you see one, it is leftover and must be deleted:

```bash
# Should show no REDIRECT entries. If a broad 0.0.0.0/0 dpt:443 REDIRECT to
# 6443 appears, delete it before pulling any images or starting sonobuoy:
iptables -t nat -L OUTPUT -n
# iptables -t nat -D OUTPUT -p tcp --dport 443 -j REDIRECT --to-ports 6443
```

### 6. Bootstrap + real CA ConfigMaps

```bash
kubectl --server https://127.0.0.1:6443 --insecure-skip-tls-verify apply -f /tmp/bootstrap-cluster.yaml
kubectl ... label node node-1 kubernetes.io/os=linux kubernetes.io/arch=arm64 kubernetes.io/hostname=node-1 --overwrite

# Extract the live self-signed cert — do NOT use a dummy value.
echo | openssl s_client -connect 127.0.0.1:6443 2>/dev/null | openssl x509 > /tmp/server.crt
for ns in kube-system default sonobuoy; do
  kubectl ... create cm kube-root-ca.crt -n $ns --from-file=ca.crt=/tmp/server.crt
done
```

**Pitfall C — dummy CA breaks in-cluster clients.** With TLS live, the
aggregator's client-go validates the API server cert against the projected
`ca.crt`. Plan 11's `--from-literal=ca.crt=dummy` yields
`x509: certificate signed by unknown authority` on every in-cluster call.
Since `--tls` generates a self-signed cert, the server cert IS the CA — extract
it via `openssl s_client` and publish that.

**Pitfall D — create the `sonobuoy` ns + CA ConfigMap BEFORE the run.**
`sonobuoy run` creates its namespace itself; the kubelet then projects a
missing/stale ca.crt into the aggregator. Instead: `sonobuoy gen --mode quick >
manifest.yaml`, pre-create ns + ConfigMap, then `kubectl apply -f manifest.yaml`.

### 7. Pre-pull + run

```bash
curl -sSL https://github.com/vmware-tanzu/sonobuoy/releases/download/v0.57.3/sonobuoy_0.57.3_linux_arm64.tar.gz \
  | tar xz -C /usr/local/bin sonobuoy
for img in docker.io/sonobuoy/sonobuoy:v0.57.3 docker.io/sonobuoy/systemd-logs:v0.4 registry.k8s.io/conformance:v1.35.0; do
  ctr -n k8s.io images pull $img
done
```

**Also pre-pull the e2e test images.** Without these, every e2e test pod
stalls 30–60s on image pull and most tests time out. The conformance image
contains the canonical list at `/usr/local/bin/e2e.test --list-images`, but
the common subset that covers most `[sig-*]` tests is:

```bash
for img in \
  registry.k8s.io/e2e-test-images/agnhost:2.55 \
  registry.k8s.io/e2e-test-images/nginx:1.14-4 \
  registry.k8s.io/e2e-test-images/busybox:1.36.1-1 \
  registry.k8s.io/e2e-test-images/echoserver:2.6 \
  registry.k8s.io/e2e-test-images/httpd:2.4.38-4 \
  registry.k8s.io/e2e-test-images/jessie-dnsutils:1.7 \
  registry.k8s.io/e2e-test-images/kitten:1.7 \
  registry.k8s.io/e2e-test-images/nautilus:1.7 \
  registry.k8s.io/e2e-test-images/perl:5.34 \
  registry.k8s.io/e2e-test-images/redis:5.0.5-3 \
  registry.k8s.io/e2e-test-images/resource-consumer:1.13 \
  registry.k8s.io/e2e-test-images/sample-apiserver:1.29.2 \
  registry.k8s.io/pause:3.10 \
  docker.io/library/nginx:alpine \
  docker.io/library/busybox:latest; do
  ctr -n k8s.io images pull "$img" &
done
wait
```

For a full run, extract the authoritative list once and loop over it:

```bash
# Extract the full image list from the conformance binary itself:
ctr -n k8s.io run --rm --tty registry.k8s.io/conformance:v1.35.0 list-imgs \
  /usr/local/bin/e2e.test --list-images 2>/dev/null | grep -E "^(registry|docker|gcr|quay)" \
  | sort -u > /tmp/e2e-images.txt
wc -l /tmp/e2e-images.txt   # ~50 images
xargs -a /tmp/e2e-images.txt -P 8 -n1 ctr -n k8s.io images pull
```

```bash
sonobuoy gen --mode quick > /tmp/sonobuoy-manifest.yaml
kubectl ... create ns sonobuoy
kubectl ... create cm kube-root-ca.crt -n sonobuoy --from-file=ca.crt=/tmp/server.crt
kubectl ... apply -f /tmp/sonobuoy-manifest.yaml
sonobuoy status   # e2e: running → complete
```

Kubeconfig: `https://127.0.0.1:6443` with `insecure-skip-tls-verify: true`
(the rusternetes-kubectl needs `--server`/`--insecure-skip-tls-verify` flags —
kubeconfig parsing warns "Could not load kubeconfig, using defaults").

## Digging into plan 11's musl TLS problem

Plan 11 was blocked by: `axum_server::bind_rustls` (0.7.3) logs
"HTTPS server listening" but the port never opens on
`aarch64-unknown-linux-musl`. Observed behavior and analysis:

- **Symptom:** process healthy, log line printed, `curl` → connection refused.
  No error, no panic. glibc build of the identical code binds fine.
- **Where it dies:** `axum_server`'s `Server::serve()` binds lazily inside the
  accept loop future. The log line is emitted by *our* code before `.serve()`
  is awaited, so it proves nothing about the bind. On musl the bind future
  stalls before `TcpListener::bind` completes.
- **Prime suspects (in likelihood order):**
  1. **Blocking DNS/getaddrinfo in musl** — musl's resolver differs from
     glibc (no `/etc/nsswitch.conf`, strict RFC behavior, IPv6-first
     quirks). If the bind address is given as a hostname (`localhost`),
     musl may resolve to `::1` only; binding `::1` inside a netns without
     IPv6 loopback configured fails or the listener lands on a socket
     nobody queries. Plan 11's "use 127.0.0.1, not localhost" DNS finding
     for the *client* side is the same musl resolver class of bug.
  2. **rustls crypto provider init** — with `aws-lc-rs` as the default
     rustls provider, musl static builds need cmake/asm support at build
     time; a silently missing provider makes `RustlsConfig` future pend
     forever. Switching rustls to the `ring` provider is the standard fix.
  3. **tokio + musl thread-spawn interaction** in the TLS acceptor's
     blocking task (less likely; would usually panic).
- **How to pin it down (if we ever need musl TLS again):**
  ```rust
  // 1. Bind eagerly, before serve():
  let listener = std::net::TcpListener::bind(addr)?;   // fails loudly
  axum_server::from_tcp_rustls(listener, config).serve(app).await
  ```
  `from_tcp_rustls` with a pre-bound std listener converts the silent
  lazy-bind into either an immediate error or a working server — this both
  diagnoses and likely fixes the issue.
  ```bash
  # 2. Confirm from outside:
  strace -f -e trace=bind,listen,socket ./rusternetes ... 2>&1 | grep 6443
  ss -ltnp | grep 6443
  ```
  If `bind()` never appears in strace, the future truly pends → suspect 2.
  If `bind()` appears with AF_INET6 `::1` → suspect 1; pass a
  `SocketAddr` (parsed `0.0.0.0:6443`), never a hostname string.
- **Decision:** not worth fixing for conformance. glibc-in-Ubuntu-pod removes
  the entire problem class and is closer to production anyway. Keep musl
  builds for Alpine-only contexts that don't need the TLS server (kubectl,
  bollard-cri).

## Comparison with plan 11 (DinD + bollard-cri)

| Aspect | Plan 11 (DinD) | Plan 12 (containerd) |
|---|---|---|
| CRI | bollard-cri shim → Docker | containerd 2.2 (native) |
| libc | musl (Alpine) — TLS broken | glibc (Ubuntu) — TLS works |
| Memory | 12–16 Gi | ≤ 8 Gi |
| Sonobuoy full run | ❌ blocked (HTTPS) | ✅ runs |
| Snapshotter quirk | n/a (Docker vfs/overlay) | must use `native` (nested overlayfs) |
| kube-proxy | works | works after nft-legacy shims |
| Fidelity | shim may mask kubelet bugs | real-world CRI path |

## Results (2026-07-20)

| Item | Status |
|---|---|
| containerd 2.2.1 + runc + native snapshotter | ✅ |
| TLS API server (glibc) reachable, healthz 200 | ✅ |
| kube-proxy nat rules via nft shims | ✅ |
| CoreDNS Running under containerd | ✅ |
| Sonobuoy aggregator in-cluster HTTPS (real CA cm) | ✅ |
| e2e plugin --mode quick | ✅ completed (0/0 — focus test didn't match) |
| e2e plugin --mode non-disruptive-conformance (full) | 🔄 running |

### Quick-mode 0/0 explained

The quick-mode test ran `ginkgo --focus="Pods should be submitted and removed"`
which didn't match any test in the `registry.k8s.io/conformance:v1.35.0` image.
The ginkgo binary ran, reported results to the aggregator, but found zero
matching specs → 0 pass, 0 fail. The infrastructure worked: the e2e container
created a namespace, watched for pods, then cleaned up.

### Gotcha G — pod name collision kills sandboxes in fresh namespaces (partially fixed)

When the e2e framework creates a new namespace and immediately creates a pod
inside it, the pod sometimes never gets scheduled:

- API server logs `Pod created successfully: pods-NNN/pod-test`
- Admission logs `Service account pods-NNN/ does not exist, but proceeding` —
  note the **empty SA name** (e2e sets `serviceAccountName: ""` explicitly)
- **No scheduler log line at all** for that pod — not even a debug skip
- Pod stays Pending until the test times out (default 5m), then fails with
  `Expected <v1.PodPhase>: Pending to equal <v1.PodPhase>: Running`

**Root cause identified (2026-07-22):** the kubelet identifies pods by
**name only** (not `namespace/name`). When two pods have the same name in
different namespaces (e.g. `pods-6553/pod-test` colliding with
`repro-fresh/pod-test`), the kubelet's orphan cleanup and `stop_pod`
functions kill the wrong sandbox. The pause container dies with exit code
137 (SIGKILL), the sandbox fails, and the app containers can't start.

**Partial fix committed (`8580d32d`):**
1. `ensure_sandbox` no longer removes sandboxes created within the last 10s
   as "stale" — they may still be initializing.
2. Orphan cleanup re-fetches the pod list from storage instead of using a
   stale snapshot from the start of the sync loop.

**Remaining issue:** the fundamental name collision bug is NOT fixed. If two
pods share the same name in different namespaces, the kubelet can still kill
the wrong sandbox. The proper fix is to use `namespace/name` as the pod
identifier throughout the kubelet (worker map keys, sandbox lookups, CNI
netns names, etc.). This is a larger refactor.

**Workaround for e2e:** ensure no leftover pods from previous runs share
the same name as the e2e test pod (`pod-test`). Delete old test pods before
running conformance.

**To debug:** if a pod stays Pending, check `containerd.log` for
`StopPodSandbox` calls within seconds of `RunPodSandbox`. If the kubelet
killed the sandbox, the pause container exits with code 137 and the app
containers fail with `sandbox container is not running`.

### CA cert rotation pitfall

When rusternetes restarts with `--tls`, it generates a **new** self-signed cert.
All existing `kube-root-ca.crt` ConfigMaps become stale. In-cluster clients
(sonobuoy worker, systemd-logs daemonset) get
`x509: certificate signed by unknown authority ... ECDSA verification failure`.

**Fix:** After every restart, re-extract the live cert and recreate ConfigMaps:
```bash
echo | openssl s_client -connect 127.0.0.1:6443 2>/dev/null | openssl x509 > /tmp/server.crt
for ns in kube-system default sonobuoy; do
  kubectl delete cm kube-root-ca.crt -n $ns
  kubectl create cm kube-root-ca.crt -n $ns --from-file=ca.crt=/tmp/server.crt
done
```

Also: always create the `sonobuoy` namespace + its `kube-root-ca.crt` **before**
applying the sonobuoy manifest (`sonobuoy gen` + `kubectl apply`). The
aggregator's projected volume is created at pod startup; if the ConfigMap
doesn't exist yet, the pod gets a stale or empty CA.

### Gotcha E — `kube-root-ca.crt` never created (namespace controller reads wrong paths)

**The most impactful pitfall in this setup.** The namespace controller
(`handlers/namespace.rs`, `create_namespace`) automatically creates a
`kube-root-ca.crt` ConfigMap in every new namespace by reading a CA cert
from disk. It tries three paths in order:

```rust
// crates/api-server/src/handlers/namespace.rs:105-112
let ca_cert = match tokio::fs::read_to_string("/etc/kubernetes/pki/ca.crt").await {
    Ok(s) => s,        // ❌ doesn't exist in our pod
    Err(_) => match tokio::fs::read_to_string("/etc/kubernetes/pki/api-server.crt").await {
        Ok(s) => s,    // ❌ doesn't exist
        Err(_) => tokio::fs::read_to_string("/root/.rusternetes/certs/ca.crt")
            .await
            .unwrap_or_default(),  // ❌ doesn't exist → ""
    },
};
```

In a bare Ubuntu pod, **none of these paths exist**. The result is
`ca_cert = ""` (empty string), `cert_len = 0`, and the ConfigMap is
skipped entirely:

```
WARN CA cert is empty, skipping kube-root-ca.crt for namespace <name>
```

**Consequence:** Every e2e test creates a namespace, then creates pods with
service accounts. Those pods require a **projected service account token
volume** which includes `ca.crt` from the `kube-root-ca.crt` ConfigMap.
Without it, pod creation fails or the pod never starts. The test sees no
running pods, retries, eventually times out, and moves to the next test —
producing 0 pass, 0 fail across the entire suite.

**This is NOT a watch bug.** The watch infrastructure was investigated and is
correct. Direct API testing confirmed:
- `?watch=true&sendInitialEvents=true` → returns ADDED + initial-events-end bookmark
- `?watch=true` (no sendInitialEvents, no rv) → returns ADDED
- `?watch=true&resourceVersion=0` → returns ADDED

All three cases correctly list existing resources and send them as initial
ADDED events before streaming. The 0/0 results are entirely caused by
missing `kube-root-ca.crt`.

**Fix:**

```bash
# Extract the live server cert (generated by --tls) and place it where the
# namespace controller expects it.
mkdir -p /etc/kubernetes/pki
echo | openssl s_client -connect 127.0.0.1:6443 2>/dev/null | openssl x509 \
  > /etc/kubernetes/pki/ca.crt
```

After this, every new namespace automatically gets a valid `kube-root-ca.crt`
ConfigMap. Existing namespaces (created before the fix) still have empty
ConfigMaps — restart the sonobuoy run so all test namespaces are fresh.

**Verification:**

```bash
kubectl create ns verify-ca
kubectl get cm kube-root-ca.crt -n verify-ca -o jsonpath='{.data.ca\.crt}' | head -c 30
# Should show: -----BEGIN CERTIFICATE-----
kubectl delete ns verify-ca
```

### Gotcha F — protobuf `Volume` decoded every source as `hostPath: {}` (THE 0/0 root cause after CA fix)

**This was the real reason conformance ran 0 pass / 0 fail even after the CA
fix.** The e2e client (`e2e.test`) sends pod-create requests as **K8s
protobuf** (`Content-Type: application/vnd.kubernetes.protobuf`), not JSON.
The api-server middleware decodes protobuf → JSON via a schema registry
(`crates/api-server/src/protobuf.rs`) before the pod handler parses it.

The `Volume` schema was **structurally wrong**. It inlined the volume-source
types directly under `Volume` using field numbers copied from `VolumeSource`,
but the real K8s proto is a **two-level nesting**:

```proto
message Volume {
  optional string name = 1;
  optional VolumeSource volumeSource = 2;   // NESTED message
}
message VolumeSource {
  optional HostPathVolumeSource   hostPath  = 1;
  optional EmptyDirVolumeSource   emptyDir  = 2;
  optional SecretVolumeSource     secret    = 6;
  optional ConfigMapVolumeSource  configMap = 19;
  optional ProjectedVolumeSource  projected = 26;
  optional CSIVolumeSource        csi       = 28;
  ...
}
```

The old schema mapped `Volume` field 2 → `hostPath` (a message). So **every**
volume — configMap, emptyDir, projected, whatever — was read as the outer
field 2 = "hostPath", and the generic decoder emitted `hostPath: {}` because
the nested `VolumeSource` bytes didn't match `HostPathVolumeSource`.
Additionally, none of the volume-source submessage schemas
(`HostPathVolumeSource`, `ConfigMapVolumeSource`, `KeyToPath`, projections,
etc.) were registered, so even a correctly-routed source decoded to `{}`.

**Consequence:** the JSON handed to the pod handler contained
`"volumes":[{"hostPath":{},"name":"..."}]`. Since `HostPathVolumeSource.path`
is a required (non-`Option`) field, serde failed with:

```
failed to decode: missing field `path` at line 1 column NNNN
code: 400, reason: BadRequest
```

Every pod-creating conformance test hit this at `podClient.Create`
(`framework/pod/output/output.go:176`) before producing any junit output, so
sonobuoy collected an empty results dir → `no valid entries in result` → 0/0.

**How it was found:** temporary body-dump logging in `handlers/pod.rs` on the
decode-error path revealed the corrupted `hostPath: {}` volumes. Cross-checked
against the upstream v1.35.0 `core/v1/generated.proto`, which showed the
nested `VolumeSource` structure.

**Fix** (`crates/api-server/src/protobuf.rs`):
1. `volume_schema`: field 2 → new `FieldType::Inlined("VolumeSource")` (decode
   the nested message and merge its keys up into the Volume object, since
   rusternetes' JSON structs flatten the source directly onto the Volume).
2. Added `volume_source_schema` with correct field numbers
   (hostPath=1, emptyDir=2, secret=6, nfs=7, persistentVolumeClaim=10,
   downwardAPI=16, configMap=19, projected=26, csi=28, ephemeral=29).
3. Registered all volume-source submessage schemas + `LocalObjectReference`,
   `KeyToPath`, and the projection types. `configMap`/`secret` inline their
   embedded `LocalObjectReference{name=1}` so `name` lands at the top level.
4. New `FieldType::Inlined(String)` variant with handling in both
   `decode_with_schema` (merge-up) and `decode_field_value` (standalone).

Regression tests: `protobuf::tests::test_decode_volume_with_nested_configmap_source`
and `..._with_hostpath_source`.

**Reproduce a single test directly** (bypasses sonobuoy, avoids the musl
`kubectl exec` WebSocket-TLS failure):

```bash
# Start a long-lived conformance pod, then exec e2e.test via ctr:
ctr -n k8s.io task exec --exec-id r1 <shell-container-id> \
  env KUBECONFIG=/tmp/kcfg \
  /usr/local/bin/e2e.test \
    --host=https://<pod-ip>:6443 \
    --ginkgo.focus="Subpath Atomic writer volumes should support subpaths with configmap pod " \
    --ginkgo.v
```

Notes for the manual harness:
- The e2e `SynchronizedBeforeSuite` needs the `kubernetes` Service in `default`
  (clusterIP 10.96.0.1, port 443→6443) or it fails with
  `services "kubernetes" not found` / `dial 127.0.0.1:443 connection refused`.
- The e2e container has its own netns — use the rusternetes pod IP, not
  127.0.0.1, and a kubeconfig with `insecure-skip-tls-verify: true` (the
  serving cert SANs are 127.0.0.1 + 10.96.0.1 only).
- Unrelated env pitfall: kubelet volume binds (`data/volumes/<pod>/hosts`) are
  resolved by runc relative to the containerd shim cwd, so rusternetes must be
  started with an **absolute** `--volume-dir` (e.g.
  `--volume-dir /opt/rusternetes/data/volumes`) or container start fails with
  `failed to fulfil mount request: open .../data/volumes/...: no such file`.

