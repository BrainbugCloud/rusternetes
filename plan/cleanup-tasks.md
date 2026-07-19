# Cleanup tasks — surfaced during the config-1 first bring-up (2026-07-19)

Small, independent fixes found while running the CRI-only kubelet against
containerd (see [test-matrix.md](test-matrix.md) config 1 and
[02-kubelet-cri-only.md](02-kubelet-cri-only.md) runtime findings). None of these
are the K5 streaming bug (tracked in plan 02 K5) — these are the side findings.

## 1. `kubectl get nodes` lacks table columns

- **Symptom:** `kubectl get nodes` prints only `NAME` + `AGE` (missing
  `STATUS`, `ROLES`, `AGE`, `VERSION`, and wide: `INTERNAL-IP`, `OS-IMAGE`,
  `KERNEL-VERSION`, `CONTAINER-RUNTIME`).
- **Cause:** [`crates/api-server/src/handlers/table.rs`](../crates/api-server/src/handlers/table.rs)
  has a rich `pods_table()` (line ~144) but **no `nodes_table()`** — the nodes
  handler falls through to `generic_table()` (line ~196), which emits NAME+AGE.
- **Fix:** add `nodes_table()` mirroring `pods_table()` (columns: NAME, STATUS
  = Ready/NotReady from conditions, ROLES, AGE, VERSION = `status.nodeInfo.kubeletVersion`;
  wide adds INTERNAL-IP, OS-IMAGE, KERNEL-VERSION, CONTAINER-RUNTIME). Dispatch
  the nodes list/get handler to it when `wants_table()` (line ~279) is true.
  Same gap likely exists for other resource types — audit the dispatch and add
  builders where `kubectl get <type>` looks bare.

## 2. Node registers `arch=amd64` on an arm64 host (hardcoded)

- **Symptom:** on the aarch64 VM the node advertised `kubernetes.io/arch=amd64`.
  Harmless for multi-arch images; wrong for arch-sensitive scheduling and
  conformance node checks.
- **Cause:** hardcoded strings in
  [`crates/kubelet/src/kubelet.rs`](../crates/kubelet/src/kubelet.rs):
  labels at **lines 390, 392** (`kubernetes.io/arch`, `beta.kubernetes.io/arch`),
  `nodeInfo.architecture` at **line 446**, and the heartbeat re-insert at
  **lines 544–550**.
- **Fix:** derive from `std::env::consts::ARCH` with the Go mapping
  (`x86_64`→`amd64`, `aarch64`→`arm64`) and `std::env::consts::OS` for
  `kubernetes.io/os`. Set once in a helper and reuse in register + heartbeat so
  they can't drift.

## 3. `RunPodSandbox` failure logs swallow the underlying gRPC error

- **Symptom:** on sandbox failure the kubelet logged only
  `Failed to start pod default/smoke: CRI RunPodSandbox` — the real cause
  (`failed to setup network for sandbox: cni plugin not initialized`) was
  invisible; had to reproduce via `crictl runp` to see it.
- **Cause:** `cri.rs:109` attaches anyhow context `.context("CRI RunPodSandbox")`
  over the tonic `Status`, but the error is logged with `{}` (Display) at
  [`crates/kubelet/src/kubelet.rs:2050`](../crates/kubelet/src/kubelet.rs) —
  plain Display shows only the outermost context, not the source chain.
- **Fix:** log the full chain — use `{:#}` (anyhow alternate Display) or `{:?}`
  for `err_msg` at the `error!("Failed to start pod …")` site (and the analogous
  container-start/stop/remove sites). Cheap, high-value for every future debug.

## 4. kube-proxy iptables rules blackhole node networking — ✅ FIXED (2026-07-19)

- **Symptom:** with kube-proxy enabled (all-in-one default), the VM's **outbound
  DNS + image pulls broke**; had to run `--disable-proxy`.
- **Root cause:** [`crates/kube-proxy/src/iptables.rs`](../crates/kube-proxy/src/iptables.rs)
  added `-A POSTROUTING -m addrtype --src-type LOCAL -j MASQUERADE` — matches
  *every* node-originated packet, including loopback DNS to 127.0.0.53
  (MASQUERADE on `lo` drops it) and all egress. (Rules land in **iptables-legacy**
  on this VM — inspect with `iptables-legacy -t nat -S`, not plain `iptables`.)
- **Fix:** removed the rule; the node→NodePort case it targeted is already
  covered by the adjacent `-m conntrack --ctstate DNAT -j MASQUERADE`.
- **Verified** with kube-proxy enabled: node DNS/egress work, pods pull+run, and
  ClusterIP DNAT still routes (wget through a Service ClusterIP → nginx backend).

## 5. ClusterIP allocator gives a normal Service `10.96.0.1`

- **Symptom:** `kubectl expose` assigned `web-svc` the ClusterIP `10.96.0.1`,
  which should be reserved for the default `kubernetes` service.
- **Cause (to investigate):** the ClusterIP allocator / default-`kubernetes`-
  service bootstrap in the api-server. Not a kube-proxy bug (DNAT worked).
- **Impact:** likely breaks conformance's `kubernetes` service expectations and
  in-cluster API discovery via `KUBERNETES_SERVICE_HOST`.

## 6. Conformance-bringup gaps found running sonobuoy (2026-07-19)

Sonobuoy runs end-to-end on the containerd cluster (aggregator + e2e plugin +
results all work). Bringup required these; several are out-of-band workarounds
that should become code fixes:

- **api-server doesn't persist its self-signed CA.** With `--tls` (self-signed)
  the CA lives only in memory, so the namespace controller (`namespace.rs`, reads
  `/etc/kubernetes/pki/ca.crt` or `/root/.rusternetes/certs/ca.crt`) can't
  publish `kube-root-ca.crt` and the kubelet can't inject `ca.crt` into SA
  mounts. **Fix:** on self-signed generation, write the CA PEM to
  `~/.rusternetes/certs/ca.crt` (+ `/etc/kubernetes/pki/ca.crt`). *(Worked around
  by extracting the serving cert with `openssl s_client` and writing it there.)*
- **`--kubernetes-service-host` defaults to `127.0.0.1`.** That's a pod's own
  loopback — regular pods can't reach the API. For a real cluster it must be the
  `kubernetes` ClusterIP (`10.96.0.1`, port 443). *(Worked around with
  `--kubernetes-service-host 10.96.0.1`.)* Consider defaulting it to the
  kubernetes ClusterIP.
- **CoreDNS Corefile hardcoded `endpoint https://api-server:6443`** (a compose
  network alias). Fixed in `bootstrap-cluster.yaml` → `https://10.96.0.1:443`.
- **Serving cert SANs** must include `10.96.0.1` + `kubernetes.default.svc.*`
  (passed via `--tls-san`); the all-in-one should add these automatically.
- **Node `status.addresses` InternalIP is `127.0.1.1`** (from the lima
  `/etc/hosts` line) and **`nodeInfo.architecture` is `amd64`** on arm64 — both
  wrong for conformance (`get nodes/<node>:10250/proxy` also 404s — node-proxy
  gap). Related to #2.

## 7. Strict decoding rejects standard Pod fields (blocks pod-creating e2e)

The first conformance test (`[sig-node] Pods should be submitted and removed`)
fails: `strict decoding error: unknown field "metadata.uid", "spec.hostIPC",
"spec.hostPID"`. Root causes:

- **`host_ipc`/`host_pid`** in [`crates/common/src/resources/pod.rs`](../crates/common/src/resources/pod.rs)
  rely on `rename_all = "camelCase"` → serialize as `hostIpc`/`hostPid`, but K8s
  uses `hostIPC`/`hostPID`. Add `#[serde(rename = "hostIPC")]` /
  `#[serde(rename = "hostPID")]`. **Audit every abbreviation field** for the same
  bug (CLAUDE.md's `podIP`/`hostIP`/`containerID` rule).
- **`metadata.uid`** (`types.rs`, `skip_serializing_if = "String::is_empty"`): an
  empty uid in the request is dropped from the canonical form, so the strict
  validator flags it. Treat empty-valued known fields like the null case (see
  the existing fix in `validation.rs`).
- **Expect a tail:** conformance will surface more field mismatches one at a
  time; each needs a struct/rename fix + rebuild.

## Priority

K5 streaming (done) and #4 kube-proxy (done) were the two original **conformance
blockers** — both fixed. **#7 (strict decoding) is the current blocker** — it
fails pod-creating e2e tests, so a full conformance number isn't meaningful until
it's addressed. #6 items are bringup fixes (worked around for the run). #1/#2/#3/#5
are lower-priority polish.
