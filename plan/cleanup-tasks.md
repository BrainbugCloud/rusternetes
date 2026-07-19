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

## 4. kube-proxy iptables rules blackhole node networking

- **Symptom:** with kube-proxy enabled (all-in-one default), the VM's **outbound
  DNS + image pulls broke**; `iptables -t nat -F` restored connectivity. Had to
  run `--disable-proxy` to verify the CRI path.
- **Cause (to investigate):**
  [`crates/kube-proxy/src/iptables.rs`](../crates/kube-proxy/src/iptables.rs) /
  [`proxy.rs`](../crates/kube-proxy/src/proxy.rs) — likely a KUBE-SERVICES /
  MASQUERADE / ClusterIP rule that intercepts more than intended (e.g. a service
  with no endpoints black-holing, or a too-broad `-j` jump on the OUTPUT/
  PREROUTING nat chains catching node egress incl. UDP 53).
- **Fix:** reproduce in the VM, dump `iptables -t nat -S` after kube-proxy syncs,
  find the offending rule, scope it correctly (only ClusterIP CIDR, reject-with
  for no-endpoint services rather than blackhole, preserve node egress).
- **Blocks:** Services and the sonobuoy aggregator (needs Service networking) —
  i.e. this gates the K7 conformance run alongside K5.

## Priority

K5 streaming (plan 02) and #4 kube-proxy are the two **conformance blockers**.
#1/#2/#3 are correctness/ergonomics polish that also help conformance and
debuggability but don't block the run.
