# Plan 13 — Conformance fixes (lima + native containerd)

Running Kubernetes conformance (sonobuoy `certified-conformance`, v1.35.0, 441
specs) against the all-in-one `rusternetes` binary talking CRI to native
containerd inside the lima `default` VM (see [plan 12](12-conformance-containerd.md)
and `scripts/lima-conformance.sh`). This file records the fixes landed while
iterating on that run and the remaining open items.

## Two systematic root-cause classes

Most failures trace to how our protobuf→JSON decoder must match Go/gogo
marshaling semantics. Fixing the *class* beats fixing each field.

### Class A — gogo marshals unset optional scalars as empty, not absent
K8s Go API types tag optional scalars `(gogoproto.nullable)=false`, so an unset
`string` is written to the wire as `""`. Our decoder used to emit `Some("")`
where a JSON client emits `None`, so code treating `None` as "unset"
misbehaved.

**Systematic fix:** `decode_with_schema` in
`crates/k8s-proto/src/lib.rs` now **omits** empty scalar `String`/`Quantity`
fields at decode time, so protobuf decode == JSON (absent → `None`). Scoped to
scalar strings only — map values, repeated elements and `IntOrString` are
untouched.

**Flip side (must pair with the above):** a required, non-`Option` `String`
field that legitimately arrives empty now decodes as *absent* → serde "missing
field". Such fields need `#[serde(default)]`. First casualty fixed:
`RoleRef.api_group` (`crates/common/src/resources/rbac.rs`).

### Class B — protobuf decode divergences (schema/dispatch)
- **Embedded-message inline/nest:** `RuleWithOperations.rule` embeds `Rule`
  (upstream `gogoproto.embed`, dropped from the vendored proto) and the Rust
  struct uses `#[serde(flatten)]`, so it must decode inline, not nested. Fixed by
  adding `"rule"` to the inline allow-list in `crates/k8s-proto/build.rs`.
- **Same kind, two groups:** `Event` exists in both `core/v1` and
  `events.k8s.io/v1` with different wire field numbers. `decode_k8s_resource`
  dispatched by `kind` only, decoding an `events.k8s.io/v1` Event with the
  `core/v1` field map. Fixed by an `EventsV1Event` schema + apiVersion-aware
  dispatch (`crates/k8s-proto/src/lib.rs`).
- **Embedded-JSON fallback preemption:** `extract_json_from_k8s_protobuf`
  (`crates/api-server/src/middleware.rs`) scanned the whole body for any
  balanced `{...}` and returned it. A native-protobuf CRD embeds JSON in its
  `openAPIV3Schema`, so the fallback returned that fragment instead of the CRD →
  `missing field \`plural\``. Fixed by removing the whole-body scan so native
  protobuf falls through to the structured decoders (a validated brace-scan still
  runs downstream as a last resort).

## Fixes landed this iteration (all uncommitted → this WIP commit)

Kubelet / runtime:
- Node InternalIP + pod-status HostIP: `detect_internal_ip` now uses a UDP
  source-IP probe (finds the real node IP; `hostname -i` only gave loopback in
  the VM), cached via `Kubelet::node_host_ip()`, and the 5 hardcoded
  `host_ip: "127.0.0.1"` pod-status sites now use it.
- Liveness restart: rewrote the failing-liveness path to restart the **single**
  container in place (`stop_and_remove_container` + `start_container_for_pod`
  into the existing sandbox) instead of a full-pod teardown that wedged the pod
  as `Failed` after one restart. `check_liveness` now returns the offending
  container name.
- **Restart-count write storm (`restartPolicy=Always`, non-zero exit):** the
  `Phase::Running if !is_running` → `"Always"` arm wrote `restart_count = prev+1`
  unconditionally every sync; each write triggered a watch event → resync → +1,
  ~180 writes/sec, and the inflated count pinned backoff to 300s so the container
  never actually restarted. Fixed: keep the existing count on the per-sync path
  and gate the write with `pod_status_equal`; bump `restart_count` and write
  exactly once on the real-restart path after backoff elapses. (This storm also
  starved the API server and caused unrelated `context deadline exceeded`
  timeouts.)
- Earlier: env-var `valueFrom` precedence, termination-message newline trim,
  emptyDir tmpfs (`medium:Memory`) + FUSE handling, httpGet probe empty
  host/scheme, CreateContainerConfigError hot-loop backoff.

API server / common / storage:
- Decoder empty-string omission (Class A) + `RoleRef.api_group` default.
- `EventsV1Event` schema + apiVersion dispatch (Class B).
- CRD embedded-JSON fallback removed (Class B).
- Webhook `rule` inline (Class B) + `rules` `#[serde(default)]`.
- `resourceVersion:""` → treated as unset on update (`storage/src/concurrency.rs`).
- CRD strict-decode false positive (`Bool(false)` droppable), Namespace phase
  default Active, PV/PVC phase empty-string alias, PodDisruptionBudget condition
  `type` rename, ValidatingAdmissionPolicy `expression` casing.
- Pod-update immutability compare normalizes empty/absent (`handlers/pod.rs`).

Controllers / scheduler:
- DaemonSet empty `serviceAccountName` → "default"; StatefulSet OrderedReady
  default on empty policy.

Proto vendoring: RBAC + flowcontrol + discovery + autoscaling + storage/v1beta1
groups vendored and generated into the registry.

## Confirmed working live (this run)
- Node InternalIP = `192.168.5.15` (not loopback).
- `ValidatingWebhookConfiguration` create succeeds; `rule` decodes inline.
- Protobuf `RoleBinding` (with apiGroup present) → 201.

## Still open
- **CRD Fix B (correctness, not yet done):** vendor
  `k8s.io/apiextensions-apiserver/.../v1/generated.proto` (CRD + Spec/Names/
  Versions/Validation/JSONSchemaProps) into the registry so CRDs decode via the
  schema decoder instead of the lossy hand decoder (better `openAPIV3Schema`
  fidelity). Only meaningful now that the embedded-JSON fallback is removed.
- **Storage under load:** the run logged `(code: 522) disk I/O error` on CRD
  read/write/delete — a rhino/SQLite-under-load issue, unrelated to decode.
- **Environmental / not bugs:** several specs need ≥2 nodes (we run 1);
  no-preemption + scheduling-predicate timeouts.
- Network `service.go` timeouts and `crd_publish_openapi` timeout were likely
  collateral from the restart-count storm — re-check after the storm fix deploys.

## Next run
Rebuild + redeploy + relaunch via `scripts/lima-conformance.sh` (supersedes the
in-flight run) to validate the storm fix, the RoleBinding/Event/CRD decode
fixes, and the liveness restart together, then diff failures against this run.
