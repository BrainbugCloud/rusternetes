# Task: Fix 2 remaining critest failures

## Context

Full critest v1.36.0 run completed: **94 Passed | 2 Failed | 26 Skipped**

Two tests still fail. Investigate and fix both.

## Failures

### 1. Image Consistency — simultaneous RemoveImage
```
[FAIL] [k8s.io] Image Consistency [It] should not fail on simultaneous RemoveImage calls [Conformance] [Serial]
/home/runner/work/cri-tools/cri-tools/pkg/validate/image_consistency.go:173
```
Likely a race condition in `remove_image` — concurrent calls may conflict. Need to add locking or make the operation idempotent under concurrent access.

### 2. ReopenContainerLog
```
[FAIL] [k8s.io] Container runtime should support log [It] runtime should support reopening container log [Conformance]
/home/runner/work/cri-tools/cri-tools/pkg/validate/container.go:720
```
`ReopenContainerLog` RPC is not working correctly. The log relay needs to properly close the current log file and reopen it so the container runtime can rotate logs.

## Resources

- JUnit report: `target/critest-bollard/critest-run.log` and any XML in that dir
- Test pod still running: `kubectl -n agent-sandbox-system exec bollard-cri-conformance -- cat /root/critest/junit_01.xml` (or similar)
- Source: `crates/bollard-cri/src/images.rs` (RemoveImage), `crates/bollard-cri/src/logs.rs` (ReopenContainerLog)
- cri-server crate has the ReopenContainerLog handler

## Acceptance Criteria
- [x] Both failures fixed
- [x] Re-run critest (or at least the focused suites) to confirm green
- [x] Commit fixes

## Build
```bash
export LIBCLANG_PATH=$HOME/.local/libclang/usr/lib/llvm-19/lib
export PATH=$HOME/.local/bin:$HOME/.npm-global/bin:$PATH
cargo build --target aarch64-unknown-linux-musl -p bollard-cri
```

## Test
The test pod is still running at `agent-sandbox-system/bollard-cri-conformance`.
Use `scripts/cri-conformance-bollard.sh` or run critest directly inside the pod.
Focus on the failing suites:
```bash
GINKGO_FOCUS="Image Consistency" critest --runtime-endpoint unix:///var/run/bollard-cri.sock
GINKGO_FOCUS="reopening container log" critest --runtime-endpoint unix:///var/run/bollard-cri.sock
```
