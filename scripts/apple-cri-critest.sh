#!/usr/bin/env bash
# critest harness for apple-cri: the CRI shim over Apple's `container` runtime.
#
# Runs the upstream cri-tools conformance suite (critest) natively on macOS
# against a throwaway apple-cri instance. Nothing here touches the developer's
# real cluster state: the shim gets its own unix socket and its own checkpoint
# root under a temp dir, and every object it creates is named `k8s_*` so
# teardown can find it.
#
# Usage:
#   bash scripts/apple-cri-critest.sh                  # the supported focus set
#   bash scripts/apple-cri-critest.sh --all            # the whole suite, no skips
#   FOCUS='PodSandbox' bash scripts/apple-cri-critest.sh
#   KEEP=1 bash scripts/apple-cri-critest.sh           # leave the shim running
set -uo pipefail

cd "$(dirname "$0")/.."
REPO="$PWD"

GREEN=$'\033[0;32m'; RED=$'\033[0;31m'; YELLOW=$'\033[1;33m'; NC=$'\033[0m'
info()  { echo "${GREEN}==>${NC} $*"; }
warn()  { echo "${YELLOW}==>${NC} $*"; }
fail()  { echo "${RED}==>${NC} $*" >&2; }

# ---- preconditions --------------------------------------------------------
[[ "$(uname -s)" == "Darwin" ]] || { fail "apple-cri only runs on macOS"; exit 1; }
command -v container >/dev/null || {
  fail "Apple's \`container\` CLI not found (brew install container)"; exit 1; }
command -v critest >/dev/null || {
  fail "critest not found (brew install cri-tools)"; exit 1; }

RUN_DIR="${RUN_DIR:-$(mktemp -d /tmp/apple-cri-critest.XXXXXX)}"
mkdir -p "$RUN_DIR"
SOCK="$RUN_DIR/apple-cri.sock"
ENDPOINT="unix://$SOCK"
ROOT_DIR="$RUN_DIR/state"
LOG="$RUN_DIR/apple-cri.log"
POD_NETWORK="${POD_NETWORK:-k8s-critest}"
# critest writes container logs under the log_directory it passes per sandbox;
# keep them inside the run dir.
export TMPDIR="${TMPDIR:-/tmp}"

# Specs that cannot pass on this runtime. Each entry is justified in
# crates/apple-cri/README.md ("critest coverage"); this list is the executable
# form of that table, not a convenience.
#
#
# Note: critest's darwin build already excludes the Linux-only suites
# (hostNetwork, sysctls, seccomp/apparmor/SELinux, capabilities, OOM,
# NamespaceOption, devices) — 5 of its 59 specs self-skip here — so this list
# only needs the specs that *are* offered but cannot hold on Apple's runtime.
DEFAULT_SKIP='runtime should support set hostname'
DEFAULT_SKIP+='|pulled from different registries'
DEFAULT_SKIP+='|should remove all tags from other registries'

# Upstream bug in `container` 1.2.0: signals never reach an *exec'd* process, so
# a timed-out ExecSync cannot be terminated and the spec's "timeout exec process
# should be gone" check finds it alive. The CLI logs
#   failed to send signal: [error: invalidArgument: "missing signal in xpc
#                           message", "signal": 15]
# because its own XPC client and server disagree on the field's type:
#   Sources/Services/ContainerAPIService/Client/ClientProcess.swift:83
#     request.set(key: .signal, value: Int64(signal))      // writes Int64
#   Sources/Services/ContainerAPIService/Server/Containers/ContainersService.swift:1154
#     guard let signal = self.string(key: .signal)         // reads String
# Container-level signals take the other path (ContainerClient.swift:168 writes a
# String), which is why StopContainer and `container kill` still work. This spec
# passed on 0.7.1; nothing in the shim changed. Not skippable by any workaround we
# control — the exec'd process's guest pid is never exposed to us.
DEFAULT_SKIP+='|runtime should support execSync with timeout'

# `--publish`'s host-port path goes through Apple's own proxy process, which
# dials the container from the host and is therefore subject to macOS Local
# Network privacy itself. It is a launchd-started helper that cannot present an
# authorization prompt, so the grant that fixes the specs below does not fix
# this one: the proxy accepts on loopback, fails its own dial, and resets
# (measured — zero requests reach the container). Not a shim limitation.
DEFAULT_SKIP+='|port mapping with host port and container port'

# Specs where the *shim* connects to a container. These pass only when the
# process running the shim holds macOS Local Network access; without it every
# host-originated packet to a container is dropped before egress. Verified both
# ways on this host: skipped-and-failing before the grant, passing after it.
# See the README "Host<->container connectivity" section.
if [[ -z "${INCLUDE_NETWORK:-}" ]]; then
  DEFAULT_SKIP+='|runtime should support portforward'
  DEFAULT_SKIP+='|port mapping with only container port'
fi

FOCUS="${FOCUS:-}"
SKIP="${SKIP-$DEFAULT_SKIP}"
if [[ "${1:-}" == "--all" ]]; then
  info "running the FULL suite with no skips (expect known failures)"
  SKIP=""
fi

# ---- teardown -------------------------------------------------------------
SHIM_PID=""
cleanup() {
  local rc=$?
  if [[ -n "${KEEP:-}" ]]; then
    warn "KEEP=1: leaving shim pid=$SHIM_PID endpoint=$ENDPOINT rundir=$RUN_DIR"
    return $rc
  fi
  if [[ -n "$SHIM_PID" ]] && kill -0 "$SHIM_PID" 2>/dev/null; then
    kill "$SHIM_PID" 2>/dev/null
    wait "$SHIM_PID" 2>/dev/null
  fi
  # Remove anything the shim named, then the test network.
  local leftovers
  leftovers=$(container list --all --format json 2>/dev/null \
    | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d=[]
print("\n".join(c["configuration"]["id"] for c in d
      if c.get("configuration",{}).get("id","").startswith("k8s_")))' 2>/dev/null)
  if [[ -n "$leftovers" ]]; then
    warn "removing leftover CRI containers:"; echo "$leftovers" | sed 's/^/    /'
    # shellcheck disable=SC2086
    echo "$leftovers" | xargs -n1 -I{} container rm --force {} >/dev/null 2>&1
  fi
  container network delete "$POD_NETWORK" >/dev/null 2>&1
  return $rc
}
trap cleanup EXIT

# ---- runtime + images -----------------------------------------------------
info "starting Apple's container runtime"
container system start >/dev/null 2>&1 || warn "\`container system start\` reported an error"

# critest's own default images (cri-tools pkg/framework/util.go). They are used
# as-is rather than overridden via --test-images-file: a conformance claim should
# be against the images the suite actually ships with. Pre-pulling only turns a
# registry hiccup into a setup error instead of a spec failure.
DEFAULT_CTR_IMAGE="registry.k8s.io/e2e-test-images/busybox:1.29-2"
WEB_SERVER_IMAGE="registry.k8s.io/e2e-test-images/nginx:1.14-2"
PLATFORM="linux/$(uname -m | sed 's/aarch64/arm64/;s/x86_64/amd64/')"

info "pre-pulling test images ($PLATFORM)"
for img in "$DEFAULT_CTR_IMAGE" "$WEB_SERVER_IMAGE"; do
  if container image inspect "$img" >/dev/null 2>&1; then
    echo "    have $img"
  else
    echo "    pulling $img"
    container image pull --progress none --platform "$PLATFORM" "$img" >/dev/null 2>&1 \
      || warn "could not pre-pull $img (the Image Manager specs pull their own)"
  fi
done

# ---- build + launch the shim ---------------------------------------------
info "building apple-cri (release-fast)"
make build-fast ARGS="-p apple-cri --bin apple-cri" >/dev/null || {
  fail "build failed"; exit 1; }
BIN="$REPO/target/release-fast/apple-cri"
[[ -x "$BIN" ]] || { fail "missing $BIN"; exit 1; }

mkdir -p "$ROOT_DIR"
info "starting apple-cri on $ENDPOINT (log: $LOG)"
RUST_LOG="${RUST_LOG:-info,apple_cri=debug}" "$BIN" \
  --cri-listen "$ENDPOINT" \
  --root-dir "$ROOT_DIR" \
  --pod-network "$POD_NETWORK" \
  --streaming-bind 127.0.0.1:0 \
  >"$LOG" 2>&1 &
SHIM_PID=$!

for _ in $(seq 1 100); do
  [[ -S "$SOCK" ]] && break
  kill -0 "$SHIM_PID" 2>/dev/null || { fail "shim exited early:"; tail -30 "$LOG"; exit 1; }
  sleep 0.1
done
[[ -S "$SOCK" ]] || { fail "socket never appeared:"; tail -30 "$LOG"; exit 1; }

info "runtime says:"
crictl -r "$ENDPOINT" -i "$ENDPOINT" version 2>/dev/null | sed 's/^/    /'
crictl -r "$ENDPOINT" -i "$ENDPOINT" info 2>/dev/null | head -20 | sed 's/^/    /'

# ---- run critest ---------------------------------------------------------
CRITEST_ARGS=(
  --runtime-endpoint "$ENDPOINT"
  --image-endpoint "$ENDPOINT"
  --ginkgo.timeout="${GINKGO_TIMEOUT:-20m}"
  --ginkgo.no-color
)
[[ -n "$FOCUS" ]] && CRITEST_ARGS+=(--ginkgo.focus "$FOCUS")
[[ -n "$SKIP" ]]  && CRITEST_ARGS+=(--ginkgo.skip "$SKIP")
[[ -n "${REPORT_DIR:-}" ]] && CRITEST_ARGS+=(--report-dir "$REPORT_DIR")

info "running critest $(critest --version 2>/dev/null | tail -1)"
echo "    focus: ${FOCUS:-<all>}"
echo "    skip:  ${SKIP:-<none>}"
RESULTS="$RUN_DIR/critest.log"
critest "${CRITEST_ARGS[@]}" 2>&1 | tee "$RESULTS"
RC=${PIPESTATUS[0]}

echo
info "summary"
grep -E "^(Ran |SUCCESS!|FAIL!|Summarizing)" "$RESULTS" | sed 's/^/    /'
if (( RC != 0 )); then
  fail "critest exited $RC — failing specs:"
  grep -E "^\[FAIL\]|^  \[FAIL\]" "$RESULTS" | sed 's/^/    /' | head -40
  echo
  warn "shim log tail:"; tail -40 "$LOG" | sed 's/^/    /'
  warn "full output: $RESULTS"
fi
exit $RC
