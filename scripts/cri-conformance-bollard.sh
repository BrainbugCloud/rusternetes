#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Run the CRI validation suite (critest) against bollard-cri inside a
# privileged Docker-in-Docker pod on a Kubernetes cluster, for hosts that
# have kubectl but no local Docker daemon or root (adapted from the aurae
# project's hack/cri-conformance-cluster.sh).
#
# The script:
#   1. (re)uses a privileged docker:dind pod pinned to NODE_NAME
#   2. streams the bollard-cri binary and version-matched cri-tools
#      (critest + crictl) into the pod over `kubectl exec`
#   3. starts bollard-cri against the pod's dockerd and runs critest
#   4. streams the run log back to REPORT_DIR
#
# Requirements:
#   - kubectl with pod create/exec/delete rights in NAMESPACE
#   - NAMESPACE must allow privileged pods (PodSecurity "privileged")
#   - a bollard-cri binary built for the target node's architecture, e.g.
#     CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
#     RUSTFLAGS="-C link-self-contained=yes" \
#     cargo build --target aarch64-unknown-linux-musl -p bollard-cri --release
#
# Environment overrides:
#   POD_NAME           pod name                       (default: bollard-cri-conformance)
#   NAMESPACE          namespace for the pod          (default: agent-sandbox-system)
#   NODE_NAME          node to pin the pod to         (default: bb-k8s-rk1b-01)
#   POD_IMAGE          DinD image                     (default: docker:28-dind)
#   BOLLARD_CRI_BIN    path to the bollard-cri binary (default: target/<arch>-unknown-linux-musl/release/bollard-cri)
#   CRI_TOOLS_VERSION  cri-tools release              (default: v1.36.0)
#   REPORT_DIR         host directory for results     (default: target/critest-bollard)
#   GINKGO_FOCUS       regex to focus critest specs   (default: run everything)
#   GINKGO_SKIP        regex to skip critest specs    (default: none)
#   KEEP_POD           "true" to leave the pod running (default: true — reused across runs)
#   RECREATE_POD       "true" to delete + recreate the pod first (default: false)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POD_NAME="${POD_NAME:-bollard-cri-conformance}"
NAMESPACE="${NAMESPACE:-agent-sandbox-system}"
NODE_NAME="${NODE_NAME:-bb-k8s-rk1b-01}"
POD_IMAGE="${POD_IMAGE:-docker:28-dind}"
CRI_TOOLS_VERSION="${CRI_TOOLS_VERSION:-v1.36.0}"
REPORT_DIR="${REPORT_DIR:-${REPO_ROOT}/target/critest-bollard}"
GINKGO_FOCUS="${GINKGO_FOCUS:-}"
GINKGO_SKIP="${GINKGO_SKIP:-}"
KEEP_POD="${KEEP_POD:-true}"
RECREATE_POD="${RECREATE_POD:-false}"

kexec() { kubectl -n "${NAMESPACE}" exec "${POD_NAME}" -- "$@"; }
kexec_in() { kubectl -n "${NAMESPACE}" exec -i "${POD_NAME}" -- "$@"; }

NODE_ARCH="$(kubectl get node "${NODE_NAME}" -o jsonpath='{.status.nodeInfo.architecture}')"
case "${NODE_ARCH}" in
    arm64) RUST_ARCH="aarch64" ;;
    amd64) RUST_ARCH="x86_64" ;;
    *)
        echo "error: unsupported node architecture '${NODE_ARCH}' on ${NODE_NAME}" >&2
        exit 1
        ;;
esac
BOLLARD_CRI_BIN="${BOLLARD_CRI_BIN:-${REPO_ROOT}/target/${RUST_ARCH}-unknown-linux-musl/release/bollard-cri}"

if [ ! -x "${BOLLARD_CRI_BIN}" ]; then
    echo "error: bollard-cri binary not found at ${BOLLARD_CRI_BIN}" >&2
    exit 1
fi

# Fetch (and cache) the cri-tools release tarballs on the host; the pod image
# has no curl and streaming over `kubectl exec` avoids relying on pod egress.
CRI_TOOLS_CACHE="${REPO_ROOT}/target/cri-tools/${CRI_TOOLS_VERSION}-${NODE_ARCH}"
mkdir -p "${CRI_TOOLS_CACHE}"
for tool in critest crictl; do
    tarball="${CRI_TOOLS_CACHE}/${tool}.tar.gz"
    if [ ! -s "${tarball}" ]; then
        echo "Downloading ${tool} ${CRI_TOOLS_VERSION} (${NODE_ARCH})"
        curl -sSfL -o "${tarball}" \
            "https://github.com/kubernetes-sigs/cri-tools/releases/download/${CRI_TOOLS_VERSION}/${tool}-${CRI_TOOLS_VERSION}-linux-${NODE_ARCH}.tar.gz"
    fi
done

cleanup() {
    if [ "${KEEP_POD}" = "true" ]; then
        echo "KEEP_POD=true — leaving pod ${NAMESPACE}/${POD_NAME} running"
        return
    fi
    echo "Deleting pod ${NAMESPACE}/${POD_NAME}"
    kubectl -n "${NAMESPACE}" delete pod "${POD_NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [ "${RECREATE_POD}" = "true" ]; then
    kubectl -n "${NAMESPACE}" delete pod "${POD_NAME}" --ignore-not-found --wait=true
fi

if ! kubectl -n "${NAMESPACE}" get pod "${POD_NAME}" >/dev/null 2>&1; then
    echo "Creating privileged DinD pod ${NAMESPACE}/${POD_NAME} on ${NODE_NAME}"
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_NAME}
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/name: bollard-cri-conformance
spec:
  nodeName: ${NODE_NAME}
  restartPolicy: Never
  containers:
    - name: dind
      image: ${POD_IMAGE}
      env:
        - name: DOCKER_TLS_CERTDIR
          value: ""
      securityContext:
        privileged: true
      resources:
        requests: { cpu: "1", memory: 4Gi }
        limits: { memory: 8Gi }
EOF
fi
kubectl -n "${NAMESPACE}" wait --for=condition=Ready "pod/${POD_NAME}" --timeout=180s

echo "Waiting for dockerd inside the pod"
kexec sh -c 'i=0; until docker info >/dev/null 2>&1; do i=$((i+1)); [ $i -gt 60 ] && echo "dockerd did not come up" && exit 1; sleep 1; done'

# critest's HostIpc specs shell out to ipcmk/ipcrm on the "host" (this pod);
# the alpine dind image does not ship util-linux.
kexec sh -c 'command -v ipcmk >/dev/null 2>&1 || apk add --no-cache util-linux >/dev/null'

echo "Copying test artifacts into the pod"
gzip -c "${BOLLARD_CRI_BIN}" | kexec_in sh -c 'gunzip > /usr/local/bin/bollard-cri.new && chmod +x /usr/local/bin/bollard-cri.new'
for tool in critest crictl; do
    kexec_in tar -xz -C /usr/local/bin < "${CRI_TOOLS_CACHE}/${tool}.tar.gz"
done

echo "(Re)starting bollard-cri inside the pod"
kexec sh -c '
    pkill -x bollard-cri 2>/dev/null || true
    sleep 1
    mv /usr/local/bin/bollard-cri.new /usr/local/bin/bollard-cri
    rm -f /var/run/bollard-cri.sock
    RUST_LOG=${RUST_LOG:-info} nohup /usr/local/bin/bollard-cri > /var/log/bollard-cri.log 2>&1 &
    i=0; until [ -S /var/run/bollard-cri.sock ]; do i=$((i+1)); [ $i -gt 30 ] && echo "bollard-cri did not come up" && cat /var/log/bollard-cri.log && exit 1; sleep 1; done
'

echo "Running critest inside the pod"
mkdir -p "${REPORT_DIR}"
RUN_LOG="${REPORT_DIR}/critest-run.log"
CRITEST_RC=0
kexec critest \
    --runtime-endpoint unix:///var/run/bollard-cri.sock \
    --image-endpoint unix:///var/run/bollard-cri.sock \
    --report-dir /root/critest \
    ${GINKGO_FOCUS:+--ginkgo.focus "${GINKGO_FOCUS}"} \
    ${GINKGO_SKIP:+--ginkgo.skip "${GINKGO_SKIP}"} \
    2>&1 | tee "${RUN_LOG}" || CRITEST_RC=$?

echo "Copying results and shim log back to ${REPORT_DIR}"
kexec sh -c 'cat /var/log/bollard-cri.log' > "${REPORT_DIR}/bollard-cri.log" 2>/dev/null || true
if kexec test -d /root/critest 2>/dev/null; then
    kexec tar -cz -C /root/critest . | tar -xz -C "${REPORT_DIR}" || true
fi

if [ "${CRITEST_RC}" -ne 0 ]; then
    echo "error: critest failed (exit ${CRITEST_RC}); see ${RUN_LOG}" >&2
    exit "${CRITEST_RC}"
fi
echo "critest run complete; report and logs in ${REPORT_DIR}"
