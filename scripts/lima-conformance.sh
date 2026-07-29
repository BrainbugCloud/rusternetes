#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Run Kubernetes conformance (sonobuoy) against the all-in-one rusternetes
# binary talking CRI to a *native* containerd inside the lima `default` VM,
# on a Mac. This is the lima counterpart to plan/12 (which targets a
# privileged pod on a Talos cluster).
#
# Design goal: the SIMPLEST setup that works on lima. Several plan-12
# workarounds are Talos-only and are deliberately NOT applied here:
#
#   * containerd native snapshotter + explicit unpack_config
#       — plan 12 needs these because the Talos pod rootfs is overlayfs and
#         containerd-on-overlay (nested) is rejected by the kernel. In the
#         lima VM containerd runs on the ext4 root, so the DEFAULT overlayfs
#         snapshotter works and there is no /etc/containerd/config.toml at all.
#   * iptables nft->legacy shims
#       — plan 12 needs these because the Talos kernel ships no legacy xtables.
#         lima's Ubuntu kernel has iptables-legacy, so kube-proxy works as-is.
#   * "re-extract the live cert + recreate every kube-root-ca.crt after each
#     restart" (CA rotation dance)
#       — replaced here by a PERSISTENT self-signed cert passed via
#         --tls-cert-file/--tls-key-file. The api-server loads it with
#         TlsConfig::from_pem_files, so the CA is stable across restarts and
#         the namespace controller (which reads /etc/kubernetes/pki/ca.crt)
#         auto-mints a correct, stable kube-root-ca.crt in every namespace.
#
# Prereqs already provisioned in the VM (see memory containerd-aio-bringup):
#   containerd 2.x + runc, CNI plugins in /opt/cni/bin + a bridge conflist,
#   protoc >= 22 in /usr/local/bin (+ includes in /usr/local/include),
#   sonobuoy + crictl, a build toolchain, and ~/rk (rsync target of the repo).
#
# Usage (from the Mac host):
#   bash scripts/lima-conformance.sh [MODE]
# MODE: sonobuoy mode (default: certified-conformance).

set -euo pipefail

VM="${LIMA_INSTANCE:-default}"
MODE="${1:-certified-conformance}"
K8S_VER="${K8S_VER:-v1.35.0}"
HOST_REPO="${HOST_REPO:-/Users/e28b0/git/rusternetes}"
RUN_DIR="${RUN_DIR:-/tmp/lima/rk-run}"      # logs (may live on the FUSE host mount)
# The SQLite DB MUST NOT live on the virtiofs host mount: WAL/mmap/checkpoints are
# slow over FUSE and stall the scheduler under load. Put it on tmpfs (/dev/shm) —
# it's a throwaway conformance DB so durability doesn't matter.
DB_DIR="${DB_DIR:-/dev/shm/rk-db}"
# Volumes MUST live on a VM-local real filesystem (ext4), not the FUSE/virtiofs
# host mount under /tmp/lima: virtiofs reports FUSE to statfs and silently drops
# file mode bits, which fails the [LinuxOnly] EmptyDir 0644/0666/0777 mode tests.
VOL_DIR="${VOL_DIR:-/var/lib/rusternetes/volumes}"
PKI=/etc/kubernetes/pki
SAN="localhost,127.0.0.1,192.168.5.15,node-1,10.96.0.1,kubernetes,kubernetes.default,kubernetes.default.svc,kubernetes.default.svc.cluster.local"

lsh() { limactl shell "$VM" -- bash -c "$1"; }

# Resolve to a real absolute path up front (not a deferred '$HOME/rk' string):
# step 5 execs the binary from inside `sudo bash -c '...'`, and sudo resets
# $HOME to /root's home, so a deferred $HOME reference silently resolves to
# the wrong (root) path there even though it works fine everywhere else.
RK="${RK:-$(limactl shell "$VM" -- printenv HOME)/rk}"

echo "==> 1/7 sync repo into the VM (preserve target/ for incremental builds)"
lsh "git config --global --add safe.directory $HOST_REPO 2>/dev/null || true
     rsync -a --delete --exclude .git/ --exclude target/ --exclude .rusternetes/ \
       $HOST_REPO/ $RK/"

echo "==> 2/7 build the all-in-one binary (glibc, native — TLS works)"
# NOTE: rsync -a preserves the host's (older) mtimes, so cargo may skip a real
# change. Touch sources to force a correct incremental rebuild.
lsh "cd $RK && find crates -name '*.rs' -exec touch {} + && touch Cargo.toml crates/*/Cargo.toml
     export PATH=\"\$HOME/.local/bin:/usr/local/bin:\$PATH\"
     export PROTOC=/usr/local/bin/protoc PROTOC_INCLUDE=/usr/local/include
     cargo build --release -p rusternetes"

echo "==> 3/7 generate a PERSISTENT self-signed cert (once) — no CA rotation"
lsh "sudo mkdir -p $PKI
     if [ ! -f $PKI/apiserver.crt ]; then
       cat > /tmp/san.cnf <<EOF
[req]
distinguished_name = dn
x509_extensions = v3
prompt = no
[dn]
CN = rusternetes-api
[v3]
subjectAltName = @alt
basicConstraints = critical,CA:TRUE
keyUsage = critical,digitalSignature,keyEncipherment,keyCertSign
extendedKeyUsage = serverAuth
[alt]
DNS.1 = localhost
DNS.2 = node-1
DNS.3 = kubernetes
DNS.4 = kubernetes.default
DNS.5 = kubernetes.default.svc
DNS.6 = kubernetes.default.svc.cluster.local
IP.1 = 127.0.0.1
IP.2 = 192.168.5.15
IP.3 = 10.96.0.1
EOF
       openssl ecparam -name prime256v1 -genkey -noout -out /tmp/apiserver.key
       openssl req -new -x509 -key /tmp/apiserver.key -out /tmp/apiserver.crt -days 3650 -config /tmp/san.cnf
       sudo cp /tmp/apiserver.crt $PKI/apiserver.crt
       sudo cp /tmp/apiserver.key $PKI/apiserver.key
       sudo cp /tmp/apiserver.crt $PKI/ca.crt   # self-signed: serving cert == CA
       sudo chmod 644 $PKI/*.crt; sudo chmod 640 $PKI/apiserver.key
     fi"

echo "==> 4/7 stop any old server, clean sandboxes, wipe the (bloat-prone) DB"
lsh "sudo pkill -x rusternetes 2>/dev/null || true; sleep 2
     sudo crictl rmp -fa 2>/dev/null || true
     sudo mkdir -p $DB_DIR; sudo rm -f $DB_DIR/rusternetes.db* $RUN_DIR/rusternetes.db*"
# Detach any medium:Memory tmpfs a prior run left mounted under the volume dir,
# else the rm -rf below hits EBUSY on the mountpoint. Deepest paths first.
lsh "findmnt -rno TARGET 2>/dev/null | grep -E '^$VOL_DIR(/|\$)' | sort -r \
     | xargs -r -I{} sudo umount -l {} 2>/dev/null || true
     sudo rm -rf $VOL_DIR; sudo mkdir -p $VOL_DIR"

echo "==> 5/7 start rusternetes with the persistent cert"
lsh "sudo bash -c 'setsid env \
       CONTAINER_RUNTIME_ENDPOINT=unix:///run/containerd/containerd.sock \
       IMAGE_SERVICE_ENDPOINT=unix:///run/containerd/containerd.sock \
       $RK/target/release/rusternetes \
       --storage-backend sqlite --data-dir $DB_DIR/rusternetes.db \
       --volume-dir $VOL_DIR --bind-address 0.0.0.0:6443 --node-name node-1 \
       --tls --tls-cert-file $PKI/apiserver.crt --tls-key-file $PKI/apiserver.key \
       --tls-san $SAN --kubernetes-service-host 10.96.0.1 --log-level info \
       > $RUN_DIR/rusternetes.log 2>&1 < /dev/null &'
     for i in \$(seq 1 30); do
       [ \"\$(curl -sk -o /dev/null -w '%{http_code}' https://127.0.0.1:6443/healthz)\" = 200 ] && break
       sleep 1
     done
     echo healthz=\$(curl -sk -o /dev/null -w '%{http_code}' https://127.0.0.1:6443/healthz)"

echo "==> 6/7 bootstrap (SA tokens, CoreDNS, kubernetes/kube-dns services)"
lsh "cd $RK
     cat > ~/rk-kubeconfig <<EOF
apiVersion: v1
kind: Config
clusters: [{name: rk, cluster: {server: 'https://127.0.0.1:6443', insecure-skip-tls-verify: true}}]
contexts: [{name: rk, context: {cluster: rk, user: rk}}]
current-context: rk
users: [{name: rk, user: {token: dummy}}]
EOF
     export KUBECONFIG=~/rk-kubeconfig
     K='kubectl --server https://127.0.0.1:6443 --insecure-skip-tls-verify --token dummy'
     bash scripts/generate-default-serviceaccounts.sh >/dev/null 2>&1
     \$K apply -f .rusternetes/default-serviceaccounts.yaml >/dev/null
     \$K apply -f bootstrap-cluster.yaml
     for i in \$(seq 1 20); do
       [ \"\$(\$K get pod coredns -n kube-system -o jsonpath='{.status.phase}' 2>/dev/null)\" = Running ] && break
       sleep 3
     done
     \$K get pods -A"

echo "==> 7/7 launch sonobuoy ($MODE, pinned $K8S_VER)"
lsh "export KUBECONFIG=~/rk-kubeconfig
     K='kubectl --server https://127.0.0.1:6443 --insecure-skip-tls-verify --token dummy'
     # pre-create the sonobuoy ns so the namespace controller mints its
     # kube-root-ca.crt (correct, stable CA) BEFORE the aggregator starts.
     \$K create ns sonobuoy 2>/dev/null || true; sleep 2
     sonobuoy gen --mode $MODE --kubernetes-version $K8S_VER > /tmp/sonobuoy-manifest.yaml
     \$K apply -f /tmp/sonobuoy-manifest.yaml
     sleep 10; sonobuoy status"

echo
echo "Conformance launched. Monitor with:"
echo "  limactl shell $VM -- env KUBECONFIG=~/rk-kubeconfig sonobuoy status"
echo "Retrieve results when complete:"
echo "  limactl shell $VM -- env KUBECONFIG=~/rk-kubeconfig sonobuoy retrieve"
