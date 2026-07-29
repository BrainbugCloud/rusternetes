#!/usr/bin/env bash
# Re-vendor the CRI proto into crates/cri-proto/proto/.
# The vendored file is checked in; builds must not hit the network.
set -euo pipefail

BRANCH="${1:-release-1.36}"
URL="https://raw.githubusercontent.com/kubernetes/cri-api/${BRANCH}/pkg/apis/runtime/v1/api.proto"
DEST="$(cd "$(dirname "$0")/.." && pwd)/crates/cri-proto/proto/${BRANCH}.proto"

echo "Fetching ${URL}"
curl -fsSL "$URL" -o "$DEST"
echo "Vendored to ${DEST}"
echo "If the branch changed, update crates/cri-proto/build.rs and Cargo.toml description."
