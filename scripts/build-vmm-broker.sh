#!/usr/bin/env bash
# Build and entitle the VMM broker.
#
# The broker owns each pod's VZVirtualMachine, which requires the
# `com.apple.security.virtualization` entitlement — so the binary must be signed
# after every build, or it fails at VM creation rather than at launch.
set -euo pipefail

cd "$(dirname "$0")/.."
BROKER_DIR="$PWD/vmm-broker"
CONFIG="${CONFIG:-debug}"

[[ "$(uname -s)" == "Darwin" ]] || { echo "the vmm broker only builds on macOS" >&2; exit 1; }
command -v swift >/dev/null || { echo "swift not found (install Xcode)" >&2; exit 1; }

echo "==> building ($CONFIG)"
swift build --package-path "$BROKER_DIR" $([[ "$CONFIG" == "release" ]] && echo -c release)

BIN="$BROKER_DIR/.build/$CONFIG/rusternetes-vmm"
[[ -x "$BIN" ]] || { echo "missing $BIN" >&2; exit 1; }

echo "==> signing with com.apple.security.virtualization"
codesign --force --sign - --entitlements "$BROKER_DIR/entitlements.plist" "$BIN"

# Fail loudly here rather than at the first CreateVm.
codesign -d --entitlements - "$BIN" 2>&1 | grep -q "com.apple.security.virtualization" \
  || { echo "entitlement did not stick" >&2; exit 1; }

echo "==> ok: $BIN"
