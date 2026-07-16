#!/bin/bash
set -e

# Cleanup script for Sonobuoy conformance tests
# This script removes all sonobuoy and test-job processes, containers, and data from etcd

echo "=== Sonobuoy and Test Cleanup Script ==="
echo ""

# Detect container runtime
# Override: CONTAINER_RUNTIME=docker or CONTAINER_RUNTIME=podman
if [ -n "$CONTAINER_RUNTIME" ]; then
  CRT="$CONTAINER_RUNTIME"
else
  HAS_PODMAN=false
  HAS_DOCKER=false
  # Use background + wait to timeout commands that may hang (e.g. docker ps when Docker Desktop is stopped)
  if command -v podman &>/dev/null; then
    podman ps &>/dev/null 2>&1 &
    PID=$!
    (
      sleep 3
      kill $PID 2>/dev/null
    ) &>/dev/null &
    wait $PID 2>/dev/null && HAS_PODMAN=true
  fi
  if command -v docker &>/dev/null; then
    docker ps &>/dev/null 2>&1 &
    PID=$!
    (
      sleep 3
      kill $PID 2>/dev/null
    ) &>/dev/null &
    wait $PID 2>/dev/null && HAS_DOCKER=true
  fi

  if $HAS_PODMAN && $HAS_DOCKER; then
    echo "ERROR: Both docker and podman are available. Set CONTAINER_RUNTIME=docker or CONTAINER_RUNTIME=podman"
    exit 1
  elif $HAS_PODMAN; then
    CRT=podman
  elif $HAS_DOCKER; then
    CRT=docker
  else
    echo "ERROR: No container runtime found"
    exit 1
  fi
fi
echo "Using container runtime: $CRT"
echo ""

# Step 1: Kill any running sonobuoy processes
echo "[1/3] Killing sonobuoy processes..."
pkill -f "sonobuoy run" 2>/dev/null || true
pkill -f "run-conformance.sh" 2>/dev/null || true
sleep 1

# Step 2: Delete all sonobuoy resources using sonobuoy CLI
# This removes all sonobuoy namespace resources from etcd
echo "[2/4] Deleting all sonobuoy namespace resources..."
sonobuoy delete --wait 2>/dev/null || echo "No sonobuoy resources to delete"

# Step 3: Delete test-job resources from default namespace (sonobuoy delete only handles sonobuoy namespace)
echo "[3/4] Deleting test-job resources from default namespace..."
# Delete test-job Jobs first (they create the pods)
TEST_JOB_JOBS=$($CRT exec rusternetes-etcd etcdctl get /registry/jobs/default/ --prefix --keys-only 2>/dev/null | grep -i test-job || true)
if [ -n "$TEST_JOB_JOBS" ]; then
  while IFS= read -r key; do
    [ -z "$key" ] && continue
    $CRT exec rusternetes-etcd etcdctl del "$key" >/dev/null 2>&1
    echo "  Deleted Job: $key"
  done <<<"$TEST_JOB_JOBS"
fi

# Delete test-job pods
TEST_JOB_PODS=$($CRT exec rusternetes-etcd etcdctl get /registry/pods/default/ --prefix --keys-only 2>/dev/null | grep -i test-job || true)
if [ -n "$TEST_JOB_PODS" ]; then
  while IFS= read -r key; do
    [ -z "$key" ] && continue
    $CRT exec rusternetes-etcd etcdctl del "$key" >/dev/null 2>&1
    echo "  Deleted pod: $key"
  done <<<"$TEST_JOB_PODS"
fi

# Step 4: Remove containers (sonobuoy delete doesn't clean up containers)
echo "[4/4] Removing containers..."
ALL_CONTAINERS=$($CRT ps -a --filter "name=sonobuoy" --filter "name=test-job" --format "{{.ID}}" 2>/dev/null || true)
if [ -n "$ALL_CONTAINERS" ]; then
  echo "$ALL_CONTAINERS" | xargs -r $CRT rm -f >/dev/null 2>&1
  CONTAINER_COUNT=$(echo "$ALL_CONTAINERS" | wc -l | tr -d ' ')
  echo "  Removed $CONTAINER_COUNT containers"
else
  echo "  No containers found"
fi

echo ""
echo "=== Cleanup Complete ==="
echo ""
