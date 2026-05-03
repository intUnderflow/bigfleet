#!/bin/bash
# entrypoint-workload.sh — runs the workload-side stack of the
# scaletest harness in its own container, isolated from the apiserver
# via separate cgroup CPU budgets.
#
# What this container runs:
#   1. bigfleet-operator   — dials BIGFLEET_SHARD_ADDR with cluster ID
#   2. load-driver         — emits CapacityRequest churn per profile
#
# Coordination with the apiserver container:
#   * Both containers share an emptyDir at /var/scaletest.
#   * The apiserver container writes the kubeconfig + certs there and
#     then touches /var/scaletest/apiserver-ready when it's serving.
#   * This script waits on that file before starting operator/load-driver.

set -euo pipefail

: "${POD_NAME:?POD_NAME required}"
: "${BIGFLEET_SHARD_ADDR:?BIGFLEET_SHARD_ADDR required}"
: "${LOAD_PROFILE:?LOAD_PROFILE required (path to YAML)}"

CLUSTER_ID="${POD_NAME}"
WORK="${WORK:-/var/scaletest}"
KCFG="$WORK/admin.kubeconfig"

log() { echo "[$(date -u +%H:%M:%S) $1] ${*:2}"; }

# ---- 1. wait for the apiserver container to be serving ----
log entrypoint "waiting for apiserver-side"
for i in {1..240}; do
  if [[ -f "$WORK/apiserver-ready" ]]; then
    break
  fi
  sleep 0.5
done
if [[ ! -f "$WORK/apiserver-ready" ]]; then
  log entrypoint "apiserver-ready file did not appear within 120s; aborting"
  exit 1
fi
log entrypoint "apiserver-side ready"

mkdir -p "$WORK/logs"

# ---- 2. start bigfleet-operator ----
op_args=(
  --cluster-id="$CLUSTER_ID"
  --shard-addr="$BIGFLEET_SHARD_ADDR"
  --kubeconfig="$KCFG"
  --metrics-addr="0.0.0.0:8770"
)
[[ -n "${OPERATOR_QPS:-}"             ]] && op_args+=(--qps="$OPERATOR_QPS")
[[ -n "${OPERATOR_BURST:-}"           ]] && op_args+=(--burst="$OPERATOR_BURST")
[[ -n "${OPERATOR_ACK_CONCURRENCY:-}" ]] && op_args+=(--ack-concurrency="$OPERATOR_ACK_CONCURRENCY")
[[ -n "${OPERATOR_ROLLUP_INTERVAL:-}" && "$OPERATOR_ROLLUP_INTERVAL" != "0s" ]] && op_args+=(--rollup-interval="$OPERATOR_ROLLUP_INTERVAL")
log operator "starting (cluster=$CLUSTER_ID shard=$BIGFLEET_SHARD_ADDR qps=${OPERATOR_QPS:-default} ack=${OPERATOR_ACK_CONCURRENCY:-default})"
bigfleet-operator "${op_args[@]}" >"$WORK/logs/operator.log" 2>&1 &
OPERATOR_PID=$!

# ---- 3. start the load-driver ----
log loadgen "starting (profile=$LOAD_PROFILE)"
load-driver \
  --kubeconfig="$KCFG" \
  --cluster-id="$CLUSTER_ID" \
  --profile="$LOAD_PROFILE" \
  --metrics-addr="0.0.0.0:8771" \
  >"$WORK/logs/loadgen.log" 2>&1 &
LOADGEN_PID=$!

log entrypoint "workload-side up"

trap 'kill $OPERATOR_PID $LOADGEN_PID 2>/dev/null || true' SIGTERM SIGINT

while true; do
  for name in OPERATOR LOADGEN; do
    pidvar="${name}_PID"
    if ! kill -0 "${!pidvar}" 2>/dev/null; then
      log entrypoint "$name (pid ${!pidvar}) exited; tailing logs and exiting"
      tail -n 50 "$WORK/logs/$(echo "$name" | tr '[:upper:]' '[:lower:]').log" || true
      exit 1
    fi
  done
  sleep 2
done
