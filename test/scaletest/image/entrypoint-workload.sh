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
: "${LOAD_PROFILE:?LOAD_PROFILE required (path to YAML)}"

CLUSTER_ID="${POD_NAME}"

# ---- multi-shard endpoint resolution ----
#
# When BIGFLEET_SHARD_HEADLESS_DNS is set, the harness is running
# against a multi-shard StatefulSet. We pick a deterministic shard
# ordinal from the kwok-cluster's StatefulSet ordinal so a pod restart
# lands on the same shard, preserving cluster-to-shard binding (per
# the "clusters are permanently bound to shards on first contact"
# hard rule).
#
# POD_NAME convention: kwok-cluster-N (StatefulSet pod). If the pod
# name doesn't end in -N (legacy Deployment install / ad-hoc), fall
# back to ordinal 0.
#
# When BIGFLEET_SHARD_HEADLESS_DNS is unset, BIGFLEET_SHARD_ADDR must
# already be set by the chart — the legacy single-shard path.
if [[ -n "${BIGFLEET_SHARD_HEADLESS_DNS:-}" ]]; then
  : "${BIGFLEET_SHARD_REPLICAS:?BIGFLEET_SHARD_REPLICAS required when BIGFLEET_SHARD_HEADLESS_DNS is set}"
  : "${BIGFLEET_SHARD_PORT:?BIGFLEET_SHARD_PORT required when BIGFLEET_SHARD_HEADLESS_DNS is set}"
  POD_ORDINAL="${POD_NAME##*-}"
  if ! [[ "$POD_ORDINAL" =~ ^[0-9]+$ ]]; then
    POD_ORDINAL=0
  fi
  SHARD_ORDINAL=$((POD_ORDINAL % BIGFLEET_SHARD_REPLICAS))
  BIGFLEET_SHARD_ADDR="bigfleet-shard-${SHARD_ORDINAL}.${BIGFLEET_SHARD_HEADLESS_DNS}:${BIGFLEET_SHARD_PORT}"
fi
: "${BIGFLEET_SHARD_ADDR:?BIGFLEET_SHARD_ADDR required (or BIGFLEET_SHARD_HEADLESS_DNS + BIGFLEET_SHARD_REPLICAS + BIGFLEET_SHARD_PORT)}"

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
[[ -n "${OPERATOR_CO_LOCATION_KEY:-}" ]] && op_args+=(--co-location-key="$OPERATOR_CO_LOCATION_KEY")
log operator "starting (cluster=$CLUSTER_ID shard=$BIGFLEET_SHARD_ADDR qps=${OPERATOR_QPS:-default} ack=${OPERATOR_ACK_CONCURRENCY:-default})"
bigfleet-operator "${op_args[@]}" >"$WORK/logs/operator.log" 2>&1 &
OPERATOR_PID=$!

# ---- 3. (M43b) start the pod-shim + unschedulable-pod-controller.
# These run in Pod-mode (M44 default). The chart passes POD_MODE
# through the entrypoint env from loadProfile.mode; unset → "pods"
# (default), explicit "cr" opts into the legacy shape and skips the
# shim + unschedulable-pod-controller.
PODSHIM_PID=""
UPC_PID=""
if [[ "${POD_MODE:-pods}" == "pods" ]]; then
  # Pod-shim and unschedulable-pod-controller both hit the same
  # per-cluster apiserver as the operator, so they share its QPS/Burst
  # budget. M44.4: the pod-shim binder does 3 writes per UpcomingNode
  # (Create Node + Status Update + Bind) — at 50 QPS / 1000 Pods that
  # is ~60 s of queueing per cluster, which dominates user-facing
  # binding latency.
  podshim_args=(--kubeconfig="$KCFG" --metrics-addr="0.0.0.0:8772")
  upc_args=(--kubeconfig="$KCFG" --metrics-addr="0.0.0.0:8773")
  [[ -n "${OPERATOR_QPS:-}"   ]] && { podshim_args+=(--qps="$OPERATOR_QPS");   upc_args+=(--qps="$OPERATOR_QPS"); }
  [[ -n "${OPERATOR_BURST:-}" ]] && { podshim_args+=(--burst="$OPERATOR_BURST"); upc_args+=(--burst="$OPERATOR_BURST"); }
  # M45.5: bind / upcoming-node reconcile concurrency overrides for
  # high-Pod-count profiles where the default 64 / 8 fan-out throttles
  # chain throughput.
  [[ -n "${PODSHIM_BINDER_CONCURRENCY:-}"        ]] && podshim_args+=(--binder-concurrency="$PODSHIM_BINDER_CONCURRENCY")
  [[ -n "${PODSHIM_UPCOMING_NODE_CONCURRENCY:-}" ]] && podshim_args+=(--upcoming-node-concurrency="$PODSHIM_UPCOMING_NODE_CONCURRENCY")
  [[ -n "${PODSHIM_PPROF_ADDR:-}"                ]] && podshim_args+=(--pprof-addr="$PODSHIM_PPROF_ADDR")

  log podshim "starting (qps=${OPERATOR_QPS:-default})"
  bigfleet-scaletest-pod-shim "${podshim_args[@]}" >"$WORK/logs/podshim.log" 2>&1 &
  PODSHIM_PID=$!

  log upc "starting (qps=${OPERATOR_QPS:-default})"
  bigfleet-unschedulable-pod-controller "${upc_args[@]}" >"$WORK/logs/upc.log" 2>&1 &
  UPC_PID=$!
fi

# ---- 3.5. wait for pod-shim + UPC controller-runtime cache sync ----
# Both managers must complete initial LIST/WATCH sync against an empty
# apiserver before load-driver starts pumping Pods. With kine (writes
# throttled by sqlite WAL) the race was harmless. With etcd the
# apiserver writes outrun the controller-runtime cache: the sync wait
# condition (cache resourceVersion caught up to current rv) never
# holds while load-driver pumps. Manager exits at CacheSyncTimeout →
# workload container restart loop → binds metric never published.
#
# Right signal: controller-runtime logs "Starting Controller" exactly
# when WaitForCacheSync returns. Watching the per-process log file is
# precise (metrics-endpoint up is too early — the metrics server is
# a runnable that races with the cache informers).
if [[ "${POD_MODE:-pods}" == "pods" ]]; then
  log entrypoint "waiting for pod-shim + UPC cache sync"
  for who in podshim upc; do
    deadline=$((SECONDS + 300))
    while (( SECONDS < deadline )); do
      if [[ -f "$WORK/logs/$who.log" ]] && grep -q '"msg":"Starting Controller"' "$WORK/logs/$who.log"; then
        log entrypoint "$who cache synced"
        break
      fi
      sleep 0.5
    done
    if (( SECONDS >= deadline )); then
      log entrypoint "$who did not signal Starting Controller within 5m; continuing anyway"
    fi
  done
fi

# ---- 4. start the load-driver ----
log loadgen "starting (profile=$LOAD_PROFILE mode=${POD_MODE:-cr})"
load-driver \
  --kubeconfig="$KCFG" \
  --cluster-id="$CLUSTER_ID" \
  --profile="$LOAD_PROFILE" \
  --metrics-addr="0.0.0.0:8771" \
  >"$WORK/logs/loadgen.log" 2>&1 &
LOADGEN_PID=$!

log entrypoint "workload-side up"

trap 'kill $OPERATOR_PID $LOADGEN_PID ${PODSHIM_PID:-} ${UPC_PID:-} 2>/dev/null || true' SIGTERM SIGINT

while true; do
  for entry in "OPERATOR:operator" "LOADGEN:loadgen"; do
    name="${entry%:*}"
    log_basename="${entry#*:}"
    pidvar="${name}_PID"
    if ! kill -0 "${!pidvar}" 2>/dev/null; then
      log entrypoint "$name (pid ${!pidvar}) exited; tailing $WORK/logs/$log_basename.log and exiting"
      tail -n 50 "$WORK/logs/$log_basename.log" || true
      exit 1
    fi
  done
  if [[ -n "$PODSHIM_PID" ]] && ! kill -0 "$PODSHIM_PID" 2>/dev/null; then
    log entrypoint "podshim (pid $PODSHIM_PID) exited; tailing podshim.log and exiting"
    tail -n 50 "$WORK/logs/podshim.log" || true
    exit 1
  fi
  if [[ -n "$UPC_PID" ]] && ! kill -0 "$UPC_PID" 2>/dev/null; then
    log entrypoint "upc (pid $UPC_PID) exited; tailing upc.log and exiting"
    tail -n 50 "$WORK/logs/upc.log" || true
    exit 1
  fi
  sleep 2
done
