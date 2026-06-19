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

CLUSTER_ID="${CLUSTER_ID_PREFIX:-}${POD_NAME}"

# ---- multi-shard endpoint resolution ----
#
# Three resolution paths, in precedence order. All three pick a
# deterministic shard ordinal from the kwok-cluster's StatefulSet
# ordinal (POD_NAME = kwok-cluster-N) so a pod restart lands on the
# same shard, preserving cluster-to-shard binding (the "clusters are
# permanently bound to shards on first contact" hard rule). If the pod
# name doesn't end in -N (legacy Deployment install / ad-hoc), the
# ordinal falls back to 0.
#
#   1. BIGFLEET_SHARD_OVERRIDE_HOST set (cross-host MULTI-shard
#      satellite): dial <host>:(portBase + ordinal) — the local end of
#      that ordinal's SSH/TCP tunnel to a remote per-ordinal NodePort.
#   2. BIGFLEET_SHARD_HEADLESS_DNS set (in-cluster multi-shard): dial
#      the per-pod headless DNS for the ordinal.
#   3. Neither set: BIGFLEET_SHARD_ADDR must already be exported by the
#      chart — the legacy single-endpoint path (single-host, or the
#      cross-host SINGLE-shard satellite via shard.overrideAddr).
shard_ordinal_from_pod() {
  local ord="${POD_NAME##*-}"
  if ! [[ "$ord" =~ ^[0-9]+$ ]]; then
    ord=0
  fi
  echo $((ord % $1))
}
if [[ -n "${BIGFLEET_SHARD_OVERRIDE_HOST:-}" ]]; then
  : "${BIGFLEET_SHARD_OVERRIDE_PORT_BASE:?BIGFLEET_SHARD_OVERRIDE_PORT_BASE required when BIGFLEET_SHARD_OVERRIDE_HOST is set}"
  : "${BIGFLEET_SHARD_REPLICAS:?BIGFLEET_SHARD_REPLICAS required when BIGFLEET_SHARD_OVERRIDE_HOST is set}"
  SHARD_ORDINAL="$(shard_ordinal_from_pod "$BIGFLEET_SHARD_REPLICAS")"
  BIGFLEET_SHARD_ADDR="${BIGFLEET_SHARD_OVERRIDE_HOST}:$((BIGFLEET_SHARD_OVERRIDE_PORT_BASE + SHARD_ORDINAL))"
elif [[ -n "${BIGFLEET_SHARD_HEADLESS_DNS:-}" ]]; then
  : "${BIGFLEET_SHARD_REPLICAS:?BIGFLEET_SHARD_REPLICAS required when BIGFLEET_SHARD_HEADLESS_DNS is set}"
  : "${BIGFLEET_SHARD_PORT:?BIGFLEET_SHARD_PORT required when BIGFLEET_SHARD_HEADLESS_DNS is set}"
  SHARD_ORDINAL="$(shard_ordinal_from_pod "$BIGFLEET_SHARD_REPLICAS")"
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

# ---- 2. bigfleet-operator deferred ----
# ADR-0036: the BigFleet operator must NOT start until the cluster's
# demand is established. A production operator joins a cluster that
# already runs workloads, so its first rollup reflects real demand
# and Phase 3's first-rollup gate releases on genuine state. The
# harness mirrors this: operator startup is deferred to step 5,
# after the load-driver has filled the cluster and signalled the
# demand-ready file. op_args is assembled here; the process starts
# below.
DEMAND_READY_FILE="$WORK/demand-ready"
rm -f "$DEMAND_READY_FILE"
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

# ---- 3. (M43b / ADR-0023) start the scheduler-side daemons.
# Two paths:
#   - HARNESS_SCHEDULER=pod-shim (default, legacy): launch the
#     bigfleet-scaletest-pod-shim binary, which does both
#     UpcomingNode→fake-Node creation AND Pod marking-Unschedulable
#     AND Pod binding.
#   - HARNESS_SCHEDULER=kube-scheduler (ADR-0023, new): launch
#     bigfleet-scaletest-node-creator (UpcomingNode→fake-Node only)
#     and rely on the real kube-scheduler running in the apiserver
#     container to mark Unschedulable + bind Pods.
#
# In both cases the unschedulable-pod-controller (UPC) runs — it
# converts Unschedulable Pods into CapacityRequests, which is BigFleet's
# input. UPC's source of Unschedulable Pods differs between paths:
# pod-shim explicitly marks them; kube-scheduler emits them when no
# Node fits (the normal Kubernetes path).
PODSHIM_PID=""
NODE_CREATOR_PID=""
UPC_PID=""
if [[ "${POD_MODE:-pods}" == "pods" ]]; then
  upc_args=(--kubeconfig="$KCFG" --metrics-addr="0.0.0.0:8773")
  [[ -n "${OPERATOR_QPS:-}"   ]] && upc_args+=(--qps="$OPERATOR_QPS")
  [[ -n "${OPERATOR_BURST:-}" ]] && upc_args+=(--burst="$OPERATOR_BURST")

  case "${HARNESS_SCHEDULER:-pod-shim}" in
    kube-scheduler)
      # ADR-0023: node-creator owns just one job (UpcomingNode →
      # fake-Node). Scheduling and binding move to the real
      # kube-scheduler in the apiserver container.
      node_creator_args=(--kubeconfig="$KCFG" --metrics-addr="0.0.0.0:8775")
      [[ -n "${OPERATOR_QPS:-}"                      ]] && node_creator_args+=(--qps="$OPERATOR_QPS")
      [[ -n "${OPERATOR_BURST:-}"                    ]] && node_creator_args+=(--burst="$OPERATOR_BURST")
      [[ -n "${PODSHIM_UPCOMING_NODE_CONCURRENCY:-}" ]] && node_creator_args+=(--concurrency="$PODSHIM_UPCOMING_NODE_CONCURRENCY")
      log node-creator "starting (kube-scheduler path)"
      bigfleet-scaletest-node-creator "${node_creator_args[@]}" >"$WORK/logs/node-creator.log" 2>&1 &
      NODE_CREATOR_PID=$!
      ;;
    pod-shim|"")
      # Legacy path: pod-shim owns marking + binding.
      podshim_args=(--kubeconfig="$KCFG" --metrics-addr="0.0.0.0:8772")
      [[ -n "${OPERATOR_QPS:-}"   ]] && podshim_args+=(--qps="$OPERATOR_QPS")
      [[ -n "${OPERATOR_BURST:-}" ]] && podshim_args+=(--burst="$OPERATOR_BURST")
      # M45.5: bind / upcoming-node reconcile concurrency overrides for
      # high-Pod-count profiles where the default 64 / 8 fan-out throttles
      # chain throughput.
      [[ -n "${PODSHIM_BINDER_CONCURRENCY:-}"        ]] && podshim_args+=(--binder-concurrency="$PODSHIM_BINDER_CONCURRENCY")
      [[ -n "${PODSHIM_UPCOMING_NODE_CONCURRENCY:-}" ]] && podshim_args+=(--upcoming-node-concurrency="$PODSHIM_UPCOMING_NODE_CONCURRENCY")
      [[ -n "${PODSHIM_PPROF_ADDR:-}"                ]] && podshim_args+=(--pprof-addr="$PODSHIM_PPROF_ADDR")
      log podshim "starting (qps=${OPERATOR_QPS:-default})"
      bigfleet-scaletest-pod-shim "${podshim_args[@]}" >"$WORK/logs/podshim.log" 2>&1 &
      PODSHIM_PID=$!
      ;;
    *)
      log entrypoint "HARNESS_SCHEDULER=$HARNESS_SCHEDULER is not a known value; expected 'pod-shim' or 'kube-scheduler'"
      exit 1
      ;;
  esac

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
  case "${HARNESS_SCHEDULER:-pod-shim}" in
    kube-scheduler) sync_targets=("node-creator" "upc") ;;
    *)              sync_targets=("podshim" "upc") ;;
  esac
  log entrypoint "waiting for ${sync_targets[*]} cache sync"
  for who in "${sync_targets[@]}"; do
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
  --demand-ready-file="$DEMAND_READY_FILE" \
  >"$WORK/logs/loadgen.log" 2>&1 &
LOADGEN_PID=$!

# ---- 5. start bigfleet-operator once demand is established ----
# ADR-0036: wait for the load-driver to finish its initial Pod fill
# (→ UPC → CapacityRequests) before starting the operator. The
# operator's first rollup then carries real demand, so Phase 3's
# first-rollup gate doesn't reclaim the seeded Configured supply.
# Generous 30-min cap: rampTo is object-creation only, so this is
# normally minutes; the cap just bounds a wedged load-driver.
log entrypoint "waiting for demand-ready signal before starting operator"
wait_deadline=$((SECONDS + 1800))
while [[ ! -f "$DEMAND_READY_FILE" ]]; do
  if ! kill -0 "$LOADGEN_PID" 2>/dev/null; then
    log entrypoint "load-driver exited before signalling demand-ready; tailing loadgen.log and exiting"
    tail -n 50 "$WORK/logs/loadgen.log" || true
    exit 1
  fi
  if (( SECONDS >= wait_deadline )); then
    log entrypoint "demand-ready signal did not appear within 30m; starting operator anyway"
    break
  fi
  sleep 2
done
log operator "starting (cluster=$CLUSTER_ID shard=$BIGFLEET_SHARD_ADDR qps=${OPERATOR_QPS:-default} ack=${OPERATOR_ACK_CONCURRENCY:-default})"
bigfleet-operator "${op_args[@]}" >"$WORK/logs/operator.log" 2>&1 &
OPERATOR_PID=$!

log entrypoint "workload-side up"

trap 'kill $OPERATOR_PID $LOADGEN_PID ${PODSHIM_PID:-} ${NODE_CREATOR_PID:-} ${UPC_PID:-} 2>/dev/null || true' SIGTERM SIGINT

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
  if [[ -n "$NODE_CREATOR_PID" ]] && ! kill -0 "$NODE_CREATOR_PID" 2>/dev/null; then
    log entrypoint "node-creator (pid $NODE_CREATOR_PID) exited; tailing node-creator.log and exiting"
    tail -n 50 "$WORK/logs/node-creator.log" || true
    exit 1
  fi
  if [[ -n "$UPC_PID" ]] && ! kill -0 "$UPC_PID" 2>/dev/null; then
    log entrypoint "upc (pid $UPC_PID) exited; tailing upc.log and exiting"
    tail -n 50 "$WORK/logs/upc.log" || true
    exit 1
  fi
  sleep 2
done
