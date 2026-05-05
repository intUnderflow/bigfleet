# BigFleet operator guide

For the human running BigFleet — installing it, monitoring it, responding when something is wrong.

## Kubernetes version support

**Minimum: Kubernetes 1.31.** The helm charts (`bigfleet`, `bigfleet-operator`, `bigfleet-unschedulable-pod-controller`) declare `kubeVersion: ">= 1.31.0-0"` and helm will refuse to install on older clusters.

The binding constraint is the `CapacityRequest` CRD's `spec.versions[].selectableFields` block (used by the on-call runbook's `kubectl --field-selector=status.phase=Pending` query). SelectableFields went GA in Kubernetes 1.31; in 1.30 they're behind the `CustomResourceFieldSelectors` feature gate and require explicit opt-in. Older clusters reject the CRD at install time.

Other features BigFleet uses, all from earlier releases (listed for completeness, not as additional requirements):

| Feature | Used by | Stable since |
|---|---|---|
| CRD `subresource:status` | All `bigfleet.lucy.sh/v1alpha1` resources | 1.16 |
| `policy/v1` Eviction | M20 ReclaimInstruction handler (PDB-respecting drain) | 1.22 |
| `coordination.k8s.io/v1` Leases | controller-runtime managers | 1.14 |
| `NetworkPolicy` egress | M17.x `partition-coordinator-from-shard-N` failover-test action | 1.8 |
| `statefulset.kubernetes.io/pod-name` label | M17.x partition NetworkPolicy podSelector | 1.13 |

If you need to run BigFleet on a 1.30 cluster, the workaround is to drop the `selectableFields` block from the CRD before applying it and use the user-stories runbook's pre-M21 `jq` command for status-phase filtering. Not officially supported; we don't run CI against it.

## Architecture in one paragraph

BigFleet is two tiers. The **coordinator** (Tier 1) owns Raft-replicated fleet state — shard membership, cluster→shard map, topology-domain→shard assignments, quota allocations, provider registry. The **shards** (Tier 2) own machines and make provisioning decisions on their hot path. Per-cluster **operators** dial a shard over a long-lived bidirectional gRPC stream. The coordinator does *not* make provisioning decisions; shards do. Static stability is the load-bearing safety property: clusters keep running with BigFleet entirely down. (See the [BigFleet paper](https://lucy.sh/bigfleet) for the full architecture, also vendored at `docs/papers/bigfleet.md`.)

## Components

| Component | Where it runs | What it owns |
|---|---|---|
| Coordinator | Standalone (3 replicas, Raft) — typically a dedicated namespace | Membership, cluster→shard, domain→shard, quotas, provider registry |
| Shard | Standalone (~200 replicas at 100M-node scale; start with 1) | Per-shard machine inventory, decision engine, operator session terminations |
| Operator | One per Kubernetes cluster you want BigFleet to manage | CapacityRequest informer + roll-up; bootstrap blob renderer; reclaim handler |
| `bigfleet-unschedulable-pod-controller` | Optional, per cluster | Watches Pods → creates CapacityRequests for unschedulable ones |
| Provider | One process per real backend (cloud / bare metal). Lives in **separate repos**. | The actual machine lifecycle |

## Install

The coordinator and shards live in your management cluster (or a dedicated cluster). Each managed cluster runs its own operator. Charts are published to GHCR as OCI artefacts on every push to main; pin to the chart version (`Chart.yaml`'s `version` field) for reproducibility.

```sh
# 1. Install the BigFleet CRDs into every cluster you want managed.
kubectl apply -f https://github.com/intUnderflow/bigfleet/raw/main/api/crd/bigfleet.lucy.sh_capacityrequests.yaml
kubectl apply -f https://github.com/intUnderflow/bigfleet/raw/main/api/crd/bigfleet.lucy.sh_upcomingnodes.yaml
kubectl apply -f https://github.com/intUnderflow/bigfleet/raw/main/api/crd/bigfleet.lucy.sh_availablecapacities.yaml

# 2. Deploy the coordinator + shard control plane on the management cluster.
helm install bigfleet oci://ghcr.io/intunderflow/charts/bigfleet \
  --version 0.1.0 \
  --namespace bigfleet-system --create-namespace \
  --set coordinator.replicas=3 \
  --set coordinator.bootstrap=true   # only for the first install
# After the first install, helm upgrade with coordinator.bootstrap=false.

# 3. On each managed cluster, install the per-cluster operator.
helm install bigfleet-operator oci://ghcr.io/intunderflow/charts/bigfleet-operator \
  --version 0.1.0 \
  --namespace bigfleet-system --create-namespace \
  --set clusterID=cluster-prod-eu \
  --set shardAddress=bigfleet-shard.bigfleet-system:7780

# 4. (Optional) Install the unschedulable-pod controller on clusters
#    where you want BigFleet to react to Pod scheduling failures.
helm install bigfleet-unschedulable-pod-controller \
  oci://ghcr.io/intunderflow/charts/bigfleet-unschedulable-pod-controller \
  --version 0.1.0 \
  --namespace bigfleet-system

# 5. Register your providers with the coordinator (one CLI per provider).
#    Provider authoring is documented in docs/provider-author-guide.md.
```

Equivalent install commands using a git checkout (useful for development or air-gapped environments) replace `oci://ghcr.io/intunderflow/charts/<chart> --version <V>` with `./deploy/helm/<chart>` and drop the version flag.

## Day-2 observability

Each binary exposes Prometheus metrics on a configurable port:

| Binary | Default port | Path |
|---|---|---|
| Coordinator | `:8790` | `/metrics` |
| Shard | `:8780` | `/metrics` |
| Operator | `:8770` | `/metrics` |
| `bigfleet-unschedulable-pod-controller` | `:8080` | `/metrics` |

### Key metrics

**Health**
- `bigfleet_coordinator_raft_term` — increases on leader elections. Rapidly increasing = network partition or stepdown loop.
- `bigfleet_coordinator_apply_total{outcome=...}` — Apply outcomes. Spike in `error` or `fsm_error` = state-machine issue.
- `bigfleet_shard_cycle_duration_seconds` — histogram. p95 should stay below ~50 ms; p99 below 100 ms at 5K-machine inventory.
- `bigfleet_operator_session_reconnects_total` — should be near zero in steady state. Bursts = shard unhealthy or network blip.

**Throughput**
- `bigfleet_shard_actions_total{kind=...}` — Bootstrap / Provision / Reclaim / Preempt counts. Sustained high Preempt = priority-inversion churn; investigate the workloads' priorities.
- `bigfleet_operator_acknowledged_total` — CRs transitioning Pending → Acknowledged. Should track the rate of unschedulable-pod arrivals.
- `bigfleet_shard_inventory_machines{state=...}` — current machine counts by state. Stable in steady state; spikes through transitional states (Creating / Configuring / Draining / Deleting) during scale events.

**Pressure**
- `bigfleet_shard_shortfalls` — unresolved demand the shard couldn't satisfy locally. Persistent non-zero = under-provisioned fleet *or* over-aggressive workload priorities.
- `bigfleet_coordinator_pending_instructions{shard=...}` — coordinator-issued instructions awaiting ack. Should drain to zero between rebalance cycles.

### Suggested dashboard layout

Two panels per group:

1. **Coordinator**: Raft term (single-stat), Apply rate by outcome (timeseries), pending instructions per shard (timeseries).
2. **Shard**: cycle duration p50/p95/p99 (heatmap), action rate by kind (timeseries), inventory by state (stacked timeseries), shortfalls (single-stat with alert).
3. **Operator (per cluster)**: rollup duration p50/p95 (heatmap), CR acknowledgement rate (timeseries), session reconnects (single-stat with alert).

## Runbook

| Alert | What's happening | What to do |
|---|---|---|
| Shard `up==0` | The shard process is down | Restart. Existing pods/CRs in managed clusters are unaffected (static stability). New provisioning pauses until the shard returns. |
| Coordinator leader stepdown | Network blip or replica restart. New leader within ~1s normally. | Investigate if it loops. Check `bigfleet_coordinator_raft_term` rate. |
| Sustained `bigfleet_shard_shortfalls > 0` | Demand exceeds available capacity in the shard's slice | (a) Check the donor-shard summaries: is there free capacity elsewhere? Cross-shard rebalance should be active. (b) If fleet-wide saturated, procurement needs to add capacity. |
| `bigfleet_operator_session_reconnects_total` rising | Shard ↔ operator network unhealthy | Check shard health; check kubelet/CNI on the cluster running the operator. Operators reconnect with backoff automatically. |
| `bigfleet_shard_cycle_duration_seconds` p99 > 1s | Hot path overloaded | (a) Is the inventory huge? Check `bigfleet_shard_inventory_machines`. (b) Provider RPCs slow? Check provider's own metrics. (c) Consider sharding more aggressively. |

## Security

- **Operator → shard**: mTLS recommended. Per-cluster client certs scoped to the cluster's claimed `cluster_id`. Production deployments should add a server-side check that the cert's CN matches the `Hello.cluster_id`.
- **Shard → coordinator**: mTLS recommended. The coordinator gRPC service is internal; a network-policy fence inside the management cluster is also worthwhile.
- **Shard → provider**: depends on the provider. Cloud providers typically use cloud-IAM for the underlying API; the BigFleet ↔ provider gRPC channel between them is process-local on the shard host or cell, so a Unix socket or localhost-only TCP is reasonable.

## Upgrades

### CRD upgrades
v1alpha1 is in flux until v1beta1. Use `kubectl apply` for in-place CRD upgrades; the operator informers re-list on schema bumps. Never delete and recreate a CRD with existing CapacityRequest objects (you'll lose state).

### Coordinator upgrades
Helm upgrade is rolling — one replica at a time. The Raft cluster tolerates one replica down out of three; leader election handles stepdown automatically. **Always** upgrade with `coordinator.bootstrap=false` after the first install.

### Shard upgrades
Rolling. Each shard's existing assignments are persisted by the coordinator (cluster → shard, domain → shard); a fresh shard process re-reads them on startup and re-establishes provider connections.

### Operator upgrades
Rolling. The Shard.Session stream is reconnect-safe; in-flight bootstrap requests are reissued by the shard on the new connection.

## Cross-references

- Architecture: [BigFleet paper](https://lucy.sh/bigfleet) (vendored at `docs/papers/bigfleet.md`)
- Operating model: [Fleet-Scale Kubernetes paper](https://lucy.sh/fleet-scale-kubernetes) (vendored at `docs/papers/fleet-scale-kubernetes.md`)
- Implementation plan: `docs/plan.md`
- Provider authoring: `docs/provider-author-guide.md`
- Scaling sizing: `docs/scaling-guide.md`
