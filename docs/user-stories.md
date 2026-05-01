# Working with BigFleet

What it's actually like to interact with BigFleet from each role. These walk through the commands, CRs, dashboards, and decision points — not abstractions or pitches. If a section feels obvious, it probably is for that role; skip ahead.

## Submitting a workload (application developer)

You're writing a Pod spec like you would on any Kubernetes cluster. BigFleet enters the picture only if the cluster runs the optional `bigfleet-unschedulable-pod-controller` and your Pod can't schedule on the existing node pool.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: trainer-2026-05-01-a
spec:
  priorityClassName: ml-research
  nodeSelector:
    node.kubernetes.io/instance-type: a3-highgpu-8g
  containers:
    - name: trainer
      image: internal/trainer:v42
      resources:
        limits: { nvidia.com/gpu: 8 }
```

If the cluster has no a3-highgpu-8g idle, the controller observes your Pod's `Pending` status and the scheduler's `0/N nodes available` message, and creates a `CapacityRequest` with the matching profile derived from the Pod:

```yaml
apiVersion: bigfleet.lucy.sh/v1alpha1
kind: CapacityRequest
metadata:
  name: pod-trainer-2026-05-01-a
  ownerReferences: [{ kind: Pod, name: trainer-2026-05-01-a, ... }]
spec:
  count: 1
  profile:
    requirements:
      - { key: node.kubernetes.io/instance-type, operator: In, values: [a3-highgpu-8g] }
    resources: { nvidia.com/gpu: "8" }
    priority: 1000000                # mapped from PriorityClass "ml-research"
    interruptionPenalty: 8192        # cluster default for this PriorityClass
    reclamationPenalty: 65536        # ditto
```

You watch `kubectl get pods -w`. The pod transitions Pending → Running once a node joins. The CR's `.status.phase` walks `Pending → Acknowledged`. If BigFleet can't satisfy (cloud quota exhausted, no preemptable victims at this priority), `.status.phase=Shortfall` and `.status.conditions` carries a reason.

What you choose: your `priorityClassName` and (if your platform team mapped it differently) your `interruptionPenalty`. Both affect cost and whether you can be preempted.

## Running a high-priority job (ML platform engineer)

You're submitting a Job whose Pods need to run together — N pods, each with the same Profile. From BigFleet's view this is N CRs with the same Profile fingerprint, aggregated to one row in the shard's NeedsTable.

If you want this run to *not* be interruptible once started, raise `interruptionPenalty`. The shard uses it in:

- **Phase 2 victim score**: `interruption_penalty + reclamation_penalty + drain_grace_remaining × hourly_cost`. A high score makes you a poor victim — Phase 2 will preempt other workloads first, even ones at slightly higher priority but with low penalties.
- **Phase 1 cost selection**: `effective_cost = price + (interruption_probability × interruption_penalty)`. A high penalty steers Phase 1 away from spot capacity (high `interruption_probability`) toward reserved/on-demand.

What you watch:

```promql
# Are my CRs being satisfied or sitting in Shortfall?
sum by (cluster) (
  bigfleet_capacity_requests{phase="Shortfall", cluster=~"$cluster"}
)

# Did Phase 2 preempt anything to make room?
sum by (cluster) (rate(bigfleet_shard_actions_total{kind="Reclaim"}[5m]))
```

If you see Reclaims firing on other clusters, look at which workloads got drained — their owners get a `NodeStateUpdate` of state `Draining` for nodes they were using. There's no global "you preempted these workloads" alert; teams instrument their own monitoring on the operator's `bigfleet_operator_node_state_updates_total{state="Draining"}` counter.

## Per-cluster operator install (cluster owner)

You own one or more clusters. The platform team owns the BigFleet shard. Your job is the operator chart:

```sh
helm install bigfleet-operator deploy/helm/bigfleet-operator \
  --namespace bigfleet-system --create-namespace \
  --set clusterID=cluster-prod-eu-1 \
  --set shardAddress=bigfleet-shard.bigfleet-system.svc:7780
```

The operator dials the shard from inside the cluster (outbound only — no inbound listener). After install, check:

```sh
kubectl -n bigfleet-system logs deploy/bigfleet-operator | head
# expect: "operator started ... rollup_interval=10s"

kubectl get availablecapacity
# CRs auto-written by the operator. If empty after 30s, the rollup loop hasn't synced.
```

You don't tune autoscaler parameters per-cluster anymore — there are none in the operator chart. The shard owns those. What stays your responsibility:

- The PriorityClasses your cluster offers (and the operator's mapping to BigFleet `priority` int values).
- Per-cluster compliance: which `nodeSelector` keys your `BootstrapTemplate` knows how to render userdata for.
- Pod Disruption Budgets your workloads carry — the operator respects them when handling `ReclaimInstruction`.

Watch `bigfleet_operator_session_reconnects_total`: a steady non-zero rate means the stream to the shard is unstable.

## Operating BigFleet itself (platform engineer)

You're running the coordinator + shards on a management cluster. Day-to-day work splits into three modes:

**Mode 1 — capacity-tier changes.** Adding a new instance type or capacity reservation means updating the provider's static config (it lives in the provider repo, separate from BigFleet) and the provider redeploys. BigFleet itself doesn't need to change. The shard discovers new inventory via the next `provider.List` reconcile.

**Mode 2 — rebalancing decisions.** When demand patterns shift (a region grows, another shrinks), you adjust shard count and topology-domain assignments. The coordinator owns those — you push a config change through Raft via the coordinator's gRPC admin endpoint, and the next `ReportShard` cycle distributes the new assignments.

**Mode 3 — incident response.** A shard's hot path is in-process; if it OOMs, restart it. State is recoverable from the provider's `List`. The bigger pathology to watch for is *coordinator quorum loss* — at that point new cross-shard rebalancing pauses, but every shard keeps running on its existing assignments. Static stability is the load-bearing property; you don't need to scramble.

Useful queries:

```promql
# Are any shards falling behind?
histogram_quantile(0.99,
  sum by (le, shard_id) (rate(bigfleet_shard_cycle_duration_seconds_bucket[5m]))
)

# Coordinator throughput.
sum(rate(bigfleet_coordinator_apply_total[5m]))

# Any cluster sessions flapping?
sum by (cluster) (rate(bigfleet_operator_session_reconnects_total[5m])) > 0
```

The shape of the day depends on whether anything is alarming. Most days: nothing.

## Cost analysis with penalty buckets (FinOps)

The penalty bucket field on `Profile` is the lever for cost analysis. Penalties are quantised to powers of 2 from $0.50 to $8.4M, so the cardinality is bounded and aggregations are stable. Useful queries:

```promql
# How much capacity sits behind each interruption-penalty tier?
sum by (interruption_penalty_bucket) (
  bigfleet_shard_inventory_machines{state="Configured"}
)

# Which clusters' workloads are clustered at high-penalty buckets?
sum by (cluster, interruption_penalty_bucket) (
  bigfleet_shard_needs_table_count
)

# Spot vs on-demand ratio per shard.
sum by (capacity_type) (bigfleet_shard_inventory_machines{state="Configured"})
  / ignoring(capacity_type) group_left
sum(bigfleet_shard_inventory_machines{state="Configured"})
```

Things to look for:

- A bucket-distribution that's bimodal at very high values, with no middle. Usually means teams copied a default value from somewhere and never tuned it. Worth checking against actual workload tolerance.
- Workloads at high `interruption_penalty` running on `capacity_type=spot`. The cost formula already discourages this, but if the price gap is small and the probability is low it can still happen — and is worth surfacing for review.
- Per-cluster aggregations that diverge from the fleet average. A cluster whose workloads are systematically higher-penalty than peers is a candidate for either dedicated reserved capacity or a conversation with the cluster owner.

## Triaging a capacity-stockout page (on-call)

The standard alert is `bigfleet_shard_shortfalls > 0 for 5m`. The runbook:

```sh
# 1. Which clusters / Profiles are unsatisfied?
kubectl get capacityrequests -A \
  -o jsonpath='{range .items[?(@.status.phase=="Shortfall")]}{.metadata.namespace}/{.metadata.name}: priority={.spec.profile.priority}{"\n"}{end}'

# 2. Is it a Phase-1 problem (no inventory) or a topology problem
#    (inventory present but constraint can't be satisfied within a shard)?
kubectl exec -n bigfleet-system bigfleet-shard-0 -- \
  /usr/local/bin/bigfleet shard dump-shortfalls
```

Decision tree:

- **Phase 1, no idle inventory, provider out of capacity.** File a quota-increase request, wait. Optionally raise the priorities of the shortfalled CRs above some other workloads — but only if you can justify the preemption to the affected teams.
- **Phase 1, no idle inventory, provider has capacity but isn't being asked.** Likely a Speculative-pool sizing issue. Check the coordinator's quota assignments for this shard.
- **Topology unsatisfiable within a shard.** A `Same`-rack request that the current shard can't fulfil. Cross-shard topology resolution is deliberately out of scope (per ADR); the workload either needs a different topology constraint or a different shard binding. This is rare in steady state.
- **Aging shortfalls escalating.** The shortfall buffer has a max age before it pages louder. Long-aged shortfalls usually mean a cluster's been mis-bound to a shard that doesn't have the right capacity profiles.

## Implementing a CapacityProvider (provider author)

You're writing a separate process that implements `CapacityProvider`. Six RPCs, no Watch.

```sh
# Stub it out:
go mod init github.com/yourcorp/your-provider
# Copy the .proto from the BigFleet repo, generate Go bindings.
# Implement Create/Configure/Drain/Delete/Get/List against your backend.

# Run the conformance suite against your endpoint:
make conformance-build
./bin/conformance --provider-addr=localhost:9001
```

The suite walks ~50 scenarios. Categories:

- **Idempotency**: re-issue the same `Create` 100x with the same machine_id. Should return the same op_id every time and only act once.
- **Transitional-state recovery**: kill the provider mid-`Configure`. Restart. The shard's next `List + Get` should observe the in-progress state correctly.
- **Cursor correctness**: if you support `since_revision`, the suite verifies that List with a cursor returns only deltas, that the cursor advances monotonically, and that resuming from an old cursor still works.
- **Drain-grace handling**: a Drain that's interrupted partway must end up in `Failed` with `last_error`, not silently revert.

What the suite *won't* catch: backend-specific edge cases (AWS quota boundaries, your private cloud's eventual-consistency window). Those are your tests, in your repo. The suite establishes that your provider is *protocol-correct*.

## Capacity planning (capacity planner)

Your input is fleet-level demand history. The query is the aggregate, not the per-cluster sum:

```promql
quantile_over_time(0.99,
  sum(bigfleet_shard_inventory_machines{state=~"Configured|Configuring"})[90d:1h]
)

# Decompose by capacity type, since reserved vs spot vs on-demand
# have different lead times and commitment shapes.
quantile_over_time(0.99,
  sum by (capacity_type) (
    bigfleet_shard_inventory_machines{state=~"Configured|Configuring"}
  )[90d:1h]
)
```

The headroom buffer you apply to the p99 is a policy choice. Two factors push it up:

- **Provisioning lead time of the underlying capacity**. If your cloud takes 4 minutes to bring a node up and your workloads spike on a 2-minute timescale, you need static headroom for the gap.
- **Demand bursts that are correlated across clusters**. The point of the fleet-aggregate query is that uncorrelated peaks cancel out — but if your fleet has a daily synchronised batch job, that's a correlated peak that won't smooth.

The scaling guide ([`scaling-guide.md`](scaling-guide.md)) tabulates per-tier sizing assumptions; calibrate against it, then look at your actual demand to decide where you actually sit.

## Pre-release validation with failover-soak (reliability)

You're gating the release on the static-stability invariant: clusters keep running with BigFleet entirely down. The `failover-soak` profile does this quantitatively.

```sh
scaletest-runner \
  --profile=test/scaletest/profiles/failover-soak.yaml \
  --duration=60m \
  --output=./results/$(date +%Y%m%d)-failover/
```

The profile spins up 50 KWOK-faked clusters at 1K CRs each, holds steady for 10 minutes, then kills the coordinator's Raft leader. It does this twice. The runner asserts:

- `bigfleet_shard_inventory_machines{state="Configured"}` total is unchanged across the leader-kill window.
- `bigfleet_operator_session_reconnects_total` increases by ≤ 1 per cluster (the unavoidable post-leader-election reconnection).
- No `CapacityRequest.status.phase` transitions backward (Acknowledged → Pending) during the kill window.

If any assertion fails, the run is marked failed and the release is blocked. The `summary.json` and the Prometheus snapshot get archived for post-mortem.

This isn't a sales pitch for static stability — it's a check that the property still holds after every change to `pkg/coordinator` or `pkg/shard`. The property is easy to break accidentally; the check is cheap.

## Notes that aren't role-specific

- **Priority + interruption-penalty + reclamation-penalty are the three numbers everyone looks at.** Different roles read them differently — workload owners as a self-description, BigFleet operators as inputs to the engine, FinOps as a cost lever — but it's the same fields.
- **Static stability is felt as the absence of incidents.** Most users never see BigFleet's failure modes because the property holds; the people who *do* see it are the ones running BigFleet itself, and even then mostly in pre-release tests.
- **Out-of-tree providers means the platform team's provider release cadence is decoupled from BigFleet's.** When BigFleet ships a new version, you don't have to redeploy your provider; when your provider ships, you don't have to coordinate with BigFleet maintainers.

## See also

- [Quickstart](quickstart/) — bring up BigFleet on a kind cluster.
- [Architecture](architecture/) — how the engine works.
- [Operator guide](operator-guide/) — install and runbook.
- [Provider author guide](provider-author-guide/) — writing your own CapacityProvider.
- [Scale-test runbook](scaletest/) — running the scenarios above against any cluster.
