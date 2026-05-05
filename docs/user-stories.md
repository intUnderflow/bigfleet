# Working with BigFleet

What it's actually like to interact with BigFleet from each role. These walk through the commands, CRs, and decision points using only what the reference implementation actually ships today; sections that describe future tooling are flagged. If a section feels obvious for your role, it probably is — skip ahead.

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

If the cluster has no `a3-highgpu-8g` idle, the controller observes your Pod's `Pending` status and the scheduler's `0/N nodes available` message, and creates a `CapacityRequest` with the matching profile derived from the Pod:

```yaml
apiVersion: bigfleet.lucy.sh/v1alpha1
kind: CapacityRequest
metadata:
  name: pod-trainer-2026-05-01-a
  ownerReferences: [{ kind: Pod, name: trainer-2026-05-01-a, ... }]
spec:
  requirements:
    - { key: node.kubernetes.io/instance-type, operator: In, values: [a3-highgpu-8g] }
  resources:
    nvidia.com/gpu: "8"
  priority: 1000000           # mapped from PriorityClass "ml-research"
  interruptionPenalty: "8192" # cluster default for this PriorityClass, in dollars
  reclamationPenalty: "65536"
```

You watch `kubectl get pods -w`. The Pod transitions Pending → Running once a node joins. The CR's `.status.phase` walks `Pending → Acknowledged` once the autoscaler has it in its NeedsTable; that's the only transition the lifecycle supports.

What you choose: your `priorityClassName` and (if your platform team mapped it differently) your `interruptionPenalty`. Both affect cost and whether you can be preempted.

## Running a high-priority job (ML platform engineer)

You're submitting a Job whose Pods need to run together — N pods, each with the same Profile. From BigFleet's view this is N CRs with the same Profile fingerprint, aggregated into one row in the shard's NeedsTable.

If you want this run to *not* be interruptible once started, raise `interruptionPenalty`. The shard uses it in two places:

- **Phase 1 cost selection.** `effective_cost = price + interruption_probability × interruption_penalty`. A high penalty pushes Phase 1 away from spot capacity (high `interruption_probability`) and toward reserved or on-demand.
- **Phase 2 victim score.** When some other workload's preemptor needs your machine, the shard scores possible victims and picks the highest-scoring ones. The score is built from *reciprocal* weights, so a high penalty on you contributes a *small* number, dragging your score down — making you a poor victim. The full formula:

  ```text
  score = priority_gap     × w_priority
        + (1 / drain_seconds)        × w_drain
        + (1 / interruption_penalty) × w_interrupt
        + (1 / reclamation_penalty)  × w_reclaim
  ```

  All four weights are positive constants. Higher `priority_gap` (preemptor much higher priority) makes you a good victim; higher penalty values keep you safe.

What you watch:

```promql
# Are any shards reporting unresolved demand?
bigfleet_shard_shortfalls > 0

# Did Phase 2 preempt anything to make room?
sum by (kind) (rate(bigfleet_shard_actions_total{kind="Preempt"}[5m]))
```

If you see Preempt actions firing, the affected nodes are written into per-cluster `UpcomingNode` CRs whose status walks toward `Draining` / `Drained`. You can watch your own cluster's nodes via `kubectl get upcomingnodes`.

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

- The PriorityClasses your cluster offers (and the unschedulable-pod-controller's mapping to BigFleet `priority` int values).
- Per-cluster compliance: which `nodeSelector` keys your `BootstrapTemplate` knows how to render userdata for.
- Pod Disruption Budgets your workloads carry — the operator respects them when handling `ReclaimInstruction`.

Watch `bigfleet_operator_session_reconnects_total`: a steady non-zero rate means the stream to the shard is unstable.

## Operating BigFleet itself (platform engineer)

You're running the coordinator + shards on a management cluster. Day-to-day work splits into three modes:

**Mode 1 — capacity-tier changes.** Adding a new instance type or capacity reservation means updating the provider's static config (the provider lives in a separate repo, not in this one) and the provider redeploys. BigFleet itself doesn't need to change. The shard discovers new inventory via the next `provider.List` reconcile.

**Mode 2 — rebalancing decisions.** When demand patterns shift (a region grows, another shrinks), you adjust shard count and topology-domain assignments. The coordinator owns those — push a config change through Raft via the coordinator's gRPC admin endpoint, and the next `ReportShard` cycle distributes the new assignments.

**Mode 3 — incident response.** A shard's hot path is in-process; if it OOMs, restart it. State is recoverable from the provider's `List`. The bigger pathology to watch for is *coordinator quorum loss* — at that point new cross-shard rebalancing pauses, but every shard keeps running on its existing assignments. Static stability is the load-bearing property; you don't need to scramble.

Useful queries:

```promql
# Are any shards falling behind?
histogram_quantile(0.99,
  sum by (le) (rate(bigfleet_shard_cycle_duration_seconds_bucket[5m]))
)

# Per-phase decomposition of the cycle (reconcile, phase1, phase2, phase3, execute).
histogram_quantile(0.99,
  sum by (le, phase) (rate(bigfleet_shard_cycle_phase_duration_seconds_bucket[5m]))
)

# Coordinator throughput.
sum(rate(bigfleet_coordinator_apply_total[5m]))

# Any cluster sessions flapping?
sum by (cluster) (rate(bigfleet_operator_session_reconnects_total[5m])) > 0
```

The shape of the day depends on whether anything is alarming. Most days: nothing.

## Cost analysis (FinOps)

The penalty bucket field on `Profile` is the cost-policy lever. Penalties are quantised to powers of 2 from $0.50 to $10M, so cardinality is bounded and aggregations are stable.

The metrics carry the cost dimensions:

```promql
# Inventory by lifecycle state across the fleet.
sum by (state) (bigfleet_shard_inventory_machines)

# Spot vs on-demand vs reserved vs bare-metal mix per shard.
sum by (capacity_type) (bigfleet_shard_inventory_machines{state=~"Configured|Idle"})

# Penalty-bucket distribution of held capacity — which workloads
# are anchoring expensive interruption penalties to which capacity
# class. High-penalty workloads on Spot are the FinOps red flag
# (Phase 1 should have routed them to OnDemand or Reserved unless
# the cluster's interruption_probability data is wrong).
sum by (capacity_type, interruption_penalty_bucket) (
  bigfleet_shard_inventory_machines{state="Configured"}
)

# Same dimensional breakdown on the demand side — what the
# NeedsTable is currently asking for, before any allocation.
sum by (interruption_penalty_bucket) (bigfleet_shard_demand_machines)

# Per-action throughput from the decision engine — useful for spotting
# Reclaim or Preempt rates that look out of character.
sum by (kind) (rate(bigfleet_shard_actions_total[5m]))
```

`capacity_type` is one of `BareMetal`, `Reserved`, `OnDemand`, `Spot`. `interruption_penalty_bucket` is the dollar value as a string: `0`, `0.5`, `1`, `2`, ..., `8388608`, `pinned`.

What's *still* not exposed (deferred for cardinality reasons):

- per-`instance_type` breakdown of inventory or demand — would explode label cardinality at fleets with hundreds of instance types
- per-`cluster` penalty histograms — same concern at 1K-cluster fleets
- a derived "estimated wasted spend" metric — better computed in Grafana / a downstream cost dashboard from the labels above plus the per-provider price feed

For an end-to-end "are we paying for the right thing" signal, layer `bigfleet_shard_demand_machines{interruption_penalty_bucket="pinned"}` against `bigfleet_shard_inventory_machines{capacity_type="Spot",state="Configured",interruption_penalty_bucket="pinned"}` — non-zero means Pinned-penalty workloads are sitting on Spot capacity, which Phase 1 shouldn't have allowed. Reconcile against the cloud bill outside BigFleet.

## Triaging a capacity-stockout page (on-call)

The standard alert is `bigfleet_shard_shortfalls > 0 for 5m`. The runbook:

```sh
# 1. Which clusters / Profiles have CRs sitting Pending for longer
#    than the runbook threshold? CRs with phase=Pending haven't
#    been included in any rollup yet; phase=Acknowledged means the
#    shard sees the demand but may still not have satisfied it.
#    CRDs don't support field-selectors on status.phase out of the
#    box (the CRD would need to declare selectableFields under
#    Kubernetes ≥1.30), so filter client-side with jq instead.
kubectl get capacityrequests -A -o json \
  | jq -r '.items[] | select(.status.phase=="Pending") | "\(.metadata.namespace)/\(.metadata.name): priority=\(.spec.priority)"'

# 2. The shard's own view of what it can't satisfy. Each cycle with
#    non-zero unresolved demand emits a "shortfalls detected" log
#    line with the top-3 oldest profile fingerprints — useful for
#    correlating against the affected CRs above.
kubectl -n bigfleet-system logs deploy/bigfleet-shard | grep -i shortfall | tail -50
```

Decision tree:

- **Phase 1, no idle inventory, provider out of capacity.** File a quota-increase request, wait. Optionally raise the priorities of the shortfalled CRs above some other workloads — but only if you can justify the preemption to the affected teams.
- **Phase 1, no idle inventory, provider has capacity but isn't being asked.** Likely a Speculative-pool sizing issue. Check the coordinator's quota assignments for this shard.
- **Topology unsatisfiable within a shard.** A `Same`-rack request that the current shard can't fulfil. Cross-shard topology resolution is a hard rule of the design — it doesn't happen — so the workload either needs a different topology constraint or a different shard binding. Rare in steady state.
- **Aging shortfalls escalating.** The shortfall buffer has a max age before it pages louder. Long-aged shortfalls usually mean a cluster's been mis-bound to a shard that doesn't have the right capacity profiles.

## Implementing a CapacityProvider (provider author)

You're writing a separate process that implements `CapacityProvider`. Six RPCs, no `Watch`.

```sh
# Stub it out:
go mod init github.com/yourcorp/your-provider
# Copy the .proto from the BigFleet repo, generate Go bindings.
# Implement Create/Configure/Drain/Delete/Get/List against your backend.

# Run the conformance suite against your endpoint. The suite is a
# Go test under build tag `conformance`, not a built binary — wire
# your provider up at TARGET=host:port.
make conformance TARGET=localhost:9001

# Smoke-test against the in-tree fake provider before shipping:
make conformance-self
```

The suite walks the lifecycle scenarios end-to-end. Categories:

- **Idempotency**: re-issue the same `Create` 100x with the same machine_id. Should return the same op_id every time and only act once.
- **Transitional-state recovery**: kill the provider mid-`Configure`. Restart. The shard's next `List + Get` should observe the in-progress state correctly.
- **Cursor correctness**: if you support `since_revision`, the suite verifies that List with a cursor returns only deltas, that the cursor advances monotonically, and that resuming from an old cursor still works.
- **Drain-grace handling**: a Drain that's interrupted partway must end up in `Failed` with `last_error`, not silently revert.

What the suite *won't* catch: backend-specific edge cases (cloud quota boundaries, your private cloud's eventual-consistency window). Those are your tests, in your repo. The suite establishes that your provider is *protocol-correct*.

## Capacity planning (capacity planner)

Your input is fleet-level demand history. The query is the aggregate, not the per-cluster sum:

```promql
quantile_over_time(0.99,
  sum(bigfleet_shard_inventory_machines{state=~"Configured|Configuring"})[90d:1h]
)
```

The headroom buffer you apply to the p99 is a policy choice. Two factors push it up:

- **Provisioning lead time of the underlying capacity**. If your cloud takes 4 minutes to bring a node up and your workloads spike on a 2-minute timescale, you need static headroom for the gap.
- **Demand bursts that are correlated across clusters**. The point of the fleet-aggregate query is that uncorrelated peaks cancel out — but if your fleet has a daily synchronised batch job, that's a correlated peak that won't smooth.

The scaling guide ([`scaling-guide.md`](scaling-guide.md)) tabulates per-tier sizing assumptions; calibrate against it, then look at your actual demand to decide where you actually sit.

## Pre-release validation (reliability)

You're gating the release on the static-stability invariant: **clusters keep running with BigFleet entirely down**. The runner ships four failover profiles, each scoped to one type of disturbance. Pick by what you want to validate:

```sh
# Single coordinator-leader-kill at t=600s. ~30 min, $0.38 on a
# 2-node Kapsule.
scaletest-runner \
  --profile=test/scaletest/profiles/failover-leader-kill.yaml \
  --output=./results/$(date +%Y%m%d)-leader-kill/

# Single shard-pod-kill (bigfleet-shard-1) at t=600s. Validates
# StatefulSet recovery + cluster-to-shard binding stability.
scaletest-runner --profile=test/scaletest/profiles/failover-shard-kill.yaml \
  --output=./results/$(date +%Y%m%d)-shard-kill/

# 60-second NetworkPolicy-based partition between bigfleet-shard-1
# and the coordinator. Validates static stability under control-plane
# disconnect. Requires a CNI that enforces NetworkPolicy (Cilium does).
scaletest-runner --profile=test/scaletest/profiles/failover-partition.yaml \
  --output=./results/$(date +%Y%m%d)-partition/

# Belt-and-braces release run: 60-min soak with two leader-kills and
# one shard-kill. Use this once before tagging a release.
scaletest-runner --profile=test/scaletest/profiles/failover-soak.yaml \
  --output=./results/$(date +%Y%m%d)-failover-soak/
```

Each profile's `runnerActions:` block declares actions with `atSeconds` offsets. The runner fires them during the soak and asserts the expected outcome via Prometheus queries (e.g. `delta(bigfleet_coordinator_raft_term[5m]) ≥ 1` after a leader-kill, `rate(bigfleet_shard_cycle_duration_seconds_count{pod=…}[1m]) > 0` after a shard-kill). The summary.json `failures: []` field is the regression signal: empty = the static-stability invariant held; non-empty = the run failed regardless of SLO numbers.

What you're checking after a passing run:
- `summary.json` `passed: true`
- `summary.json` `failures: []`
- `summary.json` `metrics.shardCycleDurationP99Seconds` ≤ 100 ms throughout
- `summary.json` `metrics.loadgenCRsActive` ≥ 99.9 % of target throughout (sustained-load gate already in pass())

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
