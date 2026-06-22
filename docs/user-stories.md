# Working with BigFleet

What it's actually like to interact with BigFleet from each role. These walk through the commands, CRs, and decision points using only what the reference implementation actually ships today; sections that describe future tooling are flagged. If a section feels obvious for your role, it probably is — skip ahead.

## Submitting a workload (application developer)

You're writing a Pod spec like you would on any Kubernetes cluster. BigFleet enters the picture if the cluster runs the optional `bigfleet-unschedulable-pod-controller`, which creates a `CapacityRequest` for your Pod — one CR per Pod, owned by it (ADR-0039) — so the fleet can provision a node that satisfies it when none already exists.

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

The controller derives a `CapacityRequest` from the Pod — its `generateName` is `cr-pod-<podName>-`, and it carries an `ownerReference` back to the Pod so it's garbage-collected with it:

```yaml
apiVersion: bigfleet.lucy.sh/v1alpha1
kind: CapacityRequest
metadata:
  generateName: cr-pod-trainer-2026-05-01-a-
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

What you choose: your `priorityClassName` and (if your platform team mapped it differently) your `interruptionPenalty`. Both affect cost and whether you can be preempted. If your workload is locality-sensitive, *express that* — see [Expressing networking locality](#expressing-networking-locality).

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
# Is any demand going unmet at all?
bigfleet_shard_shortfalls > 0

# Sharper: is any demand ABOVE the batch tier going unmet? Priority is the
# sole throttle, so under scarcity only the lowest tier should ever shortfall.
# A non-zero result here means high-priority demand is being starved.
sum(bigfleet_shard_shortfalls_by_priority{priority_class!="batch"}) > 0

# Did Phase 2 preempt anything to make room?
sum by (kind) (rate(bigfleet_shard_actions_total{kind="Preempt"}[5m]))
```

`priority_class` is one of `batch` (~100), `service` (~1000), `critical` (~1000000), or `other`; `sum(bigfleet_shard_shortfalls_by_priority)` equals `bigfleet_shard_shortfalls`. If you see Preempt actions firing, the affected nodes are written into per-cluster `UpcomingNode` CRs whose status walks toward `Draining` / `Drained`. You can watch your own cluster's nodes via `kubectl get upcomingnodes`.

## Expressing networking locality

BigFleet matches the requirements a workload **expresses**, and only those — so locality is something you ask for, not something BigFleet infers. The unit of fungibility is "any node satisfying the same requirements," and matching is requirement-gated *before* cost is consulted: a Need that pins its zone, names its `Same` domain, or selects its region can never be served by capacity that violates it (if nothing in the shard satisfies it, it becomes a shortfall, not a wrong placement).

The corollary is the footgun: a Need that **omits** its region can be served by capacity anywhere in its shard — including a different region or VPC, where it may not actually reach the cluster's network. Capacity is fungible only *within a shard*, and BigFleet does not stamp a default locality onto your Needs.

- **What you choose:** the locality your workload requires — `topology.kubernetes.io/zone`, a region selector, or a `Same` co-location domain — expressed as nodeSelector-style requirements on the Pod (and thus the CR).
- **What the platform team chooses:** to align each cluster's shard with one network domain (AZ / VPC / region), so the shard's whole pool is reachable rather than relying on every workload to self-describe.

Network/egress *cost* is deliberately not in the cost formula — if a workload's transfer cost dominates, express its locality as a hard requirement rather than expecting BigFleet to price it. Full detail: [BigFleet and networking](/networking/).

## Per-cluster operator install (cluster owner)

You own one or more clusters. The platform team owns the BigFleet shard. Your job is the operator chart:

```sh
helm install bigfleet-operator oci://ghcr.io/intunderflow/charts/bigfleet-operator \
  --version 0.1.0 \
  --namespace bigfleet-system --create-namespace \
  --set clusterID=cluster-prod-eu-1 \
  --set shardAddress=bigfleet-shard.bigfleet-system.svc:7780
```

(Helm ≥3.8 for OCI; charts are pushed to GHCR on every commit to main. The `--version` flag pins to a specific `Chart.yaml` version — bump it when upgrading. For development against a git checkout, replace the OCI reference with `./deploy/helm/bigfleet-operator` and drop `--version`.)

The operator dials the shard from inside the cluster (outbound only — no inbound listener). After install, check:

```sh
kubectl -n bigfleet-system logs deploy/bigfleet-operator | head
# expect: "operator started ... rollup_interval=10s"

kubectl get availablecapacity
# Written by the operator's roll-up loop. If empty after 30s, the operator
# hasn't completed a roll-up to the shard yet.
```

You don't tune autoscaler parameters per-cluster anymore — there are none in the operator chart. The shard owns those. What stays your responsibility:

- The PriorityClasses your cluster offers (and the unschedulable-pod-controller's mapping to BigFleet `priority` int values).
- Per-cluster compliance: which `nodeSelector` keys your `BootstrapTemplate` knows how to render userdata for.
- **Locality.** Express the region/zone/`Same` domain your locality-sensitive workloads need, and ask the platform team to align your shard with one network domain — capacity is fungible only within a shard. See [Expressing networking locality](#expressing-networking-locality).
- Pod Disruption Budgets your workloads carry — every drain (Phase 2 preempt and Phase 3 reclaim alike) reaches the operator as a `ReclaimInstruction`, and the operator evicts through the PDB-respecting `policy/v1` Eviction API within the instruction's grace period. The exception is an operator that is disconnected when the drain fires: the shard then drains via the provider directly (kubelet default grace, no PDB pass) and logs `reclaim fallback`.

A node only counts as delivered capacity once it has actually joined and reached `Ready` — a conformant provider holds it at `Configuring` until then (ADR-0056), so a node that shows up in `UpcomingNode` but never goes `Ready` is a provider/bootstrap problem on your side, not silent BigFleet over-counting.

Watch `bigfleet_operator_session_reconnects_total`: a steady non-zero rate means the stream to the shard is unstable.

## Operating BigFleet itself (platform engineer)

You're running the coordinator + shards on a management cluster. Day-to-day work splits into three modes:

**Mode 1 — capacity-tier changes.** Adding a new instance type or capacity reservation means updating the provider's static config (the provider lives in a separate repo, not in this one) and the provider redeploys. BigFleet itself doesn't need to change. The shard discovers new inventory via the next `provider.List` reconcile.

**Mode 2 — rebalancing decisions.** When demand patterns shift (a region grows, another shrinks), you adjust shard count and topology-domain assignments. The coordinator owns those — push a config change through Raft via the coordinator's gRPC admin endpoint, and the next `ReportShard` cycle distributes the new assignments.

**Mode 3 — incident response.** A shard's hot path is in-process; if it OOMs, restart it. State is recoverable from the provider's `List`. The bigger pathology to watch for is *coordinator quorum loss* — at that point new cross-shard rebalancing pauses, but every shard keeps running on its existing assignments. Static stability is the load-bearing property; you don't need to scramble.

Useful queries:

```promql
# Are any shards falling behind? (the gated cycle p99 — see "The SLO posture")
histogram_quantile(0.99,
  sum by (le) (rate(bigfleet_shard_cycle_duration_seconds_bucket[5m]))
)

# Per-phase decomposition of the cycle (reconcile, phase1, phase2, phase3, execute).
histogram_quantile(0.99,
  sum by (le, phase) (rate(bigfleet_shard_cycle_phase_duration_seconds_bucket[5m]))
)

# Coordinator throughput.
sum(rate(bigfleet_coordinator_apply_total[5m]))

# Are operator sessions flapping anywhere? (the counter has no per-cluster
# label, so aggregate across the fleet.)
sum(rate(bigfleet_operator_session_reconnects_total[5m])) > 0
```

The shape of the day depends on whether anything is alarming. Most days: nothing. For what "behind" actually means — and why a high *pod-bind* p99 is not a BigFleet regression — read the next section.

## The SLO posture (ADR-0054)

The one thing to internalise before reading dashboards: **BigFleet gates on its own capacity-delivery hops, not on the cluster's pod-bind tail.** Under a real, uncapped kube-scheduler the end-to-end "Pod created → Pod bound" time is dominated by the scheduler's retry/backoff and the reprovision back-edge — neither of which BigFleet controls — so that number is reported as *informational context*, never gated. The release gate is the set of things BigFleet is actually responsible for (production bars; dev substrates run looser):

| Gate | Bar |
|---|---|
| Capacity-materialization latency (`shardConfigurePhaseP99Seconds`) | ≤ 15 s |
| Bootstrap success ratio (`bootstrapSuccessRatio`) | ≥ 0.99 |
| Node-publish latency (`operatorNodeStateUpdateP99Seconds`) | ≤ 1.5 s |
| Roll-up turn (`operatorRollupP99Seconds`) | ≤ 1 s |
| Roll-up ack (`operatorAckP99Seconds`) | ≤ 12 s |
| Decision-engine cycle (`shardCycleDurationP99Seconds`) | ≤ 5 s |
| Unmet demand (`bigfleet_shard_shortfalls`) | == 0 |
| Common-path bind liveness (`endToEndPodBindP50Seconds`) | ≤ 10 s (loose) |

End-to-end pod-bind **p99** and raw-max are informational, not gated. The misread to avoid: a high bind p99 on an uncapped scheduler is not a BigFleet regression — check the gated hops above first. The full rationale and per-gate "why" is the [SLOs reference](/slos/).

## Cost analysis (FinOps)

The penalty bucket field on `Profile` is the cost-policy lever. Penalties are quantised to powers of 2 from $0.50 up to $8,388,608 (2²³), plus a `pinned` bucket, so cardinality is bounded and aggregations are stable.

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

# The demand side — what the NeedsTable is asking for, by penalty bucket.
# Note: this counts NeedsTable rows (aggregated demand), not machine units,
# so read it as a demand-shape signal, not a units-for-units inventory mirror.
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

The standard alert is `bigfleet_shard_shortfalls > 0 for 5m`. First question — **is this page serious?** Priority is the sole throttle, so a shortfall confined to the `batch` tier is the system working as designed; a shortfall *above* it is not:

```promql
# Real-incident discriminator: any unmet demand above the batch tier?
sum(bigfleet_shard_shortfalls_by_priority{priority_class!="batch"})
```

Then the runbook:

```sh
# 1. Which clusters / Profiles have CRs sitting Pending for longer
#    than the runbook threshold? CRs with phase=Pending haven't
#    been included in any rollup yet; phase=Acknowledged means the
#    shard sees the demand but may still not have satisfied it.
#    The CapacityRequest CRD declares status.phase as a selectable
#    field (Kubernetes ≥1.31 — the SelectableFields feature went
#    GA there), so kubectl --field-selector works directly.
kubectl get capacityrequests -A \
  --field-selector=status.phase=Pending \
  -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name}: priority={.spec.priority}{"\n"}{end}'

# 2. The shard's own view of what it can't satisfy. Each cycle with
#    non-zero unresolved demand emits a "shortfalls detected" log
#    line with the top-3 oldest profile fingerprints — useful for
#    correlating against the affected CRs above. (The shard is a
#    StatefulSet, not a Deployment.)
kubectl -n bigfleet-system logs statefulset/bigfleet-shard | grep -i shortfall | tail -50
```

Decision tree:

- **Phase 1, no idle inventory, provider out of capacity.** File a quota-increase request, wait. Optionally raise the priorities of the shortfalled CRs above some other workloads — but only if you can justify the preemption to the affected teams.
- **Phase 1, no idle inventory, provider has capacity but isn't being asked.** Likely a Speculative-pool sizing issue. Check the coordinator's quota assignments for this shard.
- **Zero shortfall but pods still Pending.** Suspect phantom capacity: a provider that reported `Configured` before the node actually reached `Ready` (ADR-0056 forbids this; a non-conformant provider can still do it). Check the affected nodes are genuinely `Ready`.
- **Topology unsatisfiable within a shard.** A `Same`-rack request that the current shard can't fulfil. Cross-shard topology resolution is a hard rule of the design — it doesn't happen — so the workload either needs a different topology constraint or a different shard binding. Rare in steady state.
- **Aging shortfalls escalating.** The shortfall buffer has a max age before it pages louder. Long-aged shortfalls usually mean a cluster's been mis-bound to a shard that doesn't have the right capacity profiles.

## Capacity planning (capacity planner)

Your input is fleet-level demand history. The query is the fleet aggregate across shards, not a per-cluster sum:

```promql
# Provisioned-inventory p99 over 90 days (size to this + headroom).
quantile_over_time(0.99,
  sum(bigfleet_shard_inventory_machines{state=~"Configured|Configuring"})[90d:1h]
)

# The demand-side complement — what was actually being asked for.
quantile_over_time(0.99,
  sum(bigfleet_shard_demand_machines)[90d:1h]
)
```

The headroom buffer you apply to the p99 is a policy choice. Two factors push it up:

- **Provisioning lead time of the underlying capacity**. If your cloud takes 4 minutes to bring a node up and your workloads spike on a 2-minute timescale, you need static headroom for the gap.
- **Demand bursts that are correlated across clusters**. The point of the fleet-aggregate query is that uncorrelated peaks cancel out — but if your fleet has a daily synchronised batch job, that's a correlated peak that won't smooth.

The [scaling guide](/scaling-guide/) tabulates per-tier sizing assumptions against the measured `uber-*` ladder; calibrate against it, then look at your actual demand to decide where you actually sit.

## Writing a provider (provider author)

You're adding support for a new substrate. You almost never hand-roll the wire contract: real providers build on the out-of-tree **`providerkit`** library, which implements the cross-cutting BigFleet obligations once — fencing, idempotency, async dispatch, transition timeouts, the `shard_metadata` lifecycle, machine field-shape — so you write a small substrate backend (create / configure / drain, and optionally delete a host) and the kit speaks the protocol. The contract underneath is six RPCs, no `Watch`: `Create`, `Configure`, `Drain`, `Delete`, `Get`, `List`; reconciliation is `List` + `Get`, never a stream.

The one rule whose violation is silently invisible: **a machine must not be reported `Configured` until the node has actually joined and reached `Ready` on its target cluster** (ADR-0056) — otherwise you credit capacity that isn't schedulable. Hold it at `Configuring` until then; on timeout, `Failed`.

Conformance is what "BigFleet-compatible" means — a frozen catalogue of **93 behaviours across 11 areas** (idempotency, transitional-state recovery, cursor correctness, drain-grace, the readiness gate, and more). Run it against your endpoint, and smoke-test the in-tree reference fake before shipping:

```sh
make conformance TARGET=localhost:9001   # your provider
make conformance-self                    # the in-tree reference fake
```

What conformance *won't* catch: backend-specific edge cases (cloud quota boundaries, your private cloud's eventual-consistency window). Those are your tests, in your repo. The deep guide is the [provider author guide](/provider-author-guide/); a dozen conformance-certified providers (AWS, GCP, Azure, Hetzner, bare metal, and more) already exist at [bigfleet-providers.lucy.sh](https://bigfleet-providers.lucy.sh).

## Reading a scale-test receipt

You don't have to take BigFleet's published numbers on faith. Every run on the [scale-test results page](/scaletest-results/) ships a full **receipt**: a Grafana-loadable Prometheus TSDB snapshot, scrubbed per-node logs and config/state, and a `LOAD-RECIPE.md` describing exactly how the run was constructed — all as release assets.

So the workflow is: open the run, load its TSDB into Grafana, and check every gate number on the page against the raw series yourself — the scorecard is generated from each run's committed summary, not hand-entered. Use the same receipts to sanity-check your own capacity model against a real fleet's behaviour at scale.

## Pre-release validation (reliability)

You're gating the release on the static-stability invariant: **clusters keep running with BigFleet entirely down**. The runner ships four failover profiles, each scoped to one type of disturbance. Run them through the `make scaletest` entrypoint, pairing each V2 profile with your substrate:

```sh
# Single coordinator-leader-kill mid-soak. Validates coordinator failover.
make scaletest PROFILE=failover-leader-kill SUBSTRATE=<your-substrate>

# Single shard-pod-kill mid-soak. Validates StatefulSet recovery +
# cluster-to-shard binding stability.
make scaletest PROFILE=failover-shard-kill SUBSTRATE=<your-substrate>

# NetworkPolicy-based partition between a shard and the coordinator.
# Validates static stability under control-plane disconnect. Requires a
# CNI that enforces NetworkPolicy (Cilium does).
make scaletest PROFILE=failover-partition SUBSTRATE=<your-substrate>

# Belt-and-braces release run: a longer soak with leader-kills and a
# shard-kill. Use this once before tagging a release.
make scaletest PROFILE=failover-soak SUBSTRATE=<your-substrate>
```

Each profile's `runnerActions:` block declares disturbances with `atSeconds` offsets. The runner fires them during the soak and asserts the expected outcome via Prometheus — e.g. that the coordinator's raft term advanced after a leader-kill, and that the killed shard's cycle counter resumed after a shard-kill. The `summary.json` `failures: []` field is the regression signal: empty = the static-stability invariant held; non-empty = the run failed regardless of SLO numbers.

What you're checking after a passing run:

- `summary.json` `passed: true` and `failures: []`
- the [ADR-0054 capacity-delivery gates](#the-slo-posture-adr-0054) held throughout — cycle p99 ≤ 5 s, configure-phase p99 ≤ 15 s, bootstrap success ratio ≥ 0.99, node-state-update p99 ≤ 1.5 s, and `bigfleet_shard_shortfalls` == 0
- the end-to-end pod-bind tail is *informational* — read it for context, don't gate on it

Every run also produces a receipt you (or anyone) can re-inspect — see [Reading a scale-test receipt](#reading-a-scale-test-receipt).

## Notes that aren't role-specific

- **Priority + interruption-penalty + reclamation-penalty are the three numbers everyone looks at.** Different roles read them differently — workload owners as a self-description, BigFleet operators as inputs to the engine, FinOps as a cost lever — but it's the same fields.
- **BigFleet enforces only the requirements a workload expresses.** Locality, fabric, and residency are capacity requirements, not a separate networking subsystem — and a Need that omits them is matched without them. Express what you need; align shards to network domains. See [BigFleet and networking](/networking/).
- **Static stability is felt as the absence of incidents.** Most users never see BigFleet's failure modes because the property holds; the people who *do* see it are the ones running BigFleet itself, and even then mostly in pre-release tests.
- **Out-of-tree providers means the platform team's provider release cadence is decoupled from BigFleet's.** When BigFleet ships a new version, you don't have to redeploy your provider; when your provider ships, you don't have to coordinate with BigFleet maintainers.

## See also

- [Quickstart](/quickstart/) — bring up BigFleet on a kind cluster.
- [Architecture](/architecture/) — how the engine works.
- [BigFleet and networking](/networking/) — how locality is expressed and matched.
- [The SLOs reference](/slos/) — the full gate table and the per-gate rationale.
- [Operator guide](/operator-guide/) — install and runbook.
- [Provider author guide](/provider-author-guide/) — writing your own provider on `providerkit`.
- [Scale-test results](/scaletest-results/) — published runs, each with a Grafana-loadable receipt.
- [Scale-test runbook](/scaletest/) — running the scenarios above against any cluster.
