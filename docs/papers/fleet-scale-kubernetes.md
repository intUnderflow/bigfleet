# Fleet-Scale Kubernetes: An Operating Model for Homogeneous Clusters with Decoupled Capacity

Kubernetes was designed for a single cluster. As organisations scale to fleets of tens, hundreds, or thousands of clusters, the operational model hasn't kept up. This paper proposes one that does.

Lucy Sweet — April 2026
Disclosure: Assisted by AI tools (Claude Opus 4.6 1M Context)

## 1. The problem

Kubernetes was designed for a single cluster. A team deploys workloads. kube-scheduler places pods. The cluster autoscaler adds nodes. Everything operates within one control plane, one etcd, one scheduling domain.

The industry outgrew this model. Organisations now operate fleets of 10 to 10,000 clusters, but the tooling and operational patterns haven't evolved to match.

Capacity is fragmented. Each cluster manages its own nodes independently. One cluster is GPU-starved while another has idle GPUs. There's no mechanism to rebalance. Datadog's State of Cloud Costs report shows average CPU utilisation of ~18% across enterprise Kubernetes fleets, with overprovisioning factors of 2–5× and estimates of annual waste ranging from $50,000 to $500,000 per cluster. The capacity exists in the fleet — it's just trapped in per-cluster islands.

Maintenance doesn't scale. Upgrading, patching, and draining are per-cluster operations that require per-cluster knowledge. When clusters are snowflakes — different configurations, node types, workload assumptions — each upgrade requires its own runbook. Airbnb reached 30+ distinct cluster types with 100+ total clusters and found upgrades untenable because each type required individual testing. Large operators routinely spend months on fleet-wide upgrades and years building proprietary lifecycle tooling.

The cluster became an unnecessary unit of concern. Teams think about "which cluster do I deploy to" when they'd rather think about "I need resources." The cluster is infrastructure plumbing that could be invisible, like a rack or an availability zone. But because scheduling is cluster-scoped and capacity is cluster-managed, the cluster is the unit everyone has to reason about.

AI/ML outgrew the single-cluster model. GPU scarcity, gang scheduling, multi-node training with topology constraints, preemption across priority tiers — these all require fleet-level capacity decisions.

There's no fleet-level control plane. The hyperscalers built them — Google has Borg, Meta has Twine — but most organisations manage their fleets through GitOps tooling and per-cluster automation.

Autoscalers duplicate the scheduler. Both Cluster Autoscaler and Karpenter embed full scheduling simulators to decide what to provision. These simulators diverge from kube-scheduler.

## 5. The operating model

Clusters are disposable scheduling domains. A cluster is not a capacity pool. It's a logical boundary in a wider fleet — a scheduling domain where kube-scheduler can see nodes and place pods.

Every cluster is homogeneous. There's no "GPU cluster" or "batch cluster." There are just clusters.

Capacity is decoupled from cluster identity. The autoscaler owns the nodes. It provisions them, tracks them, and reclaims them. Clusters request capacity through a standard contract.

Scheduling stays in the scheduler. The autoscaler does not simulate the scheduler.

The contract is the minimum viable interface. Three CRDs and a protobuf message: CapacityRequest, UpcomingNode, AvailableCapacity.

## 6. The capacity contract

### 6.1 CapacityRequest

> **Revised by [ADR-0027](../adr/0027-rollup-demand-is-a-constrained-resource-request.md) (2026-05-14):** the `CapacityRequest` CRD itself is unchanged and remains per-pod — but it is now explicitly the *operator's input*, not the wire format. The operator aggregates these into the constrained aggregate **resource** request in §7; it no longer rolls them up into per-pod-shaped `CapacityNeed`s. "One CR per pod" stays; what the roll-up aggregates *into* is what changed.

A namespaced CRD that declares a resource need. Two phases, one transition: Pending → Acknowledged.

```yaml
apiVersion: bigfleet.lucy.sh/v1alpha1
kind: CapacityRequest
metadata:
  name: cr-trainer-worker-42
  namespace: training
  ownerReferences:
    - apiVersion: v1
      kind: Pod
      name: trainer-worker-42
spec:
  requirements:
    - key: node.kubernetes.io/instance-type
      operator: In
      values: ["a3-highgpu-8g", "p5.48xlarge"]
  resources:
    requests:
      cpu: "96"
      memory: "768Gi"
      nvidia.com/gpu: "8"
  priority: 1000000
  topologySpread:
    - topologyKey: topology.kubernetes.io/zone
      maxSkew: 1
      whenUnsatisfiable: DoNotSchedule
status:
  phase: Acknowledged  # Pending | Acknowledged
```

One CR per pod. Roll-up aggregates. Withdrawal is implicit via ownerRef GC.

> **Reinforced by [ADR-0039](../adr/0039-capacityrequest-per-pod-not-per-unschedulable-pod.md) (2026-05-21):** "one CR per pod" is unconditional and load-bearing — the CR exists for the Pod's entire lifetime, not only while the Pod is unschedulable. The roll-up is thereby the cluster's *total* desired capacity. Phase 3's surplus arithmetic depends on this: a CR source that produces CRs only for unmet demand makes the autoscaler see phantom surplus and thrash supply against stable demand.

### 6.2 AvailableCapacity

An eventually-consistent hint from the autoscaler about what capacity could be provisioned. Confidence signal: High / Medium / Low / None.

### 6.3 UpcomingNode

A node matching this spec is being provisioned. Status phases: Provisioning | Launched | Registered | Ready | Failed.

## 7. The roll-up protocol

> **Revised by [ADR-0027](../adr/0027-rollup-demand-is-a-constrained-resource-request.md) (2026-05-14):** the `CapacityNeed` message changed from `(per-pod-shape, count)` to a **constrained aggregate resource request**. `resources` (per-pod) → `aggregate_resources` (the total resource vector for a constraint set); `count` is removed — machine count is the autoscaler's *output*, never the cluster's input; `min_unit` is added — the largest atomic schedulable unit, a per-machine floor that preserves indivisibility. The autoscaler diffs `aggregate_resources` against `Σ machine.Allocatable` in resource-vector space; there is no per-pod density reconstruction. The proto below reflects the revised message. See ADR-0027 for the full rationale.

The operator runs anywhere that can reach the cluster's API server and the autoscaler. It does three things:

1. Roll up and send (every cycle, full replacement).
2. Write UpcomingNode CRDs when the autoscaler provisions nodes.
3. (Optional) Write AvailableCapacity CRDs.

```protobuf
syntax = "proto3";
package bigfleet.lucy.sh.v1alpha1;
import "google/protobuf/timestamp.proto";

service InfrastructureAutoscaler {
  rpc UpdateClusterNeeds(ClusterCapacityNeeds) returns (Acknowledgement);
}

message ClusterCapacityNeeds {
  string cluster_id = 1;
  google.protobuf.Timestamp timestamp = 2;
  repeated CapacityNeed needs = 3;
}

message CapacityNeed {
  // ADR-0027: a constrained aggregate resource request, not (per-pod-shape, count).
  repeated NodeSelectorRequirement requirements = 1;  // per-machine constraints
  map<string, string> aggregate_resources = 2;        // ADR-0027: total resource demand for this constraint set
  int32 priority = 3;
  reserved 4;                                         // ADR-0027: was per-pod `count`; machine count is the autoscaler's output
  repeated TopologySpread spread = 5;
  PenaltyBucket interruption_penalty_bucket = 6;      // powers-of-2 dollar bucket; carried per Need
  PenaltyBucket reclamation_penalty_bucket  = 7;      // powers-of-2 dollar bucket; carried per Need
  map<string, string> min_unit = 8;                   // ADR-0027: largest atomic schedulable unit — per-machine floor
}

message TopologySpread {
  string topology_key = 1;
  int32 max_skew = 2;
  string when_unsatisfiable = 3;
}

message NodeSelectorRequirement {
  string key = 1;
  string operator = 2;  // In | NotIn | Exists | DoesNotExist | Same
  repeated string values = 3;
}

message Acknowledgement { bool acknowledged = 1; }
```

## 8. Topology handling

`Same` is the only new concept. During roll-up, the operator translates from CRDs to protobuf, adding `Same` requirements where co-location is needed.

Spread (topology spread constraints) passes through DoNotSchedule / ScheduleAnyway semantics.

## 9. Priority and preemption

Priority is the pod's PriorityClass value. Preemption is the autoscaler's decision. Reclamation is standard node shutdown.

## 10. How it works in practice

Training job with topology, capacity stockout, withdrawal — all illustrated in the source paper.

## 11. Scaling analysis

Up to 100M nodes (~20K clusters × 5K nodes). Roll-up message ~2KB regardless of fleet size. Per-cluster behaviour is identical at any scale.

## 12. Optional: per-pod capacity request controller

Ships separately from the contract. Watches `PodScheduled=False, reason=Unschedulable` and creates one CR per pod. Not required if Kueue or a custom controller handles CR creation.

> **Superseded by [ADR-0039](../adr/0039-capacityrequest-per-pod-not-per-unschedulable-pod.md) (2026-05-21):** the reference controller watches **all** Pods and creates one CR per Pod unconditionally. The `Unschedulable` filter yields one-CR-per-pod only while every Pod is unschedulable at birth; whenever a Pod binds without that transition (spare capacity, controller-recreated Pods rescheduling after a drain), the filter under-produces CRs and the roll-up degrades to unmet-demand-only — violating §6.1/§13 and breaking Phase 3. The "not required if Kueue or a custom controller handles CR creation" escape hatch stands, with the same contract obligation: any CR source must produce one CR per Pod for the Pod's lifetime.

## 13. What this doesn't specify (intentionally)

- How nodes are provisioned (cloud, bare metal, etc.)
- How nodes are deprovisioned (standard graceful shutdown)
- How clusters are managed (fleet orchestration concern)
- Where anything runs

## 14. kubectl experience

```
$ kubectl get availablecapacity -n fleet-system
$ kubectl get capacityrequests -A
$ kubectl get upcomingnodes -n fleet-system
```

(See source paper for full content; this file is a working summary for design reference.)
