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

A namespaced CRD that declares a resource need. Two phases, one transition: Pending → Acknowledged.

```yaml
apiVersion: fleet.lucy.sh/v1alpha1
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

### 6.2 AvailableCapacity

An eventually-consistent hint from the autoscaler about what capacity could be provisioned. Confidence signal: High / Medium / Low / None.

### 6.3 UpcomingNode

A node matching this spec is being provisioned. Status phases: Provisioning | Launched | Registered | Ready | Failed.

## 7. The roll-up protocol

The operator runs anywhere that can reach the cluster's API server and the autoscaler. It does three things:

1. Roll up and send (every cycle, full replacement).
2. Write UpcomingNode CRDs when the autoscaler provisions nodes.
3. (Optional) Write AvailableCapacity CRDs.

```protobuf
syntax = "proto3";
package fleet.lucy.sh.v1alpha1;
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
  repeated NodeSelectorRequirement requirements = 1;
  map<string, string> resources = 2;
  int32 priority = 3;
  int32 count = 4;
  repeated TopologySpread spread = 5;
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
