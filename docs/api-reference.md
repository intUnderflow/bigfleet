# BigFleet API reference

The user-facing surfaces of BigFleet:

1. **CRDs** — what cluster users and the operator read and write.
2. **gRPC services** — what BigFleet components talk to each other and to providers over.

The proto sources are authoritative; this page is a navigable summary.

## Custom resource definitions (Kubernetes)

All CRDs live under `bigfleet.lucy.sh/v1alpha1`. YAML is in `api/crd/`; Go types in `pkg/apis/bigfleet/v1alpha1/`.

### `CapacityRequest`

User-facing. A workload (or the optional `bigfleet-unschedulable-pod-controller`) creates a `CapacityRequest` to ask BigFleet for machines.

```yaml
apiVersion: bigfleet.lucy.sh/v1alpha1
kind: CapacityRequest
metadata:
  name: training-job-1
spec:
  count: 8
  profile:
    requirements:
      - key: node.kubernetes.io/instance-type
        operator: In
        values: [a3-highgpu-8g]
    resources:
      - name: nvidia.com/gpu
        quantity: "8"
    spread:
      - topologyKey: topology.kubernetes.io/zone
        maxSkew: 1
    priority: 1000000
    interruptionPenalty: 8192
    reclamationPenalty: 65536
status:
  phase: Acknowledged   # Pending | Acknowledged | Shortfall | Released
  observedGeneration: 1
  acknowledgedCount: 8
  shortfallCount: 0
  conditions: [...]
```

Notes:

- `spec.profile.requirements[].operator` accepts `In`, `NotIn`, `Exists`, `DoesNotExist`. **Not** `Same` — that's protobuf-only and the operator translates co-location signals to it during rollup.
- `priority` is a plain int32; higher wins.
- `interruptionPenalty` and `reclamationPenalty` are dollars; the operator quantises to a `PenaltyBucket` (powers of 2, $0.50–$8.4M) when emitting the rollup.
- `status.phase=Acknowledged` means the shard accepted the rollup and either has the inventory or is provisioning it. `Shortfall` means demand is unsatisfied (capacity stockout, topology unsatisfiable, etc.).

### `AvailableCapacity`

Read-back. The operator writes one per Profile fingerprint reflecting what's currently idle in the shard's inventory and matches the cluster.

```yaml
apiVersion: bigfleet.lucy.sh/v1alpha1
kind: AvailableCapacity
metadata:
  name: a3-highgpu-8g
spec:
  profile: {...}        # mirrors a CapacityRequest.spec.profile
  count: 14
```

Use it for `kubectl get availablecapacity` to see what BigFleet could give you without provisioning.

### `UpcomingNode`

Read-back. The operator writes one per machine that the shard is currently bringing up for this cluster. Lets `kubectl describe pod` show users that BigFleet is acting on their unschedulable pod.

```yaml
apiVersion: bigfleet.lucy.sh/v1alpha1
kind: UpcomingNode
metadata:
  name: gpu-7f3a
spec:
  machineId: gpu-7f3a
  profile: {...}
  estimatedReadyTime: "2026-05-01T15:34:00Z"
```

## gRPC services

Four `.proto` files under `api/proto/bigfleet/v1alpha1/`. Generated Go bindings in `pkg/proto/bigfleet/v1alpha1/`.

### `Shard.Session` (operator ↔ shard)

`shard.proto`. The single, operator-initiated bidi stream that carries everything between a managed cluster and its shard.

```protobuf
service Shard {
  rpc Session(stream OperatorMessage) returns (stream ShardMessage);
}
```

**Operator → shard**:

| Message | Purpose |
|---|---|
| `Hello` | Initial handshake; declares cluster ID and capabilities |
| `ClusterCapacityNeeds` | Full-replacement rollup, every 10 s |
| `BootstrapBlobResponse` | Shard pulled a bootstrap blob; this is the answer |
| `ReclaimAck` | Operator finished draining a node the shard asked for |
| `Acknowledgement` | Generic stream-level ack |

**Shard → operator**:

| Message | Purpose |
|---|---|
| `BootstrapRequest` | "Render me a join token + kubelet config for this Profile" |
| `ReclaimInstruction` | "Drain this node — I want to reclaim it" |
| `NodeStateUpdate` | "This node has just transitioned to <state>" |
| `AvailableCapacityUpdate` | New AvailableCapacity numbers to write back as CRs |

The stream is operator-initiated (outbound dial); the operator never opens an inbound listener. On disconnect, the operator reconnects with exponential backoff and sends a fresh `Hello`.

Coalescing message types carry a `supersedes_key` so the receiver can drop superseded messages on reconnect without ordering subtleties.

### `Coordinator.ReportShard` (shard ↔ coordinator)

`coordinator.proto`. Unary, shard-initiated. Replaces what could have been a streaming RPC; fits "v1 surface is request/response".

```protobuf
service Coordinator {
  rpc ReportShard(ShardReport) returns (ReportAck);
}
```

The shard pulls every few seconds with its current `ShardSummary` and any `Shortfall` rows. The `ReportAck` carries piggy-backed `CoordinatorInstruction`s — at most a handful per response, each with a unique `instruction_id` for ack-on-next-report dedup.

Instruction kinds:

| Kind | What it does |
|---|---|
| `AssignDomain` | "You now own topology domain X" |
| `UnassignDomain` | "You no longer own topology domain X" |
| `ReassignSpeculative` | Reallocate speculative quota slots |
| `CrossShardDrain` | (Reserved; cross-shard reassignment deferred post-v1) |
| `TransferOwnership` | Move a cluster's binding (very rare; only on shard decommission) |

### `CapacityProvider` (shard ↔ provider, out-of-tree)

`provider.proto`. The contract every provider implements. **No `Watch`** — reconciliation is `List + Get`.

```protobuf
service CapacityProvider {
  rpc Create   (CreateRequest)    returns (TransitionAck);
  rpc Configure(ConfigureRequest) returns (TransitionAck);
  rpc Drain    (DrainRequest)     returns (TransitionAck);
  rpc Delete   (MachineRef)       returns (TransitionAck);
  rpc Get      (MachineRef)       returns (Machine);
  rpc List     (ListFilter)       returns (MachineList);
}
```

| RPC | Transition | Async? | Idempotent on |
|---|---|---|---|
| `Create` | Speculative → Creating → Idle | Yes | `(machine_id, opKind)` |
| `Configure` | Idle → Configuring → Configured | Yes | same |
| `Drain` | Configured → Draining → Idle | Yes | same |
| `Delete` | Idle → Deleting → Speculative (or gone) | Yes | same |
| `Get` | Read-only | No | n/a |
| `List` | Read-only; supports `since_revision` cursor | No | n/a |

Async semantics: the four lifecycle RPCs return `TransitionAck` immediately; the actual transition is observed via subsequent `Get`/`List`. See [`provider-author-guide.md`](provider-author-guide.md) for the full contract.

## Wire-format invariants

Cross-cutting rules every consumer relies on:

- **Roll-ups are full replacement.** `ClusterCapacityNeeds.needs` is the cluster's complete desired state. No deltas.
- **Penalty buckets are powers of 2** ($0.50 to $8,388,608, plus `Pinned` sentinel). `PenaltyBucket` enum in `capacity.proto`.
- **`PROFILE_OPERATOR_SAME` is wire-only.** CR YAML uses `In/NotIn/Exists/DoesNotExist`; the operator translates co-location signals to `Same` during rollup.
- **`since_revision` is opaque bytes.** Providers may return any cursor; conformance gates incremental `List` above a documented threshold.
- **`supersedes_key` defines coalescing identity** for stream messages whose semantics is "newer always wins" (e.g., `ClusterCapacityNeeds` per cluster, `AvailableCapacityUpdate` per profile).

## Where to look in the source

| Surface | Proto | Generated Go | Implementation |
|---|---|---|---|
| Capacity model | `capacity.proto` | `pkg/proto/bigfleet/v1alpha1/capacity.pb.go` | `pkg/needs/` |
| Shard ↔ operator | `shard.proto` | `pkg/proto/bigfleet/v1alpha1/shard*.go` | `pkg/shard/`, `pkg/operator/` |
| Coordinator | `coordinator.proto` | `pkg/proto/bigfleet/v1alpha1/coordinator*.go` | `pkg/coordinator/`, `pkg/shard/coordclient/` |
| Provider | `provider.proto` | `pkg/proto/bigfleet/v1alpha1/provider*.go` | `pkg/provider/` (client), `pkg/provider/fake/` (test fake) |
| CRDs | n/a | `pkg/apis/bigfleet/v1alpha1/*.go` | `pkg/operator/`, `pkg/controller/cr/` |

## Versioning

Everything is `v1alpha1` until v1 is cut. Compatibility bar: any field added under `v1alpha1` after v1 must be backward-compatible (additive only). Breaking changes ship as `v1alpha2`, never as silent renames.
