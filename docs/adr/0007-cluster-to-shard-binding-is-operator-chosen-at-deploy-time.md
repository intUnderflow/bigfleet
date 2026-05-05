# ADR-0007: Cluster-to-shard binding is operator-chosen at deploy time

**Status**: Accepted

**Date**: 2026-05-05

## Context

The architecture's hard rule "clusters are permanently bound to shards on first contact" needs an operational mechanism. When a per-cluster operator starts up, it has to dial a specific shard. Two candidate shapes:

1. **Coordinator-driven routing.** Operator dials a coordinator-fronted Service; coordinator returns "your shard is shard-N", operator dials shard-N. Adds a hot-path coordinator dependency for every operator session re-establishment.
2. **Operator-chosen at deploy time.** The cluster owner deploys the operator with `--shard-addr=…` pointing at a specific shard. First contact establishes the binding shard-side; reconnects always go to the same shard ordinal because the address is static.

Option 1 violates static stability in spirit: every operator's first connection — and every reconnect after a session interruption — needs the coordinator up to learn its target shard. Option 2 keeps the coordinator out of the data-plane re-establishment path entirely.

The harness's M12.5 mapping (`kwok-cluster-N` → `bigfleet-shard-(N % shardReplicas)`) demonstrates the deterministic-mapping pattern but doesn't generalise — real fleets need a deliberate decision per cluster, not a modulo.

## Decision

The per-cluster operator's `--shard-addr` flag is the canonical binding mechanism. The cluster owner picks a shard ordinal at chart-install time:

```
helm install bigfleet-operator … \
  --set shardAddress=bigfleet-shard-2.bigfleet-shard-headless.bigfleet-system.svc:7780
```

The shard records the cluster on first session via the existing `Shard.Session` stream. The coordinator's `clusterToShard` Raft state is populated by an admin RPC if the operator wants the binding visible system-wide (M15's `AssignDomain` covers the topology-domain side; cluster-binding admin commands are deferred — see M-something-future).

Operator reconnects always dial the same `--shard-addr`, so the first-contact-wins binding is preserved across session lifetimes. Pod restarts of the shard are absorbed by the StatefulSet's stable per-pod DNS (M12.4): `bigfleet-shard-2.bigfleet-shard-headless.…` resolves to the same shard ordinal even if the underlying pod IP changes.

## Consequences

- **No hot-path coordinator dependency for operator dial / reconnect.** Operators can establish their session even with the coordinator down; the binding is asymmetric — shard-side, not coordinator-side.
- **The platform team is responsible for the cluster→shard map.** Tooling has to surface "which clusters dial which shard" — it's a deploy-time chart value, not a runtime discoverable. v1's answer is "grep the helm releases."
- **Re-binding requires a chart upgrade + operator restart.** Changing `--shard-addr` and re-deploying is the documented rebind flow. The shard's record of "this cluster used to dial me" eventually times out (heartbeat-driven cleanup) but the operational expectation is that re-binding is rare and human-initiated.
- **Multi-shard chart change is the prerequisite** (M12.4): `bigfleet-shard` had to become a StatefulSet with stable per-pod DNS for `--shard-addr=bigfleet-shard-N.…` to mean the same pod across restarts. Pre-M12 the chart used a Deployment + round-robin Service, which would have broken first-contact-wins by re-binding each operator to a random pod on every reconnect.
- **Coordinator-driven routing is post-v1.** A future ADR will revisit if cross-shard cluster migration becomes a real operational need; today the binding is permanent by design and the only "migration" is removing + re-adding the cluster.
