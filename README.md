# BigFleet

BigFleet is a fleet-level infrastructure autoscaler. It receives capacity needs from many Kubernetes clusters and provisions or reclaims machines through pluggable, **out-of-tree** `CapacityProvider` backends. It is the reference implementation of the design described in two papers:

- [BigFleet](https://lucy.sh/bigfleet) — the system architecture (also vendored in this repo at [`docs/papers/bigfleet.md`](docs/papers/bigfleet.md)).
- [Fleet-Scale Kubernetes](https://lucy.sh/fleet-scale-kubernetes) — the operating model BigFleet plugs into (also at [`docs/papers/fleet-scale-kubernetes.md`](docs/papers/fleet-scale-kubernetes.md)).

BigFleet is **not** a scheduler. It does not place pods, simulate kube-scheduler, manage cluster lifecycle, or run quota / admission. It sits one layer below the cluster autoscaler: many clusters, one fleet, one decision engine.

## What BigFleet does

```
                  ┌───────────────────────┐
                  │  BigFleet coordinator │   Raft-replicated fleet state
                  │  (Tier 1, 3 replicas) │   (cluster→shard, quotas, providers)
                  └───────────┬───────────┘
                              │ rebalance instructions
                ┌─────────────┼─────────────┐
                ▼             ▼             ▼
        ┌───────────┐  ┌───────────┐  ┌───────────┐
        │  Shard A  │  │  Shard B  │  │  Shard …  │   Tier 2: hot path
        │ inventory │  │ inventory │  │ inventory │   decision engine
        └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
              │              │              │
   bidi gRPC  │              │              │  dial-out
   (operator- │              │              │  to providers
    initiated)│              │              │
              ▼              ▼              ▼
   ┌──────────────┐   ┌──────────────┐
   │ k8s cluster  │   │ k8s cluster  │   ...
   │   operator   │   │   operator   │
   └──────────────┘   └──────────────┘
```

Each managed Kubernetes cluster runs one **operator**. The operator dials a shard, holds one bidirectional stream, and rolls up the cluster's *full desired capacity* every 10 seconds. The shard diffs the rollup against its inventory and decides what to provision, preempt, or reclaim. Decisions execute through `CapacityProvider` RPCs against a separately-running provider process.

## Quickstart

Five-minute walkthrough on a local kind cluster: see [`docs/quickstart.md`](docs/quickstart.md).

For real installs: [`docs/operator-guide.md`](docs/operator-guide.md).

## Documentation

| Doc | What it covers |
|---|---|
| [`docs/index.md`](docs/index.md) | Documentation map |
| [`docs/user-stories.md`](docs/user-stories.md) | Day-in-the-life user stories, by role |
| [`docs/architecture.md`](docs/architecture.md) | Two-tier architecture, decision engine phases, static stability |
| [`docs/concepts.md`](docs/concepts.md) | Glossary: Need, Profile, Penalty, Cost, Phase 1/2/3, victim score |
| [`docs/api-reference.md`](docs/api-reference.md) | CRDs and gRPC services |
| [`docs/quickstart.md`](docs/quickstart.md) | kind-based five-minute demo |
| [`docs/operator-guide.md`](docs/operator-guide.md) | Install, metrics, runbook |
| [`docs/scaling-guide.md`](docs/scaling-guide.md) | Sizing from 10K to 100M machines |
| [`docs/provider-author-guide.md`](docs/provider-author-guide.md) | Writing a `CapacityProvider` |
| [`docs/plan.md`](docs/plan.md) | Implementation plan (milestones, scale ceilings) |
| [`docs/adr/`](docs/adr/) | Architecture decision records |
| [`docs/papers/`](docs/papers/) | Vendored copies of the source papers ([BigFleet](https://lucy.sh/bigfleet), [Fleet-Scale Kubernetes](https://lucy.sh/fleet-scale-kubernetes)) |

## Development

Common Make targets:

```sh
make generate        # regenerate proto/CRD code (requires buf, controller-gen)
make test            # race-detector unit tests
make sim             # simulator scenarios + golden verification
make soak            # long-running invariant soak
make e2e             # kind-based multi-cluster e2e
make scale           # synthetic scale-ceiling tests (M5 Max-sized)
make conformance     # provider conformance against the in-tree fake
make helm-render     # render all three Helm charts (smoke test)
make verify          # gofmt + vet + lint + test
```

## License

See [`LICENSE`](LICENSE).
