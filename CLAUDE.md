# CLAUDE.md — working brief for the BigFleet reference implementation

This file is the brief you read first when joining this repo. It's small on purpose: read the papers and the plan for everything else.

## What this repo is

BigFleet is a fleet-level infrastructure autoscaler — the reference implementation of the design described in `docs/papers/bigfleet.md` and the operating model in `docs/papers/fleet-scale-kubernetes.md`. It receives `ClusterCapacityNeeds` protobuf messages from per-cluster operators, diffs them against its own provisioned inventory, and provisions or reclaims nodes through pluggable, **out-of-tree** `CapacityProvider` backends.

BigFleet is not a scheduler. It does not place pods. It does not simulate kube-scheduler. It does not manage cluster lifecycle. It does not manage cloud commitments. It does not run quota or admission. Priority is the sole throttling mechanism.

## Sources of truth (in order)

1. **The two papers** — `docs/papers/bigfleet.md` and `docs/papers/fleet-scale-kubernetes.md`. Always read these before recommending design. Do not rely on web summaries; they hallucinate.
2. **Author decisions in memory** — `MEMORY.md` indexes them. `project_design_decisions.md` is the authoritative list of things the author confirmed (cost formula, units, victim aggregation, etc.). Author is Lucy Sweet (`user_role.md`).
3. **`docs/plan.md`** — the comprehensive implementation plan. Repo layout, wire formats, components, milestones, scalability concerns, decisions required before M0.
4. **`docs/adr/`** — architecture decision records. Read recent ones before changing anything they cover.

When the papers, the memory, and the plan disagree, the order above wins. Update memory or the plan when you discover a divergence.

## Decisions locked in for v1

These came out of the §0.1 review in `docs/plan.md`. Don't relitigate without the author:

- **Coordinator assignment granularity**: per-topology-domain, not per-machine. ~100K entries, not 100M.
- **Penalty bucketing**: powers-of-2 dollars, $0.50 to $10M. CRs are aggregated at bucket boundaries. Published on the `CapacityRequest` CRD.
- **`provider.List` incremental**: optional `since_revision` (opaque bytes) on `ListFilter` from v1alpha1. Conformance-gated above a documented threshold.
- **Stream coalescing**: explicit `supersedes_key` field on coalescing message types so reconnect ordering is safe.
- **Coordinator topology**: single Raft group, single region. Cross-region is post-v1; document the tradeoff.

## Hard rules — do not break without explicit author sign-off

These are the easy ways to ship a wrong implementation. Each has a reason.

- **No in-tree providers.** The repo ships the `provider.proto` contract, the dial-out client (`pkg/provider/`), and a test-fixture fake (`pkg/provider/fake/`, never deployed). Real providers live in separate repos. Kubernetes spent years undoing in-tree providers; we don't repeat that.
- **No inbound listener on the cluster operator.** The operator dials the shard and holds one bidirectional stream (`Shard.Session`). All cluster ↔ shard traffic — roll-ups, bootstrap requests, reclaim instructions, node state updates — is multiplexed there. Operators are outbound-only.
- **The cost formula is fixed**: `effective_cost = price + (interruption_probability × interruption_penalty)`. `price` is per-hour, `penalty` is dollars. Not pluggable. Not configurable.
- **`interruption_probability` is provider-declared only.** No cluster-side override, no max-merge.
- **Two penalties, distinct**: `interruption_penalty` (cost of interrupting the workload — used in `effective_cost` and victim scoring) and `reclamation_penalty` (operational value tied to the specific machine — used in idle tiebreak, victim scoring, Phase 3 release). Not the same thing. Not derivable from priority.
- **Six provider RPCs only**: `Create, Configure, Drain, Delete, Get, List`. No `Watch`. Reconciliation is via `List + Get`.
- **Topology constraints do not cross shard boundaries.** A `Same`-rack request that can't be satisfied within a shard becomes a shortfall; it is never resolved cross-shard.
- **Clusters are permanently bound to shards on first contact.** No cluster-lifecycle API. No registration / deregistration. A cluster exists in BigFleet's world the moment it sends a roll-up.
- **Static stability is non-negotiable.** Clusters keep running with BigFleet entirely down. The data plane (shards) operates autonomously during coordinator failover. Don't introduce a hot-path dependency from `pkg/shard` on the coordinator.
- **Roll-ups are full replacement.** Every `ClusterCapacityNeeds` message is the cluster's complete desired state. Never partial.
- **`Same` operator is protobuf-only.** The CRD uses standard operators (`In`, `NotIn`, `Exists`, `DoesNotExist`); the operator translates co-location signals to `Same` during roll-up.

## Common hallucinations — don't add these

- ❌ `operational_value` field. Use `reclamation_penalty`.
- ❌ Cluster-supplied interruption probability.
- ❌ Pluggable cost function.
- ❌ Cross-shard topology resolution.
- ❌ Quota / admission / entitlement APIs.
- ❌ Cluster-lifecycle / registration RPCs.
- ❌ `Watch` on `CapacityProvider`.
- ❌ Inbound `GenerateBootstrap` RPC on the operator (replaced by stream).

## Repo navigation

| Path | What's there |
|------|--------------|
| `api/proto/` | Wire formats: `capacity.proto`, `shard.proto` (operator-initiated bidi stream), `provider.proto`, `coordinator.proto` |
| `api/crd/` | CRDs: `CapacityRequest`, `AvailableCapacity`, `UpcomingNode` |
| `pkg/apis/fleet/v1alpha1/` | Generated Go CRD types |
| `pkg/machine/` | Machine state machine (3 stable + 4 transitional + Failed) |
| `pkg/needs/` | NeedsTable: priority-sorted, full-replacement per cluster |
| `pkg/inventory/` | In-memory machine inventory per shard |
| `pkg/decision/` | Worker loop: Phase 1 (assign) / Phase 2 (inversions) / Phase 3 (reclaim). Cost / victim score / drain grace |
| `pkg/shortfall/` | Shortfall buffer, aging, escalation |
| `pkg/shard/` | Shard controller. Hot path. **Must not import `pkg/coordinator`.** |
| `pkg/coordinator/` | Global coordinator. Raft (`hashicorp/raft`) + BoltDB. Cluster→shard, domain→shard, quota |
| `pkg/provider/` | Provider client + plugin registry. `provider/fake/` is the only in-tree provider, test-only |
| `pkg/operator/` | Per-cluster agent (`cmd/operator`). Stream-only, outbound dial. |
| `pkg/controller/cr/` | The optional `bigfleet-unschedulable-pod-controller` |
| `pkg/fencing/` | Term / epoch / sequence helpers |
| `cmd/bigfleet/` | Single binary, subcommands `coordinator`, `shard`, `all-in-one` |
| `cmd/operator/` | Per-cluster agent |
| `cmd/bigfleet-unschedulable-pod-controller/` | Optional per-pod CR creator |
| `cmd/fauxctl/` | Borg/Twine-style simulation harness |
| `sim/` | Workload generators, scenarios, replay |
| `test/conformance/` | Provider conformance suite — what an out-of-tree provider runs to claim compatibility |
| `test/integration/` | In-process multi-component tests |
| `test/e2e/` | kind-based multi-cluster end-to-end |
| `deploy/helm/` | Charts: `bigfleet`, `bigfleet-operator`, `bigfleet-unschedulable-pod-controller`. **No provider charts** — providers are out of tree. |
| `docs/papers/` | Source papers. Read these. |
| `docs/plan.md` | Comprehensive plan |
| `docs/adr/` | Decision records |

## Working discipline

- **Read papers before recommending design.** Especially before changing anything in `api/proto/` or `pkg/decision/`.
- **YAGNI.** No speculative pluggability. No future-proof fields. When the author gives a single answer, it's the answer.
- **Don't add comments that restate the code.** Only add a comment when the *why* is non-obvious — a hidden constraint, a paper reference, a workaround. Reference the paper section or ADR if relevant.
- **Tests live next to the code they test.** Property tests for invariants (aggregation correctness, idempotency, Phase 3 conservation). Race detector is the safety net for the lock-light hot path.
- **Generated code is generated.** `make generate` is the single entry point. Don't edit generated files by hand.
- **Static-stability-first commits.** Any change touching `pkg/shard/` should be reviewed against the question "does this introduce a hot-path coordinator dependency?" If yes, it doesn't ship.
- **E2E as we go.** From M3 onwards (once the shard controller exists), every milestone that ships real code is exercised against a real Kubernetes cluster via `kind` before being declared done. The local dev box runs Docker Desktop, so `kind create cluster` works without setup. Type-checking and unit tests verify code correctness; e2e verifies behaviour. Don't batch e2e to the end.

## When you're stuck

- Disagreement between papers and code: paper wins, fix the code, note the divergence.
- Disagreement between memory and code: memory wins for author-confirmed items; for everything else, ask.
- Disagreement between plan and code: plan wins until you've updated the plan; the plan is the living target.
- Anything not covered: ADR it, then implement.
