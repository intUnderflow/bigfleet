# Contributing to BigFleet

BigFleet is the reference implementation of a fleet-level infrastructure autoscaler
(see [`docs/papers/bigfleet.md`](docs/papers/bigfleet.md) and
[`docs/papers/fleet-scale-kubernetes.md`](docs/papers/fleet-scale-kubernetes.md)). This
guide is the practical contributor brief: what blocks a PR, how the dev loop works, and
where things live. Read [`CLAUDE.md`](CLAUDE.md) first — it is the working brief and these
rules are drawn from it. For the architecture, start at [`docs/index.md`](docs/index.md);
for the code-level deep dives, [`docs/internals/`](docs/internals/).

---

## 1. Source-of-truth ordering

When two sources disagree, the higher one wins. **Document the divergence — never silently
paper over it.**

1. **The two papers** — [`docs/papers/bigfleet.md`](docs/papers/bigfleet.md),
   [`docs/papers/fleet-scale-kubernetes.md`](docs/papers/fleet-scale-kubernetes.md). Read
   them before recommending design, especially before touching `api/proto/` or
   `pkg/decision/`. Don't rely on web summaries; they hallucinate.
2. **Author decisions** — [`docs/adr/`](docs/adr/) and [`CLAUDE.md`](CLAUDE.md). An ADR can
   supersede a paper on a specific point; the ADR header records this.
3. **The plan** — [`docs/plan.md`](docs/plan.md), the living implementation target.
4. **The code.**

When the paper and the code disagree, the paper wins: fix the code and note the divergence
in the PR. When the plan and the code disagree, the plan wins until you've updated the plan.

---

## 2. Hard rules that block a PR

These are the easy ways to ship a wrong implementation. Each has a reason. Changing any of
them needs explicit author (Lucy Sweet) sign-off — don't relitigate in a PR.

- **No in-tree providers.** The repo ships the `provider.proto` contract, the dial-out
  client (`pkg/provider/`), and one test-only fake (`pkg/provider/fake/`, never deployed).
  Real providers live in separate repos and prove compatibility via the conformance suite
  ([`test/conformance/`](test/conformance/)). Kubernetes spent years undoing in-tree
  providers; we don't repeat that. **No provider Helm charts** under `deploy/helm/`.
- **No inbound listener on the operator.** The operator dials the shard and holds **one**
  bidirectional `Shard.Session` stream. All cluster ↔ shard traffic — roll-ups, bootstrap
  requests, reclaim instructions, node-state updates — multiplexes there. Operators are
  outbound-only. (No inbound `GenerateBootstrap` RPC; it was replaced by the stream.)
- **The cost formula is fixed:**
  `effective_cost = price + (interruption_probability × interruption_penalty)`. `price` is
  per-hour, `interruption_penalty` is dollars. Not pluggable, not configurable.
  `interruption_probability` is **provider-declared only** — no cluster-side override, no
  max-merge.
- **Two penalties, distinct.** `interruption_penalty` (cost of interrupting the workload;
  used in `effective_cost` and victim scoring) vs `reclamation_penalty` (operational value
  tied to a specific machine; used in idle tiebreak, victim scoring, Phase 3 release). Not
  the same; not derivable from priority. (There is **no** `operational_value` field.)
- **Six provider RPCs only:** `Create, Configure, Drain, Delete, Get, List`. No `Watch`.
  Reconciliation is `List + Get`.
- **No cross-shard topology resolution.** A `Same`-rack request that can't be satisfied
  within a shard becomes a **shortfall** — it is never resolved cross-shard.
- **`pkg/shard` must not import `pkg/coordinator`.** Static stability is non-negotiable:
  clusters keep running with BigFleet entirely down, and shards operate autonomously during
  coordinator failover. Any change touching `pkg/shard/` is reviewed against the question
  "does this introduce a hot-path coordinator dependency?" If yes, it doesn't ship.
- **Roll-ups are full replacement.** Every `ClusterCapacityNeeds` message is the cluster's
  complete desired state. Never partial.
- **Clusters are permanently bound to shards on first contact.** No cluster-lifecycle API,
  no registration/deregistration RPC. A cluster exists the moment it sends a roll-up.
- **Priority is the sole throttling mechanism.** BigFleet is not a scheduler — it does not
  place pods. No quota / admission / entitlement APIs.

If a PR needs to cross one of these lines, open an ADR and get author sign-off **before**
writing the code.

### Common hallucinations — do not add these

❌ `operational_value` field (use `reclamation_penalty`) · ❌ cluster-supplied or
overridable `interruption_probability` · ❌ a pluggable/configurable cost function ·
❌ cross-shard topology resolution · ❌ quota / admission / cluster-lifecycle / registration
RPCs · ❌ a `Watch` provider RPC · ❌ an inbound `GenerateBootstrap` RPC on the operator.

---

## 3. Dev workflow

Install the pinned toolchain once: `make tools` drops `buf`, `protoc-gen-go(-grpc)`,
`golangci-lint`, and `controller-gen` into `./bin` at the versions pinned in the
[`Makefile`](Makefile). Optionally `make install-hooks` to wire `.githooks/` (pre-commit
`make lint`, pre-push `make verify`).

The repo targets **Go 1.26** (`go.mod`) and **Kubernetes ≥ 1.31** (ADR-0010).

### Generated code

**`make generate` is the single entry point.** It runs `buf generate` (proto → Go +
gRPC) and `controller-gen` (CRD deepcopy + `api/crd/` manifests) against
`pkg/apis/...`. Generated files are generated — **never hand-edit them**. Change the
`.proto` / the `+kubebuilder` markers, then regenerate. If you touch wire format, also run
`make buf-breaking` (compares against `origin/main`'s merge-base; it's part of `verify`).

### Before every commit

```sh
make lint   # golangci-lint + buf lint — this is CI's gate; skipping = red CI
```

`make lint` matches CI's verify gate. Run it locally before committing Go code.

### Before opening / merging a PR

```sh
make verify   # vet + lint + buf-breaking + test + integration — what CI runs on every PR
```

`make test` runs the unit suite **with the race detector** (`-race`); the lock-light shard
hot path relies on it as the safety net. `make integration` runs the in-process
multi-component suite (build tag `integration` — it has rotted invisibly when nothing
compiled it, so `vet`/`verify` compile every tagged package deliberately).

### e2e and scale as you go

Don't batch validation to the end (ADR / CLAUDE working discipline):

- **e2e-as-we-go** — from M3 onwards, every milestone shipping real code is exercised
  against a real cluster via `kind` before being declared done. `make e2e` (needs `kind`,
  `kubectl`, a running Docker daemon). The five-minute path is
  [`docs/quickstart.md`](docs/quickstart.md).
- **Scale ceilings-as-we-go** — every milestone defines its own ceiling targets
  ([`docs/plan.md`](docs/plan.md) §5.1) and the achieved numbers become its baseline.
  Two layers: `make scale` runs the synthetic Go-simulator tests (build tag `scale`); the
  kind-based scaletest harness runs the same scenarios through real binaries / real gRPC /
  real Kubernetes (`make scaletest`, see [`docs/scaletest.md`](docs/scaletest.md) and
  [`docs/internals/scaletest-harness.md`](docs/internals/scaletest-harness.md)).
  Regressing a prior milestone's ceiling is a release blocker.

### The validation ladder (before any cloud scale brief)

Climb the ladder; **never cloud-first** — a cloud run is the last confirmation of a change,
not the discovery instrument ([`docs/scaletest.md`](docs/scaletest.md) §"The validation
ladder", ADR-0043's "demand realism before mechanism"):

```
make prevalidate        rung 0/1/2, Docker-free, ~3 min: profile preflight + closed-loop
                        sim + hot-path benches. Every SHA bound for a cloud brief passes
                        this first.
        │
        ▼
make prevalidate-kind   the kind/dev-50 integration rung (~8 min warm, needs Docker).
                        Runs DEVPOD-SIDE as step 0 of every cloud brief, fail-fast; this
                        local variant is for when you're working ON the harness.
        │
        ▼
cloud                   the last confirmation, not the discovery instrument.
```

A cloud failure that a lower rung would have caught is a process bug.

### Provider conformance

An out-of-tree provider runs `make conformance TARGET=addr:port` to claim compatibility;
`make conformance-self` runs the suite against the in-tree fake with no target.

---

## 4. Where things live

- **Architecture & concepts** — [`docs/index.md`](docs/index.md) is the doc landing page;
  [`docs/architecture.md`](docs/architecture.md) and [`docs/concepts.md`](docs/concepts.md)
  are the high-level guides.
- **Code-level deep dives** — [`docs/internals/`](docs/internals/):
  [`decision-map.md`](docs/internals/decision-map.md),
  [`machine-lifecycle.md`](docs/internals/machine-lifecycle.md),
  [`shard-hot-path.md`](docs/internals/shard-hot-path.md),
  [`scaletest-harness.md`](docs/internals/scaletest-harness.md).
- **Repo navigation table** — [`CLAUDE.md`](CLAUDE.md) §"Repo navigation" maps every
  package and command to what it does. The high-signal entries:

| Path | What's there |
|------|--------------|
| `api/proto/` | Wire formats: `capacity`, `shard` (operator-initiated bidi stream), `provider`, `coordinator` |
| `api/crd/` | Generated CRD manifests (`CapacityRequest`, `AvailableCapacity`, `UpcomingNode`) |
| `pkg/decision/` | Worker loop: Phase 1 (assign) / Phase 2 (inversions) / Phase 3 (reclaim). Cost / victim score / drain grace |
| `pkg/shard/` | Shard controller. Hot path. **Must not import `pkg/coordinator`.** |
| `pkg/coordinator/` | Global coordinator. Raft (`hashicorp/raft`) + BoltDB. Cluster→shard, domain→shard |
| `pkg/provider/` | Provider client + registry. `provider/fake/` is the only in-tree provider, test-only |
| `pkg/operator/` | Per-cluster agent. Stream-only, outbound dial |
| `test/conformance/` | Provider conformance suite |
| `sim/`, `cmd/fauxctl/` | Simulator + Borg/Twine-style harness |
| `deploy/helm/` | Charts: `bigfleet`, `bigfleet-operator`, `bigfleet-unschedulable-pod-controller`. **No provider charts.** |

---

## 5. The ADR process

Anything not covered by an existing decision: **ADR it, then implement.**

1. Add `docs/adr/NNNN-title.md` (next number; follow the format of recent ADRs —
   ADR-0049/0050/0051 are good models, with a Status and the superseded/amended links).
2. **In the same commit, add a matching row to
   [`docs/adr/index.md`](docs/adr/index.md):**
   `| [N](/adr/NNNN-title/) | <Status> | <Title> |`. The published `/adr/` page is synced
   from that index and silently stops listing past the last update — a missing row means
   the ADR is invisible on the site.
3. If the ADR supersedes/amends another, set the older ADR's Status accordingly (e.g.
   "Superseded by ADR-NNNN" / "Amended by ADR-NNNN").

---

## 6. Working discipline (quick reference)

- **YAGNI.** No speculative pluggability, no future-proof fields. A single author answer is
  the answer.
- **Comments explain *why*, not *what*.** Only when the why is non-obvious — a hidden
  constraint, a paper reference, a workaround. Cite the paper section or ADR.
- **Tests live next to the code they test.** Property tests for invariants (aggregation
  correctness, idempotency, Phase 3 conservation). The race detector guards the hot path.
- **Static-stability-first commits.** Re-check every `pkg/shard/` change against the
  no-coordinator-dependency rule.
- **Demand realism before mechanism (ADR-0043).** A mechanism motivated by harness-observed
  evidence must first answer: would a production fleet emit the demand shape that triggers
  it? If not, fix the harness and re-measure before designing mechanism.

Author is **Lucy Sweet**. Design forks go to her; diagnostic and validation runs do not
need a permission round-trip.
