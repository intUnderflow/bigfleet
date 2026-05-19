# ADR-0036: Phase 3 reclaim must not fire before a cluster's first rollup has arrived

## Status

Accepted, 2026-05-20.

## Context

Phase 3 reclaims Configured supply that isn't currently backing demand. The attribution machinery (ADR-0027) walks each cluster's `Configured` slice, claims supply against the cluster's Needs via `MatchProfile`, and emits Reclaim actions for the unclaimed remainder. The decision is correct given the inputs: if a Configured machine is in cluster C and cluster C's NeedsTable has no Need that matches the machine's Profile, the machine isn't backing anything → reclaim it.

This is correct steady-state behaviour, but produces a **latent and serious failure mode at startup or shard restart**: the shard's NeedsTable for each cluster starts empty. Operators populate it by sending `RollupReport` messages over the per-cluster session, which arrive ~tens of seconds after operator-side startup. Phase 3, however, runs on its normal cycle (~5s) from the moment the shard's worker pool starts. During the window between shard startup and the first rollup arrival per cluster, Phase 3 cannot distinguish "this cluster genuinely has zero demand" from "this cluster hasn't told me yet."

The current implementation treats both cases identically: empty NeedsTable → no claim → reclaim every Configured machine that belongs to the cluster.

The diagnostic chain (M48 → M52 → the May 2026 investigation thread) surfaced this when M52.A introduced `seed.configuredFraction: 1.0` and the scaletest reported `Reclaim: 5,365 / Bootstrap: 2,593` at uber-5k — a 2:1 reclaim:bootstrap ratio in a regime where the seeded Configured supply should have been stable.

Investigation (with code traces and three unit-test reproductions) confirmed the mechanism:

1. `cmd/bigfleet/shard.go:seedFakeInventory` inserts Configured machines directly into `s.inv` at startup.
2. Phase 3 cycle (`pkg/decision/phase3_reclaim.go`) fires every ~5s. For each cluster, it walks `Configured` and calls `claimMatching(clusterNeeds, configured)`. If `clusterNeeds` is empty, `claimMatching` returns zero claimed; the trailing loop emits Reclaim for every unclaimed Configured.
3. Operator-side roll-up (`pkg/operator/rollup.go:54-63`, `:153`) is built purely from CapacityRequests — there is no cluster-state walking, no `client.List(&pods)`. The first non-empty rollup requires CRs to exist, which requires Pods → UPC → CR translation, which takes ~60-90s post-install.

By the time the first rollup lands, Phase 3 has chewed through a large fraction of the seed.

### The production blast radius

This is **not a scaletest-harness artefact**. The shard's inventory is purely in-memory (no persistence layer in `pkg/shard/`). On shard restart, `reconcileFull` (`pkg/shard/reconcile.go:39-62`) pulls the provider's Configured machines and inserts them into the local inventory via `applyReconciledMachine`. Operators reconnect over the per-cluster session; their first post-reconnect rollup arrives ~tens of seconds later.

In that window, Phase 3 fires against an empty NeedsTable for every cluster, and **reclaims every Configured machine in the fleet**.

This violates the hard rule [`static-stability`](../papers/bigfleet.md): "Clusters keep running with BigFleet entirely down. The data plane (shards) operates autonomously during coordinator failover." Today, the data plane operates autonomously *during coordinator failover* — but BigFleet coming back up after a *shard* restart could drain every customer's Configured supply before the operators have re-established their sessions. The harm is asymmetric: shard restart should be no-op for the workload, not catastrophic.

This is latent since M29 (commit `895f1cf`, 2026-05-05, four days before M44.4 Drop F arrived). It was masked in earlier scaletests by the load-driver shape — CRs arrived at ~60-90s and the post-rollup Bootstrap activity hid the install-time reclaim spike. ADR-0035's pre-bind path made the spike visible because pre-bound Pods don't generate CRs (UPC only watches Unschedulable Pods), so the rollup-driven Bootstrap activity that masked the install spike stayed low.

## Goals

1. **Static stability holds across shard restart.** Configured supply across the fleet does not reclaim merely because the shard's worker pool started running before operators reported.
2. **Pre-seeded Configured supply is stable at install** (ADR-0026, M29). Operators of BigFleet who ship pre-seeded inventory at install-time get the supply they configured, not a transient drain to zero.
3. **No false-positive reclaims** — Phase 3 must not reclaim machines that haven't had a fair chance to be claimed by demand that exists in the world but hasn't been reported to the shard yet.
4. **No false-negative inhibition** — once an operator reports (even reporting zero demand), Phase 3 must resume normal reclaim behaviour for that cluster. An operator that has reported "I have no demand right now" is genuinely saying its Configured supply is excess.
5. **Minimal change to Phase 3's attribution logic** ([ADR-0027] stage 5.1 invariant). The shape of `claimMatching` and per-cluster supply walking stays unchanged; only the trailing "anything unclaimed becomes Reclaim" step gains a per-cluster guard.

## Non-goals

- **Bound-Pod accounting in operator rollups.** Per #36's H2: pre-bound Pods don't generate CRs because UPC only watches Unschedulable Pods. Fixing that would mean either UPC also tracks bound Pods (controller scope expansion) or operator rollups walk cluster Pod state (operator scope expansion). Both are bigger changes than this ADR addresses. The fix here is sufficient for both the seeded-supply install case and the production shard-restart case; ADR-0035's pre-bind story still depends on bound demand being visible somehow but that's a separate decision.
- **Persistence of shard inventory across restart.** The current in-memory model is intentional ([ADR-0003] shard snapshot eventual-consistency). This ADR fixes the reclaim window without persisting inventory.
- **Cluster decommissioning / dead-operator timeout.** A cluster whose operator never reconnects after restart would, under this ADR, retain its Configured supply indefinitely. That edge case wants a separate timeout rule (e.g. "if no rollup in N minutes, declare cluster dead and reclaim"). Deferred to a future ADR; the immediate fix doesn't require it.
- **Phase 1 / Phase 2 changes.** Phase 1 (assignment) and Phase 2 (priority-driven preemption) are independent — they already operate against the populated NeedsTable, and an empty per-cluster Needs slice produces no Phase 1 work for that cluster (nothing to allocate against). Phase 3 is the only phase that interprets empty Needs as "reclaim everything."

## Decision

**The shard tracks `firstRollupReceived map[ClusterID]bool`. Phase 3 skips reclaim for any cluster whose entry is `false`. The entry is set to `true` the first time the shard receives any rollup for that cluster — including a rollup that contains zero Needs.**

The flag's lifecycle:

- **Created false** when the shard's worker pool starts (post-install or post-restart).
- **Set true** when a `RollupReport` arrives for the cluster, regardless of whether the report's Needs slice is empty or populated. The signal is "the operator told me something," not "the operator told me about demand."
- **Stays true** for the cluster's lifetime within this shard process. A subsequent disconnect/reconnect cycle does not reset the flag — the last-known rollup is still authoritative until the new one arrives, per the existing protocol.
- **Cleared** only on shard process restart (because the map is in-memory). After restart, every cluster's flag starts false again until each cluster reports its first post-restart rollup.

Phase 3's per-cluster loop gains one early-return: if `!firstRollupReceived[c]`, skip the cluster entirely (no claim attempt, no Reclaim actions emitted). This preserves [ADR-0027]'s attribution invariant — Phase 1 and Phase 3 still attribute supply identically — because the unclaimed remainder is simply not evaluated rather than evaluated against stale state.

The flag does not affect:
- The reception of subsequent rollups (those flow through unchanged into `NeedsTable`).
- Phase 1's allocation against the cluster's Needs (Phase 1 sees whatever the NeedsTable says, including empty).
- Phase 2's preemption decisions (same rationale as Phase 1).
- Other shards (the flag is per-`(shard, cluster)` pair, not global).

## Alternatives considered

### Harness-only fix: load-driver creates CRs alongside pre-bound Pods

The load-driver creates a CapacityRequest for every pre-bound Pod, so the operator's rollup carries demand even when UPC doesn't see Unschedulable Pods.

**Rejected** because (a) it doesn't fix the production shard-restart case — the latent fleet-drain bug stays latent — and (b) it widens the scaletest harness's behaviour past the production UPC semantics, blurring the line between "what the test models" and "what the test fixes up." If the harness can lie to make tests pass, future tests will lie more.

### Phase 3 timeout: declare cluster dead after N minutes without rollup

Phase 3 skips reclaim for the first N minutes after shard startup, then proceeds as today.

**Rejected** because the right value of N depends on the operator-reconnect latency, which varies wildly (laptop kind: <5s; cross-region production: tens of seconds; pathological: minutes). Picking a number means either (a) being too aggressive and draining supply for slow-reconnecting operators, or (b) being too conservative and leaving stale Configured supply around indefinitely. The signal-based gate (`firstRollupReceived`) is exact in a way a timeout cannot be.

### Persist shard inventory + Needs across restart

A persistence layer (BoltDB, similar to coordinator) holds inventory and NeedsTable through restart so Phase 3 always has accurate state.

**Rejected** for this ADR — bigger change than the problem warrants, contradicts [ADR-0003]'s "shard snapshot eventual consistency" decision, and doesn't address the install-time case (a freshly-installed shard has nothing to persist yet). The signal-based gate fixes both restart and install with one mechanism.

### UPC watches bound Pods too

UPC translates both Unschedulable Pods and bound Pods into CRs, so the rollup always carries the cluster's actual demand state.

**Rejected** for this ADR because (a) UPC's scope today is "make BigFleet aware of demand that the scheduler couldn't satisfy" — adding "and demand that the scheduler did satisfy" changes the controller's purpose meaningfully, and (b) the change doesn't help the shard-restart case unless we also persist rollup state. The first-rollup gate handles both cleanly without expanding UPC's responsibility.

## Mechanism

`pkg/shard/shard.go` (or wherever the per-cluster session state lives):

```go
type Shard struct {
    // ... existing fields ...
    firstRollupMu       sync.RWMutex
    firstRollupReceived map[machine.ClusterID]bool
}
```

`HandleRollupReport` (the path that updates `NeedsTable`):

```go
func (s *Shard) HandleRollupReport(c machine.ClusterID, report *pb.RollupReport) {
    s.needs.Replace(c, report.Needs)
    s.firstRollupMu.Lock()
    s.firstRollupReceived[c] = true
    s.firstRollupMu.Unlock()
}
```

`pkg/decision/phase3_reclaim.go`:

```go
func (a *Allocator) Phase3(...) {
    for _, c := range clusters {
        if !a.shard.FirstRollupReceived(c) {
            continue   // skip reclaim until operator has reported once
        }
        // existing per-cluster logic unchanged
    }
}
```

`Shard.FirstRollupReceived(c)` is a read-locked accessor.

Tests:

1. **Pre-rollup window**: shard with seeded Configured supply + no rollup → Phase 3 emits zero Reclaim. Mirrors brief #36's `TestBrief36_H1_EmptyNeedsAtInstall` but as a regression test, not a diagnostic.
2. **Post-rollup window with empty Needs**: shard receives empty rollup → flag set true → Phase 3 reclaims excess. Confirms that "operator reported zero demand" correctly triggers reclaim.
3. **Per-cluster isolation**: cluster A reports, cluster B doesn't → Phase 3 reclaims excess in A but not B. Confirms the gate is per-cluster.
4. **Restart simulation**: instantiate two shards, the second post-restart; verify the second instance correctly defers reclaim until its first rollup arrives.

## Migration

1. **Stage 0**: ADR sign-off (this document).
2. **Stage 1**: Implement the flag + Phase 3 gate. Add unit tests per the test list above. Update [ADR-0027]'s stage-5.1 attribution invariant comment to note this gate (no semantic change; just documenting where it lives).
3. **Stage 2**: Re-run the M52.D (ADR-0035) validation under the new shard. Expect the Reclaim/Bootstrap ratio to invert (Reclaim much lower than Bootstrap, because the seed sticks until demand arrives).
4. **Stage 3**: Memory entry capturing the static-stability implication so this isn't re-derived.

No proto change, no operator-side change, no chart change. The fix is entirely shard-side.

## Hard rules touched

This ADR strengthens the static-stability hard rule rather than touching anything else:

- **Static stability is non-negotiable** (CLAUDE.md): preserved and strengthened. Before this ADR, shard restart had a fleet-drain window. After, it doesn't.
- **No in-tree providers**: unchanged.
- **No inbound listener on the cluster operator**: unchanged.
- **Cost formula is fixed**: unchanged.
- **Provider RPC surface unchanged**: unchanged.
- **Topology constraints do not cross shard boundaries**: unchanged.
- **Clusters are permanently bound to shards on first contact**: unchanged.
- **Roll-ups are full replacement**: unchanged. Empty rollups are still legal full-replacement reports (now correctly distinguished from "no report yet").

## References

- [ADR-0003] Shard snapshot eventual consistency on the cycle hot path.
- [ADR-0026] Scaletest harness models the Speculative tier.
- [ADR-0027] Roll-up demand is a constrained aggregate resource request — Phase 1 / Phase 3 attribution invariant.
- [ADR-0029] Phase 1 Omega-style OCC.
- [ADR-0035] Scaletest SLOs at steady state — the methodology that made this latent issue visible.

[ADR-0003]: ./0003-shard-snapshot-eventual-consistency-on-the-cycle-hot-path.md
[ADR-0026]: ./0026-scaletest-models-speculative-tier.md
[ADR-0027]: ./0027-rollup-demand-is-a-constrained-resource-request.md
[ADR-0029]: ./0029-phase1-omega-style-occ.md
[ADR-0035]: ./0035-scaletest-slos-at-steady-state.md
