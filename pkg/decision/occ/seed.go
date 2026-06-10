package occ

import (
	"sort"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// SeedConfiguredSupply runs the priority-sorted credit-existing-
// supply pre-pass before the OCC worker pool starts. For each Need
// in priority-descending order, it walks the matching Configured /
// Configuring machines in the Need's cluster and claims them via
// state.SeedClaim until either the cluster's matching supply
// exhausts or the Need's AggregateResources are covered.
//
// Same-Profiles are domain-aware (ADR-0040): the domain is chosen
// once per Need, jointly over creditable supply (the cluster's
// Configured/Configuring) and acquirable supply (shard-wide Idle +
// Speculative), credit is confined to it, and the choice is recorded
// on state so acquisition (FindSame) is confined to it too — see
// seedSameProfile. Crediting across domains made the pre-pass see
// scattered supply as satisfied while FindSame (strict) saw the same
// Needs as unsatisfiable; choosing the domain twice (creditable-only
// here, best-Idle in FindSame) assembled cross-domain groups Phase 3
// then reclaimed — both halves of the Bootstrap↔Reclaim equilibrium
// the ADR and its Addendum document.
//
// Sequential by design: priority ordering preserves bigfleet.md §16
// (priority is the sole throttling mechanism). Mirrors the pre-OCC
// phase1Allocator.creditExistingSupply method but writes to the OCC
// SharedState directly. The seeded claims participate in normal
// displacement during the OCC pass.
//
// retryBudget is forwarded to each seeded claim — if a worker
// later displaces a seeded claim, the displaced Need re-enters
// the queue with retryBudget-1.
//
// Returns *needs.Need pointers in the same order needsByIdx
// presented; element i corresponds to needsByIdx[i]. Each Need's
// "residual deficit after pre-pass" is encoded in the returned
// NeedResult.Deficit. Callers that need stable Need pointers must
// guarantee needsByIdx doesn't shift the underlying storage after
// SeedConfiguredSupply returns.
func SeedConfiguredSupply(state *SharedState, needsByIdx []*needs.Need, retryBudget int) []NeedResult {
	// Sort an index slice (not the Needs themselves) so the caller's
	// pointers remain stable. Priority-descending walk.
	order := make([]int, len(needsByIdx))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return needsByIdx[order[a]].Profile.Priority() > needsByIdx[order[b]].Profile.Priority()
	})

	results := make([]NeedResult, len(needsByIdx))
	snap := state.Snapshot()
	acquirable := NewSameSupplyIndex(snap)

	for _, idx := range order {
		n := needsByIdx[idx]
		profile := n.Profile
		prec := PrecedenceFromProfile(profile)
		remaining := n.AggregateResources

		if sameKey, ok := SameRequirementKey(profile); ok {
			remaining = seedSameProfile(state, snap, acquirable, n, prec, sameKey, retryBudget)
		} else {
			for _, st := range []machine.State{machine.StateConfigured, machine.StateConfiguring} {
				if needs.IsZero(remaining) {
					break
				}
				for _, m := range snap.ListByClusterState(n.ClusterID, st) {
					if needs.IsZero(remaining) {
						break
					}
					if state.IsClaimed(m.ID) {
						continue
					}
					if !matchProfile(profile, m) {
						continue
					}
					alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
					if !needs.Covers(alloc, n.MinUnit) {
						continue
					}
					state.SeedClaim(m.ID, n, prec, retryBudget)
					remaining = needs.SubResources(remaining, alloc)
				}
			}
		}

		results[idx] = NeedResult{
			Need:    n,
			Deficit: remaining,
		}
	}
	return results
}

// seedSameProfile is SeedConfiguredSupply's Same-Profile arm
// (ADR-0040 + Addendum): collect the matching, unclaimed,
// MinUnit-covering Configured + Configuring machines of the Need's
// cluster, bucket them by the machine's value for the Same key, add
// each domain's acquirable potential (shard-wide unclaimed Idle +
// Speculative from the per-fingerprint index — Idle has no cluster
// binding), and choose the single best bucket via ChooseSameBucket
// over those JOINT totals. The choice is recorded on state so
// findCandidatesFor confines acquisition to the same domain, and
// SeedClaim credits only the creditable members within it —
// Configured before Configuring, the same preference order as the
// non-Same walk. Returns the residual demand vector.
//
// Ranking creditable-only here while FindSame re-picked the best
// Idle bucket chose the domain twice per cycle: Phase 1 assembled
// cross-domain groups, Phase 3 (correctly strict) reclaimed the
// off-domain half next cycle, and it re-bootstrapped scattered — a
// reclaim↔re-bootstrap oscillation at cycle rate.
func seedSameProfile(state *SharedState, snap *inventory.Snapshot, acquirable *SameSupplyIndex, n *needs.Need, prec Precedence, sameKey string, retryBudget int) []needs.ResourceQty {
	type candidate struct {
		id    machine.ID
		alloc []needs.ResourceQty
	}
	remaining := n.AggregateResources
	index := make(map[string]int)
	var buckets []SameBucket
	var members [][]candidate
	for _, st := range []machine.State{machine.StateConfigured, machine.StateConfiguring} {
		for _, m := range snap.ListByClusterState(n.ClusterID, st) {
			if state.IsClaimed(m.ID) {
				continue
			}
			if !matchProfile(n.Profile, m) {
				continue
			}
			alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
			if !needs.Covers(alloc, n.MinUnit) {
				continue
			}
			v, ok := lookupAttribute(sameKey, m)
			if !ok {
				continue
			}
			i, exists := index[v]
			if !exists {
				i = len(buckets)
				index[v] = i
				buckets = append(buckets, SameBucket{Value: v})
				members = append(members, nil)
			}
			buckets[i].Count++
			buckets[i].Total = needs.AddResources(buckets[i].Total, alloc)
			members[i] = append(members[i], candidate{id: m.ID, alloc: alloc})
		}
	}
	// Joint potential: fold in the acquirable half. Domains with only
	// acquirable supply get an empty members list — choosing one
	// credits nothing and leaves acquisition (confined there) to fill
	// the deficit.
	for v, ab := range acquirable.AcquirableTotals(n.Profile, sameKey, n.MinUnit, state.IsClaimed) {
		i, exists := index[v]
		if !exists {
			i = len(buckets)
			index[v] = i
			buckets = append(buckets, SameBucket{Value: v})
			members = append(members, nil)
		}
		buckets[i].Count += ab.Count
		buckets[i].Total = needs.AddResources(buckets[i].Total, ab.Total)
	}
	best := ChooseSameBucket(buckets, remaining)
	if best < 0 {
		return remaining
	}
	state.recordSameDomain(n, buckets[best].Value)
	for _, c := range members[best] {
		if needs.IsZero(remaining) {
			break
		}
		state.SeedClaim(c.id, n, prec, retryBudget)
		remaining = needs.SubResources(remaining, c.alloc)
	}
	return remaining
}
