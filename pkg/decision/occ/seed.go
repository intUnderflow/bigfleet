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
// Same-Profiles are domain-aware (ADR-0040): their credit is confined
// to the single best Same-domain bucket — see seedSameProfile.
// Crediting across domains made the pre-pass see scattered supply as
// satisfied while FindSame (strict) saw the same Needs as
// unsatisfiable, a self-sustaining Bootstrap↔Reclaim equilibrium.
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

	for _, idx := range order {
		n := needsByIdx[idx]
		profile := n.Profile
		prec := PrecedenceFromProfile(profile)
		remaining := n.AggregateResources

		if sameKey, ok := SameRequirementKey(profile); ok {
			remaining = seedSameProfile(state, snap, n, prec, sameKey, retryBudget)
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
// (ADR-0040): collect the matching, unclaimed, MinUnit-covering
// Configured + Configuring machines of the Need's cluster, bucket them
// by the machine's value for the Same key, choose the single best
// bucket via ChooseSameBucket, and SeedClaim within it only —
// Configured before Configuring, the same preference order as the
// non-Same walk. Returns the residual demand vector.
func seedSameProfile(state *SharedState, snap *inventory.Snapshot, n *needs.Need, prec Precedence, sameKey string, retryBudget int) []needs.ResourceQty {
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
	best := ChooseSameBucket(buckets, remaining)
	if best < 0 {
		return remaining
	}
	for _, c := range members[best] {
		if needs.IsZero(remaining) {
			break
		}
		state.SeedClaim(c.id, n, prec, retryBudget)
		remaining = needs.SubResources(remaining, c.alloc)
	}
	return remaining
}
