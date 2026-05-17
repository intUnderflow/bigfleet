package occ

import (
	"sort"

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

		results[idx] = NeedResult{
			Need:    n,
			Deficit: remaining,
		}
	}
	return results
}
