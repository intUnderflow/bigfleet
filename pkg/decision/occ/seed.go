package occ

import (
	"sort"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// SeedConfiguredSupply runs the priority-sorted credit-existing-
// supply pre-pass before the OCC worker pool starts. For each Need
// in priority-descending order, it walks the matching Configured /
// Configuring machines in the Need's cluster, claims them via
// state.SeedClaim until either the cluster's matching supply
// exhausts or the Need's AggregateResources are covered.
//
// Sequential by design: priority ordering preserves bigfleet.md §16
// (priority is the sole throttling mechanism). Mirrors the pre-OCC
// phase1Allocator.creditExistingSupply method but writes to the OCC
// SharedState directly. The seeded claims participate in normal
// displacement during the OCC pass, so a higher-precedence Need
// proposing for the same machine in a later cycle's pre-pass would
// still evict (though in practice the pre-pass walks priority-
// descending so this never happens within one cycle).
//
// retryBudget is forwarded to each seeded claim — if a worker
// later displaces a seeded claim, the displaced Need re-enters
// the queue with retryBudget-1.
//
// Returns the per-Need outcomes (BootstrapMachines / ProvisionMachines
// empty at this point; only Deficit is populated). The OCC worker
// pool fills in Bootstrap/Provision machines as it runs.
func SeedConfiguredSupply(state *SharedState, allNeeds []needs.Need, retryBudget int) []NeedResult {
	sorted := make([]needs.Need, len(allNeeds))
	copy(sorted, allNeeds)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Profile.Priority() > sorted[j].Profile.Priority()
	})

	results := make([]NeedResult, len(sorted))
	snap := state.Snapshot()
	for i := range sorted {
		n := &sorted[i]
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

		results[i] = NeedResult{
			Need:    n,
			Deficit: remaining,
		}
	}
	return results
}
