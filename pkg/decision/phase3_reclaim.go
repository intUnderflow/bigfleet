package decision

import (
	"sort"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// Phase3Result is the output of a Phase 3 pass: a list of Reclaim
// actions for machines whose cluster no longer has a need that matches
// them.
type Phase3Result struct {
	Actions []Action
}

// Phase3 walks each cluster's configured machines and reclaims those
// not currently needed. The matching algorithm is: a configured machine
// is *kept* if any of its cluster's current needs can match it (and
// hasn't already been "claimed" by another machine within the cluster's
// budget). Otherwise it's reclaimed.
//
// The release order is cheapest-per-hour first, tiebreak by lowest
// reclamation_penalty (per the paper §8 / design memory). The intuition
// is to release on-demand before reserved before bare-metal, and within
// each cost tier to release the machines whose loss costs the operator
// the least.
func Phase3(snap *inventory.Snapshot, allNeeds []needs.Need) Phase3Result {
	out := Phase3Result{}

	// Group needs by cluster.
	byCluster := make(map[machine.ClusterID][]needs.Need)
	for _, n := range allNeeds {
		byCluster[n.ClusterID] = append(byCluster[n.ClusterID], n)
	}

	// For each cluster present in the inventory, compute reclaim actions.
	for cluster := range collectClustersWithConfigured(snap) {
		configured := snap.ListByClusterState(cluster, machine.StateConfigured)
		if len(configured) == 0 {
			continue
		}

		// Sort by *keep-priority* descending: machines we'd most prefer
		// to retain come first and get matched to need slots. Whatever
		// is left after every slot is filled becomes excess.
		//
		// Keep-priority is the inverse of release-priority:
		//   primary: cheapest per-hour first (we hold onto bare metal
		//     and dump on-demand)
		//   tiebreak: highest reclamation_penalty first (within the
		//     same price tier, hold the machines whose loss costs more)
		sort.SliceStable(configured, func(i, j int) bool {
			if configured[i].PricePerHour != configured[j].PricePerHour {
				return configured[i].PricePerHour < configured[j].PricePerHour
			}
			if configured[i].AssignedReclamationPenaltyDollars != configured[j].AssignedReclamationPenaltyDollars {
				return configured[i].AssignedReclamationPenaltyDollars > configured[j].AssignedReclamationPenaltyDollars
			}
			return configured[i].ID < configured[j].ID
		})

		// Track remaining budget per need; a machine "claims" a slot
		// when a need can use it and the budget is non-zero.
		remaining := make([]int, len(byCluster[cluster]))
		for i, n := range byCluster[cluster] {
			remaining[i] = n.Count
		}

		for _, m := range configured {
			kept := false
			for i, n := range byCluster[cluster] {
				if remaining[i] <= 0 {
					continue
				}
				if MatchProfile(n.Profile, m) {
					remaining[i]--
					kept = true
					break
				}
			}
			if !kept {
				out.Actions = append(out.Actions, Action{
					Kind:        ActionKindReclaim,
					MachineID:   m.ID,
					Cluster:     cluster,
					GracePeriod: 0, // Reclaim is voluntary; the operator picks a normal grace at apply time.
					Reason:      "phase3.excess",
				})
			}
		}
	}

	return out
}

func collectClustersWithConfigured(snap *inventory.Snapshot) map[machine.ClusterID]struct{} {
	out := make(map[machine.ClusterID]struct{})
	for _, m := range snap.ListByState(machine.StateConfigured) {
		if m.Cluster != "" {
			out[m.Cluster] = struct{}{}
		}
	}
	return out
}
