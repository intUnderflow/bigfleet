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
//
// Performance (M11.23): needs are pre-grouped per cluster by Profile
// fingerprint with summed counts. The inner match loop calls
// MatchProfile once per (configured, distinct fingerprint) instead of
// once per (configured, need). At 1K configured × 1K needs × 50
// clusters that's 50M calls collapsing to 50K when all needs in a
// cluster share a single fingerprint (the common workload shape).
func Phase3(snap *inventory.Snapshot, allNeeds []needs.Need) Phase3Result {
	out := Phase3Result{}

	// Group needs by cluster, then within each cluster collapse by
	// Profile fingerprint and sum counts.
	rawByCluster := make(map[machine.ClusterID][]needs.Need)
	for _, n := range allNeeds {
		rawByCluster[n.ClusterID] = append(rawByCluster[n.ClusterID], n)
	}
	byCluster := make(map[machine.ClusterID][]profileBudget, len(rawByCluster))
	for cluster, ns := range rawByCluster {
		byCluster[cluster] = collapseByFingerprint(ns)
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

		groups := byCluster[cluster] // nil for clusters with no needs

		for _, m := range configured {
			kept := false
			for i := range groups {
				if groups[i].remaining <= 0 {
					continue
				}
				if MatchProfile(groups[i].profile, m) {
					groups[i].remaining--
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

// profileBudget is one row per distinct Profile fingerprint within a
// cluster's needs: which Profile to MatchProfile against, and how many
// machines that fingerprint can collectively claim.
type profileBudget struct {
	profile   needs.Profile
	remaining int
}

// collapseByFingerprint collapses a cluster's needs into one row per
// distinct fingerprint. Two needs sharing a fingerprint match the same
// machines, so for Phase 3's purposes the counts add. Order is the
// first-seen order of fingerprints in the input — deterministic given
// allNeeds is stable, and the fingerprint-collision order doesn't
// affect correctness because all groups satisfying a machine are
// equivalent.
func collapseByFingerprint(ns []needs.Need) []profileBudget {
	if len(ns) == 0 {
		return nil
	}
	idx := make(map[string]int, len(ns))
	out := make([]profileBudget, 0, len(ns))
	for _, n := range ns {
		fp := n.Profile.Fingerprint()
		if i, ok := idx[fp]; ok {
			out[i].remaining += n.Count
			continue
		}
		idx[fp] = len(out)
		out = append(out, profileBudget{profile: n.Profile, remaining: n.Count})
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
