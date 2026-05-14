package decision

import (
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
// once per (configured, need).
//
// Performance (M27): when a group's profile pins to specific instance
// types via `node.kubernetes.io/instance-type In [...]`, the match
// candidates are restricted to the snapshot's pre-built per-(cluster,
// state, instance-type) bucket — which at 500K configured / 50
// clusters / 5 instance types is ~2K candidates instead of the
// cluster's full ~10K configured. Cuts the inner MatchProfile loop
// 5× at the M13.gate scale shape (cycle p99 was 416 ms before;
// target is ≤100 ms after).
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
	// M27: iterate snap.bucketsByClusterState directly via the per-
	// cluster keys we already have in the needs map; the snapshot's
	// SortedClusterStateBucket returns the per-cluster Configured
	// slice pre-sorted by Phase 3's keep-priority order, so the
	// per-call ListByClusterState alloc + sort.SliceStable is gone.
	for cluster := range clustersWithConfigured(snap) {
		configured := snap.SortedClusterStateBucket(cluster, machine.StateConfigured)
		if len(configured) == 0 {
			continue
		}

		groups := byCluster[cluster] // nil for clusters with no needs

		// M44.4 Drop F: keep on AssignedNeedFingerprint equality, not
		// MatchProfile. Same bug class as Drop B/C in Phase 1: with
		// label-axis fingerprint multiplicity (M35), a machine bound
		// for fp_X also satisfies any sibling Need whose requirements
		// are a subset of fp_X — Phase 3 then "kept" the stale
		// machine against an unrelated Need's budget, never reclaiming
		// it. With churn this caused steady-state inventory to fill
		// up with machines bound to long-dead fingerprints; new Pods
		// could only bind via Phase 2 preemption thrash. Surfaced in
		// the scaleway-50k Drop D run: Reclaim emit 0/sec, Preempt
		// 1/sec, bindingLatency p99 = 102 s (45 % of binds >102 s).
		//
		// Correct keep semantics: a machine is kept iff a Need exists
		// for the *same* (cluster, fingerprint) and that Need still
		// has budget. Otherwise reclaim — its workload is gone.
		// Machines with empty AssignedNeedFingerprint (never bound,
		// shouldn't happen in practice for Configured) reclaim by
		// default.
		//
		// The map lookup also retires the M27 instance-type prefilter
		// and the M30.2 instance-type-pin-only fast path; both were
		// optimisations that assumed the legacy single-archetype shape.
		// Under M35 / Drop F, the per-machine fingerprint key gives the
		// answer directly — no prefilter needed, and the fast path's
		// "match by instance-type alone" semantics are actively wrong
		// (a machine pinned to type T can have AssignedNeedFingerprint
		// for any of several distinct Needs that all use T).
		fpIdx := make(map[string]int, len(groups))
		for i := range groups {
			fpIdx[groups[i].profile.Fingerprint()] = i
		}
		for _, m := range configured {
			i, ok := fpIdx[m.AssignedNeedFingerprint]
			kept := ok && !needs.IsZero(groups[i].remaining)
			if kept {
				// ADR-0027: budget is a resource vector. The kept machine
				// covers its own EffectiveAllocatable worth of the
				// fingerprint's remaining demand; once the vector reaches
				// zero the rest of the fingerprint's Configured machines
				// are excess and reclaimed.
				groups[i].remaining = needs.SubResources(
					groups[i].remaining,
					needs.ResourceQtysFromMap(m.EffectiveAllocatable()),
				)
				continue
			}
			out.Actions = append(out.Actions, Action{
				Kind:        ActionKindReclaim,
				MachineID:   m.ID,
				Cluster:     cluster,
				GracePeriod: 0, // Reclaim is voluntary; the operator picks a normal grace at apply time.
				Reason:      "phase3.excess",
			})
		}
	}

	return out
}

// profileBudget is one row per distinct Profile fingerprint within a
// cluster's needs: which Profile this fingerprint represents, and the
// aggregate resource demand it can collectively claim.
//
// ADR-0027: remaining is a resource vector — the summed AggregateResources
// of every Need sharing this Profile. Each kept Configured machine
// decrements it by that machine's EffectiveAllocatable; once the vector
// is zero, further machines of the fingerprint are excess and reclaimed.
type profileBudget struct {
	profile   needs.Profile
	remaining []needs.ResourceQty
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
			out[i].remaining = needs.AddResources(out[i].remaining, n.AggregateResources)
			continue
		}
		idx[fp] = len(out)
		out = append(out, profileBudget{profile: n.Profile, remaining: n.AggregateResources})
	}
	return out
}

// clustersWithConfigured returns the set of cluster IDs that have at
// least one Configured machine. M27: iterates the snapshot's per-
// cluster index keys instead of walking the full Configured slice
// (which at 500K configured was ~9 % of Phase3's hot path).
func clustersWithConfigured(snap *inventory.Snapshot) map[machine.ClusterID]struct{} {
	out := make(map[machine.ClusterID]struct{})
	for cl := range snap.ClusterIDs() {
		if snap.CountByClusterState(cl, machine.StateConfigured) > 0 {
			out[cl] = struct{}{}
		}
	}
	return out
}
