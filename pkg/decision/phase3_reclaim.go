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

// Phase3 reclaims a cluster's Configured machines that no current Need
// claims. It is the mirror of Phase 1 (ADR-0027): Phase 1 walks Needs in
// priority order and claims existing matching supply before provisioning
// the deficit; Phase 3 walks the same Needs in the same order, claims
// the matching machines each Need needs to *keep*, and reclaims whatever
// Configured machine no Need claimed — the slack.
//
// Critically, both phases attribute supply the same way: a machine is
// claimed for a Need iff it MatchProfiles the Need's requirements and can
// host one MinUnit, claimed once via a shared per-cluster set, in
// priority order. Pre-ADR-0027 Phase 1 used MatchProfile while Phase 3
// keyed on Machine.AssignedNeedFingerprint — that inconsistency made
// Phase 1 provision a machine Phase 3 then reclaimed (and vice versa),
// a Bootstrap<->Reclaim thrash. Mirroring the attribution removes it.
//
// Configuring machines (Bootstrap in flight) count toward a Need's
// claimed budget — they are committed supply and cannot be reclaimed
// anyway — so Phase 3 never reclaims a Configured machine that an
// in-flight bootstrap was about to make redundant of.
//
// The release order within the reclaimable slack is cheapest-per-hour
// first, tiebreak by lowest reclamation_penalty (paper §8): the per-
// cluster Configured bucket is pre-sorted in keep-priority order
// (highest-value first), so claimMatching keeps the high-value machines
// and the leftover — reclaimed — is the low-value tail.
// clusterReady reports whether the named cluster has been heard from
// (first rollup received) since this shard process started.
// ADR-0036: Phase 3 must skip reclaim for clusters that haven't yet
// reported, because their NeedsTable slice is empty for the trivial
// "I haven't told you anything yet" reason, not for the meaningful
// "I have no demand right now" reason. Without the gate, the
// install / shard-restart window drains every Configured machine
// before the operator pipeline catches up.
//
// Callers in tests that don't care about the gate (most decision-
// package unit tests) pass `AlwaysReady`. Callers in tests that
// specifically exercise the gate's behaviour pass a stub.
type ClusterReadyFn func(machine.ClusterID) bool

// AlwaysReady is a ClusterReadyFn that always returns true. Used by
// tests that don't exercise the ADR-0036 gate.
func AlwaysReady(machine.ClusterID) bool { return true }

func Phase3(snap *inventory.Snapshot, allNeeds []needs.Need, clusterReady ClusterReadyFn) Phase3Result {
	out := Phase3Result{}

	needsByCluster := make(map[machine.ClusterID][]needs.Need)
	for _, n := range allNeeds {
		needsByCluster[n.ClusterID] = append(needsByCluster[n.ClusterID], n)
	}

	for cluster := range clustersWithConfigured(snap) {
		// ADR-0036 gate: skip clusters that haven't yet reported.
		// Their empty NeedsTable slice is "haven't told me yet",
		// not "have no demand"; reclaiming on that signal would
		// drain seeded supply during install / restart windows.
		if !clusterReady(cluster) {
			continue
		}
		configured := snap.SortedClusterStateBucket(cluster, machine.StateConfigured)
		if len(configured) == 0 {
			continue
		}
		// In-flight bootstraps are committed supply for their cluster;
		// they count toward demand so Phase 3 doesn't reclaim a Configured
		// machine an arriving one is about to make redundant.
		configuring := snap.ListByClusterState(cluster, machine.StateConfiguring)

		clusterNeeds := append([]needs.Need(nil), needsByCluster[cluster]...)
		sort.SliceStable(clusterNeeds, func(i, j int) bool {
			return clusterNeeds[i].Profile.Priority() > clusterNeeds[j].Profile.Priority()
		})

		claimed := make(map[machine.ID]struct{})
		for _, n := range clusterNeeds {
			// Configuring supply first (claimed so it isn't double-counted
			// across Needs; never reclaimed), then Configured — the
			// machines Phase 3 keeps.
			remaining := claimMatching(configuring, n.Profile, n.MinUnit, claimed, n.AggregateResources)
			if needs.IsZero(remaining) {
				continue
			}
			claimMatching(configured, n.Profile, n.MinUnit, claimed, remaining)
		}

		// Any Configured machine no Need claimed is excess → reclaim.
		for _, m := range configured {
			if _, kept := claimed[m.ID]; kept {
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

// claimMatching walks machines in keep-priority order, claiming each
// unclaimed machine that MatchProfiles profile and can host one minUnit,
// subtracting its EffectiveAllocatable from remaining, until remaining is
// covered or the slice is exhausted. Claimed machines are recorded in
// the shared claimed set so a peer Need can't double-count them. Returns
// the residual demand vector.
//
// This is the Phase 3 counterpart of phase1Allocator.creditExistingSupply
// — identical attribution rules, so the two phases agree on which
// machine serves which Need.
func claimMatching(
	machines []machine.Machine,
	profile needs.Profile,
	minUnit []needs.ResourceQty,
	claimed map[machine.ID]struct{},
	remaining []needs.ResourceQty,
) []needs.ResourceQty {
	for _, m := range machines {
		if needs.IsZero(remaining) {
			return remaining
		}
		if _, taken := claimed[m.ID]; taken {
			continue
		}
		if !MatchProfile(profile, m) {
			continue
		}
		alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
		if !needs.Covers(alloc, minUnit) {
			continue
		}
		claimed[m.ID] = struct{}{}
		remaining = needs.SubResources(remaining, alloc)
	}
	return remaining
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
