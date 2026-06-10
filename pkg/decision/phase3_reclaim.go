package decision

import (
	"sort"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
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

	// ADR-0040 Addendum: Phase 3 ranks Same-domains by the same joint
	// potential Phase 1's pre-pass uses — creditable plus shard-wide
	// acquirable (Idle + Speculative) — so the two phases choose the
	// same domain on the same snapshot. One index per cycle; Phase 3
	// never claims Idle/Speculative, so its per-fingerprint member
	// lists stay valid for the whole walk.
	acquirable := occ.NewSameSupplyIndex(snap)

	// ADR-0041 rider: the per-cycle virtual-consumption ledger for
	// acquirable machines. As each Same Need's chosen domain falls
	// short of its creditable supply, the domain's acquirable members
	// are consumed here, so a later Need ranks against what is actually
	// left — Phase 1's workers consume the same supply via OCC claims.
	// Shard-wide (the acquirable pool has no cluster binding), so the
	// cluster walk below must be deterministic for the consumption
	// order — and therefore the actions — to be reproducible.
	consumed := make(map[machine.ID]struct{})

	for _, cluster := range clustersWithConfigured(snap) {
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
			if sameKey, ok := occ.SameRequirementKey(n.Profile); ok {
				// One joint domain choice per Need per cycle over both
				// creditable tiers at once, exactly like Phase 1's
				// seedSameProfile — see claimMatchingSame.
				claimMatchingSame(configuring, configured, n.Profile, n.MinUnit, claimed, n.AggregateResources, sameKey, acquirable, consumed)
				continue
			}
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
// the residual demand vector. Same-Profile Needs never reach here —
// Phase3 routes them to claimMatchingSame.
//
// This is the Phase 3 counterpart of Phase 1's existing-supply credit
// (occ.SeedConfiguredSupply) — identical attribution rules, so the two
// phases agree on which machine serves which Need.
func claimMatching(
	machines []machine.Machine,
	profile needs.Profile,
	minUnit []needs.ResourceQty,
	claimed map[machine.ID]struct{},
	remaining []needs.ResourceQty,
) []needs.ResourceQty {
	if needs.IsZero(remaining) {
		return remaining
	}
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

// claimMatchingSame is the Same-Profile claim arm (ADR-0040 +
// Addendum + ADR-0041 rider): bucket the cluster's eligible
// Configuring and Configured machines by their value for the Same key,
// add each domain's acquirable potential (shard-wide Idle +
// Speculative — the same per-fingerprint index Phase 1's pre-pass
// ranks with), choose the single best bucket against the Need's whole
// demand over those joint totals, and claim within it only —
// Configuring members first (committed in-flight supply, never
// reclaimed), then Configured in keep-priority order. Everything
// outside the chosen domain is scatter the Need cannot use and falls
// through to reclaim. Only creditable machines are ever claimed — the
// acquirable half steers WHICH domain's Configured the Need keeps,
// never what Phase 3 may keep.
//
// The domain is chosen ONCE per Need per cycle, over both creditable
// tiers jointly — the exact bucket fold occ.seedSameProfile performs —
// so the two phases agree by construction. (The earlier two-walk
// structure chose per tier; an empty Configuring walk would have made
// the ADR-0041 consumption below fire on the full demand before the
// Configured walk ran.)
//
// ADR-0041 rider: the acquirable fold is consumption-aware (consumed
// instead of the old nil view), and whatever part of the Need's demand
// its creditable claims did not cover virtually consumes the chosen
// domain's acquirable members. Phase 1's workers consume that same
// supply sequentially via OCC claims; without the mirror, the moment
// idle Same-capacity appeared every gang ranked the same fresh domain
// best and Phase 3 mass-reclaimed healthy bound gangs.
func claimMatchingSame(
	configuring, configured []machine.Machine,
	profile needs.Profile,
	minUnit []needs.ResourceQty,
	claimed map[machine.ID]struct{},
	remaining []needs.ResourceQty,
	sameKey string,
	acquirable *occ.SameSupplyIndex,
	consumed map[machine.ID]struct{},
) []needs.ResourceQty {
	if needs.IsZero(remaining) {
		return remaining
	}
	type candidate struct {
		id    machine.ID
		alloc []needs.ResourceQty
	}
	// Bucket totals are the index's integer vectors (ParseVec) so the
	// per-Need walk does no quantity parsing; candidates keep their
	// []ResourceQty alloc for the claim loop's SubResources boundary.
	minUnitVec := acquirable.ParseVec(minUnit)
	index := make(map[string]int)
	var buckets []occ.SameBucket
	var members [][]candidate
	for _, machines := range [][]machine.Machine{configuring, configured} {
		for i := range machines {
			m := &machines[i]
			if _, taken := claimed[m.ID]; taken {
				continue
			}
			if !MatchProfile(profile, *m) {
				continue
			}
			alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
			vec := acquirable.ParseVec(alloc)
			if !occ.VecCovers(vec, minUnitVec) {
				continue
			}
			v, ok := lookupAttribute(sameKey, *m)
			if !ok {
				continue
			}
			idx, exists := index[v]
			if !exists {
				idx = len(buckets)
				index[v] = idx
				buckets = append(buckets, occ.SameBucket{Value: v})
				members = append(members, nil)
			}
			buckets[idx].Count++
			buckets[idx].CreditableCount++
			buckets[idx].Total = occ.VecAdd(buckets[idx].Total, vec)
			members[idx] = append(members[idx], candidate{id: m.ID, alloc: alloc})
		}
	}
	// Joint potential (ADR-0040 Addendum): fold in the acquirable
	// half, consumption-aware (ADR-0041 rider). Acquirable-only
	// domains get an empty members list — when one wins, this Need
	// keeps no Configured machine and the off-domain scatter is
	// reclaimed once, mirroring Phase 1's acquisition into that
	// domain. CreditableCount stays 0 for that half by construction.
	isConsumed := func(id machine.ID) bool {
		_, ok := consumed[id]
		return ok
	}
	for v, ab := range acquirable.AcquirableTotals(profile, sameKey, minUnit, isConsumed) {
		idx, exists := index[v]
		if !exists {
			idx = len(buckets)
			index[v] = idx
			buckets = append(buckets, occ.SameBucket{Value: v})
			members = append(members, nil)
		}
		buckets[idx].Count += ab.Count
		buckets[idx].Total = occ.VecAdd(buckets[idx].Total, ab.Total)
	}
	best := occ.ChooseSameBucket(buckets, acquirable.ParseVec(remaining))
	if best < 0 {
		return remaining
	}
	for _, c := range members[best] {
		if needs.IsZero(remaining) {
			break
		}
		claimed[c.id] = struct{}{}
		remaining = needs.SubResources(remaining, c.alloc)
	}
	// ADR-0041 rider: the creditable members didn't fully cover the
	// Need — Phase 1 would fill the residual by acquiring the chosen
	// domain's Idle/Speculative, so consume them from later Needs'
	// joint view.
	if !needs.IsZero(remaining) {
		acquirable.ConsumeAcquirable(profile, sameKey, buckets[best].Value, minUnit, remaining, consumed)
	}
	return remaining
}

// clustersWithConfigured returns the cluster IDs that have at least
// one Configured machine, sorted. M27: iterates the snapshot's per-
// cluster index keys instead of walking the full Configured slice
// (which at 500K configured was ~9 % of Phase3's hot path). Sorted
// because the ADR-0041 consumption ledger is shard-wide: cluster walk
// order affects which Need consumes contested acquirable supply, and
// map-iteration order would make Phase 3's actions non-reproducible.
func clustersWithConfigured(snap *inventory.Snapshot) []machine.ClusterID {
	out := make([]machine.ClusterID, 0, len(snap.ClusterIDs()))
	for cl := range snap.ClusterIDs() {
		if snap.CountByClusterState(cl, machine.StateConfigured) > 0 {
			out = append(out, cl)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
