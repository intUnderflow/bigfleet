package occ

import (
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// sameSupplyMember is one acquirable machine inside a Same-domain
// member list: its ID (for claimed-filtering) and its
// EffectiveAllocatable vector (pre-converted once at index build).
type sameSupplyMember struct {
	id    machine.ID
	alloc []needs.ResourceQty
}

// SameSupplyIndex indexes, lazily per Profile fingerprint, the
// shard-wide Idle + Speculative machines that match the Profile,
// bucketed by their value for the Profile's Same key.
//
// ADR-0040 Addendum: the Same domain is chosen once per Need per
// cycle over the JOINT potential — creditable supply (the Need's
// cluster's Configured + Configuring) plus acquirable supply
// (shard-wide unclaimed Idle + Speculative; Idle has no cluster
// binding). Both choosing sites need the acquirable half:
// SeedConfiguredSupply (Phase 1's pre-pass) and decision's
// claimMatching (Phase 3) rank domains against the same index so the
// two phases pick the same domain on the same snapshot instead of
// fighting at cycle rate.
//
// The lazy per-fingerprint build is the perf contract: Needs of the
// same archetype share Profile content, so there are few fingerprint
// classes and the full Idle+Speculative pool is walked (with
// matchProfile) once per class per cycle — never once per Need. The
// key derivation matches the PoolCache's: equal fingerprints mean
// equal Profiles (needs.Profile contract), so caching on
// Profile.Fingerprint is exact. A Profile carries at most one Same
// requirement (ADR-0024), so the fingerprint also pins the Same key.
//
// Not safe for concurrent use. Both consumers are single-threaded by
// construction: the pre-pass runs before the OCC worker pool starts,
// and Phase 3's per-cluster walk is sequential.
type SameSupplyIndex struct {
	snap *inventory.Snapshot
	byFP map[string]map[string][]sameSupplyMember
}

// NewSameSupplyIndex returns an empty index over snap. Building is
// lazy — a cycle with no Same-Profile Needs never walks the pools.
func NewSameSupplyIndex(snap *inventory.Snapshot) *SameSupplyIndex {
	return &SameSupplyIndex{
		snap: snap,
		byFP: make(map[string]map[string][]sameSupplyMember),
	}
}

// domains returns the domain→members index for profile, building it
// on first use. Mirrors PoolCache.buildPool's source selection: a
// pinned instance-type set narrows the walk to the per-type buckets;
// otherwise the full per-state list is scanned. No sorting — the
// index feeds bucket totals, not head-ordered acquisition.
func (ix *SameSupplyIndex) domains(profile needs.Profile, sameKey string) map[string][]sameSupplyMember {
	fp := profile.Fingerprint()
	if d, ok := ix.byFP[fp]; ok {
		return d
	}
	d := make(map[string][]sameSupplyMember)
	types := pinnedInstanceTypes(profile)
	for _, st := range []machine.State{machine.StateIdle, machine.StateSpeculative} {
		var srcs [][]machine.Machine
		if len(types) == 0 {
			srcs = [][]machine.Machine{ix.snap.ListByState(st)}
		} else {
			for _, t := range types {
				srcs = append(srcs, ix.snap.ListByStateInstanceType(st, t))
			}
		}
		for _, src := range srcs {
			for i := range src {
				m := &src[i]
				if !matchProfile(profile, *m) {
					continue
				}
				v, ok := lookupAttribute(sameKey, *m)
				if !ok {
					continue
				}
				d[v] = append(d[v], sameSupplyMember{
					id:    m.ID,
					alloc: needs.ResourceQtysFromMap(m.EffectiveAllocatable()),
				})
			}
		}
	}
	ix.byFP[fp] = d
	return d
}

// AcquirableTotals returns, per Same-domain value, the count and
// Σ EffectiveAllocatable of the unclaimed, minUnit-covering
// acquirable machines for profile. The per-Need filters (isClaimed,
// minUnit) run over the cached member lists, so the per-Need cost is
// the matching-machine count, not the pool size.
//
// isClaimed may be nil: Phase 3 never claims Idle/Speculative
// machines, so its walk treats the whole index as available.
func (ix *SameSupplyIndex) AcquirableTotals(profile needs.Profile, sameKey string, minUnit []needs.ResourceQty, isClaimed func(machine.ID) bool) map[string]SameBucket {
	out := make(map[string]SameBucket)
	for v, members := range ix.domains(profile, sameKey) {
		b := SameBucket{Value: v}
		for _, m := range members {
			if isClaimed != nil && isClaimed(m.id) {
				continue
			}
			if !needs.Covers(m.alloc, minUnit) {
				continue
			}
			b.Count++
			b.Total = needs.AddResources(b.Total, m.alloc)
		}
		if b.Count > 0 {
			out[v] = b
		}
	}
	return out
}
