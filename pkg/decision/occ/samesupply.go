package occ

import (
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// sameSupplyMember is one acquirable machine inside a Same-domain
// member list: its ID (for claimed-filtering) and its
// EffectiveAllocatable as a dimension-interned milli-unit vector,
// parsed exactly once at index build.
type sameSupplyMember struct {
	id  machine.ID
	vec []int64
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
// Quantities are parsed once, at index build, into dimension-interned
// int64 milli-unit vectors (ParseVec); the per-Need AcquirableTotals
// walk is integer adds and compares only. The first implementation
// re-parsed resource.Quantity strings per member per Need
// (needs.Covers / needs.AddResources both round-trip through
// ParseQuantity) and that was 58 % of shard CPU at ~2,500 co-located
// Needs — ~100 s cycles, a starved shard.
// BenchmarkAcquirableTotals_Uber5KShape guards the path.
//
// Not safe for concurrent use. Both consumers are single-threaded by
// construction: the pre-pass runs before the OCC worker pool starts,
// and Phase 3's per-cluster walk is sequential.
type SameSupplyIndex struct {
	snap *inventory.Snapshot
	byFP map[string]map[string][]sameSupplyMember
	dims map[string]int
}

// NewSameSupplyIndex returns an empty index over snap. Building is
// lazy — a cycle with no Same-Profile Needs never walks the pools.
func NewSameSupplyIndex(snap *inventory.Snapshot) *SameSupplyIndex {
	return &SameSupplyIndex{
		snap: snap,
		byFP: make(map[string]map[string][]sameSupplyMember),
		dims: make(map[string]int),
	}
}

// dim interns a resource name, returning its vector index. Vectors
// parsed before a dimension first appeared are simply shorter — a
// missing index reads as zero, which is exact (the machine genuinely
// had none of that resource, or ParseVec would have interned it).
func (ix *SameSupplyIndex) dim(name string) int {
	if i, ok := ix.dims[name]; ok {
		return i
	}
	i := len(ix.dims)
	ix.dims[name] = i
	return i
}

// ParseVec converts a []ResourceQty into the index's dimension-interned
// milli-unit vector form. This is the only place quantity strings are
// parsed on the Same-attribution path — callers parse each input once
// (per machine at index/bucket build, per Need for MinUnit and the
// remaining-deficit) and the hot loops run on integers. Unparseable
// quantities degrade to zero, matching the needs package's vector ops.
//
// MilliValue is exact for every quantity the harness and catalog use;
// it saturates only beyond ~9.2e15 units (petabyte-scale per-bucket
// sums), far above any per-domain aggregate.
func (ix *SameSupplyIndex) ParseVec(qs []needs.ResourceQty) []int64 {
	if len(qs) == 0 {
		return nil
	}
	vec := []int64(nil)
	for _, r := range qs {
		q, err := resource.ParseQuantity(r.Quantity)
		if err != nil {
			continue
		}
		i := ix.dim(r.Name)
		for len(vec) <= i {
			vec = append(vec, 0)
		}
		vec[i] = q.MilliValue()
	}
	return vec
}

// VecAdd returns dst with src added per dimension, growing dst as
// needed. Vectors must come from the same SameSupplyIndex so their
// dimensions align.
func VecAdd(dst, src []int64) []int64 {
	for len(dst) < len(src) {
		dst = append(dst, 0)
	}
	for i, v := range src {
		dst[i] += v
	}
	return dst
}

// VecCovers reports whether have satisfies want on every dimension —
// the integer mirror of needs.Covers. A dimension beyond a vector's
// length is zero.
func VecCovers(have, want []int64) bool {
	for i, w := range want {
		if w <= 0 {
			continue
		}
		if i >= len(have) || have[i] < w {
			return false
		}
	}
	return true
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
					id:  m.ID,
					vec: ix.ParseVec(needs.ResourceQtysFromMap(m.EffectiveAllocatable())),
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
// minUnit) run over the cached member lists as integer compares and
// adds — the per-Need cost is the matching-machine count, with no
// quantity parsing.
//
// isClaimed may be nil: Phase 3 never claims Idle/Speculative
// machines, so its walk treats the whole index as available.
func (ix *SameSupplyIndex) AcquirableTotals(profile needs.Profile, sameKey string, minUnit []needs.ResourceQty, isClaimed func(machine.ID) bool) map[string]SameBucket {
	minUnitVec := ix.ParseVec(minUnit)
	out := make(map[string]SameBucket)
	for v, members := range ix.domains(profile, sameKey) {
		b := SameBucket{Value: v}
		for _, m := range members {
			if isClaimed != nil && isClaimed(m.id) {
				continue
			}
			if !VecCovers(m.vec, minUnitVec) {
				continue
			}
			b.Count++
			b.Total = VecAdd(b.Total, m.vec)
		}
		if b.Count > 0 {
			out[v] = b
		}
	}
	return out
}
