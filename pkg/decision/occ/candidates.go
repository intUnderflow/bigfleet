package occ

import (
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// Candidates is one bucket's worth of proposal-ready machines, plus
// the BucketKey the resulting Proposal must carry. Workers translate
// Candidates → Proposal by adding ObservedSeq, Precedence, Mode, and
// the Need reference.
//
// Machines is empty when no eligible candidates exist (the pool was
// exhausted, or no bucket satisfies the Same/Spread constraint). The
// caller treats an empty Candidates as "this Need cannot make
// further progress against this (state, profile) pool right now".
type Candidates struct {
	Machines []machine.ID
	Bucket   BucketKey
}

// FindBasic returns up to enough unclaimed, MatchProfile-passing,
// minUnit-fitting machines from pool to cover deficit. Walks pool.src
// in price/cost order and stops as soon as the running allocatable
// sum covers deficit. Mirrors phase1Allocator.take from the legacy
// allocator at pkg/decision/phase1_allocator.go:112; the head-cursor
// amortisation is gone (workers run in parallel, no cursor state),
// replaced with a per-Need rewalk that filters via state.IsClaimed.
func (p *Pool) FindBasic(state *SharedState, st machine.State, deficit, minUnit []needs.ResourceQty) Candidates {
	if needs.IsZero(deficit) {
		return Candidates{}
	}
	bucket := BucketKey{State: st, ProfileFP: p.profile.Fingerprint()}
	out := make([]machine.ID, 0, 4)
	remaining := deficit
	for i := range p.src {
		m := &p.src[i]
		if state.IsClaimed(m.ID) {
			continue
		}
		if !matchProfile(p.profile, *m) {
			continue
		}
		alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
		if !needs.Covers(alloc, minUnit) {
			continue
		}
		out = append(out, m.ID)
		remaining = needs.SubResources(remaining, alloc)
		if needs.IsZero(remaining) {
			break
		}
	}
	return Candidates{Machines: out, Bucket: bucket}
}

// FindSame returns candidates honouring a Profile's Same requirement:
// all returned machines share one value for sameKey. Picks the best
// single-value bucket — atomic-satisfiable preferred, then cheapest
// head, then most-available — and takes from it. Mirrors
// phase1Allocator.takeCoLocated at pkg/decision/phase1_allocator.go:184.
//
// Note: cross-bucket scoring re-walks the (claimed-filtered) bucket
// each call. The legacy allocator's coLocatedBuilt cache + per-bucket
// head cursors don't survive concurrency — every worker filters
// fresh via state.IsClaimed. For realistic-catalog scale (few-hundred-
// machine pools, ~3% Same-carrying Needs) this stays under the
// per-Need cost envelope.
func (p *Pool) FindSame(state *SharedState, st machine.State, deficit, minUnit []needs.ResourceQty, sameKey string) Candidates {
	if needs.IsZero(deficit) {
		return Candidates{}
	}

	type sameBucket struct {
		value    string
		machines []*machine.Machine
	}
	index := make(map[string]int)
	buckets := make([]sameBucket, 0)
	for i := range p.src {
		m := &p.src[i]
		if !matchProfile(p.profile, *m) {
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
			buckets = append(buckets, sameBucket{value: v})
		}
		buckets[idx].machines = append(buckets[idx].machines, m)
	}

	bestIdx := -1
	bestAtomic := false
	bestHeadPrice := 0.0
	bestAvail := -1
	for i := range buckets {
		b := &buckets[i]
		avail := 0
		var capacity []needs.ResourceQty
		headPrice := 0.0
		headSet := false
		for _, m := range b.machines {
			if state.IsClaimed(m.ID) {
				continue
			}
			alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
			if !needs.Covers(alloc, minUnit) {
				continue
			}
			if !headSet {
				headPrice = m.PricePerHour
				headSet = true
			}
			avail++
			capacity = needs.AddResources(capacity, alloc)
		}
		if avail == 0 {
			continue
		}
		atomic := needs.Covers(capacity, deficit)
		better := false
		switch {
		case bestIdx < 0:
			better = true
		case atomic && !bestAtomic:
			better = true
		case atomic == bestAtomic:
			bestKey := buckets[bestIdx].value
			if atomic {
				switch {
				case headPrice < bestHeadPrice:
					better = true
				case headPrice == bestHeadPrice && b.value < bestKey:
					better = true
				}
			} else {
				switch {
				case avail > bestAvail:
					better = true
				case avail == bestAvail && headPrice < bestHeadPrice:
					better = true
				case avail == bestAvail && headPrice == bestHeadPrice && b.value < bestKey:
					better = true
				}
			}
		}
		if better {
			bestIdx = i
			bestAtomic = atomic
			bestHeadPrice = headPrice
			bestAvail = avail
		}
	}
	if bestIdx < 0 {
		return Candidates{}
	}

	best := &buckets[bestIdx]
	out := make([]machine.ID, 0, 4)
	remaining := deficit
	for _, m := range best.machines {
		if state.IsClaimed(m.ID) {
			continue
		}
		alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
		if !needs.Covers(alloc, minUnit) {
			continue
		}
		out = append(out, m.ID)
		remaining = needs.SubResources(remaining, alloc)
		if needs.IsZero(remaining) {
			break
		}
	}
	return Candidates{
		Machines: out,
		Bucket: BucketKey{
			State:     st,
			ProfileFP: p.profile.Fingerprint(),
			SameKey:   sameKey,
			SameValue: best.value,
		},
	}
}

// FindSpread returns candidates honouring a DoNotSchedule
// TopologySpread: bucket-pick counts never exceed (current min +
// maxSkew). Picks cheapest-head among eligible buckets at each step.
// Mirrors phase1Allocator.takeSpread at pkg/decision/phase1_allocator.go:370.
//
// The bucket key carries the topology key but an empty SameValue —
// Spread proposals touch machines across multiple topology domains,
// so per-value bucketing would mean multiple proposals; instead the
// broker conflict-grain is the (state, profile, topology-key)
// triple. Workers competing for Spread on the same key share one
// CAS line.
func (p *Pool) FindSpread(state *SharedState, st machine.State, deficit, minUnit []needs.ResourceQty, topoKey string, maxSkew int32) Candidates {
	if needs.IsZero(deficit) {
		return Candidates{}
	}
	skew := int(maxSkew)
	if skew < 1 {
		skew = 1
	}

	type spreadBucket struct {
		machines []*machine.Machine
		head     int
	}
	buckets := make(map[string]*spreadBucket)
	keys := make([]string, 0)
	for i := range p.src {
		m := &p.src[i]
		if state.IsClaimed(m.ID) {
			continue
		}
		if !matchProfile(p.profile, *m) {
			continue
		}
		if !needs.Covers(needs.ResourceQtysFromMap(m.EffectiveAllocatable()), minUnit) {
			continue
		}
		v, ok := lookupAttribute(topoKey, *m)
		if !ok {
			continue
		}
		b, exists := buckets[v]
		if !exists {
			b = &spreadBucket{}
			buckets[v] = b
			keys = append(keys, v)
		}
		b.machines = append(b.machines, m)
	}
	if len(keys) == 0 {
		return Candidates{}
	}

	counts := make(map[string]int, len(keys))
	remaining := deficit
	out := make([]machine.ID, 0, 4)
	for !needs.IsZero(remaining) {
		minCount := -1
		for _, k := range keys {
			c := counts[k]
			if minCount == -1 || c < minCount {
				minCount = c
			}
		}

		var bestKey string
		var bestPrice float64
		bestSet := false
		for _, k := range keys {
			b := buckets[k]
			if b.head >= len(b.machines) {
				continue
			}
			if counts[k] > minCount+skew-1 {
				continue
			}
			head := b.machines[b.head]
			if !bestSet ||
				head.PricePerHour < bestPrice ||
				(head.PricePerHour == bestPrice && k < bestKey) {
				bestSet = true
				bestKey = k
				bestPrice = head.PricePerHour
			}
		}
		if !bestSet {
			break
		}

		b := buckets[bestKey]
		m := b.machines[b.head]
		b.head++
		out = append(out, m.ID)
		counts[bestKey]++
		remaining = needs.SubResources(remaining, needs.ResourceQtysFromMap(m.EffectiveAllocatable()))
	}
	return Candidates{
		Machines: out,
		Bucket: BucketKey{
			State:     st,
			ProfileFP: p.profile.Fingerprint(),
			SameKey:   topoKey,
		},
	}
}

// SameRequirementKey returns the Same operator's key on the profile,
// if any. Used by callers to route between FindBasic and FindSame
// before constructing a proposal.
func SameRequirementKey(p needs.Profile) (string, bool) {
	for _, r := range p.Requirements() {
		if r.Operator == needs.OperatorSame {
			return r.Key, true
		}
	}
	return "", false
}

// StrictSpread returns the topology key + max skew of the first
// DoNotSchedule TopologySpread on the profile, if any. Callers that
// also see a Same key route to FindSame instead (Same is the stronger
// constraint).
func StrictSpread(p needs.Profile) (string, int32, bool) {
	for _, s := range p.Spread() {
		if s.WhenUnsatisfiable == needs.WhenUnsatisfiableDoNotSchedule {
			return s.TopologyKey, s.MaxSkew, true
		}
	}
	return "", 0, false
}
