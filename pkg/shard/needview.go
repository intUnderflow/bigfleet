package shard

import (
	"sort"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// NeedUnmetReason classifies why a Need is unmet (ADR-0061). The satisfied
// case is NeedReasonUnspecified.
type NeedUnmetReason int

const (
	NeedReasonUnspecified NeedUnmetReason = iota
	NeedReasonPriorityStarved
	NeedReasonNoMatchingSupply
	NeedReasonTopologyUnsatisfiable
	NeedReasonPreemptionExhausted
)

// NeedView is the shard's retained last-cycle verdict for one Need
// (ADR-0061). A TRIMMED projection: it carries the demand shape + the
// engine's verdict, but NOT the per-Need machine-ID lists (those approach
// the shard's whole claimed set across ~50K needs). Vectors are copied so
// the ledger never pins the cycle's demand slice.
type NeedView struct {
	ClusterID          machine.ClusterID
	Profile            needs.Profile
	AggregateResources []needs.ResourceQty
	MinUnit            []needs.ResourceQty
	ArrivalUnixNanos   int64
	Group              string

	Satisfied         bool
	Deficit           []needs.ResourceQty
	ClaimedCount      int
	BootstrapCount    int
	ProvisionCount    int
	SameDomain        string
	SameSatisfiable   bool
	AcquisitionParked bool
	AgeCyclesUnmet    int
	UnmetReason       NeedUnmetReason
}

// needViewSnapshot is one cycle's complete ledger, immutable after the
// build-then-swap in recordNeedViews. Indexed by cluster so the
// per-cluster read filter is O(1).
type needViewSnapshot struct {
	cycle      int64
	computedAt time.Time
	byCluster  map[machine.ClusterID][]NeedView
	total      int
}

// NeedViewResult is a read of the last-cycle needs ledger. Cycle == 0 with
// no views means no cycle has run yet (rebuilding), not "no demand".
type NeedViewResult struct {
	Cycle      int64
	ComputedAt time.Time
	Total      int // matching the cluster filter, before limit
	Views      []NeedView
}

// recordNeedViews captures the per-Need last-cycle verdict from Phase 1's
// per-Need verdicts and Phase 2's unresolved set (ADR-0061). Called once
// per cycle at the recordShortfalls capture point, where p1/p2 are live and
// the shortfall ledger (joined for the unmet age) is current. Builds the
// full snapshot then swaps the pointer under the write lock, so concurrent
// reads never wait on the O(needs) rebuild.
func (s *Shard) recordNeedViews(cycle int64, p1 decision.Phase1Result, p2 decision.Phase2Result) {
	type urKey struct {
		cluster machine.ClusterID
		fp      string
		group   string
	}
	// Phase 2's still-unresolved needs, keyed by aggregation identity, with
	// the Preempted discriminator (found victims but fell short).
	unresolved := make(map[urKey]bool, len(p2.Unresolved))
	for _, u := range p2.Unresolved {
		unresolved[urKey{u.Need.ClusterID, u.Need.Profile.Fingerprint(), u.Need.Group}] = u.Preempted
	}

	// Snapshot the per-fingerprint unmet age from the shortfall ledger
	// (already updated for this cycle by recordShortfalls).
	s.shortfallMu.Lock()
	ages := make(map[string]int, len(s.shortfalls))
	for fp, e := range s.shortfalls {
		ages[fp] = e.AgeCycles
	}
	s.shortfallMu.Unlock()

	byCluster := make(map[machine.ClusterID][]NeedView)
	total := 0
	for _, v := range p1.Verdicts {
		if v.Need == nil {
			continue
		}
		n := v.Need
		fp := n.Profile.Fingerprint()
		preempted, inUnresolved := unresolved[urKey{n.ClusterID, fp, n.Group}]

		nv := NeedView{
			ClusterID:          n.ClusterID,
			Profile:            n.Profile,
			AggregateResources: cloneQty(n.AggregateResources),
			MinUnit:            cloneQty(n.MinUnit),
			ArrivalUnixNanos:   n.ArrivalUnixNanos,
			Group:              n.Group,
			Satisfied:          v.Satisfied,
			ClaimedCount:       v.ClaimedCount,
			BootstrapCount:     v.BootstrapCount,
			ProvisionCount:     v.ProvisionCount,
			SameDomain:         v.SameDomain,
			SameSatisfiable:    v.SameSatisfiable,
			AcquisitionParked:  n.AcquisitionParked,
			UnmetReason:        deriveUnmetReason(v, inUnresolved, preempted),
		}
		if !v.Satisfied {
			nv.Deficit = cloneQty(v.Deficit)
			nv.AgeCyclesUnmet = ages[fp]
		}
		byCluster[n.ClusterID] = append(byCluster[n.ClusterID], nv)
		total++
	}

	snap := &needViewSnapshot{
		cycle:      cycle,
		computedAt: time.Now(),
		byCluster:  byCluster,
		total:      total,
	}
	s.needViewMu.Lock()
	s.needViews = snap
	s.needViewMu.Unlock()
}

// deriveUnmetReason classifies an unmet Need from the engine's signals
// (ADR-0061). inUnresolved/preempted come from Phase 2's unresolved set.
func deriveUnmetReason(v decision.NeedVerdict, inUnresolved, preempted bool) NeedUnmetReason {
	if v.Satisfied {
		return NeedReasonUnspecified
	}
	// A Same/co-location Need the engine couldn't structurally satisfy
	// within the shard (no domain covered it, none was choosable, or it has
	// aged into acquisition parking).
	if profileHasSame(v.Need.Profile) && (!v.SameSatisfiable || v.Need.AcquisitionParked || v.SameDomain == "") {
		return NeedReasonTopologyUnsatisfiable
	}
	// No machine of the right shape exists anywhere in the inventory.
	if !v.MatchingSupplyExists {
		return NeedReasonNoMatchingSupply
	}
	// Matching supply exists but is held above the cut-line. Phase 2 found
	// lower-precedence victims and freed some, but fell short.
	if inUnresolved && preempted {
		return NeedReasonPreemptionExhausted
	}
	// Otherwise the Need is below the cut-line with nothing displaceable to
	// take (or preemption is underway): priority-starved.
	return NeedReasonPriorityStarved
}

// profileHasSame reports whether the Profile carries a Same (co-location)
// requirement. Local check to avoid importing pkg/decision/occ here.
func profileHasSame(p needs.Profile) bool {
	for _, r := range p.RequirementsRO() {
		if r.Operator == needs.OperatorSame {
			return true
		}
	}
	return false
}

// NeedViews returns the shard's last-cycle per-Need verdicts (ADR-0061),
// optionally filtered to one cluster (empty = all) and capped at limit
// (0 = no cap). Views are sorted priority desc, then arrival asc, then
// cluster asc — the NeedsTable Snapshot order. Cycle == 0 with no views
// means no cycle has run yet.
func (s *Shard) NeedViews(cluster machine.ClusterID, limit int) NeedViewResult {
	s.needViewMu.RLock()
	snap := s.needViews
	s.needViewMu.RUnlock()
	if snap == nil {
		return NeedViewResult{}
	}

	var views []NeedView
	if cluster != "" {
		views = append(views, snap.byCluster[cluster]...)
	} else {
		for _, vs := range snap.byCluster {
			views = append(views, vs...)
		}
	}
	total := len(views)
	sort.Slice(views, func(i, j int) bool {
		pi, pj := views[i].Profile.Priority(), views[j].Profile.Priority()
		if pi != pj {
			return pi > pj
		}
		if views[i].ArrivalUnixNanos != views[j].ArrivalUnixNanos {
			return views[i].ArrivalUnixNanos < views[j].ArrivalUnixNanos
		}
		return views[i].ClusterID < views[j].ClusterID
	})
	if limit > 0 && len(views) > limit {
		views = views[:limit]
	}
	return NeedViewResult{
		Cycle:      snap.cycle,
		ComputedAt: snap.computedAt,
		Total:      total,
		Views:      views,
	}
}

func cloneQty(in []needs.ResourceQty) []needs.ResourceQty {
	if len(in) == 0 {
		return nil
	}
	out := make([]needs.ResourceQty, len(in))
	copy(out, in)
	return out
}
