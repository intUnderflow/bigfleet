package shard

import (
	"sort"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// Summary is a coarse-grained snapshot of the shard's inventory. Built
// for reporting up to the coordinator on each ReportShard cycle. Bounded
// in size; not the per-machine inventory.
type Summary struct {
	TotalMachines      int32
	FreeMachines       int32
	InstanceTypeCounts map[string]int32
	ZoneCounts         map[string]int32
	UtilCPUFraction    float64
	UtilMemoryFraction float64
}

// Summary returns a coarse-grained view of the shard's current
// inventory. Safe to call from any goroutine; takes a Snapshot() under
// the inventory's RWMutex.
func (s *Shard) Summary() Summary {
	snap := s.inv.Snapshot()
	out := Summary{
		TotalMachines:      int32(snap.Len()),
		InstanceTypeCounts: make(map[string]int32),
		ZoneCounts:         make(map[string]int32),
	}
	for _, m := range snap.All() {
		switch m.State {
		case machine.StateIdle, machine.StateSpeculative:
			out.FreeMachines++
		}
		if m.Profile.InstanceType != "" {
			out.InstanceTypeCounts[m.Profile.InstanceType]++
		}
		if m.Profile.Zone != "" {
			out.ZoneCounts[m.Profile.Zone]++
		}
	}
	// Utilisation hints are placeholder values until real metrics are
	// wired (M11 production-readiness). The coordinator doesn't
	// interpret precise values; soft state for reporting.
	return out
}

// Shortfall is one unresolved need the shard couldn't satisfy from its
// own inventory and couldn't resolve via in-shard preemption. Reported
// to the coordinator for cross-shard rebalancing.
//
// ADR-0027: Deficit is the residual aggregate resource demand — a
// resource vector, not a machine count.
type Shortfall struct {
	Profile                   needs.Profile
	Deficit                   []needs.ResourceQty
	AgeCycles                 int
	InterruptionPenaltyBucket needs.PenaltyBucket
}

// ShortfallCount returns the number of unresolved-need entries
// currently tracked. O(1) — does not allocate. Use this for telemetry
// gauges where the full Shortfalls() build is wasted work.
func (s *Shard) ShortfallCount() int {
	s.shortfallMu.Lock()
	defer s.shortfallMu.Unlock()
	return len(s.shortfalls)
}

// Shortfalls returns the current set of unresolved needs, sorted by
// priority desc and bounded to the top maxPerReport entries (matches
// paper §3.5 / shortfall protocol bound of 100 per shard).
func (s *Shard) Shortfalls() []Shortfall {
	const maxPerReport = 100
	s.shortfallMu.Lock()
	defer s.shortfallMu.Unlock()
	out := make([]Shortfall, 0, len(s.shortfalls))
	for _, e := range s.shortfalls {
		out = append(out, Shortfall{
			Profile:                   e.Profile,
			Deficit:                   e.Deficit,
			AgeCycles:                 e.AgeCycles,
			InterruptionPenaltyBucket: e.InterruptionPenaltyBucket,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Profile.Priority() != out[j].Profile.Priority() {
			return out[i].Profile.Priority() > out[j].Profile.Priority()
		}
		// Tiebreak: older first so persistent shortfalls bubble up.
		return out[i].AgeCycles > out[j].AgeCycles
	})
	if len(out) > maxPerReport {
		out = out[:maxPerReport]
	}
	return out
}

// recordShortfalls is called by the cycle to update the shortfall
// tracker from Phase 2's unresolved set. Existing entries age by one
// cycle; entries no longer present are dropped (eventually-consistent —
// shortfalls that resolve naturally just disappear).
func (s *Shard) recordShortfalls(unresolved []shortfallSeed) {
	s.shortfallMu.Lock()
	defer s.shortfallMu.Unlock()

	seen := make(map[string]struct{}, len(unresolved))
	for _, u := range unresolved {
		fp := u.Profile.Fingerprint()
		seen[fp] = struct{}{}
		if existing, ok := s.shortfalls[fp]; ok {
			existing.Deficit = u.Deficit
			existing.AgeCycles++
			continue
		}
		s.shortfalls[fp] = &shortfallEntry{
			Profile:                   u.Profile,
			Deficit:                   u.Deficit,
			AgeCycles:                 1,
			InterruptionPenaltyBucket: u.Profile.InterruptionPenaltyBucket(),
		}
	}
	// Drop entries that have aged out (no longer present in this cycle).
	for fp := range s.shortfalls {
		if _, still := seen[fp]; !still {
			delete(s.shortfalls, fp)
		}
	}
}

// shortfallSeed is the per-cycle input to recordShortfalls. Decoupled
// from decision.UnsatisfiedNeed so this file doesn't import
// pkg/decision (keeps the cycle helper portable).
type shortfallSeed struct {
	Profile needs.Profile
	Deficit []needs.ResourceQty
}

// AssignDomain marks a topology domain as owned by this shard. Called
// from the coordinator-instruction handler in pkg/shard/coordclient.
// Idempotent.
func (s *Shard) AssignDomain(key, value string) {
	s.domainsMu.Lock()
	defer s.domainsMu.Unlock()
	s.assignedDomains[domainKey{Key: key, Value: value}] = struct{}{}
}

// UnassignDomain releases a previously-assigned domain. Idempotent.
func (s *Shard) UnassignDomain(key, value string) {
	s.domainsMu.Lock()
	defer s.domainsMu.Unlock()
	delete(s.assignedDomains, domainKey{Key: key, Value: value})
}

// AssignedDomains returns a snapshot of the domains assigned to this
// shard. Empty list means "all domains" (dev / single-shard mode).
func (s *Shard) AssignedDomains() []DomainAssignment {
	s.domainsMu.Lock()
	defer s.domainsMu.Unlock()
	out := make([]DomainAssignment, 0, len(s.assignedDomains))
	for d := range s.assignedDomains {
		out = append(out, DomainAssignment{Key: d.Key, Value: d.Value})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Value < out[j].Value
	})
	return out
}

// DomainAssignment is the public form of a domain ownership record.
type DomainAssignment struct {
	Key   string
	Value string
}
