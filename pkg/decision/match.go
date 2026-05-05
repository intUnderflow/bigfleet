package decision

import (
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// MatchProfile reports whether the candidate machine satisfies the
// requirements of the given need profile. Used by Phase 1 to filter
// idle / speculative inventory and by Phase 2 to filter preemption
// candidates.
//
// The implementation honours the standard set of node-selector operators
// (In, NotIn, Exists, DoesNotExist) plus the protobuf-only Same operator
// (which the cluster operator translates into during roll-up). Same
// constrains a *group* of machines to share a value; the per-machine
// match here is satisfied by any machine that has a value for the key
// and (if Values is non-empty) a value drawn from the listed values.
//
// Match also enforces that the machine's resources cover the need's
// resources by exact-string equality. This is intentionally cheap; the
// cluster operator is responsible for sending canonicalised quantity
// strings.
func MatchProfile(p needs.Profile, m machine.Machine) bool {
	// RO accessors — the per-call defensive-copy alloc was the
	// dominant cost at 450K Phase 3 candidates per cycle (M30.2).
	for _, r := range p.RequirementsRO() {
		if !matchRequirement(r, m) {
			return false
		}
	}
	for _, want := range p.ResourcesRO() {
		got, ok := m.Profile.Resources[want.Name]
		if !ok || got != want.Quantity {
			return false
		}
	}
	return true
}

func matchRequirement(r needs.Requirement, m machine.Machine) bool {
	val, has := lookupAttribute(r.Key, m)
	switch r.Operator {
	case needs.OperatorIn:
		if !has {
			return false
		}
		for _, v := range r.Values {
			if v == val {
				return true
			}
		}
		return false
	case needs.OperatorNotIn:
		if !has {
			return true
		}
		for _, v := range r.Values {
			if v == val {
				return false
			}
		}
		return true
	case needs.OperatorExists:
		return has
	case needs.OperatorDoesNotExist:
		return !has
	case needs.OperatorSame:
		// Per-machine match: any machine with a value for the key (and,
		// if Values is non-empty, a value drawn from the list) is a
		// candidate. Group-wide co-location is enforced by the autoscaler
		// when picking nodes — not at this match step.
		if !has {
			return false
		}
		if len(r.Values) == 0 {
			return true
		}
		for _, v := range r.Values {
			if v == val {
				return true
			}
		}
		return false
	}
	// Unspecified operator: be conservative and reject.
	return false
}

func lookupAttribute(key string, m machine.Machine) (string, bool) {
	// A few well-known attributes are projected from the machine's
	// profile rather than read from labels, so a provider that doesn't
	// duplicate them in labels still matches correctly.
	switch key {
	case "node.kubernetes.io/instance-type":
		if m.Profile.InstanceType != "" {
			return m.Profile.InstanceType, true
		}
	case "topology.kubernetes.io/zone":
		if m.Profile.Zone != "" {
			return m.Profile.Zone, true
		}
	}
	if v, ok := m.Profile.Labels[key]; ok {
		return v, true
	}
	return "", false
}
