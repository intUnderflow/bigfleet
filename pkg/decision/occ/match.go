package occ

import (
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// matchProfile mirrors decision.MatchProfile. Duplicated here so the
// occ package has no import-cycle dependency on pkg/decision. The
// two implementations must stay aligned — if Profile semantics
// change, update both.
func matchProfile(p needs.Profile, m machine.Machine) bool {
	for _, r := range p.RequirementsRO() {
		if !matchRequirement(r, m) {
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
		// Same on a single machine is satisfied by any machine with
		// a value for the key (Values, if non-empty, constrains the
		// permissible values). The group constraint is enforced by
		// the bucketing logic in candidates.go.
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
	return false
}

func lookupAttribute(key string, m machine.Machine) (string, bool) {
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
