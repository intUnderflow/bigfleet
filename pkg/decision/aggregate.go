package decision

import (
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// profileResourcesToMap converts a Profile's resource slice (the canonical
// shape inside needs.Profile) to the map shape the aggregate-math helpers
// take. Allocated once per call site; the helpers themselves are
// allocation-free.
func profileResourcesToMap(qs []needs.ResourceQty) map[string]string {
	if len(qs) == 0 {
		return nil
	}
	out := make(map[string]string, len(qs))
	for _, q := range qs {
		out[q.Name] = q.Quantity
	}
	return out
}

// PodsPerMachine returns how many replicas of perReplica resource shape
// fit on a single machine with the given allocatable resources.
//
// Math is bottleneck-dimension floor-division: for each resource key in
// perReplica, density_dim = floor(allocatable[k] / perReplica[k]); the
// machine's actual capacity is min(density_dim) across all dimensions in
// perReplica. If any dimension is missing from allocatable, or if any
// perReplica quantity is zero/unparseable, the function returns 0 (the
// machine doesn't fit any replicas of this shape).
//
// For pre-ADR-0022 inventory where perReplica == allocatable, this
// returns 1 — preserving the historical 1 Pod = 1 machine math.
//
// ADR-0022 / M45.1: this is the primitive Phase 1 uses to translate
// aggregate Pod demand into machine count.
func PodsPerMachine(perReplica, allocatable map[string]string) int {
	if len(perReplica) == 0 {
		return 0
	}
	minDensity := -1
	for k, perReplicaStr := range perReplica {
		allocStr, ok := allocatable[k]
		if !ok {
			return 0
		}
		perReplicaQty, err := resource.ParseQuantity(perReplicaStr)
		if err != nil {
			return 0
		}
		allocQty, err := resource.ParseQuantity(allocStr)
		if err != nil {
			return 0
		}
		perReplicaVal := perReplicaQty.MilliValue()
		allocVal := allocQty.MilliValue()
		if perReplicaVal <= 0 {
			return 0
		}
		density := int(allocVal / perReplicaVal)
		if minDensity == -1 || density < minDensity {
			minDensity = density
		}
	}
	if minDensity < 0 {
		return 0
	}
	return minDensity
}

// MachinesForAggregate returns how many machines of shape allocatable are
// needed to host podCount replicas of perReplica shape.
//
//	machines = ceil(podCount / PodsPerMachine(perReplica, allocatable))
//
// Returns 0 if podCount <= 0. Returns podCount (one machine per pod) if
// no machine can fit a single replica — that's the conservative fallback
// the pre-ADR-0022 code implicitly performed.
//
// ADR-0022 / M45.1: bridges aggregate Pod demand to a machine emit count
// at Phase 1 / Phase 3 boundaries.
func MachinesForAggregate(perReplica, allocatable map[string]string, podCount int) int {
	if podCount <= 0 {
		return 0
	}
	density := PodsPerMachine(perReplica, allocatable)
	if density <= 0 {
		// No replicas fit per machine — fall back to 1:1. Phase 1 will
		// still emit Bootstraps in this case; if the chosen machine
		// genuinely doesn't fit even one replica that's a configuration
		// bug worth surfacing as a per-Pod machine, not silently dropping
		// to zero machines.
		return podCount
	}
	return (podCount + density - 1) / density
}
