// Package preflight is the validation ladder's rung 0.5: static
// matching-capacity arithmetic over a scaletest profile, catching
// seed-shape vs demand-shape mismatches in milliseconds instead of a
// burned ramp budget on kind (10m) or a cloud host (60m).
//
// Born from the 2026-06-11 dev-50 incident: the no-catalog legacy
// demand is single-shape (every Pod nvidia.com/gpu=8 on a3-highgpu-8g)
// while the no-catalog seed rotates five instance types, so only ⅕ of
// the seeded pool could ever host demand — 4,800 matching Pod slots
// against a 4,950 bind gate, a stall no soak duration could fix. The
// arithmetic was knowable from the profile alone.
//
// The package is also the single source of truth for the two shape
// tables that made the incident possible by living in two package
// mains where nothing could cross-check them: the no-catalog seed
// rotation (formerly cmd/bigfleet/shard.go) and the legacy demand
// shape (formerly the load-driver's buildLegacyPodTemplate constants).
// cmd/bigfleet and the load-driver now import them from here, so the
// check and the behaviour it models cannot drift.
//
// Scope honesty: this is a SHORTFALL DETECTOR, not a green-guarantee.
// It models neither bind throughput, preemption, gang/rack packing,
// nor spread constraints — a profile can pass preflight and still fail
// for those reasons. It only proves the converse: when matching
// capacity < the bind gate, the run cannot pass, so don't start it.
package preflight

import (
	"fmt"
)

// LegacyInstanceTypes is the no-catalog seed rotation: without an
// archetype catalog, seedFakeInventory assigns Idle and Speculative
// machine i the instance type LegacyInstanceTypes[i % 5]. Order
// matters — LegacyDemandInstanceType must stay index 0 so the
// matching-share arithmetic (ceil(n/5)) holds.
var LegacyInstanceTypes = []string{
	"a3-highgpu-8g", "m6i.large", "c6i.4xlarge", "n2-standard-32", "r6i.xlarge",
}

// LegacyResources is each rotation type's per-replica resource map
// (Allocatable = density × this, per ADR-0022).
var LegacyResources = map[string]map[string]string{
	"a3-highgpu-8g":  {"nvidia.com/gpu": "8"},
	"m6i.large":      {"cpu": "2", "memory": "8Gi"},
	"c6i.4xlarge":    {"cpu": "16", "memory": "32Gi"},
	"n2-standard-32": {"cpu": "32", "memory": "128Gi"},
	"r6i.xlarge":     {"cpu": "4", "memory": "32Gi"},
}

// LegacyDemandInstanceType / LegacyDemandResources are the load-
// driver's no-catalog Pod template: every Pod requests exactly these
// resources with a required nodeAffinity on this instance type. The
// Configured seed and the legacy CR profile hardcode the same type so
// supply and demand meet (see the chart values' seedConfiguredPerCluster
// comment).
const LegacyDemandInstanceType = "a3-highgpu-8g"

// LegacyDemandResources returns a fresh copy of the legacy per-Pod
// request map (callers mutate resource lists).
func LegacyDemandResources() map[string]string {
	return map[string]string{"nvidia.com/gpu": "8"}
}

// LegacySeed is the seed configuration of a no-catalog profile — the
// knobs that determine matching capacity.
type LegacySeed struct {
	Machines             int // shard.seedMachines (Idle tier, rotated)
	Speculative          int // shard.seedSpeculative (rotated)
	ConfiguredPerCluster int // shard.seedConfiguredPerCluster (all demand-typed)
	Density              int // shard.seedDensityMultiplier; ≤0 → 1
	Clusters             int // kwok.clusterCount
	TargetPerCluster     int // loadProfile.target
}

// matchingFromRotation counts how many of n rotated machines carry the
// legacy demand type: it is index 0 of the rotation, so ceil(n/w).
func matchingFromRotation(n int) int {
	w := len(LegacyInstanceTypes)
	return (n + w - 1) / w
}

// MatchingSlots is the number of Pod slots the seed can ever offer the
// legacy single-shape demand: demand-typed machines × density. Only
// ⅕ of the rotated Idle/Speculative tiers match; the Configured seed
// is entirely demand-typed.
func (s LegacySeed) MatchingSlots() int {
	density := s.Density
	if density <= 0 {
		density = 1
	}
	machines := matchingFromRotation(s.Machines) +
		matchingFromRotation(s.Speculative) +
		s.ConfiguredPerCluster*s.Clusters
	return machines * density
}

// BindGate is the runner's chain-alive threshold: 99 % of total
// target Pods (waitForSteadyState's chainAliveThreshold).
func (s LegacySeed) BindGate() int {
	return int(0.99 * float64(s.Clusters*s.TargetPerCluster))
}

// Check returns an error when the profile's bind gate is provably
// unreachable: matching capacity below the gate means the fill
// plateaus at MatchingSlots and the ramp budget burns to no purpose.
func (s LegacySeed) Check() error {
	slots, gate := s.MatchingSlots(), s.BindGate()
	if slots >= gate {
		return nil
	}
	return fmt.Errorf(
		"preflight: matching capacity %d Pod slots < bind gate %d (demand %d Pods of %s; rotation matches ⅕ of seedMachines=%d and seedSpeculative=%d → %d machines, +%d Configured/cluster × %d clusters, × density %d) — the fill will plateau at %d; raise seedSpeculative to ≥ %d or add a catalog",
		slots, gate, s.Clusters*s.TargetPerCluster, LegacyDemandInstanceType,
		s.Machines, s.Speculative,
		matchingFromRotation(s.Machines)+matchingFromRotation(s.Speculative),
		s.ConfiguredPerCluster, s.Clusters, max(1, s.Density), slots,
		s.speculativeFor(gate),
	)
}

// speculativeFor inverts MatchingSlots for the error message's
// suggestion: the smallest seedSpeculative whose ⅕ share closes the
// gap to the gate.
func (s LegacySeed) speculativeFor(gate int) int {
	density := s.Density
	if density <= 0 {
		density = 1
	}
	have := (matchingFromRotation(s.Machines) + s.ConfiguredPerCluster*s.Clusters) * density
	missing := gate - have
	if missing <= 0 {
		return 0
	}
	machinesNeeded := (missing + density - 1) / density
	return machinesNeeded * len(LegacyInstanceTypes)
}
