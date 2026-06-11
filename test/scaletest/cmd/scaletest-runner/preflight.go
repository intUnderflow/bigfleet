package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/intUnderflow/bigfleet/pkg/scaletest/preflight"
)

// preflightProfile is the generic parse of the preflight-relevant
// knobs. The legacy profileFile struct deliberately parses only what
// the runner's own flow needs; the preflight needs the seed knobs the
// legacy path otherwise passes to helm verbatim.
type preflightProfile struct {
	APIVersion string `yaml:"apiVersion"` // non-empty → profileV2 (catalog injected by the runner)
	KWOK       struct {
		ClusterCount int `yaml:"clusterCount"`
	} `yaml:"kwok"`
	Shard struct {
		SeedMachines             int `yaml:"seedMachines"`
		SeedSpeculative          int `yaml:"seedSpeculative"`
		SeedConfiguredPerCluster int `yaml:"seedConfiguredPerCluster"`
		SeedDensityMultiplier    int `yaml:"seedDensityMultiplier"`
	} `yaml:"shard"`
	LoadProfile struct {
		Target     int   `yaml:"target"`
		Archetypes []any `yaml:"archetypes"`
	} `yaml:"loadProfile"`
}

// parsePreflightSeed extracts the matching-capacity inputs from a raw
// profile. The bool reports whether the profile is catalog-driven
// (inline archetypes or profileV2) — out of the single-shape
// arithmetic's scope, because a catalog-driven seed draws machine
// shapes from the same catalog as its demand (M42).
func parsePreflightSeed(raw []byte) (preflight.LegacySeed, bool, error) {
	var p preflightProfile
	if err := yaml.Unmarshal(raw, &p); err != nil {
		return preflight.LegacySeed{}, false, err
	}
	catalogDriven := p.APIVersion != "" || len(p.LoadProfile.Archetypes) > 0
	return preflight.LegacySeed{
		Machines:             p.Shard.SeedMachines,
		Speculative:          p.Shard.SeedSpeculative,
		ConfiguredPerCluster: p.Shard.SeedConfiguredPerCluster,
		Density:              p.Shard.SeedDensityMultiplier,
		Clusters:             p.KWOK.ClusterCount,
		TargetPerCluster:     p.LoadProfile.Target,
	}, catalogDriven, nil
}

// legacyPreflight is the runner-side rung 0.5 (M60): refuse to install
// a no-catalog profile whose bind gate is arithmetically unreachable.
// Catalog-driven profiles pass through — their shape matching is by
// construction, and per-archetype share noise is a different (softer)
// problem than a hard single-shape ceiling.
func legacyPreflight(profilePath string) error {
	raw, err := os.ReadFile(profilePath)
	if err != nil {
		return fmt.Errorf("preflight: read profile: %w", err)
	}
	seed, catalogDriven, err := parsePreflightSeed(raw)
	if err != nil {
		return fmt.Errorf("preflight: parse profile: %w", err)
	}
	if catalogDriven {
		return nil
	}
	// A profile with no parsed seed knobs is shaped differently (the
	// failover drills predate the seed schema) — observe, don't block.
	if seed.Machines+seed.Speculative+seed.ConfiguredPerCluster == 0 ||
		seed.Clusters == 0 || seed.TargetPerCluster == 0 {
		fmt.Fprintln(os.Stderr, "preflight: profile has no recognisable seed/demand knobs — skipping matching-capacity check")
		return nil
	}
	if err := seed.Check(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "preflight: matching capacity %d Pod slots ≥ bind gate %d\n",
		seed.MatchingSlots(), seed.BindGate())
	return nil
}
