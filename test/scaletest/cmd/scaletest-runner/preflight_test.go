package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/scaletest/preflight"
)

// TestCommittedProfiles_MatchingCapacityPreflight is the validation
// ladder's rung 0.5 over the committed profiles: every no-catalog
// profile the local gate actually runs must have its bind gate
// arithmetically reachable. The 2026-06-11 dev-50 incident — 4,800
// matching Pod slots against a 4,950 gate, three 10-minute stalls
// before the arithmetic was done by hand — is the test's reason to
// exist; it fails in milliseconds with the same arithmetic in the
// message.
//
// Profiles outside the gated set get the arithmetic LOGGED, not
// asserted, each with the reason it isn't gated. Shrinking the skip
// list is part of M50.7 (legacy profile deletion) and M59 (dev-50 →
// profileV2).
func TestCommittedProfiles_MatchingCapacityPreflight(t *testing.T) {
	const dir = "../../profiles"

	// gated: the runner executes these no-catalog, locally, as a gate —
	// their arithmetic MUST pass. dev-50 is back in the set: its V2
	// successor (dev-50-v2.yaml, catalog-driven, arithmetic not
	// applicable) is parked behind the consumed-capacity engine
	// investigation, so the legacy single-shape profile remains the
	// gate the devpods run.
	gated := map[string]bool{
		"dev-50.yaml": true,
	}
	// skipReasons documents why the rest are observed, not gated.
	skipReasons := map[string]string{
		"dev-500.yaml": "kept for occasional manual checks, not a gate; both kine-bound and (recorded here) supply-short on the single-shape arithmetic",
		"uber-":        "legacy files pending M50.7 deletion; the operating runs use an injected archetype catalog, so single-shape arithmetic does not describe them",
		"scaleway-":    "legacy files pending M50.7 deletion; no longer run (uber substrate only)",
		"failover-":    "failover drills exercise control-plane recovery, not the bind gate",
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read profiles dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		name := e.Name()
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		seed, catalogDriven, err := parsePreflightSeed(raw)
		if err != nil {
			t.Errorf("%s: parse: %v", name, err)
			continue
		}
		if catalogDriven {
			t.Logf("%s: catalog-driven — single-shape arithmetic not applicable", name)
			continue
		}
		checkErr := seed.Check()
		if gated[name] {
			if checkErr != nil {
				t.Errorf("%s (gated): %v", name, checkErr)
			} else {
				t.Logf("%s (gated): matching capacity %d ≥ bind gate %d", name, seed.MatchingSlots(), seed.BindGate())
			}
			continue
		}
		reason := "no reason recorded — add one to skipReasons or gate it"
		for prefix, r := range skipReasons {
			if strings.HasPrefix(name, prefix) || name == prefix {
				reason = r
				break
			}
		}
		if checkErr != nil {
			t.Logf("%s (observed, not gated — %s): %v", name, reason, checkErr)
		} else {
			t.Logf("%s (observed, not gated — %s): matching capacity %d ≥ bind gate %d", name, reason, seed.MatchingSlots(), seed.BindGate())
		}
	}
}

// TestLegacySeed_DevFiftyIncidentArithmetic pins the incident numbers
// exactly: the pre-fix dev-50 seed (60/180) yields 4,800 slots against
// the 4,950 gate and must fail; the fixed seed (60/240) yields 6,000
// and must pass. If the rotation tables or the math drift, this
// catches it without reading any profile file.
func TestLegacySeed_DevFiftyIncidentArithmetic(t *testing.T) {
	broken := preflight.LegacySeed{
		Machines: 60, Speculative: 180, Density: 100,
		Clusters: 2, TargetPerCluster: 2500,
	}
	if got := broken.MatchingSlots(); got != 4800 {
		t.Errorf("pre-fix slots = %d, want 4800", got)
	}
	if got := broken.BindGate(); got != 4950 {
		t.Errorf("gate = %d, want 4950", got)
	}
	if err := broken.Check(); err == nil {
		t.Error("pre-fix seed must fail preflight")
	}
	fixed := broken
	fixed.Speculative = 240
	if got := fixed.MatchingSlots(); got != 6000 {
		t.Errorf("fixed slots = %d, want 6000", got)
	}
	if err := fixed.Check(); err != nil {
		t.Errorf("fixed seed must pass preflight: %v", err)
	}
}
