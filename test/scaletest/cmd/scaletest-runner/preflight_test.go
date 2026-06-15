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
// asserted, each with the reason it isn't gated. The skip list (and
// this whole rung) shrinks to nothing with M77b's legacy-demand-mode
// deletion; until then the test keeps the observed arithmetic visible
// for the legacy files that still exist.
func TestCommittedProfiles_MatchingCapacityPreflight(t *testing.T) {
	const dir = "../../profiles"

	// gated: the runner executes these no-catalog, locally, as a gate —
	// their arithmetic MUST pass. Empty since M77a: dev-50 became the
	// catalog-driven V2 profile (the M67 / ADR-0045 engine fix unparked
	// it), and a catalog-driven seed draws machine shapes from the same
	// catalog as its demand, so the single-shape arithmetic doesn't
	// apply. No remaining no-catalog profile is run locally as a gate.
	// The map, this test, and pkg/scaletest/preflight survive until
	// M77b deletes the legacy demand mode along with the profiles in
	// skipReasons below.
	gated := map[string]bool{}
	// skipReasons documents why the rest are observed, not gated.
	skipReasons := map[string]string{
		"dev-500.yaml": "kept for occasional manual checks, not a gate; both kine-bound and (recorded here) supply-short on the single-shape arithmetic",
		// M50.7: the legacy V1 uber-*/scaleway-* profiles are deleted; the
		// canonical realism profiles are the V2 {5k,50k,…}.yaml + a
		// substrate. No exemption needed for files that no longer exist.
		"failover-": "failover drills exercise control-plane recovery, not the bind gate",
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
