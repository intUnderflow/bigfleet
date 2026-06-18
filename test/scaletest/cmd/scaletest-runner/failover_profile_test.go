package main

import (
	"path/filepath"
	"testing"
)

// TestFailoverProfiles_ValidV2 validates that the BYO-ported failover
// profiles load through the real readProfileV2 → validate → merge path and
// carry a well-formed runnerAction. The ladder-only TestBYO_ProfileSubstrateMatrix
// does not cover the failover-*.yaml profiles, so this is their guard against
// a malformed port or a future regression of the V2 runnerActions plumbing.
//
// Only profiles already on the BYO contract are listed; legacy-format
// failover profiles are added here as they are ported.
func TestFailoverProfiles_ValidV2(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	subPath := filepath.Join(root, "test", "scaletest", "substrates", "example-mid-host.yaml")
	s, err := readSubstrate(subPath)
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}

	cases := []struct {
		profile string
		action  string
	}{
		{"failover-leader-kill", "kill-coordinator-leader"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.profile, func(t *testing.T) {
			t.Parallel()
			p, err := readProfileV2(filepath.Join(root, "test", "scaletest", "profiles", tc.profile+".yaml"))
			if err != nil {
				t.Fatalf("readProfileV2 %s: %v", tc.profile, err)
			}
			if err := p.validate(); err != nil {
				t.Fatalf("%s validate: %v", tc.profile, err)
			}
			cfg, err := merge(p, s)
			if err != nil {
				t.Fatalf("%s merge: %v", tc.profile, err)
			}
			if cfg.ClusterCount <= 0 {
				t.Errorf("%s merged ClusterCount = %d (want > 0)", tc.profile, cfg.ClusterCount)
			}
			if len(p.RunnerActions) != 1 || p.RunnerActions[0].Action != tc.action {
				t.Errorf("%s runnerActions = %+v (want one %q)", tc.profile, p.RunnerActions, tc.action)
			}
			if p.RunnerActions[0].AtSeconds <= 0 {
				t.Errorf("%s runnerAction AtSeconds = %d (want > 0)", tc.profile, p.RunnerActions[0].AtSeconds)
			}
		})
	}
}
