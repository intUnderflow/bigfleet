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
		profile     string
		minActions  int
		firstAction string
		wantShards  int // expected topology.shardReplicas (0 = size-derived single shard)
	}{
		{"failover-leader-kill", 1, "kill-coordinator-leader", 0},
		{"failover-shard-kill", 1, "kill-shard-bigfleet-shard-1", 2},
		{"failover-partition", 1, "partition-coordinator-from-shard-bigfleet-shard-1", 2},
		{"failover-soak", 3, "kill-coordinator-leader", 2},
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
			if len(p.RunnerActions) < tc.minActions {
				t.Fatalf("%s runnerActions = %+v (want ≥ %d)", tc.profile, p.RunnerActions, tc.minActions)
			}
			if p.RunnerActions[0].Action != tc.firstAction {
				t.Errorf("%s first runnerAction = %q (want %q)", tc.profile, p.RunnerActions[0].Action, tc.firstAction)
			}
			for i, a := range p.RunnerActions {
				if a.AtSeconds <= 0 {
					t.Errorf("%s runnerAction[%d] AtSeconds = %d (want > 0)", tc.profile, i, a.AtSeconds)
				}
			}
			if p.Topology.ShardReplicas != tc.wantShards {
				t.Errorf("%s topology.shardReplicas = %d (want %d)", tc.profile, p.Topology.ShardReplicas, tc.wantShards)
			}
		})
	}
}
