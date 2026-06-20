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

// TestRunNowBatchProfiles_ValidV2 guards the two depth-slate profiles that
// run on the standing fleet next: reclaim-cycle-50k (the 50k twin of the
// reclaim drill, to land the bounded-reclaim gate on measured data) and
// 50k-2shard (the first 2-shard throughput run). Neither is in the
// ladder-only BYO matrix, so this pins their parse → validate → merge and
// the load-bearing fields (50k scaleDowns + generous bound; 2-shard
// topology).
func TestRunNowBatchProfiles_ValidV2(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	s, err := readSubstrate(filepath.Join(root, "test", "scaletest", "substrates", "example-mid-host.yaml"))
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}

	t.Run("reclaim-cycle-50k", func(t *testing.T) {
		t.Parallel()
		p, err := readProfileV2(filepath.Join(root, "test", "scaletest", "profiles", "reclaim-cycle-50k.yaml"))
		if err != nil {
			t.Fatalf("readProfileV2: %v", err)
		}
		if err := p.validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		if _, err := merge(p, s); err != nil {
			t.Fatalf("merge: %v", err)
		}
		if p.Scale.Machines != 50000 {
			t.Errorf("scale.machines = %d (want 50000 — the 50k twin)", p.Scale.Machines)
		}
		if len(p.LoadProfile.ScaleDowns) != 1 || p.LoadProfile.ScaleDowns[0].TargetMultiplier != 0.5 {
			t.Errorf("scaleDowns = %+v (want one 0.5 shed)", p.LoadProfile.ScaleDowns)
		}
		// Inverted posture: reclaim is expected, bound is generous (10x the
		// uber-5k drill's 6000) so the drill doesn't fail for doing its job.
		if p.SLO.MaxReclaimActionsDuringSoak < 6000 {
			t.Errorf("maxReclaimActionsDuringSoak = %d (want a generous 50k bound)", p.SLO.MaxReclaimActionsDuringSoak)
		}
	})

	t.Run("50k-2shard", func(t *testing.T) {
		t.Parallel()
		p, err := readProfileV2(filepath.Join(root, "test", "scaletest", "profiles", "50k-2shard.yaml"))
		if err != nil {
			t.Fatalf("readProfileV2: %v", err)
		}
		if err := p.validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		if _, err := merge(p, s); err != nil {
			t.Fatalf("merge: %v", err)
		}
		if p.Topology.ShardReplicas != 2 {
			t.Errorf("topology.shardReplicas = %d (want 2)", p.Topology.ShardReplicas)
		}
		// Steady-state throughput run: no disturbance.
		if len(p.RunnerActions) != 0 {
			t.Errorf("runnerActions = %+v (want none — steady-state throughput)", p.RunnerActions)
		}
	})
}

// TestSpotChurnProfile_ValidV2 validates the spot/hardware-fault chaos
// profile parses → validates → merges and carries the elevated failure
// rate the new profileV2.Shard.FailureRatePerSec passthrough renders into
// the chart (the chaos test's whole point), plus the un-relaxed steady
// reclaim bound (the load-bearing assertion: Failed re-provision must not
// inflate Phase 3).
func TestSpotChurnProfile_ValidV2(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	s, err := readSubstrate(filepath.Join(root, "test", "scaletest", "substrates", "example-mid-host.yaml"))
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}
	p, err := readProfileV2(filepath.Join(root, "test", "scaletest", "profiles", "spot-churn-50k.yaml"))
	if err != nil {
		t.Fatalf("readProfileV2: %v", err)
	}
	if err := p.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if _, err := merge(p, s); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if p.Shard.FailureRatePerSec <= 0.0000001160 {
		t.Errorf("shard.failureRatePerSec = %v (want elevated above the ~1.16e-7 chart default)", p.Shard.FailureRatePerSec)
	}
	// The steady 50k reclaim bound must NOT be relaxed for this run — the
	// assertion is that Failed-recovery doesn't leak into Phase 3.
	if p.SLO.MaxReclaimActionsDuringSoak != 15000 {
		t.Errorf("maxReclaimActionsDuringSoak = %d (want the un-relaxed 50k steady 15000)", p.SLO.MaxReclaimActionsDuringSoak)
	}
}

// TestReclaimCycleProfile_ValidV2 validates the scale-down/reclaim drill
// profile loads through readProfileV2 → validate → merge and carries the
// scaleDowns demand-drop the load-driver consumes (the ladder-only BYO
// matrix test does not cover it). Also pins the generous reclaim bound
// (the inverted SLO posture: reclaim is expected here).
func TestReclaimCycleProfile_ValidV2(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	s, err := readSubstrate(filepath.Join(root, "test", "scaletest", "substrates", "example-mid-host.yaml"))
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}
	p, err := readProfileV2(filepath.Join(root, "test", "scaletest", "profiles", "reclaim-cycle.yaml"))
	if err != nil {
		t.Fatalf("readProfileV2: %v", err)
	}
	if err := p.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	cfg, err := merge(p, s)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if cfg.ClusterCount <= 0 {
		t.Errorf("merged ClusterCount = %d (want > 0)", cfg.ClusterCount)
	}
	if len(p.LoadProfile.ScaleDowns) != 1 {
		t.Fatalf("scaleDowns = %+v (want exactly one)", p.LoadProfile.ScaleDowns)
	}
	sd := p.LoadProfile.ScaleDowns[0]
	if sd.TargetMultiplier != 0.5 {
		t.Errorf("scaleDown TargetMultiplier = %v (want 0.5)", sd.TargetMultiplier)
	}
	if sd.AtSeconds <= 0 {
		t.Errorf("scaleDown AtSeconds = %d (want > 0)", sd.AtSeconds)
	}
	// The inverted SLO posture: reclaim is expected, so the bound must be
	// generous (non-zero), unlike the steady-state ~0 guard.
	if p.SLO.MaxReclaimActionsDuringSoak <= 0 {
		t.Errorf("maxReclaimActionsDuringSoak = %d (want > 0 — reclaim is expected in this drill)", p.SLO.MaxReclaimActionsDuringSoak)
	}
}
