package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// validProfileV2YAML is a substrate-agnostic test definition: 50K
// machines, density=100, realistic catalog, 30-min ramp + 30-min
// soak. Total Pods = 5M. The shape mirrors today's uber-50k /
// scaleway-50k pre-migration profiles minus the substrate-specific
// fields (clusterCount, kwokPod resources, cost).
const validProfileV2YAML = `apiVersion: bigfleet.io/scaletest/v1
kind: Profile
metadata:
  name: "50k"
  description: "50K machines, 5M aggregated Pods at density=100."
scale:
  machines: 50000
  density: 100
catalog:
  archetypes: realistic
seed:
  configuredFraction: 0.0
  speculativeMultiplier: 3
  idleHeadroomFraction: 0.2
loadProfile:
  rampSeconds: 1800
  soakSeconds: 1800
  churnPerMinute: 0.02
`

func TestProfileV2RoundTrip(t *testing.T) {
	t.Parallel()

	var p profileV2
	if err := yaml.Unmarshal([]byte(validProfileV2YAML), &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got, want := p.Metadata.Name, "50k"; got != want {
		t.Errorf("metadata.name = %q, want %q", got, want)
	}
	if got, want := p.Scale.Machines, 50000; got != want {
		t.Errorf("scale.machines = %d, want %d", got, want)
	}
	if got, want := p.Scale.Density, 100; got != want {
		t.Errorf("scale.density = %d, want %d", got, want)
	}
	if got, want := p.Catalog.Archetypes, "realistic"; got != want {
		t.Errorf("catalog.archetypes = %q, want %q", got, want)
	}
	if got, want := p.LoadProfile.RampSeconds, 1800; got != want {
		t.Errorf("loadProfile.rampSeconds = %d, want %d", got, want)
	}
	if err := p.validate(); err != nil {
		t.Errorf("validate(valid profile): %v", err)
	}
}

func TestReadProfileV2(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "profile.yaml")
	if err := os.WriteFile(path, []byte(validProfileV2YAML), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	p, err := readProfileV2(path)
	if err != nil {
		t.Fatalf("readProfileV2: %v", err)
	}
	if p.Metadata.Name != "50k" {
		t.Errorf("got name %q, want 50k", p.Metadata.Name)
	}
}

// TestProfileV2Validation covers the rejection paths on profileV2.validate().
func TestProfileV2Validation(t *testing.T) {
	t.Parallel()
	type mutator func(*profileV2)
	cases := []struct {
		name    string
		mutate  mutator
		wantErr string
	}{
		{
			name:    "machines zero",
			mutate:  func(p *profileV2) { p.Scale.Machines = 0 },
			wantErr: "scale.machines",
		},
		{
			name:    "density zero",
			mutate:  func(p *profileV2) { p.Scale.Density = 0 },
			wantErr: "scale.density",
		},
		{
			name:    "rampSeconds zero",
			mutate:  func(p *profileV2) { p.LoadProfile.RampSeconds = 0 },
			wantErr: "loadProfile.rampSeconds",
		},
		{
			name:    "soakSeconds negative",
			mutate:  func(p *profileV2) { p.LoadProfile.SoakSeconds = -1 },
			wantErr: "loadProfile.soakSeconds",
		},
		{
			name:    "negative churn",
			mutate:  func(p *profileV2) { p.LoadProfile.ChurnPerMinute = -0.5 },
			wantErr: "loadProfile.churnPerMinute",
		},
		{
			name:    "configuredFraction > 1",
			mutate:  func(p *profileV2) { p.Seed.ConfiguredFraction = 1.5 },
			wantErr: "seed.configuredFraction",
		},
		{
			name:    "negative idleHeadroom",
			mutate:  func(p *profileV2) { p.Seed.IdleHeadroomFraction = -0.1 },
			wantErr: "seed.idleHeadroomFraction",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var p profileV2
			if err := yaml.Unmarshal([]byte(validProfileV2YAML), &p); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			tc.mutate(&p)
			err := p.validate()
			if err == nil {
				t.Fatalf("expected validate() to fail with %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// fixtureProfile is a helper for merge tests.
func fixtureProfile(t *testing.T) profileV2 {
	t.Helper()
	var p profileV2
	if err := yaml.Unmarshal([]byte(validProfileV2YAML), &p); err != nil {
		t.Fatalf("fixture profile unmarshal: %v", err)
	}
	return p
}

// fixtureSubstrate is a helper for merge tests — pulls the canonical
// substrate fixture from the substrate_test.go file.
func fixtureSubstrate(t *testing.T) substrateFile {
	t.Helper()
	var s substrateFile
	if err := yaml.Unmarshal([]byte(validSubstrateYAML), &s); err != nil {
		t.Fatalf("fixture substrate unmarshal: %v", err)
	}
	return s
}

// TestMerge_50kOnFatHost covers the canonical case from ADR-0034: 50K
// machines × density 100 = 5M Pods, on the fat-host substrate (25K
// Pods/cluster, 10 clusters/host). Expected geometry: 200 clusters,
// 21 hosts (200/10 + 1 for system-under-test).
func TestMerge_50kOnFatHost(t *testing.T) {
	t.Parallel()
	p := fixtureProfile(t)
	s := fixtureSubstrate(t)
	cfg, err := merge(p, s)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	if got, want := cfg.TotalPods, 5_000_000; got != want {
		t.Errorf("TotalPods = %d, want %d", got, want)
	}
	if got, want := cfg.ClusterCount, 200; got != want {
		t.Errorf("ClusterCount = %d, want %d", got, want)
	}
	if got, want := cfg.HostsNeeded, 21; got != want {
		t.Errorf("HostsNeeded = %d, want %d (200 clusters / 10 per host + 1 SUT host)", got, want)
	}
	if got, want := cfg.ProfileName, "50k"; got != want {
		t.Errorf("ProfileName = %q, want %q", got, want)
	}
	if got, want := cfg.SubstrateName, "example-fat-host"; got != want {
		t.Errorf("SubstrateName = %q, want %q", got, want)
	}
	// Free substrate → zero cost.
	if cfg.EstimatedUSD != 0 {
		t.Errorf("EstimatedUSD = %g, want 0 (free substrate)", cfg.EstimatedUSD)
	}
	// 1800s ramp + 1800s soak + 600s teardown = 4200s = ~1.17h.
	if got := cfg.DurationHours; got < 1.16 || got > 1.18 {
		t.Errorf("DurationHours = %g, want ~1.17 (ramp+soak+teardown)", got)
	}
}

// TestMerge_RampFeasibility covers the demand vs supply comparison.
func TestMerge_RampFeasibility(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		machines       int
		density        int
		rampSeconds    int
		clusterPodsPS  int
		wantFeasible   bool
		wantNoteSubstr string
	}{
		{
			// 5M Pods / 1800s = 2778 Pods/s demand. 200 clusters × 30 Pods/s = 6000 supply. Feasible.
			name:           "feasible with headroom",
			machines:       50000,
			density:        100,
			rampSeconds:    1800,
			clusterPodsPS:  30,
			wantFeasible:   true,
			wantNoteSubstr: "≤ substrate supply",
		},
		{
			// 5M Pods / 60s = 83333 Pods/s demand. Way over supply.
			name:           "infeasible — ramp too aggressive",
			machines:       50000,
			density:        100,
			rampSeconds:    60,
			clusterPodsPS:  30,
			wantFeasible:   false,
			wantNoteSubstr: "ramp will tail off",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := fixtureProfile(t)
			p.Scale.Machines = tc.machines
			p.Scale.Density = tc.density
			p.LoadProfile.RampSeconds = tc.rampSeconds
			s := fixtureSubstrate(t)
			s.Cluster.BindThroughputPodsPerSec = tc.clusterPodsPS
			cfg, err := merge(p, s)
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			if cfg.RampFeasible != tc.wantFeasible {
				t.Errorf("RampFeasible = %v, want %v (note: %s)", cfg.RampFeasible, tc.wantFeasible, cfg.RampFeasibleNote)
			}
			if !strings.Contains(cfg.RampFeasibleNote, tc.wantNoteSubstr) {
				t.Errorf("RampFeasibleNote %q does not contain %q", cfg.RampFeasibleNote, tc.wantNoteSubstr)
			}
		})
	}
}

// TestMerge_PaidSubstrate confirms cost scales with hosts × hours.
func TestMerge_PaidSubstrate(t *testing.T) {
	t.Parallel()
	p := fixtureProfile(t)
	s := fixtureSubstrate(t)
	s.CostEstimate.PerHostUSDPerHour = 2.50

	cfg, err := merge(p, s)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	// HostsNeeded = 21, durationHours ≈ 1.1667, perHost = $2.50.
	// Expected: 21 × 2.50 × 1.1667 ≈ $61.25.
	wantMin, wantMax := 60.0, 62.0
	if cfg.EstimatedUSD < wantMin || cfg.EstimatedUSD > wantMax {
		t.Errorf("EstimatedUSD = %g, want in [%g, %g]", cfg.EstimatedUSD, wantMin, wantMax)
	}
}

// TestMerge_Geometry covers a few ceil-rounding cases.
func TestMerge_Geometry(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name              string
		machines, density int
		podsPerCluster    int
		clustersPerHost   int
		wantClusters      int
		wantHosts         int
	}{
		{
			// 5K Pods / 25K per cluster → 1 cluster (ceil), 1+1 = 2 hosts.
			name: "small profile rounds up", machines: 50, density: 100,
			podsPerCluster: 25000, clustersPerHost: 10,
			wantClusters: 1, wantHosts: 2,
		},
		{
			// 500K Pods / 25K = 20 clusters → 2+1 = 3 hosts.
			name: "5K machines", machines: 5000, density: 100,
			podsPerCluster: 25000, clustersPerHost: 10,
			wantClusters: 20, wantHosts: 3,
		},
		{
			// 5M Pods / 25K = 200 clusters → 20+1 = 21 hosts.
			name: "50K machines (canonical)", machines: 50000, density: 100,
			podsPerCluster: 25000, clustersPerHost: 10,
			wantClusters: 200, wantHosts: 21,
		},
		{
			// 50M Pods / 25K = 2000 clusters → 200+1 = 201 hosts.
			name: "500K machines", machines: 500000, density: 100,
			podsPerCluster: 25000, clustersPerHost: 10,
			wantClusters: 2000, wantHosts: 201,
		},
		{
			// 100 Pods / 25K = 1 cluster (ceil), 1+1 = 2 hosts.
			// Verifies the ceil never drops to 0 clusters.
			name: "tiny non-zero demand", machines: 1, density: 100,
			podsPerCluster: 25000, clustersPerHost: 10,
			wantClusters: 1, wantHosts: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := fixtureProfile(t)
			p.Scale.Machines = tc.machines
			p.Scale.Density = tc.density
			s := fixtureSubstrate(t)
			s.Cluster.PodsPerCluster = tc.podsPerCluster
			s.Cluster.ClustersPerHost = tc.clustersPerHost
			cfg, err := merge(p, s)
			if err != nil {
				t.Fatalf("merge: %v", err)
			}
			if cfg.ClusterCount != tc.wantClusters {
				t.Errorf("ClusterCount = %d, want %d", cfg.ClusterCount, tc.wantClusters)
			}
			if cfg.HostsNeeded != tc.wantHosts {
				t.Errorf("HostsNeeded = %d, want %d", cfg.HostsNeeded, tc.wantHosts)
			}
		})
	}
}

func TestCeilDiv(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b, want int
	}{
		{0, 1, 0},
		{1, 1, 1},
		{1, 2, 1},
		{2, 2, 1},
		{3, 2, 2},
		{25_000, 25_000, 1},
		{25_001, 25_000, 2},
		{4_999_999, 25_000, 200},
		{5_000_000, 25_000, 200},
		{5_000_001, 25_000, 201},
	}
	for _, tc := range cases {
		if got := ceilDiv(tc.a, tc.b); got != tc.want {
			t.Errorf("ceilDiv(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestCeilDiv_PanicsOnZeroDivisor(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on b=0")
		}
	}()
	_ = ceilDiv(10, 0)
}
