package main

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
)

// testArchetypes is a minimal valid archetype list standing in for the
// catalog the runner injects from profile.catalog.archetypes.
var testArchetypes = []any{
	map[string]any{
		"name":          "test-archetype",
		"weight":        1,
		"instanceTypes": []any{"m6i.large"},
	},
}

// testTypedArchetypes is the typed twin of testArchetypes — the form
// renderHelmValues sizes the seed from (ADR-0044). A single
// core-resource archetype keeps machinesEffective == ceil(totalPods /
// density), i.e. the nominal machine count, so the seed-math
// assertions below stay in pre-ADR-0044 terms.
var testTypedArchetypes = []archetype.Archetype{
	{Name: "test-archetype", Weight: 1, InstanceTypes: []string{"m6i.large"}},
}

// fixtureMerged is a helper that returns (profile, substrate,
// mergedConfig) for a canonical 50K-machine run on the fat-host
// example substrate.
func fixtureMerged(t *testing.T) (profileV2, substrateFile, mergedConfig) {
	t.Helper()
	p := fixtureProfile(t)
	s := fixtureSubstrate(t)
	cfg, err := merge(p, s)
	if err != nil {
		t.Fatalf("merge fixtures: %v", err)
	}
	return p, s, cfg
}

func TestRenderHelmValues_CanonicalShape(t *testing.T) {
	t.Parallel()
	p, s, cfg := fixtureMerged(t)
	values := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)

	// kwok block — geometry + substrate kwokPod.
	kwok, ok := values["kwok"].(map[string]any)
	if !ok {
		t.Fatalf("kwok block missing or wrong type: %T", values["kwok"])
	}
	if got, want := kwok["clusterCount"], 200; got != want {
		t.Errorf("kwok.clusterCount = %v, want %d", got, want)
	}
	if got, want := kwok["storage"], "etcd"; got != want {
		t.Errorf("kwok.storage = %v, want %q", got, want)
	}
	if got, want := kwok["sharedVolumeSizeLimit"], "2Gi"; got != want {
		t.Errorf("kwok.sharedVolumeSizeLimit = %v, want %q", got, want)
	}

	apiserver, _ := kwok["apiserverResources"].(map[string]any)
	apiReqs, _ := apiserver["requests"].(map[string]string)
	if apiReqs["cpu"] != "2" || apiReqs["memory"] != "4Gi" {
		t.Errorf("apiserver.requests = %v, want cpu=2 memory=4Gi", apiReqs)
	}

	workload, _ := kwok["workloadResources"].(map[string]any)
	workReqs, _ := workload["requests"].(map[string]string)
	if workReqs["cpu"] != "2" || workReqs["memory"] != "4Gi" {
		t.Errorf("workload.requests = %v, want cpu=2 memory=4Gi (identical to apiserver per substrate semantics)", workReqs)
	}

	// shard block — derived from profile.scale + seed tiers (ADR-0035).
	// Fixture has configuredFraction=0, idleHeadroomFraction=0.2,
	// speculativeMultiplier=3.
	shard, _ := values["shard"].(map[string]any)
	// 50K machines / 500K ceiling = 1 shard.
	if got, want := shard["replicas"], 1; got != want {
		t.Errorf("shard.replicas = %v, want %d", got, want)
	}
	// idle = 50000 × 0.2 / 1 = 10000.
	if got, want := shard["seedMachines"], 10000; got != want {
		t.Errorf("shard.seedMachines = %v, want %d (machines × idleHeadroom / replicas)", got, want)
	}
	// speculative = 50000 × 3 / 1 = 150000.
	if got, want := shard["seedSpeculative"], 150000; got != want {
		t.Errorf("shard.seedSpeculative = %v, want %d (machines × speculativeMultiplier / replicas)", got, want)
	}
	// configured = 0 (fixture has configuredFraction: 0).
	if got, want := shard["seedConfiguredPerCluster"], 0; got != want {
		t.Errorf("shard.seedConfiguredPerCluster = %v, want %d (configuredFraction=0 fixture)", got, want)
	}
	if got, want := shard["seedDensityMultiplier"], 100; got != want {
		t.Errorf("shard.seedDensityMultiplier = %v, want %d", got, want)
	}
	if got, want := shard["incrementalReconcile"], true; got != want {
		t.Errorf("shard.incrementalReconcile = %v, want %v (clusterCount=200 ≥ 100)", got, want)
	}

	// loadProfile.target == substrate's per-cluster Pod ceiling.
	loadProfile, _ := values["loadProfile"].(map[string]any)
	if got, want := loadProfile["target"], 25000; got != want {
		t.Errorf("loadProfile.target = %v, want %d", got, want)
	}
	if got, want := loadProfile["durationSeconds"], 1800; got != want {
		t.Errorf("loadProfile.durationSeconds = %v, want %d (== profile.loadProfile.soakSeconds)", got, want)
	}

	// costEstimate carries geometry × per-host cost.
	cost, _ := values["costEstimate"].(map[string]any)
	if got := cost["vCPU"]; got != 80*21 {
		t.Errorf("costEstimate.vCPU = %v, want %d (host vCPU × hostsNeeded)", got, 80*21)
	}
}

func TestRenderHelmValues_MultiShard(t *testing.T) {
	t.Parallel()
	p, s, _ := fixtureMerged(t)
	// 1M machines triggers 2 shards (500K ceiling × 2).
	p.Scale.Machines = 1_000_000
	cfg, err := merge(p, s)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	values := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)
	shard, _ := values["shard"].(map[string]any)
	if got, want := shard["replicas"], 2; got != want {
		t.Errorf("shard.replicas = %v, want %d (1M machines / 500K ceiling = 2 shards)", got, want)
	}
	// idle = 1M × 0.2 / 2 = 100K per shard.
	if got, want := shard["seedMachines"], 100_000; got != want {
		t.Errorf("shard.seedMachines = %v, want %d (machines × idleHeadroom / replicas)", got, want)
	}
}

// TestRenderHelmValues_SteadyStateSeed asserts the ADR-0035 shape:
// when configuredFraction=1.0 the Configured tier covers the full
// demand pre-bound at install, with Idle headroom + Speculative
// elasticity layered on top.
func TestRenderHelmValues_SteadyStateSeed(t *testing.T) {
	t.Parallel()
	p, s, _ := fixtureMerged(t)
	p.Seed.ConfiguredFraction = 1.0 // ADR-0035 default
	cfg, err := merge(p, s)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	values := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)
	shard, _ := values["shard"].(map[string]any)

	// configured per-cluster = 50000 × 1.0 / 200 clusters = 250.
	if got, want := shard["seedConfiguredPerCluster"], 250; got != want {
		t.Errorf("shard.seedConfiguredPerCluster = %v, want %d (machines × configuredFraction / clusters)", got, want)
	}
	// idle headroom unchanged from the prior test (50000 × 0.2 / 1 = 10000).
	if got, want := shard["seedMachines"], 10000; got != want {
		t.Errorf("shard.seedMachines = %v, want %d", got, want)
	}
}

// TestRenderHelmValues_MachineDemandSeedSizing asserts the ADR-0044
// wiring: the seed fractions multiply the catalog-derived effective
// machine total, so a whole-machine (extended-resource) archetype
// inflates the seed well past scale.machines — and the cost note
// records nominal vs effective so the run is self-describing.
func TestRenderHelmValues_MachineDemandSeedSizing(t *testing.T) {
	t.Parallel()
	p, s, cfg := fixtureMerged(t)
	typed := []archetype.Archetype{
		{Name: "cpu", Weight: 1, InstanceTypes: []string{"m6i.large"}, Resources: map[string]string{"cpu": "2"}},
		{Name: "gpu", Weight: 1, InstanceTypes: []string{"a3-highgpu-1g"}, Resources: map[string]string{"nvidia.com/gpu": "1"}},
	}
	values := renderHelmValues(p, s, cfg, testArchetypes, typed)
	shard, _ := values["shard"].(map[string]any)
	// totalPods = 50,000 × 100 = 5M, split evenly by pod share;
	// machinesEffective = ceil(2.5M / 100) + ceil(2.5M / 1) =
	// 2,525,000. idle = × 0.2 fixture headroom.
	if got, want := shard["seedMachines"], 505_000; got != want {
		t.Errorf("shard.seedMachines = %v, want %d (idleHeadroom × machinesEffective)", got, want)
	}
	cost, _ := values["costEstimate"].(map[string]any)
	notes, _ := cost["notes"].(string)
	if !strings.Contains(notes, "50000 nominal → 2525000 effective") {
		t.Errorf("costEstimate.notes missing nominal → effective machine counts: %q", notes)
	}
}

func TestRenderHelmValues_TinyScalePrometheusFootprint(t *testing.T) {
	t.Parallel()
	p, s, _ := fixtureMerged(t)
	p.Scale.Machines = 50 // dev-50 territory
	cfg, err := merge(p, s)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	values := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)
	prom, _ := values["prometheus"].(map[string]any)
	res, _ := prom["resources"].(map[string]any)
	reqs, _ := res["requests"].(map[string]string)
	// Small cluster count → tier-1 Prometheus footprint.
	if reqs["cpu"] != "1" || reqs["memory"] != "4Gi" {
		t.Errorf("prometheus.resources.requests = %v, want cpu=1 memory=4Gi for tiny cluster", reqs)
	}
}

// TestRenderHelmValues_YAMLRoundTrip confirms the rendered values
// round-trip through gopkg.in/yaml.v3 cleanly — i.e. helm will
// accept the output without parse errors. Helm-template smoke test
// runs separately in TestRenderHelmValues_HelmTemplate.
func TestRenderHelmValues_YAMLRoundTrip(t *testing.T) {
	t.Parallel()
	p, s, cfg := fixtureMerged(t)
	values := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)
	b, err := yaml.Marshal(values)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), "clusterCount: 200") {
		t.Errorf("rendered YAML missing kwok.clusterCount=200:\n%s", string(b))
	}
	var back map[string]any
	if err := yaml.Unmarshal(b, &back); err != nil {
		t.Errorf("unmarshal round-trip: %v", err)
	}
}

// TestRenderHelmValues_HelmTemplate is the integration smoke test:
// render values, write to a temp file, and run `helm template`
// against the chart. A passing template means the BYO values shape
// is structurally compatible with the chart. Skipped if helm isn't
// on PATH (e.g. local dev without the kubectl/helm stack).
func TestRenderHelmValues_HelmTemplate(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not on PATH; skipping integration smoke")
	}
	t.Parallel()

	p, s, cfg := fixtureMerged(t)
	values := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)
	path, err := writeRenderedValues(values, t.TempDir())
	if err != nil {
		t.Fatalf("writeRenderedValues: %v", err)
	}

	// Resolve repo root via git rev-parse so the chart path works
	// regardless of test cwd.
	rootBytes, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Fatalf("git rev-parse: %v", err)
	}
	repoRoot := strings.TrimSpace(string(rootBytes))
	chart := filepath.Join(repoRoot, "test", "scaletest", "chart")

	out, err := exec.Command("helm", "template", "scaletest", chart, "-f", path, "--set", "runId=test").CombinedOutput()
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "kind: ") {
		t.Errorf("helm template output missing 'kind:' lines:\n%s", out)
	}
}
