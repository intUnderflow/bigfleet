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

// TestRenderHelmValues_BurstsPlumbThrough is the #327 V2-plumbing pin:
// loadProfile.bursts from a V2 profile must reach the rendered
// loadProfile map (the chart toYaml's that map verbatim into the
// load-driver's profile.yaml, which parses bursts as burstSpec). Without
// the profileV2.LoadProfile.Bursts field + this render step, a bursts:
// block would be silently dropped. The absence case asserts no stray key
// for profiles without bursts.
func TestRenderHelmValues_BurstsPlumbThrough(t *testing.T) {
	t.Parallel()
	p, s, cfg := fixtureMerged(t)

	// Absence: no bursts → no loadProfile.bursts key (default chart shape).
	noBurst := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)
	lp, _ := noBurst["loadProfile"].(map[string]any)
	if _, ok := lp["bursts"]; ok {
		t.Errorf("loadProfile.bursts present with no bursts configured: %v", lp["bursts"])
	}

	// Presence: a gpu-training-large gang burst round-trips into the
	// rendered loadProfile and survives a YAML marshal (what the chart
	// consumes).
	p.LoadProfile.Bursts = []profileBurst{{
		AtSeconds: 600, Archetype: "gpu-training-large",
		ExtraTarget: 1, DurationSeconds: 600, Selectivity: 1.0,
	}}
	withBurst := renderHelmValues(p, s, cfg, testArchetypes, testTypedArchetypes)
	lp2, _ := withBurst["loadProfile"].(map[string]any)
	bursts, ok := lp2["bursts"].([]profileBurst)
	if !ok || len(bursts) != 1 {
		t.Fatalf("loadProfile.bursts = %#v, want one profileBurst", lp2["bursts"])
	}
	if bursts[0].Archetype != "gpu-training-large" || bursts[0].ExtraTarget != 1 {
		t.Errorf("rendered burst = %+v, want gpu-training-large extraTarget 1", bursts[0])
	}
	b, err := yaml.Marshal(withBurst)
	if err != nil {
		t.Fatalf("marshal rendered values with bursts: %v", err)
	}
	if !strings.Contains(string(b), "gpu-training-large") {
		t.Errorf("marshalled values missing the burst archetype:\n%s", string(b))
	}
}

// TestBursts_RealProfile_PlumbThrough is the #327 end-to-end pin that
// TestRenderHelmValues_BurstsPlumbThrough missed: that test hand-sets
// p.LoadProfile.Bursts on a fixture, so it never exercises the parse +
// merge path. This one loads the REAL committed 5k.yaml and asserts the
// gpu-training-large burst survives every stage the production run takes
// — parse → merge → render → marshal (what the chart toYaml's into the
// load-driver's profile.yaml). bigfleet-uber #73 (the burst never fired
// in cloud) is the reason this test exists; if the burst silently drops
// at any stage, the first assertion below names it.
func TestBursts_RealProfile_PlumbThrough(t *testing.T) {
	t.Parallel()

	// Stage 1 — parse: the committed bursts block must land in the struct.
	pv2, err := readProfileV2("../../profiles/5k.yaml")
	if err != nil {
		t.Fatalf("read 5k.yaml: %v", err)
	}
	if len(pv2.LoadProfile.Bursts) != 1 {
		t.Fatalf("5k.yaml loadProfile.bursts parsed to %d entries, want 1: %#v",
			len(pv2.LoadProfile.Bursts), pv2.LoadProfile.Bursts)
	}
	b0 := pv2.LoadProfile.Bursts[0]
	if b0.Archetype != "gpu-training-large" || b0.Selectivity != 1.0 || b0.ExtraTarget != 1 {
		t.Errorf("parsed burst = %+v, want {gpu-training-large selectivity 1.0 extraTarget 1}", b0)
	}

	// Stage 2 — merge + render: the burst must reach the rendered values.
	sub, err := readSubstrate("../../substrates/example-fat-host.yaml")
	if err != nil {
		t.Fatalf("read substrate: %v", err)
	}
	cfg, err := merge(pv2, sub)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	arch, typed, err := loadCatalogArchetypes("../../profiles/5k.yaml", pv2.Catalog.Archetypes)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	values := renderHelmValues(pv2, sub, cfg, arch, typed)
	lp, ok := values["loadProfile"].(map[string]any)
	if !ok {
		t.Fatalf("rendered values missing loadProfile map: %#v", values["loadProfile"])
	}
	if _, ok := lp["bursts"]; !ok {
		t.Fatalf("rendered loadProfile dropped bursts: %#v", lp)
	}

	// Stage 3 — marshal: the load-driver parses this YAML, so the burst
	// archetype must survive the round-trip the chart's toYaml performs.
	out, err := yaml.Marshal(values)
	if err != nil {
		t.Fatalf("marshal rendered values: %v", err)
	}
	if !strings.Contains(string(out), "gpu-training-large") {
		t.Errorf("marshalled values missing burst archetype:\n%s", out)
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
