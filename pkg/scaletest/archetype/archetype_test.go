package archetype_test

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
)

func TestLoadCatalog_ParsesYAML(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "cat.yaml")
	if err := os.WriteFile(path, []byte(`archetypes:
  - name: gpu
    weight: 5
    instanceTypes: [a3-highgpu-8g]
    zones: [zone-a]
    resources: {nvidia.com/gpu: "8"}
    priorityClasses: [1000]
    interruptionPenalty: 8192
    reclamationPenalty: 65536
  - name: cpu
    weight: 30
    instanceTypes: [c6i.4xlarge, m6i.large]
    priorityClasses: [100, 1000]
    interruptionPenalty: 64
    reclamationPenalty: 256
`), 0o644); err != nil {
		t.Fatal(err)
	}

	cat, err := archetype.LoadCatalog(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cat.Archetypes) != 2 {
		t.Fatalf("got %d archetypes, want 2", len(cat.Archetypes))
	}
	gpu := cat.Archetypes[0]
	if gpu.Name != "gpu" || gpu.Weight != 5 || len(gpu.InstanceTypes) != 1 {
		t.Errorf("gpu archetype = %+v", gpu)
	}
	if gpu.MaxPriority() != 1000 {
		t.Errorf("gpu MaxPriority = %d, want 1000", gpu.MaxPriority())
	}
	cpu := cat.Archetypes[1]
	if cpu.MaxPriority() != 1000 {
		t.Errorf("cpu MaxPriority = %d, want 1000", cpu.MaxPriority())
	}
}

func TestLoadCatalog_RejectsMissingName(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte(`archetypes:
  - weight: 1
    instanceTypes: [a]
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := archetype.LoadCatalog(path); err == nil {
		t.Error("expected error for missing archetype name")
	}
}

func TestPicker_RoughlyHonoursWeights(t *testing.T) {
	t.Parallel()
	arches := []archetype.Archetype{
		{Name: "rare", Weight: 5, InstanceTypes: []string{"a"}},
		{Name: "common", Weight: 95, InstanceTypes: []string{"b"}},
	}
	p := archetype.NewPicker(arches)
	if p == nil {
		t.Fatal("nil picker for non-empty input")
	}
	rng := rand.New(rand.NewSource(1))
	counts := map[string]int{}
	const n = 10_000
	for i := 0; i < n; i++ {
		a := p.Pick(rng)
		counts[a.Name]++
	}
	// rare ~5% (500) ± a few percent variance acceptable; common ~95%
	if counts["rare"] < 200 || counts["rare"] > 1000 {
		t.Errorf("rare picks = %d, want ~500 (±100%%)", counts["rare"])
	}
	if counts["common"] < 9000 || counts["common"] > 9800 {
		t.Errorf("common picks = %d, want ~9500", counts["common"])
	}
}

func TestPicker_NilForEmptyInput(t *testing.T) {
	t.Parallel()
	if archetype.NewPicker(nil) != nil {
		t.Error("nil input should produce nil picker")
	}
	if archetype.NewPicker([]archetype.Archetype{}) != nil {
		t.Error("empty slice should produce nil picker")
	}
}
