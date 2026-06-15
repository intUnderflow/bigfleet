package archetype

import (
	"testing"
)

// realisticCatalogPath is the cloud realism catalog, relative to this
// package. The dev-50 coverage catalog (realistic-dev.yaml) is a
// separate file and deliberately NOT pinned here — its skew is a feature
// (every path drawn every run), per ADR-0050.
const realisticCatalogPath = "../../../test/scaletest/profiles/archetypes/realistic.yaml"

// seedDensity is the global node-packing density the cloud profiles run
// the seed at (scale.density / seedDensityMultiplier = 100). It is the
// fallback PodsPerMachine uses for archetypes that don't set podsPerNode.
const seedDensity = 100

// catalogMachineShares computes the realized per-archetype machine-demand
// share the seed would produce, using exactly the functions the seed and
// runner use (podShare = weight x E[replicas]; machine-share =
// podShare / PodsPerMachine). This is the share the fleet's machine mix
// converges to as the load-driver draws workload objects by weight.
func catalogMachineShares(arches []Archetype, density int) map[string]float64 {
	contrib := make(map[string]float64, len(arches))
	total := 0.0
	for i := range arches {
		a := &arches[i]
		c := podShare(a) / float64(PodsPerMachine(a, density))
		contrib[a.Name] = c
		total += c
	}
	for name := range contrib {
		contrib[name] /= total
	}
	return contrib
}

// catalogPodShares computes the realized per-archetype POD-count share —
// the property that is now DERIVED, not calibrated (ADR-0050). Reported
// so a reviewer can see what the pod mix became.
func catalogPodShares(arches []Archetype) map[string]float64 {
	shares := make(map[string]float64, len(arches))
	total := 0.0
	for i := range arches {
		s := podShare(&arches[i])
		shares[arches[i].Name] = s
		total += s
	}
	for name := range shares {
		shares[name] /= total
	}
	return shares
}

// TestRealisticCatalog_MachineMix is the ADR-0050 regression pin: the
// cloud realism catalog must produce a realistic MACHINE-demand mix, not
// the pre-ADR-0050 ~92% GPU one. It loads realistic.yaml and asserts the
// machine-share each tier produces — computed with the same functions
// the seed uses (ExpectedReplicas / PodsPerMachine) — stays within
// tolerance of the ADR-0050 target. If a weight or podsPerNode drifts,
// or the M66.2 GPU=1 special case creeps back, the mix moves and this
// fails in milliseconds.
func TestRealisticCatalog_MachineMix(t *testing.T) {
	t.Parallel()
	cat, err := LoadCatalog(realisticCatalogPath)
	if err != nil {
		t.Fatalf("load realistic catalog: %v", err)
	}
	arches := cat.Archetypes
	if len(arches) == 0 {
		t.Fatal("empty catalog")
	}

	machine := catalogMachineShares(arches, seedDensity)

	// Per-tier targets and absolute tolerances (percentage points).
	// Tolerances are tight for the high-share tiers (they should land
	// near-exactly) and looser for the few-percent GPU/DB tiers where
	// integer weights and E[replicas]=21.375-vs-21.4 rounding move the
	// last fraction of a point.
	type want struct {
		name      string
		targetPct float64
		tolPct    float64
	}
	// #327: gpu-training-large is now burstOnly — it contributes 0 to the
	// steady mix, so the remaining shares renormalize up slightly (tiny
	// 64.5→66.4, cpu-service 11.1→11.4, etc.). It is asserted at exactly
	// 0 below, not listed as a steady target.
	targets := []want{
		{"tiny-stateless", 66.4, 1.0},
		{"cpu-service", 11.4, 0.5},
		{"cpu-batch", 5.7, 0.5},
		{"critical-realtime", 0.95, 0.3},
		{"memory-cache", 1.5, 0.3},
		{"stateful-db", 1.5, 0.3},
		{"gpu-inference", 5.2, 0.5},
		{"gpu-training-small", 3.1, 0.5},
		{"gpu-training-medium", 4.2, 0.5},
	}

	t.Log("ADR-0050 realized machine-demand mix (via ExpectedReplicas / PodsPerMachine):")
	for _, w := range targets {
		got, ok := machine[w.name]
		if !ok {
			t.Errorf("archetype %q missing from catalog", w.name)
			continue
		}
		gotPct := got * 100
		t.Logf("  %-22s %6.3f%%  (target %4.1f%% +/- %.1f)", w.name, gotPct, w.targetPct, w.tolPct)
		if d := gotPct - w.targetPct; d < -w.tolPct || d > w.tolPct {
			t.Errorf("%s machine-share = %.3f%%, want %.1f%% +/- %.1f", w.name, gotPct, w.targetPct, w.tolPct)
		}
	}

	// #327: gpu-training-large is burstOnly, so it must contribute ZERO
	// to the steady machine mix — no draw, no seed, no gang floor. This is
	// the assertion that catches a burstOnly regression (e.g. the gang
	// floor creeping back, which weight:0 alone could not suppress).
	if got, ok := machine["gpu-training-large"]; !ok {
		t.Error("gpu-training-large missing from catalog (its definition must remain for the burst event)")
	} else if got != 0 {
		t.Errorf("gpu-training-large (burstOnly) steady machine-share = %.4f%%, want exactly 0", got*100)
	}

	// The headline ADR-0050 assertion: total GPU machine-share is a sane,
	// realistic fraction. #327 dropped gpu-training-large from the steady
	// mix, so this fell ~15%→~12.4% — still well inside the 10-20%
	// regression-guard band and nowhere near the ~92% the pre-ADR-0050
	// catalog produced. The per-tier checks above pin the shape within it.
	gpu := machine["gpu-inference"] + machine["gpu-training-small"] +
		machine["gpu-training-medium"] + machine["gpu-training-large"]
	t.Logf("  GPU machine-share total = %.3f%%", gpu*100)
	if gpu < 0.10 || gpu > 0.20 {
		t.Errorf("GPU machine-share = %.3f%%, want within [10%%, 20%%] (ADR-0050 realistic fleet)", gpu*100)
	}

	// General-compute (the cpu tier) should be the bulk of the fleet
	// (~84% after #327's renormalization) — the realistic shape BigFleet
	// baselines against.
	general := machine["tiny-stateless"] + machine["cpu-service"] +
		machine["cpu-batch"] + machine["critical-realtime"]
	if general < 0.78 || general > 0.88 {
		t.Errorf("general-compute machine-share = %.2f%%, want ~84%% ([78%%, 88%%])", general*100)
	}

	// Derived pod mix — reported, not asserted as a target (ADR-0050: the
	// pod distribution is now an emergent property). The check is only
	// that it diverges sharply from the machine mix (whole-machine GPU
	// gangs are a tiny pod-share while a few percent of machines), which
	// is the entire reason the catalog had to be machine-calibrated.
	pod := catalogPodShares(arches)
	gpuPods := pod["gpu-inference"] + pod["gpu-training-small"] +
		pod["gpu-training-medium"] + pod["gpu-training-large"]
	t.Logf("DERIVED pod mix (now an emergent property, ADR-0050):")
	t.Logf("  tiny-stateless pod-share = %.2f%%", pod["tiny-stateless"]*100)
	t.Logf("  GPU pod-share total      = %.2f%%  (vs %.2f%% GPU machine-share)", gpuPods*100, gpu*100)
	if gpuPods >= gpu {
		t.Errorf("GPU pod-share (%.2f%%) should be far below GPU machine-share (%.2f%%) — the ADR-0050 divergence", gpuPods*100, gpu*100)
	}
}

// TestRealisticCatalog_GPUDensification pins the PART 1 node-packing
// model on the actual catalog: gpu-inference is densified (8 gpu:1 Pods
// per 8-GPU node) and gpu-training is whole-machine (1 gpu:8 Pod per
// node). Both seed to gpu:8 nodes — the difference is PodsPerMachine. If
// the M66.2 "GPU is always 1" special case returns, gpu-inference's
// PodsPerMachine drops to 1 and this fails.
func TestRealisticCatalog_GPUDensification(t *testing.T) {
	t.Parallel()
	cat, err := LoadCatalog(realisticCatalogPath)
	if err != nil {
		t.Fatalf("load realistic catalog: %v", err)
	}
	find := func(name string) *Archetype {
		for i := range cat.Archetypes {
			if cat.Archetypes[i].Name == name {
				return &cat.Archetypes[i]
			}
		}
		t.Fatalf("archetype %q not in catalog", name)
		return nil
	}

	inf := find("gpu-inference")
	if got := PodsPerMachine(inf, seedDensity); got != 8 {
		t.Errorf("gpu-inference PodsPerMachine = %d, want 8 (densified 8-GPU node)", got)
	}
	if factor, scaleExt := SeedScale(inf, seedDensity); factor != 8 || !scaleExt {
		t.Errorf("gpu-inference SeedScale = (%d, %v), want (8, true)", factor, scaleExt)
	}

	for _, name := range []string{"gpu-training-small", "gpu-training-medium", "gpu-training-large"} {
		a := find(name)
		if got := PodsPerMachine(a, seedDensity); got != 1 {
			t.Errorf("%s PodsPerMachine = %d, want 1 (whole-machine gang)", name, got)
		}
		if factor, scaleExt := SeedScale(a, seedDensity); factor != 1 || !scaleExt {
			t.Errorf("%s SeedScale = (%d, %v), want (1, true)", name, factor, scaleExt)
		}
	}

	// The cpu tier must NOT opt into ADR-0050 scaling — podsPerNode unset,
	// so SeedScale keeps M66.2 (factor = density, extended unscaled).
	for _, name := range []string{"tiny-stateless", "cpu-service", "cpu-batch", "critical-realtime", "memory-cache", "stateful-db"} {
		a := find(name)
		if factor, scaleExt := SeedScale(a, seedDensity); factor != seedDensity || scaleExt {
			t.Errorf("%s SeedScale = (%d, %v), want (%d, false) — cpu tier keeps the global density", name, factor, scaleExt, seedDensity)
		}
	}
}
