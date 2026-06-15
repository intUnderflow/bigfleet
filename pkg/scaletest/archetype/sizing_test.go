package archetype

import (
	"math"
	"math/rand"
	"testing"
)

func TestPickReplicas_WithinBuckets(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(1))
	// Every draw must land inside one of the declared buckets.
	for i := 0; i < 5000; i++ {
		n := PickReplicas(rng, false)
		if n < 1 {
			t.Fatalf("stateless draw %d < 1", n)
		}
		inBucket := false
		for _, b := range replicaDistribution {
			if n >= b.lo && n <= b.hi {
				inBucket = true
				break
			}
		}
		if !inBucket {
			t.Fatalf("stateless draw %d fell outside every bucket", n)
		}
	}
}

func TestPickReplicas_StatefulCap(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 5000; i++ {
		n := PickReplicas(rng, true)
		if n < 1 {
			t.Fatalf("stateful draw %d < 1", n)
		}
		if n > StatefulReplicaCap {
			t.Fatalf("stateful draw %d exceeds cap %d", n, StatefulReplicaCap)
		}
	}
}

func TestPickReplicas_HitsLargeBucket(t *testing.T) {
	t.Parallel()
	// The heavy tail must be reachable: over many draws at least one
	// stateless service should land in the largest bucket.
	rng := rand.New(rand.NewSource(3))
	large := replicaDistribution[len(replicaDistribution)-1]
	hitLarge := false
	for i := 0; i < 20000 && !hitLarge; i++ {
		if n := PickReplicas(rng, false); n >= large.lo {
			hitLarge = true
		}
	}
	if !hitLarge {
		t.Fatal("never drew from the large-service bucket in 20000 draws")
	}
}

// TestExpectedReplicas_MatchesSampleMean cross-checks the analytic
// expectation against the empirical mean of PickReplicas — the
// property that ties seed sizing (ADR-0044) to demand generation.
func TestExpectedReplicas_MatchesSampleMean(t *testing.T) {
	t.Parallel()
	a := &Archetype{Name: "cpu-service"}
	for _, stateful := range []bool{false, true} {
		want := ExpectedReplicas(a, stateful)
		rng := rand.New(rand.NewSource(7))
		const n = 200_000
		sum := 0
		for i := 0; i < n; i++ {
			sum += PickReplicas(rng, stateful)
		}
		got := float64(sum) / n
		if math.Abs(got-want)/want > 0.05 {
			t.Errorf("stateful=%v: sample mean %.3f vs analytic %.3f (>5%% off)", stateful, got, want)
		}
	}
}

func TestExpectedReplicas_GangIsMeanOfRange(t *testing.T) {
	t.Parallel()
	a := &Archetype{Name: "gpu-training-medium", SameZone: true, GroupSizeRange: [2]int{4, 8}}
	if got := ExpectedReplicas(a, false); got != 6 {
		t.Errorf("ExpectedReplicas(gang [4,8]) = %g, want 6", got)
	}
	// Unset range normalises like PickGroupSize: [1,1].
	rackGang := &Archetype{Name: "memory-cache", SameRack: true}
	if got := ExpectedReplicas(rackGang, true); got != 1 {
		t.Errorf("ExpectedReplicas(gang, unset range) = %g, want 1", got)
	}
}

func TestPodsPerMachine(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		a       Archetype
		density int
		want    int
	}{
		{
			name:    "core-only buckets pack density",
			a:       Archetype{SizeBuckets: []SizeBucket{{Weight: 1, Resources: map[string]string{"cpu": "2", "memory": "4Gi"}}}},
			density: 100,
			want:    100,
		},
		{
			name: "extended resource in any bucket forces whole-machine",
			a: Archetype{SizeBuckets: []SizeBucket{
				{Weight: 9, Resources: map[string]string{"cpu": "2"}},
				{Weight: 1, Resources: map[string]string{"cpu": "8", "nvidia.com/gpu": "1"}},
			}},
			density: 100,
			want:    1,
		},
		{
			name:    "flat Resources fallback with extended resource",
			a:       Archetype{Resources: map[string]string{"nvidia.com/gpu": "8"}},
			density: 100,
			want:    1,
		},
		{
			name:    "ephemeral-storage is core",
			a:       Archetype{Resources: map[string]string{"cpu": "1", "ephemeral-storage": "10Gi"}},
			density: 50,
			want:    50,
		},
		{
			name:    "density floor of 1",
			a:       Archetype{Resources: map[string]string{"cpu": "1"}},
			density: 0,
			want:    1,
		},
	}
	for _, c := range cases {
		if got := PodsPerMachine(&c.a, c.density); got != c.want {
			t.Errorf("%s: PodsPerMachine = %d, want %d", c.name, got, c.want)
		}
	}
}

func archZones(a *Archetype) int { return len(a.Zones) }

// TestMachineAllocation_PackingAsymmetry is ADR-0044's motivating
// case: a whole-machine archetype with a small workload weight must
// get a machine pool sized to its pod share, not its weight — at
// density 100 a 70:1 weight split still leaves the GPU archetype with
// more machines than the CPU one.
func TestMachineAllocation_PackingAsymmetry(t *testing.T) {
	t.Parallel()
	arches := []Archetype{
		{Name: "cpu", Weight: 70, InstanceTypes: []string{"c"}, Resources: map[string]string{"cpu": "2"}},
		{Name: "gpu", Weight: 1, InstanceTypes: []string{"g"}, Resources: map[string]string{"nvidia.com/gpu": "1"}},
	}
	alloc := MachineAllocation(arches, 100, 1000, archZones)
	if alloc[1] <= alloc[0] {
		t.Errorf("gpu alloc %d ≤ cpu alloc %d; whole-machine archetype must dominate (pod shares 70:1, machine shares 0.7:1)", alloc[1], alloc[0])
	}
	if got := alloc[0] + alloc[1]; got != 1000 {
		t.Errorf("non-gang allocation sums to %d, want exactly 1000", got)
	}
}

func TestMachineAllocation_LargestRemainderSumsExactly(t *testing.T) {
	t.Parallel()
	arches := []Archetype{
		{Name: "a", Weight: 3, InstanceTypes: []string{"x"}},
		{Name: "b", Weight: 3, InstanceTypes: []string{"x"}},
		{Name: "c", Weight: 3, InstanceTypes: []string{"x"}},
	}
	// 10 does not divide 3 ways evenly; the remainder must still land.
	for _, total := range []int{1, 2, 10, 99, 100} {
		alloc := MachineAllocation(arches, 1, total, archZones)
		sum := 0
		for _, n := range alloc {
			sum += n
		}
		if sum != total {
			t.Errorf("total=%d: allocation %v sums to %d", total, alloc, sum)
		}
	}
}

func TestMachineAllocation_GangFloors(t *testing.T) {
	t.Parallel()
	arches := []Archetype{
		{Name: "cpu", Weight: 1000, InstanceTypes: []string{"c"}, Resources: map[string]string{"cpu": "2"}},
		{
			Name: "gang", Weight: 1, InstanceTypes: []string{"g"},
			Zones:          []string{"zone-a", "zone-b", "zone-c"},
			SameZone:       true,
			GroupSizeRange: [2]int{4, 8},
			Resources:      map[string]string{"cpu": "2"},
		},
	}
	alloc := MachineAllocation(arches, 1, 100, archZones)
	// Share-derived gang count would be ~0; the floor is
	// max(GroupSizeRange) × zones = 8 × 3 = 24, applied ON TOP — the
	// sum may exceed the requested total.
	if alloc[1] != 24 {
		t.Errorf("gang alloc = %d, want floor 24 (8 machines × 3 zones)", alloc[1])
	}
	if sum := alloc[0] + alloc[1]; sum <= 100 {
		t.Errorf("allocation %v sums to %d; gang floor should push it past the nominal 100", alloc, sum)
	}

	// No zones declared → floor of one zone.
	arches[1].Zones = nil
	alloc = MachineAllocation(arches, 1, 100, archZones)
	if alloc[1] != 8 {
		t.Errorf("gang alloc with no zones = %d, want 8 (one-zone floor)", alloc[1])
	}
}

func TestMachineAllocation_DisabledTierGetsNothing(t *testing.T) {
	t.Parallel()
	arches := []Archetype{
		{Name: "gang", Weight: 1, InstanceTypes: []string{"g"}, SameRack: true, GroupSizeRange: [2]int{2, 4}, Zones: []string{"zone-a"}},
	}
	for _, n := range MachineAllocation(arches, 1, 0, archZones) {
		if n != 0 {
			t.Fatalf("totalMachines=0 must allocate nothing (no gang floors in a disabled tier), got %v", n)
		}
	}
}

func TestMachinesForPods(t *testing.T) {
	t.Parallel()
	// Single core archetype: effective == ceil(totalPods / density),
	// the pre-ADR-0044 nominal — catalogs without whole-machine
	// archetypes keep their old sizing.
	core := []Archetype{{Name: "cpu", Weight: 1, InstanceTypes: []string{"c"}, Resources: map[string]string{"cpu": "2"}}}
	if got := MachinesForPods(core, 100, 5000); got != 50 {
		t.Errorf("core-only MachinesForPods = %d, want 50", got)
	}

	// Adding a whole-machine archetype at equal pod share moves half
	// the pods to 1-per-machine packing: 2500/100 + 2500/1 = 2525.
	mixed := append([]Archetype{}, core...)
	mixed = append(mixed, Archetype{Name: "gpu", Weight: 1, InstanceTypes: []string{"g"}, Resources: map[string]string{"nvidia.com/gpu": "1"}})
	if got := MachinesForPods(mixed, 100, 5000); got != 2525 {
		t.Errorf("mixed MachinesForPods = %d, want 2525 (25 cpu + 2500 gpu)", got)
	}

	// Gang floors are added on top of the share-derived count, and
	// multiply by zone count. Pod shares: cpu 21.375, gpu 21.375, gang
	// 1×6 = 6 (weight 0 counts as 1, matching NewPicker; E = mean of
	// [4,8]); total 48.75. cpu: ceil(2192.31/100) = 22; gpu:
	// ceil(2192.31) = 2193; gang: ceil(615.38/100) = 7 plus floor
	// 8 × 3 zones = 24. Σ = 2246 — a max-with-floor would give 2239.
	gang := append([]Archetype{}, mixed...)
	gang = append(gang, Archetype{
		Name: "gang", Weight: 0, InstanceTypes: []string{"g"},
		Zones: []string{"zone-a", "zone-b", "zone-c"}, SameZone: true,
		GroupSizeRange: [2]int{4, 8}, Resources: map[string]string{"cpu": "2"},
	})
	if got := MachinesForPods(gang, 100, 5000); got != 2246 {
		t.Errorf("gang catalog MachinesForPods = %d, want 2246 (floor added on top, per zone)", got)
	}
	// Zone count only moves the floor: dropping to one zone removes
	// exactly 8 × 2 machines.
	gang[2].Zones = []string{"zone-a"}
	if got := MachinesForPods(gang, 100, 5000); got != 2246-16 {
		t.Errorf("one-zone gang catalog MachinesForPods = %d, want %d", got, 2246-16)
	}

	// Empty catalog: legacy uniform packing.
	if got := MachinesForPods(nil, 100, 5000); got != 50 {
		t.Errorf("empty-catalog MachinesForPods = %d, want 50", got)
	}
}

// TestBurstOnly_ExcludedFromSteadyModel pins #327: a burstOnly archetype
// contributes nothing to the steady-state model — not to the draw
// (NewPicker), the share math (podShare / machineShares /
// MachinesForPods), the allocation (MachineAllocation), or its gang floor
// (gangFloor) — while a non-burstOnly archetype is unchanged.
func TestBurstOnly_ExcludedFromSteadyModel(t *testing.T) {
	t.Parallel()
	// A normal gang plus a burstOnly gang with an identical (large) shape.
	// Without burstOnly the second would dominate via its gang floor.
	burst := Archetype{
		Name: "burst-gang", Weight: 1, InstanceTypes: []string{"g"},
		Zones: []string{"zone-a", "zone-b", "zone-c"}, SameZone: true,
		GroupSizeRange: [2]int{64, 256}, BurstOnly: true,
		Resources: map[string]string{"nvidia.com/gpu": "8"},
	}
	arches := []Archetype{
		{Name: "cpu", Weight: 100, InstanceTypes: []string{"c"}, Resources: map[string]string{"cpu": "2"}},
		burst,
	}

	// podShare: zero for burstOnly.
	if got := podShare(&arches[1]); got != 0 {
		t.Errorf("podShare(burstOnly) = %g, want 0", got)
	}
	if podShare(&arches[0]) == 0 {
		t.Error("podShare(non-burstOnly) must be non-zero")
	}

	// machineShares: zero for burstOnly.
	if got := machineShares(arches, 100)[1]; got != 0 {
		t.Errorf("machineShares[burstOnly] = %g, want 0", got)
	}

	// gangFloor: zero for burstOnly even though it IS a gang.
	if got := gangFloor(&arches[1], 3); got != 0 {
		t.Errorf("gangFloor(burstOnly gang) = %d, want 0", got)
	}
	// Sanity: drop burstOnly and the same shape floors at 256×3.
	notBurst := burst
	notBurst.BurstOnly = false
	if got := gangFloor(&notBurst, 3); got != 256*3 {
		t.Errorf("gangFloor(non-burstOnly gang) = %d, want %d", got, 256*3)
	}

	// MachineAllocation: burstOnly gets exactly 0, and the non-burstOnly
	// archetype still absorbs the full nominal total (no leakage).
	alloc := MachineAllocation(arches, 100, 1000, archZones)
	if alloc[1] != 0 {
		t.Errorf("MachineAllocation[burstOnly] = %d, want 0 (no draw share, no gang floor)", alloc[1])
	}
	if alloc[0] != 1000 {
		t.Errorf("MachineAllocation[cpu] = %d, want 1000 (burstOnly takes none)", alloc[0])
	}

	// MachinesForPods: identical with and without the burstOnly archetype
	// — it adds neither share-derived machines nor a gang floor.
	withBurst := MachinesForPods(arches, 100, 5000)
	withoutBurst := MachinesForPods(arches[:1], 100, 5000)
	if withBurst != withoutBurst {
		t.Errorf("MachinesForPods with burstOnly = %d, without = %d; burstOnly must add nothing", withBurst, withoutBurst)
	}

	// NewPicker: burstOnly is never drawn; a catalog of only burstOnly is
	// a nil picker.
	rng := rand.New(rand.NewSource(1))
	p := NewPicker(arches)
	if p == nil {
		t.Fatal("picker nil for a catalog with a drawable archetype")
	}
	for i := 0; i < 5000; i++ {
		if got := p.Pick(rng); got.Name == "burst-gang" {
			t.Fatal("NewPicker drew a burstOnly archetype")
		}
	}
	if NewPicker([]Archetype{burst}) != nil {
		t.Error("NewPicker over only-burstOnly archetypes must be nil")
	}
}

func TestIsStateful(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		want bool
	}{
		{"stateful-db", true},
		{"memory-cache", true},
		{"cpu-service", false},
		{"gpu-training-large", false},
		{"tiny-stateless", false},
		{"", false},
	}
	for _, c := range cases {
		if got := IsStateful(c.name); got != c.want {
			t.Errorf("IsStateful(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}
