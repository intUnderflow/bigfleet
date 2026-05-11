package decision_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
)

func TestPodsPerMachine_EqualShapes_ReturnsOne(t *testing.T) {
	// Pre-ADR-0022 inventory: perReplica == allocatable. Density is 1.
	t.Parallel()
	d := decision.PodsPerMachine(
		map[string]string{"cpu": "2", "memory": "4Gi"},
		map[string]string{"cpu": "2", "memory": "4Gi"},
	)
	if d != 1 {
		t.Errorf("density = %d, want 1", d)
	}
}

func TestPodsPerMachine_LargerAllocatable_ReturnsDensity(t *testing.T) {
	t.Parallel()
	d := decision.PodsPerMachine(
		map[string]string{"cpu": "1", "memory": "4Gi"},
		map[string]string{"cpu": "16", "memory": "32Gi"},
	)
	// Bottleneck is memory: 32 / 4 = 8. CPU would allow 16.
	if d != 8 {
		t.Errorf("density = %d, want 8 (memory bottleneck)", d)
	}
}

func TestPodsPerMachine_CPUBottleneck(t *testing.T) {
	t.Parallel()
	d := decision.PodsPerMachine(
		map[string]string{"cpu": "4", "memory": "1Gi"},
		map[string]string{"cpu": "16", "memory": "32Gi"},
	)
	// CPU: 16 / 4 = 4. Memory: 32. Bottleneck is CPU at 4.
	if d != 4 {
		t.Errorf("density = %d, want 4 (cpu bottleneck)", d)
	}
}

func TestPodsPerMachine_MissingDimension_ReturnsZero(t *testing.T) {
	// allocatable doesn't expose a resource perReplica wants → can't fit.
	t.Parallel()
	d := decision.PodsPerMachine(
		map[string]string{"cpu": "1", "nvidia.com/gpu": "1"},
		map[string]string{"cpu": "16"},
	)
	if d != 0 {
		t.Errorf("density = %d, want 0 (GPU missing from allocatable)", d)
	}
}

func TestPodsPerMachine_GPU(t *testing.T) {
	t.Parallel()
	d := decision.PodsPerMachine(
		map[string]string{"cpu": "8", "memory": "32Gi", "nvidia.com/gpu": "1"},
		map[string]string{"cpu": "96", "memory": "768Gi", "nvidia.com/gpu": "8"},
	)
	// CPU: 96/8 = 12. Memory: 768/32 = 24. GPU: 8/1 = 8. Bottleneck: GPU at 8.
	if d != 8 {
		t.Errorf("density = %d, want 8 (gpu bottleneck on a3-highgpu-8g shape)", d)
	}
}

func TestMachinesForAggregate_ZeroCount(t *testing.T) {
	t.Parallel()
	got := decision.MachinesForAggregate(
		map[string]string{"cpu": "1"},
		map[string]string{"cpu": "16"},
		0,
	)
	if got != 0 {
		t.Errorf("machines for 0 pods = %d, want 0", got)
	}
}

func TestMachinesForAggregate_OnePodPerMachine(t *testing.T) {
	// Pre-ADR-0022: perReplica == allocatable, density = 1.
	t.Parallel()
	got := decision.MachinesForAggregate(
		map[string]string{"cpu": "2", "memory": "4Gi"},
		map[string]string{"cpu": "2", "memory": "4Gi"},
		100,
	)
	if got != 100 {
		t.Errorf("machines = %d, want 100 (1:1 mapping)", got)
	}
}

func TestMachinesForAggregate_DensityCeiling(t *testing.T) {
	t.Parallel()
	got := decision.MachinesForAggregate(
		map[string]string{"cpu": "1", "memory": "4Gi"},
		map[string]string{"cpu": "16", "memory": "32Gi"},
		100,
	)
	// Density = 8 (memory). 100 / 8 = 12.5 → ceil → 13.
	if got != 13 {
		t.Errorf("machines = %d, want 13 (ceil(100/8))", got)
	}
}

func TestMachinesForAggregate_ExactMultiple(t *testing.T) {
	t.Parallel()
	got := decision.MachinesForAggregate(
		map[string]string{"cpu": "1", "memory": "4Gi"},
		map[string]string{"cpu": "16", "memory": "32Gi"},
		16,
	)
	// Density = 8. 16 / 8 = 2 exactly.
	if got != 2 {
		t.Errorf("machines = %d, want 2 (exact multiple)", got)
	}
}

func TestMachinesForAggregate_NoFitFallsBackTo1Per1(t *testing.T) {
	t.Parallel()
	got := decision.MachinesForAggregate(
		map[string]string{"cpu": "1", "nvidia.com/gpu": "1"},
		map[string]string{"cpu": "16"}, // no GPU
		100,
	)
	// Density = 0 → fallback to 1:1.
	if got != 100 {
		t.Errorf("machines = %d, want 100 (no-fit fallback)", got)
	}
}
