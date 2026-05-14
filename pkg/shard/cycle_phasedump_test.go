//go:build scale

package shard

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
)

// TestCyclePhaseDump_500K runs a few cycles of the steady-state shard
// at 500K inventory + 50K demand and dumps the per-phase histogram.
// Diagnostic only — not a pass/fail check. Used to chase the cloud-vs-
// bench gap by decomposing the cycle into reconcile / phase1 / phase2
// / phase3 / execute durations.
//
// Run with: go test -run TestCyclePhaseDump_500K -v ./pkg/shard/...
//
// Build tag `scale` keeps it out of the default `go test ./...` path
// (matches the convention used in test/scale/...). Run with:
//
//	go test -tags=scale -run TestCyclePhaseDump_500K -v ./pkg/shard/...
func TestCyclePhaseDump_500K(t *testing.T) {
	s := setupBenchShard(t, 500_000, 50, 1000)
	_ = s.Inventory().Snapshot()
	ctx := t.Context()

	const cycles = 20
	for i := 0; i < cycles; i++ {
		_ = s.Step(ctx)
	}

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		switch mf.GetName() {
		case "bigfleet_shard_cycle_duration_seconds",
			"bigfleet_shard_cycle_phase_duration_seconds":
		default:
			continue
		}
		t.Logf("=== %s ===", mf.GetName())
		for _, m := range mf.Metric {
			label := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "phase" {
					label = l.GetValue()
				}
			}
			h := m.GetHistogram()
			count := h.GetSampleCount()
			total := h.GetSampleSum()
			mean := 0.0
			if count > 0 {
				mean = total / float64(count)
			}
			if label == "" {
				label = "(total)"
			}
			t.Logf("  phase=%-10s count=%d total=%.3fs mean=%.3fs",
				label, count, total, mean)
		}
	}
}

// setupBenchShard mirrors the BenchmarkShardCycle_Steady setup but on
// *testing.T so the diagnostic test can reuse the same machinery.
func setupBenchShard(t *testing.T, invSize, clusters, perCluster int) *Shard {
	t.Helper()
	dir := t.TempDir()
	epoch, err := fencing.LoadEpoch(filepath.Join(dir, "epoch"))
	if err != nil {
		t.Fatalf("epoch: %v", err)
	}
	prov := fake.New(fake.Options{InstantTransitions: true, Seed: 1})
	s, err := New(Config{
		ID:                   "phasedump",
		Epoch:                epoch,
		Provider:             prov,
		CycleInterval:        1 * time.Second,
		BootstrapTimeout:     1 * time.Second,
		IncrementalReconcile: true,
		LocalBootstrap: func(ctx context.Context, _ machine.ClusterID, _ []needs.Requirement) ([]byte, error) {
			return []byte("# phasedump\n"), nil
		},
	})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}

	types := []string{"a3-highgpu-8g", "m6i.large", "c6i.4xlarge", "n2-standard-32", "r6i.xlarge"}
	zones := []string{"zone-a", "zone-b", "zone-c"}
	resources := map[string]map[string]string{
		"a3-highgpu-8g":  {"nvidia.com/gpu": "8"},
		"m6i.large":      {"cpu": "2", "memory": "8Gi"},
		"c6i.4xlarge":    {"cpu": "16", "memory": "32Gi"},
		"n2-standard-32": {"cpu": "32", "memory": "128Gi"},
		"r6i.xlarge":     {"cpu": "4", "memory": "32Gi"},
	}
	for i := 0; i < invSize; i++ {
		typ := types[i%len(types)]
		zone := zones[i%len(zones)]
		id := machine.ID("seed-" + strconv.Itoa(i))
		profile := machine.Profile{
			InstanceType: typ,
			Zone:         zone,
			CapacityType: machine.CapacityTypeBareMetal,
			Resources:    resources[typ],
		}
		prov.AddIdle(id, profile, machine.CapacityTypeBareMetal, 0, 0)
		_ = s.SeedInventory(machine.Machine{ID: id, State: machine.StateIdle, Profile: profile})
	}

	// Per-replica unit shapes matching the seeded machines so each Need's
	// MinUnit fits on one machine of its instance type.
	units := map[string][]needs.ResourceQty{
		"a3-highgpu-8g":  {{Name: "nvidia.com/gpu", Quantity: "8"}},
		"m6i.large":      {{Name: "cpu", Quantity: "2"}, {Name: "memory", Quantity: "8Gi"}},
		"c6i.4xlarge":    {{Name: "cpu", Quantity: "16"}, {Name: "memory", Quantity: "32Gi"}},
		"n2-standard-32": {{Name: "cpu", Quantity: "32"}, {Name: "memory", Quantity: "128Gi"}},
		"r6i.xlarge":     {{Name: "cpu", Quantity: "4"}, {Name: "memory", Quantity: "32Gi"}},
	}

	for c := 0; c < clusters; c++ {
		clusterID := machine.ClusterID(fmt.Sprintf("phasedump-cluster-%d", c))
		ns := make([]needs.Need, 0, perCluster)
		for i := 0; i < perCluster; i++ {
			typ := types[(c+i)%len(types)]
			p := needs.NewProfile(
				[]needs.Requirement{{
					Key:      "node.kubernetes.io/instance-type",
					Operator: needs.OperatorIn,
					Values:   []string{typ},
				}},
				nil,
				int32(1000+(i%10)*1000),
				needs.PenaltyBucket8192,
				needs.PenaltyBucket8192,
			)
			ns = append(ns, needs.Need{ClusterID: clusterID, Profile: p, AggregateResources: units[typ], MinUnit: units[typ]})
		}
		s.NeedsTable().Replace(clusterID, ns)
	}
	return s
}
