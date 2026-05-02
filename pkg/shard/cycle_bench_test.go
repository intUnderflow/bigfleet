package shard

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
)

// BenchmarkShardCycle_Steady measures `Shard.Step` wall-clock with a
// fixed seeded inventory and a fixed NeedsTable. This is the
// closest-to-production microbench we have for the data-plane scaling
// question: does the shard's runCycle stay under SLO when inventory
// gets large?
//
// Sub-benchmarks scale by inventory count (1K → 500K) holding demand
// fixed at 50K (50 clusters × 1K Needs each). Demand is spread across
// 5 instance types so Phase 1 has work to do picking matches.
//
// Run on the M5 Max with: go test -bench=ShardCycle ./pkg/shard/...
func BenchmarkShardCycle_Steady(b *testing.B) {
	for _, invSize := range []int{1_000, 10_000, 50_000, 100_000, 500_000} {
		b.Run(fmt.Sprintf("inv%d_demand50k", invSize), func(b *testing.B) {
			s := buildShardForBench(b, invSize)
			loadDemand(b, s, 50, 1000)
			// Force a synchronous snapshot to seed the cache that
			// CycleSnapshot reads from on the cycle hot path. Without
			// this, the first Step would race the background fold
			// goroutine and the bench would either sleep for the
			// fold or measure an empty snapshot.
			_ = s.Inventory().Snapshot()
			ctx := b.Context()

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = s.Step(ctx)
			}
		})
	}
}

func buildShardForBench(b *testing.B, invSize int) *Shard {
	b.Helper()
	dir := b.TempDir()
	epoch, err := fencing.LoadEpoch(filepath.Join(dir, "epoch"))
	if err != nil {
		b.Fatalf("epoch: %v", err)
	}
	prov := fake.New(fake.Options{InstantTransitions: true, Seed: 1})
	s, err := New(Config{
		ID:               "bench",
		Epoch:            epoch,
		Provider:         prov,
		CycleInterval:    1 * time.Second,
		BootstrapTimeout: 1 * time.Second,
		LocalBootstrap: func(ctx context.Context, cluster machine.ClusterID, _ []needs.Requirement) ([]byte, error) {
			return []byte("# bench\n"), nil
		},
	})
	if err != nil {
		b.Fatalf("shard.New: %v", err)
	}

	// Seed N idle machines spread across 5 instance types and 3 zones.
	// All zero-cost / zero-interruption-prob so Phase 1's cost
	// arithmetic is uniform — we're measuring the inventory walk,
	// not cost-comparison fanout.
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
		t := types[i%len(types)]
		z := zones[i%len(zones)]
		prov.AddIdle(
			machine.ID("seed-"+strconv.Itoa(i)),
			machine.Profile{
				InstanceType: t,
				Zone:         z,
				CapacityType: machine.CapacityTypeBareMetal,
				Resources:    resources[t],
			},
			machine.CapacityTypeBareMetal, 0, 0,
		)
		// Mirror into the shard's inventory so Phase 1 sees the
		// machines without waiting for a provider List reconcile.
		_ = s.SeedInventory(machine.Machine{
			ID:    machine.ID("seed-" + strconv.Itoa(i)),
			State: machine.StateIdle,
			Profile: machine.Profile{
				InstanceType: t,
				Zone:         z,
				CapacityType: machine.CapacityTypeBareMetal,
				Resources:    resources[t],
			},
		})
	}
	return s
}

// loadDemand fills the NeedsTable with N clusters × M needs each. Each
// Need targets one of the five seeded instance types, distributed
// round-robin so every instance type gets roughly equal demand.
func loadDemand(b *testing.B, s *Shard, clusters, perCluster int) {
	b.Helper()
	types := []string{"a3-highgpu-8g", "m6i.large", "c6i.4xlarge", "n2-standard-32", "r6i.xlarge"}
	resources := map[string]string{
		"a3-highgpu-8g":  "nvidia.com/gpu=8",
		"m6i.large":      "cpu=2",
		"c6i.4xlarge":    "cpu=16",
		"n2-standard-32": "cpu=32",
		"r6i.xlarge":     "cpu=4",
	}
	_ = resources

	for c := 0; c < clusters; c++ {
		clusterID := machine.ClusterID("bench-cluster-" + strconv.Itoa(c))
		ns := make([]needs.Need, 0, perCluster)
		for i := 0; i < perCluster; i++ {
			t := types[(c+i)%len(types)]
			p := needs.NewProfile(
				[]needs.Requirement{{
					Key:      "node.kubernetes.io/instance-type",
					Operator: needs.OperatorIn,
					Values:   []string{t},
				}},
				nil, nil,
				int32(1000+(i%10)*1000),
				needs.PenaltyBucket8192,
				needs.PenaltyBucket8192,
			)
			ns = append(ns, needs.Need{
				ClusterID: clusterID,
				Profile:   p,
				Count:     1,
			})
		}
		s.NeedsTable().Replace(clusterID, ns)
	}
}
