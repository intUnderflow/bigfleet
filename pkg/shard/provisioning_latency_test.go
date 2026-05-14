package shard

import (
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// emptyShard returns a Shard with just the demand-observed map
// initialised — enough to exercise observeRolledUpDemand without the
// full New() machinery (which requires a Provider, Epoch, etc.).
func emptyShard() *Shard {
	return &Shard{
		demandObservedAt: make(map[machine.ClusterID]map[string]time.Time),
	}
}

// gpuUnitSh is a stand-in per-replica resource vector for the
// fingerprint-tracking tests, which only care about Profile identity.
var gpuUnitSh = []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}}

func mkProfile(it string) needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{it},
		}},
		nil, 1000,
		needs.PenaltyBucket8192,
		needs.PenaltyBucketPinned,
	)
}

// First-seen timestamps are recorded only on first observation; a
// repeat rollup containing the same fingerprint preserves the
// original timestamp (so latency reflects how long the demand has
// been waiting, not how recent the latest rollup was).
func TestObserveRolledUpDemand_PreservesFirstSeen(t *testing.T) {
	t.Parallel()
	s := emptyShard()
	p := mkProfile("a3-highgpu-8g")

	s.observeRolledUpDemand("cluster-x", []needs.Need{{ClusterID: "cluster-x", Profile: p, AggregateResources: gpuUnitSh, MinUnit: gpuUnitSh}})
	first := s.demandObservedAt["cluster-x"][p.Fingerprint()]
	if first.IsZero() {
		t.Fatalf("expected a first-seen time")
	}

	time.Sleep(10 * time.Millisecond)
	s.observeRolledUpDemand("cluster-x", []needs.Need{{ClusterID: "cluster-x", Profile: p, AggregateResources: needs.ScaleResources(gpuUnitSh, 5), MinUnit: gpuUnitSh}})
	second := s.demandObservedAt["cluster-x"][p.Fingerprint()]
	if !first.Equal(second) {
		t.Errorf("first-seen time changed across repeat observation: %v -> %v", first, second)
	}
}

// Fingerprints absent from the latest rollup are pruned (so a long-
// completed workload's stale entry doesn't haunt the map forever).
func TestObserveRolledUpDemand_PrunesAbsent(t *testing.T) {
	t.Parallel()
	s := emptyShard()
	p1 := mkProfile("a3-highgpu-8g")
	p2 := mkProfile("c6i.4xlarge")

	s.observeRolledUpDemand("cluster-x", []needs.Need{
		{ClusterID: "cluster-x", Profile: p1, AggregateResources: gpuUnitSh, MinUnit: gpuUnitSh},
		{ClusterID: "cluster-x", Profile: p2, AggregateResources: gpuUnitSh, MinUnit: gpuUnitSh},
	})
	if len(s.demandObservedAt["cluster-x"]) != 2 {
		t.Fatalf("expected 2 tracked, got %d", len(s.demandObservedAt["cluster-x"]))
	}

	// Rollup now only contains p1 — p2 should be pruned.
	s.observeRolledUpDemand("cluster-x", []needs.Need{
		{ClusterID: "cluster-x", Profile: p1, AggregateResources: gpuUnitSh, MinUnit: gpuUnitSh},
	})
	if _, ok := s.demandObservedAt["cluster-x"][p1.Fingerprint()]; !ok {
		t.Errorf("p1 should still be tracked")
	}
	if _, ok := s.demandObservedAt["cluster-x"][p2.Fingerprint()]; ok {
		t.Errorf("p2 should be pruned (no longer in rollup)")
	}
}
