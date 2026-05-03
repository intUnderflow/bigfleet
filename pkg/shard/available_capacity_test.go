package shard

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// Pure-function tests for buildAvailableCapacityUpdate. Cover the
// confidence ladder, the cheapest-price computation, and both the
// pinned-instance-type path and the unpinned fallback.

func gpuProfile() needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"a3-highgpu-8g"},
		}},
		nil, nil,
		1000,
		needs.PenaltyBucket8192,
		needs.PenaltyBucketPinned,
	)
}

func unpinnedProfile() needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{{
			Key:      "topology.kubernetes.io/zone",
			Operator: needs.OperatorExists,
		}},
		nil, nil,
		1000,
		needs.PenaltyBucket8192,
		needs.PenaltyBucketPinned,
	)
}

func snapshotWith(t *testing.T, ms ...machine.Machine) *inventory.Snapshot {
	t.Helper()
	inv := inventory.New()
	for _, m := range ms {
		if err := inv.Insert(m); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}
	return inv.Snapshot()
}

func TestBuildAvailableCapacity_HighWhenIdleExists(t *testing.T) {
	t.Parallel()
	snap := snapshotWith(t,
		machine.Machine{ID: "i-1", State: machine.StateIdle, Host: machine.HostRef{Provider: "test", Ref: "h"}, Profile: machine.Profile{InstanceType: "a3-highgpu-8g"}, PricePerHour: 6.0},
		machine.Machine{ID: "i-2", State: machine.StateIdle, Host: machine.HostRef{Provider: "test", Ref: "h"}, Profile: machine.Profile{InstanceType: "a3-highgpu-8g"}, PricePerHour: 4.5},
	)
	upd := buildAvailableCapacityUpdate(snap, gpuProfile(), gpuProfile().Fingerprint())
	if upd.GetConfidence() != pb.AvailableCapacityUpdate_CONFIDENCE_HIGH {
		t.Errorf("confidence = %v, want HIGH", upd.GetConfidence())
	}
	if upd.GetAvailableCount() != 2 {
		t.Errorf("available_count = %d, want 2", upd.GetAvailableCount())
	}
	if upd.GetCostPerHour() != 4.5 {
		t.Errorf("cost_per_hour = %v, want 4.5 (cheapest of the two idle)", upd.GetCostPerHour())
	}
	if upd.GetSupersedesKey() != "available:"+gpuProfile().Fingerprint() {
		t.Errorf("supersedes_key = %q", upd.GetSupersedesKey())
	}
}

func TestBuildAvailableCapacity_MediumWhenOnlySpeculative(t *testing.T) {
	t.Parallel()
	snap := snapshotWith(t,
		machine.Machine{ID: "s-1", State: machine.StateSpeculative, Profile: machine.Profile{InstanceType: "a3-highgpu-8g"}, PricePerHour: 6.0},
	)
	upd := buildAvailableCapacityUpdate(snap, gpuProfile(), gpuProfile().Fingerprint())
	if upd.GetConfidence() != pb.AvailableCapacityUpdate_CONFIDENCE_MEDIUM {
		t.Errorf("confidence = %v, want MEDIUM", upd.GetConfidence())
	}
	if upd.GetAvailableCount() != 1 {
		t.Errorf("available_count = %d, want 1", upd.GetAvailableCount())
	}
	if upd.GetCostPerHour() != 0 {
		t.Errorf("cost_per_hour = %v, want 0 (no idle, no cheapest known)", upd.GetCostPerHour())
	}
}

func TestBuildAvailableCapacity_NoneWhenNothingMatches(t *testing.T) {
	t.Parallel()
	snap := snapshotWith(t,
		machine.Machine{ID: "i-1", State: machine.StateIdle, Host: machine.HostRef{Provider: "test", Ref: "h"}, Profile: machine.Profile{InstanceType: "different-type"}},
	)
	upd := buildAvailableCapacityUpdate(snap, gpuProfile(), gpuProfile().Fingerprint())
	if upd.GetConfidence() != pb.AvailableCapacityUpdate_CONFIDENCE_NONE {
		t.Errorf("confidence = %v, want NONE", upd.GetConfidence())
	}
	if upd.GetAvailableCount() != 0 {
		t.Errorf("available_count = %d, want 0", upd.GetAvailableCount())
	}
}

func TestBuildAvailableCapacity_UnpinnedFallsBackToAllStateCounts(t *testing.T) {
	t.Parallel()
	snap := snapshotWith(t,
		machine.Machine{ID: "i-1", State: machine.StateIdle, Host: machine.HostRef{Provider: "test", Ref: "h"}, Profile: machine.Profile{InstanceType: "x"}},
		machine.Machine{ID: "i-2", State: machine.StateIdle, Host: machine.HostRef{Provider: "test", Ref: "h"}, Profile: machine.Profile{InstanceType: "y"}},
		machine.Machine{ID: "s-1", State: machine.StateSpeculative, Profile: machine.Profile{InstanceType: "x"}},
	)
	prof := unpinnedProfile()
	upd := buildAvailableCapacityUpdate(snap, prof, prof.Fingerprint())
	if upd.GetConfidence() != pb.AvailableCapacityUpdate_CONFIDENCE_HIGH {
		t.Errorf("confidence = %v, want HIGH (idle exists)", upd.GetConfidence())
	}
	// 2 idle + 1 speculative = 3
	if upd.GetAvailableCount() != 3 {
		t.Errorf("available_count = %d, want 3 (all-state count for unpinned profile)", upd.GetAvailableCount())
	}
}

func TestBuildAvailableCapacity_RequirementsAndResourcesPreserved(t *testing.T) {
	t.Parallel()
	prof := needs.NewProfile(
		[]needs.Requirement{
			{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"a3-highgpu-8g"}},
			{Key: "topology.kubernetes.io/zone", Operator: needs.OperatorIn, Values: []string{"zone-a", "zone-b"}},
		},
		[]needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "8"}},
		nil,
		1000,
		needs.PenaltyBucket8192,
		needs.PenaltyBucketPinned,
	)
	snap := snapshotWith(t,
		machine.Machine{ID: "i-1", State: machine.StateIdle, Host: machine.HostRef{Provider: "test", Ref: "h"}, Profile: machine.Profile{InstanceType: "a3-highgpu-8g"}},
	)
	upd := buildAvailableCapacityUpdate(snap, prof, prof.Fingerprint())
	if got := len(upd.GetRequirements()); got != 2 {
		t.Errorf("requirements len = %d, want 2", got)
	}
	if upd.GetResources() == nil || upd.GetResources().GetResources()["nvidia.com/gpu"] != "8" {
		t.Errorf("resources missing or wrong: %+v", upd.GetResources())
	}
}
