package shard

import (
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// Cache tests: coalesce (skip when tuple unchanged), rate-limit (skip
// when interval not elapsed even if tuple changed), and forget (clear
// cluster's entries on session loss so the reconnect re-emits).

func TestACCache_CoalescesUnchangedTuple(t *testing.T) {
	t.Parallel()
	c := newAvailableCapacityCache(5 * time.Second)
	v := acCacheValue{count: 10, confidence: pb.AvailableCapacityUpdate_CONFIDENCE_HIGH, cost: 6.0}
	if !c.shouldEmit("c1", "fp1", v) {
		t.Fatalf("first emit should fire")
	}
	if c.shouldEmit("c1", "fp1", v) {
		t.Errorf("second emit with same tuple should be coalesced")
	}
}

func TestACCache_RateLimitsChangedTuple(t *testing.T) {
	t.Parallel()
	c := newAvailableCapacityCache(5 * time.Second)
	now := time.Now()
	c.now = func() time.Time { return now }

	v1 := acCacheValue{count: 10, confidence: pb.AvailableCapacityUpdate_CONFIDENCE_HIGH, cost: 6.0}
	v2 := acCacheValue{count: 11, confidence: pb.AvailableCapacityUpdate_CONFIDENCE_HIGH, cost: 6.0}

	if !c.shouldEmit("c1", "fp1", v1) {
		t.Fatalf("first emit should fire")
	}
	now = now.Add(100 * time.Millisecond) // far less than the 5s interval
	if c.shouldEmit("c1", "fp1", v2) {
		t.Errorf("changed-but-too-soon emit should be rate-limited")
	}
	now = now.Add(5 * time.Second) // window elapsed
	if !c.shouldEmit("c1", "fp1", v2) {
		t.Errorf("changed-and-window-elapsed emit should fire")
	}
}

func TestACCache_ForgetClustersDropsEntries(t *testing.T) {
	t.Parallel()
	c := newAvailableCapacityCache(5 * time.Second)
	v := acCacheValue{count: 10, confidence: pb.AvailableCapacityUpdate_CONFIDENCE_HIGH, cost: 6.0}
	_ = c.shouldEmit("c1", "fp1", v)
	_ = c.shouldEmit("c2", "fp1", v)

	c.forget("c1")

	if !c.shouldEmit("c1", "fp1", v) {
		t.Errorf("after forget, c1 should re-emit")
	}
	if c.shouldEmit("c2", "fp1", v) {
		t.Errorf("c2 should still be coalesced (forget was scoped to c1)")
	}
}

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
		nil,
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
		nil,
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

// ADR-0027: a Profile no longer carries a resource shape, so the
// AvailableCapacityUpdate carries only the requirement set (plus count /
// cost / confidence). This test confirms the full requirement list is
// preserved through buildAvailableCapacityUpdate.
func TestBuildAvailableCapacity_RequirementsPreserved(t *testing.T) {
	t.Parallel()
	prof := needs.NewProfile(
		[]needs.Requirement{
			{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"a3-highgpu-8g"}},
			{Key: "topology.kubernetes.io/zone", Operator: needs.OperatorIn, Values: []string{"zone-a", "zone-b"}},
		},
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
	if upd.GetResources() != nil {
		t.Errorf("resources should be nil under ADR-0027 (Profile carries no shape): %+v", upd.GetResources())
	}
}
