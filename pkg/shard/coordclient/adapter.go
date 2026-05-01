package coordclient

import (
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// ViewFromShard returns a ShardView that delegates to a *shard.Shard.
// Production glue between the shard's own surface and the coordinator
// client's polymorphic interface.
//
// Tests that want to stub the shard can supply their own ShardView
// implementation directly to coordclient.New.
func ViewFromShard(s *shard.Shard) ShardView { return shardAdapter{s: s} }

type shardAdapter struct{ s *shard.Shard }

func (a shardAdapter) ID() string   { return a.s.ID() }
func (a shardAdapter) Epoch() int64 { return a.s.Epoch() }

func (a shardAdapter) Summary() ShardSummary {
	s := a.s.Summary()
	return ShardSummary{
		TotalMachines:      s.TotalMachines,
		FreeMachines:       s.FreeMachines,
		InstanceTypeCounts: s.InstanceTypeCounts,
		ZoneCounts:         s.ZoneCounts,
		UtilCPUFraction:    s.UtilCPUFraction,
		UtilMemoryFraction: s.UtilMemoryFraction,
	}
}

func (a shardAdapter) Shortfalls() []ShardShortfall {
	in := a.s.Shortfalls()
	if len(in) == 0 {
		return nil
	}
	out := make([]ShardShortfall, 0, len(in))
	for _, s := range in {
		reqs := make([]ShortfallRequirement, 0, len(s.Profile.Requirements()))
		for _, r := range s.Profile.Requirements() {
			reqs = append(reqs, ShortfallRequirement{
				Key:      r.Key,
				Operator: operatorString(int(r.Operator)),
				Values:   r.Values,
			})
		}
		resources := make(map[string]string, len(s.Profile.Resources()))
		for _, rq := range s.Profile.Resources() {
			resources[rq.Name] = rq.Quantity
		}
		out = append(out, ShardShortfall{
			Requirements:              reqs,
			Resources:                 resources,
			Priority:                  s.Profile.Priority(),
			Count:                     int32(s.Count),
			AgeCycles:                 int32(s.AgeCycles),
			InterruptionPenaltyBucket: pb.PenaltyBucket(s.InterruptionPenaltyBucket),
		})
	}
	return out
}

func (a shardAdapter) OnAssignDomain(key, value string) error {
	a.s.AssignDomain(key, value)
	return nil
}
func (a shardAdapter) OnUnassignDomain(key, value string) error {
	a.s.UnassignDomain(key, value)
	return nil
}

// The next three are stubs in M6.3; the actual work lands with the
// rebalancing logic in M6.4.
func (a shardAdapter) OnReassignSpeculative(_ []string) error            { return nil }
func (a shardAdapter) OnCrossShardDrain(_ []string, _ int32) error       { return nil }
func (a shardAdapter) OnTransferOwnership(_ []string, _, _ string) error { return nil }

// operatorString turns a pkg/needs operator integer back into the
// string our coordclient.parseOperator can decode.
func operatorString(op int) string {
	switch op {
	case 1:
		return "In"
	case 2:
		return "NotIn"
	case 3:
		return "Exists"
	case 4:
		return "DoesNotExist"
	case 5:
		return "Same"
	}
	return ""
}
