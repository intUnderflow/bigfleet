package shard

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/intUnderflow/bigfleet/pkg/conv"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// ReadServer implements the ADR-0061 ShardRead read-only service over a
// Shard. Register it on the shard's gRPC server alongside the
// operator-initiated Shard.Session service.
type ReadServer struct {
	pb.UnimplementedShardReadServer
	s *Shard
}

// NewReadServer wraps a Shard for the ShardRead service.
func NewReadServer(s *Shard) *ReadServer { return &ReadServer{s: s} }

// InspectNeeds streams the shard's last-cycle per-Need verdicts (ADR-0061):
// a header frame (cycle + freshness + total) then one NeedView per Need,
// optionally filtered to one cluster and capped at limit.
func (r *ReadServer) InspectNeeds(req *pb.InspectNeedsRequest, stream pb.ShardRead_InspectNeedsServer) error {
	if err := r.requireReadIdentity(stream.Context(), "InspectNeeds"); err != nil {
		return err
	}
	res := r.s.NeedViews(machine.ClusterID(req.GetClusterId()), int(req.GetLimit()))

	var computedAt int64
	if !res.ComputedAt.IsZero() {
		computedAt = res.ComputedAt.UnixNano()
	}
	if err := stream.Send(&pb.InspectNeedsResponse{
		Frame: &pb.InspectNeedsResponse_Header{Header: &pb.InspectNeedsHeader{
			Cycle:               res.Cycle,
			ComputedAtUnixNanos: computedAt,
			TotalNeeds:          int32(res.Total),
		}},
	}); err != nil {
		return err
	}
	for i := range res.Views {
		if err := stream.Send(&pb.InspectNeedsResponse{
			Frame: &pb.InspectNeedsResponse_Need{Need: toProtoNeedView(&res.Views[i])},
		}); err != nil {
			return err
		}
	}
	return nil
}

// requireReadIdentity gates a read RPC on the bigfleet://readonly (or admin)
// URI SAN under mTLS, mirroring the coordinator's read gate (ADR-0060). On
// plaintext the check is skipped — identity is only as strong as the
// transport (ADR-0048).
func (r *ReadServer) requireReadIdentity(ctx context.Context, rpc string) error {
	uri, mtls, err := grpcutil.PeerIdentity(ctx)
	if !mtls {
		return nil
	}
	if err != nil || (uri != grpcutil.ReadonlyURI && uri != grpcutil.AdminURI) {
		r.s.log.Error("shard rejected read identity", "rpc", rpc, "presented_identity", uri, "err", err)
		return status.Errorf(codes.PermissionDenied,
			"shard: %s requires client certificate URI SAN %q or %q (got %q)", rpc, grpcutil.ReadonlyURI, grpcutil.AdminURI, uri)
	}
	return nil
}

func toProtoNeedView(v *NeedView) *pb.NeedView {
	p := v.Profile
	cn := &pb.CapacityNeed{
		Requirements:              conv.RequirementsToProto(p.Requirements()),
		AggregateResources:        qtyMap(v.AggregateResources),
		MinUnit:                   qtyMap(v.MinUnit),
		Priority:                  p.Priority(),
		Group:                     v.Group,
		InterruptionPenaltyBucket: pb.PenaltyBucket(p.InterruptionPenaltyBucket()),
		ReclamationPenaltyBucket:  pb.PenaltyBucket(p.ReclamationPenaltyBucket()),
	}
	for _, sp := range p.Spread() {
		var w pb.TopologySpread_WhenUnsatisfiable
		switch sp.WhenUnsatisfiable {
		case needs.WhenUnsatisfiableDoNotSchedule:
			w = pb.TopologySpread_WHEN_UNSATISFIABLE_DO_NOT_SCHEDULE
		case needs.WhenUnsatisfiableScheduleAnyway:
			w = pb.TopologySpread_WHEN_UNSATISFIABLE_SCHEDULE_ANYWAY
		default:
			w = pb.TopologySpread_WHEN_UNSATISFIABLE_UNSPECIFIED
		}
		cn.Spread = append(cn.Spread, &pb.TopologySpread{
			TopologyKey:       sp.TopologyKey,
			MaxSkew:           sp.MaxSkew,
			WhenUnsatisfiable: w,
		})
	}

	out := &pb.NeedView{
		Need:                cn,
		ClusterId:           string(v.ClusterID),
		ArrivalUnixNanos:    v.ArrivalUnixNanos,
		ProfileFingerprint:  p.Fingerprint(),
		Satisfied:           v.Satisfied,
		ClaimedMachineCount: int32(v.ClaimedCount),
		BootstrapCount:      int32(v.BootstrapCount),
		ProvisionCount:      int32(v.ProvisionCount),
		SameDomain:          v.SameDomain,
		SameSatisfiable:     v.SameSatisfiable,
		AcquisitionParked:   v.AcquisitionParked,
		AgeCyclesUnmet:      int32(v.AgeCyclesUnmet),
		UnmetReason:         toProtoReason(v.UnmetReason),
	}
	if !v.Satisfied && len(v.Deficit) > 0 {
		out.ResidualDeficit = &pb.Resources{Resources: qtyMap(v.Deficit)}
	}
	return out
}

func toProtoReason(r NeedUnmetReason) pb.UnmetReason {
	switch r {
	case NeedReasonPriorityStarved:
		return pb.UnmetReason_UNMET_REASON_PRIORITY_STARVED
	case NeedReasonNoMatchingSupply:
		return pb.UnmetReason_UNMET_REASON_NO_MATCHING_SUPPLY
	case NeedReasonTopologyUnsatisfiable:
		return pb.UnmetReason_UNMET_REASON_TOPOLOGY_UNSATISFIABLE
	case NeedReasonPreemptionExhausted:
		return pb.UnmetReason_UNMET_REASON_PREEMPTION_EXHAUSTED
	default:
		return pb.UnmetReason_UNMET_REASON_UNSPECIFIED
	}
}

func qtyMap(rs []needs.ResourceQty) map[string]string {
	if len(rs) == 0 {
		return nil
	}
	out := make(map[string]string, len(rs))
	for _, r := range rs {
		out[r.Name] = r.Quantity
	}
	return out
}
