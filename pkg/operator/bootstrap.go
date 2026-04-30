package operator

import (
	"context"

	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// handleBootstrapRequest renders a kubelet bootstrap blob via the
// configured BootstrapTemplate and enqueues a BootstrapBlobResponse on
// the session outbox. Errors from the template are echoed back to the
// shard via the response's Error field — the shard treats a non-empty
// error as an unsatisfiable requirement and falls back to a shortfall.
func (o *Operator) handleBootstrapRequest(ctx context.Context, sess *session, req *pb.BootstrapRequest) error {
	in := BootstrapRendererInput{
		ClusterID:    o.cfg.ClusterID,
		Requirements: requirementsFromProto(req.GetRequirements()),
	}
	out, err := o.cfg.BootstrapTemplate(ctx, in)
	resp := &pb.BootstrapBlobResponse{
		RequestId: req.GetRequestId(),
	}
	if err != nil {
		resp.Error = err.Error()
	} else {
		resp.UserData = out.UserData
		resp.TtlSeconds = out.TTLSeconds
	}
	return sess.enqueue(ctx, &pb.OperatorMessage{
		Payload: &pb.OperatorMessage_BootstrapResponse{BootstrapResponse: resp},
	})
}

// requirementsFromProto translates wire requirements into the
// renderer's input shape. Decoupled from the proto so renderer
// implementations don't pull in proto-generated types.
func requirementsFromProto(in []*pb.NodeSelectorRequirement) []RequirementInput {
	if len(in) == 0 {
		return nil
	}
	out := make([]RequirementInput, 0, len(in))
	for _, r := range in {
		out = append(out, RequirementInput{
			Key:      r.GetKey(),
			Operator: requirementOperatorString(r.GetOperator()),
			Values:   append([]string(nil), r.GetValues()...),
		})
	}
	return out
}

func requirementOperatorString(op pb.NodeSelectorRequirement_Operator) string {
	switch op {
	case pb.NodeSelectorRequirement_OPERATOR_IN:
		return "In"
	case pb.NodeSelectorRequirement_OPERATOR_NOT_IN:
		return "NotIn"
	case pb.NodeSelectorRequirement_OPERATOR_EXISTS:
		return "Exists"
	case pb.NodeSelectorRequirement_OPERATOR_DOES_NOT_EXIST:
		return "DoesNotExist"
	case pb.NodeSelectorRequirement_OPERATOR_SAME:
		return "Same"
	}
	return ""
}
