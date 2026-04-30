package operator

import (
	"context"
	"time"

	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// handleReclaimInstruction processes a ReclaimInstruction from the
// shard. In M4, the operator logs the instruction and acks immediately
// — we don't have a real kubelet to drive graceful node shutdown
// against until M5's kind-based e2e. The ack timing exercises the
// instruction_id correlation path on the shard side, which is what we
// need today.
//
// In M5 this becomes: cordon the named nodes, set the kubelet's
// graceful-shutdown grace period from the instruction's
// grace_period_seconds, then send the ack only once the cordon has
// taken effect.
func (o *Operator) handleReclaimInstruction(ctx context.Context, sess *session, instr *pb.ReclaimInstruction) error {
	o.log.Info("reclaim instruction (M4: log + ack)",
		"id", instr.GetInstructionId(),
		"nodes", instr.GetNodes(),
		"grace_seconds", instr.GetGracePeriodSeconds(),
		"preemptor_priority", instr.GetPreemptorPriority(),
	)
	// Defensive: keep the call ctx alive for at least the grace period
	// so the ack doesn't get cancelled mid-flight on a fast shutdown
	// path.
	_ = time.Duration(instr.GetGracePeriodSeconds()) * time.Second

	return sess.enqueue(ctx, &pb.OperatorMessage{
		Payload: &pb.OperatorMessage_ReclaimAck{ReclaimAck: &pb.ReclaimAck{
			InstructionId: instr.GetInstructionId(),
			NodesStarted:  int32(len(instr.GetNodes())),
		}},
	})
}
