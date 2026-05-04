package operator

import (
	"context"
	"errors"
	"testing"

	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// TestEnqueueRollup_ReplacesPending verifies that consecutive
// enqueueRollup calls keep the latest message and drop older pending
// rollups (paper §3.1: rollups are full replacement).
func TestEnqueueRollup_ReplacesPending(t *testing.T) {
	t.Parallel()
	sess := newSession(nil, nil)

	first := &pb.OperatorMessage{Payload: &pb.OperatorMessage_Rollup{
		Rollup: &pb.ClusterCapacityNeeds{ClusterId: "v1"},
	}}
	second := &pb.OperatorMessage{Payload: &pb.OperatorMessage_Rollup{
		Rollup: &pb.ClusterCapacityNeeds{ClusterId: "v2"},
	}}

	sess.enqueueRollup(first)
	sess.enqueueRollup(second)

	got := sess.pendingRollup.Load()
	if got != second {
		t.Errorf("pendingRollup = %p, want %p (latest replaces)", got, second)
	}
	if got.GetRollup().GetClusterId() != "v2" {
		t.Errorf("pending payload = %s, want v2", got.GetRollup().GetClusterId())
	}
}

// TestEnqueue_DropsWhenFull verifies the bounded outbox returns
// errOutboxFull instead of blocking when at capacity.
func TestEnqueue_DropsWhenFull(t *testing.T) {
	t.Parallel()
	sess := newSession(nil, nil)
	ctx := context.Background()

	// Fill outbox to capacity. Each enqueue should succeed.
	for i := 0; i < outboxCap; i++ {
		if err := sess.enqueue(ctx, &pb.OperatorMessage{}); err != nil {
			t.Fatalf("enqueue %d/%d: %v", i, outboxCap, err)
		}
	}

	// Next enqueue should drop with errOutboxFull.
	err := sess.enqueue(ctx, &pb.OperatorMessage{})
	if !errors.Is(err, errOutboxFull) {
		t.Errorf("enqueue at full = %v, want errOutboxFull", err)
	}
}
