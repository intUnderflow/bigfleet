//go:build conformance

// Fencing conformance (paper §11, M71). Every mutating RPC carries a
// (shard_id, shard_epoch, sequence_number) token; the provider keeps a
// per-shard_id high-water mark of accepted tokens and MUST reject a
// token that is not strictly newer with FAILED_PRECONDITION — that is
// what stops a zombie shard from actuating a stale view of the fleet.
// Full contract text lives in api/proto/bigfleet/v1alpha1/provider.proto.
//
// Each test uses a run-unique shard_id, so repeated suite runs against a
// long-lived provider never collide with marks established earlier.
package conformance_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// fenceShardID mints a shard_id no prior run (or prior test) has used.
func fenceShardID(prefix string) string {
	return fmt.Sprintf("conformance-fence-%s-%d", prefix, time.Now().UnixNano())
}

// fencedCreate is the mutating probe all fencing tests use: Create is
// idempotent on (machine_id, target=Idle), so absent fencing a repeat
// would succeed — any FAILED_PRECONDITION can only come from the token.
func fencedCreate(ctx context.Context, cli pb.CapacityProviderClient, machineID, shardID string, epoch, seq int64) error {
	_, err := cli.Create(ctx, &pb.CreateRequest{
		MachineId:      machineID,
		ShardId:        shardID,
		ShardEpoch:     epoch,
		SequenceNumber: seq,
	})
	return err
}

// TestConformance_FencingUnknownShardAccepted: first contact from an
// unknown shard_id is accepted and establishes the high-water mark.
func TestConformance_FencingUnknownShardAccepted(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)
	shard := fenceShardID("unknown")
	if err := fencedCreate(ctx, cli, id, shard, 1, 1); err != nil {
		t.Fatalf("first contact from unknown shard_id must be accepted: %v", err)
	}
	// The accepted token is now the mark: replaying it must be rejected.
	err := fencedCreate(ctx, cli, id, shard, 1, 1)
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("replay of the accepted token: got %v (code=%s), want FAILED_PRECONDITION", err, status.Code(err))
	}
}

// TestConformance_FencingStaleEpochRejected: a mutating call whose
// shard_epoch is below the high-water mark is rejected regardless of its
// sequence_number.
func TestConformance_FencingStaleEpochRejected(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)
	shard := fenceShardID("stale-epoch")
	if err := fencedCreate(ctx, cli, id, shard, 2, 1); err != nil {
		t.Fatalf("establish high-water mark: %v", err)
	}
	err := fencedCreate(ctx, cli, id, shard, 1, 99)
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("stale epoch: got %v (code=%s), want FAILED_PRECONDITION", err, status.Code(err))
	}
}

// TestConformance_FencingStaleSequenceRejected: within the same epoch,
// a sequence_number at or below the high-water mark is rejected.
func TestConformance_FencingStaleSequenceRejected(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)
	shard := fenceShardID("stale-seq")
	if err := fencedCreate(ctx, cli, id, shard, 1, 5); err != nil {
		t.Fatalf("establish high-water mark: %v", err)
	}
	if err := fencedCreate(ctx, cli, id, shard, 1, 5); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("equal sequence (replay): got %v (code=%s), want FAILED_PRECONDITION", err, status.Code(err))
	}
	if err := fencedCreate(ctx, cli, id, shard, 1, 4); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("lower sequence: got %v (code=%s), want FAILED_PRECONDITION", err, status.Code(err))
	}
}

// TestConformance_FencingNewEpochResetsSequence: a restarted shard's new
// epoch starts a fresh sequence space — a low sequence_number under a
// higher epoch must be accepted even though the prior epoch's sequence
// ran far ahead.
func TestConformance_FencingNewEpochResetsSequence(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)
	shard := fenceShardID("epoch-reset")
	if err := fencedCreate(ctx, cli, id, shard, 1, 1000); err != nil {
		t.Fatalf("establish high-water mark: %v", err)
	}
	// Idempotent on (machine_id, target=Idle), so the only failure mode
	// here is a provider wrongly requiring cross-epoch seq monotonicity.
	if err := fencedCreate(ctx, cli, id, shard, 2, 1); err != nil {
		t.Errorf("new epoch with low sequence must be accepted: %v", err)
	}
}

// TestConformance_FencingReadsUnaffected: Get and List carry no fencing
// token by design (their request messages have no token fields — reads
// don't fence) and keep working for any caller no matter what the
// mutation-side high-water marks say.
func TestConformance_FencingReadsUnaffected(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)
	shard := fenceShardID("reads")
	if err := fencedCreate(ctx, cli, id, shard, 9, 9); err != nil {
		t.Fatalf("establish high-water mark: %v", err)
	}
	// A fenced-out mutation right before the reads, so a provider that
	// wrongly entangles read paths with the fence state would surface it.
	if err := fencedCreate(ctx, cli, id, shard, 1, 1); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale token: got %v (code=%s), want FAILED_PRECONDITION", err, status.Code(err))
	}
	if _, err := cli.Get(ctx, &pb.MachineRef{Id: id}); err != nil {
		t.Errorf("Get after fenced mutation: %v", err)
	}
	if _, err := cli.List(ctx, &pb.ListFilter{}); err != nil {
		t.Errorf("List after fenced mutation: %v", err)
	}
}
