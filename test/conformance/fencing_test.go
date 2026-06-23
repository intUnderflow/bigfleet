//go:build conformance

// Fencing conformance (paper §11, M71). Every mutating RPC carries a
// (shard_id, shard_epoch, sequence_number) token; the provider keeps a
// per-(shard_id, machine_id) high-water mark of accepted tokens and MUST
// reject a token that is not strictly newer with FAILED_PRECONDITION — that
// is what stops a zombie shard from actuating a stale view of the fleet. The
// mark is per (shard, machine), NOT per shard: a single live shard's
// concurrent execute pool draws monotonic sequence numbers but races the
// sends, so a per-shard mark would fence the shard against its own
// out-of-order arrivals on different machines (a false zombie). A true
// zombie still fails on the shard's epoch (strictly lower), per machine.
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

// TestConformance_FencingStaleSequenceRejected: within the same epoch and
// on the same machine, a sequence_number at or below the high-water mark is
// rejected.
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

// TestConformance_FencingPerMachineIsolation: the high-water mark is per
// (shard_id, machine_id), not per shard. On ONE shard, a high mark
// established on machine A must NOT fence a LOWER-sequence token aimed at a
// DIFFERENT machine B — the out-of-order arrival a shard's concurrent
// execute pool produces. Each machine keeps its own monotonicity (B's own
// stale replay is still rejected), and a true zombie (lower epoch) is still
// rejected per machine.
func TestConformance_FencingPerMachineIsolation(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := cli.List(ctx, &pb.ListFilter{
		States:     []pb.MachineState{pb.MachineState_MACHINE_STATE_SPECULATIVE},
		MaxResults: 2,
	})
	if err != nil {
		t.Fatalf("List speculative: %v", err)
	}
	if len(resp.GetMachines()) < 2 {
		t.Skip("conformance: need 2 Speculative machines for per-machine fence isolation; seed at least 2 and re-run")
	}
	mA := resp.GetMachines()[0].GetId()
	mB := resp.GetMachines()[1].GetId()
	shard := fenceShardID("per-machine")

	// Machine A established at a HIGH sequence (a worker that won the send race).
	if err := fencedCreate(ctx, cli, mA, shard, 1, 30); err != nil {
		t.Fatalf("establish high mark on machine A: %v", err)
	}
	// Same shard, same epoch, LOWER sequence, DIFFERENT machine — must be
	// accepted. A per-shard mark would wrongly reject this as a zombie.
	if err := fencedCreate(ctx, cli, mB, shard, 1, 7); err != nil {
		t.Errorf("lower-sequence token on a different machine of the same shard must be accepted (per-(shard,machine) fence): %v", err)
	}
	// Machine B keeps its own monotonicity: replaying its mark is rejected.
	if err := fencedCreate(ctx, cli, mB, shard, 1, 7); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("machine B replay: got %v (code=%s), want FAILED_PRECONDITION", err, status.Code(err))
	}
	// Machine A's mark is intact and independent of B's: A's stale replay is rejected.
	if err := fencedCreate(ctx, cli, mA, shard, 1, 30); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("machine A replay: got %v (code=%s), want FAILED_PRECONDITION", err, status.Code(err))
	}
	// A true zombie (strictly lower epoch) is still rejected, per machine.
	if err := fencedCreate(ctx, cli, mB, shard, 0, 999); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("stale-epoch zombie on machine B: got %v (code=%s), want FAILED_PRECONDITION", err, status.Code(err))
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
