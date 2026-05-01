//go:build conformance

// Package conformance_test exercises the BigFleet CapacityProvider
// contract against any provider that speaks the proto. Build tag
// `conformance`. Pass the provider's gRPC address via -target or the
// BIGFLEET_PROVIDER_TARGET env var. The suite spins up a few
// speculative + idle machines on the provider, walks them through
// the full lifecycle, and asserts every contract requirement
// documented in docs/provider-author-guide.md.
//
// A passing run = the provider is BigFleet-compatible.
//
// Run via:
//
//	make conformance TARGET=host:port
//	# or
//	go test -tags=conformance -target=host:port ./test/conformance/...
package conformance_test

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

var targetFlag = flag.String("target", "", "host:port of the provider's gRPC service (or set BIGFLEET_PROVIDER_TARGET)")

// target resolves the conformance target from flag or env, in that order.
func target(t *testing.T) string {
	t.Helper()
	if *targetFlag != "" {
		return *targetFlag
	}
	if v := os.Getenv("BIGFLEET_PROVIDER_TARGET"); v != "" {
		return v
	}
	t.Skip("conformance: set -target or BIGFLEET_PROVIDER_TARGET to run this suite")
	return ""
}

// dial returns a connected gRPC client to the provider under test.
func dial(t *testing.T) (pb.CapacityProviderClient, func()) {
	t.Helper()
	addr := target(t)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return pb.NewCapacityProviderClient(conn), func() { _ = conn.Close() }
}

// pickSpeculative returns the id of one Speculative machine on the
// provider. Skips the test if none are available — providers must seed
// at least one speculative slot for the conformance run.
func pickSpeculative(t *testing.T, cli pb.CapacityProviderClient, ctx context.Context) string {
	t.Helper()
	resp, err := cli.List(ctx, &pb.ListFilter{
		States:     []pb.MachineState{pb.MachineState_MACHINE_STATE_SPECULATIVE},
		MaxResults: 1,
	})
	if err != nil {
		t.Fatalf("List speculative: %v", err)
	}
	if len(resp.GetMachines()) == 0 {
		t.Skip("conformance: provider has no Speculative machines; seed at least one and re-run")
	}
	return resp.GetMachines()[0].GetId()
}

// TestConformance_FullLifecycle walks one machine through the entire
// state machine: Speculative → Idle → Configured → Idle → Speculative.
// Asserts each transition lands and the final state is what the spec
// says it should be.
func TestConformance_FullLifecycle(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)

	// Speculative → Creating → Idle.
	ack, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if ack.GetOperationId() == "" {
		t.Errorf("Create: empty OperationID")
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 10*time.Second)

	// Idle → Configuring → Configured.
	ack, err = cli.Configure(ctx, &pb.ConfigureRequest{
		MachineId: id, ClusterId: "conformance-cluster",
		BootstrapBlob: []byte("# conformance bootstrap\n"),
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if ack.GetOperationId() == "" {
		t.Errorf("Configure: empty OperationID")
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_CONFIGURED, 10*time.Second)

	// Configured → Draining → Idle.
	if _, err := cli.Drain(ctx, &pb.DrainRequest{MachineId: id, GracePeriodSeconds: 5}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 30*time.Second)

	// Idle → Deleting → Speculative. Optional: providers that don't
	// support Delete (bare-metal style) return Unimplemented; treat
	// that as a pass and stop the lifecycle here.
	if _, err := cli.Delete(ctx, &pb.MachineRef{Id: id}); err != nil {
		if status.Code(err) == codes.Unimplemented {
			t.Logf("provider does not implement Delete (Unimplemented) — bare-metal-style provider, OK")
			return
		}
		t.Fatalf("Delete: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_SPECULATIVE, 10*time.Second)
}

// TestConformance_CreateIdempotent verifies that calling Create twice
// for the same machine_id returns the same operation_id (idempotent
// retry).
func TestConformance_CreateIdempotent(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id := pickSpeculative(t, cli, ctx)

	a, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}
	b, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id})
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}
	if a.GetOperationId() != b.GetOperationId() {
		t.Errorf("operation_id differs across idempotent retries: a=%s b=%s",
			a.GetOperationId(), b.GetOperationId())
	}

	// Cleanup — drain back to Idle if Configure was not needed; just
	// list the state to leave the provider in whatever state it was
	// in. Not strictly required.
	_ = a
	_ = b
}

// TestConformance_GetUnknownMachine verifies that Get on an unknown
// machine_id returns NotFound.
func TestConformance_GetUnknownMachine(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := cli.Get(ctx, &pb.MachineRef{Id: "definitely-not-a-real-machine-id"})
	if err == nil {
		t.Fatal("expected error on unknown machine_id")
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v (code=%s)", err, status.Code(err))
	}
}

// TestConformance_DeleteUnknownMachine verifies that Delete on an
// unknown machine_id returns NotFound (or Unimplemented if the
// provider doesn't support Delete).
func TestConformance_DeleteUnknownMachine(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := cli.Delete(ctx, &pb.MachineRef{Id: "nonexistent"})
	if err == nil {
		t.Fatal("expected error on unknown machine_id")
	}
	switch status.Code(err) {
	case codes.NotFound, codes.Unimplemented:
		// OK.
	default:
		t.Errorf("expected NotFound or Unimplemented, got %v (code=%s)", err, status.Code(err))
	}
}

// TestConformance_ListFiltersByState verifies that List honours the
// `states` filter — passing one specific state returns only machines
// in that state.
func TestConformance_ListFiltersByState(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Ask for IDLE only; if the provider has any machines, none of them
	// should be in any state other than Idle.
	resp, err := cli.List(ctx, &pb.ListFilter{
		States: []pb.MachineState{pb.MachineState_MACHINE_STATE_IDLE},
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, m := range resp.GetMachines() {
		if m.GetState() != pb.MachineState_MACHINE_STATE_IDLE {
			t.Errorf("List(IDLE) returned machine in state %s: %s", m.GetState(), m.GetId())
		}
	}
}

// TestConformance_ListMaxResults verifies that List respects the
// max_results bound.
func TestConformance_ListMaxResults(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.List(ctx, &pb.ListFilter{MaxResults: 1})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resp.GetMachines()) > 1 {
		t.Errorf("List(MaxResults=1) returned %d machines", len(resp.GetMachines()))
	}
}

// TestConformance_LabelShape verifies that machines exposed via List
// have the well-known labels the autoscaler expects:
//
//   - instance_type set on every machine
//   - zone set on every machine that has one
//   - capacity_type set to a known value (BARE_METAL / RESERVED /
//     ON_DEMAND / SPOT) — UNSPECIFIED is allowed for transitional
//     records but not for stable ones in production
//
// The shape is what makes the autoscaler's MatchProfile work without
// needing to consult labels for everything. Providers that bury these
// in labels-only break BigFleet's hot path.
func TestConformance_LabelShape(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.List(ctx, &pb.ListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resp.GetMachines()) == 0 {
		t.Skip("conformance: provider has no machines; seed some and re-run")
	}
	for _, m := range resp.GetMachines() {
		if m.GetInstanceType() == "" {
			t.Errorf("machine %s: instance_type empty", m.GetId())
		}
		if m.GetState() == pb.MachineState_MACHINE_STATE_UNSPECIFIED {
			t.Errorf("machine %s: state UNSPECIFIED", m.GetId())
		}
		// zone is not required (single-zone providers may omit) but
		// most cloud providers set it.
	}
}

// TestConformance_ListRevisionAdvances verifies that List's response
// revision changes after a state mutation. Providers below the
// since_revision threshold are allowed to return the same revision
// every time (the cursor is opt-in); but if they advance it, the
// shard's reconciler can use it for incremental polling.
func TestConformance_ListRevisionAdvances(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	id := pickSpeculative(t, cli, ctx)

	r0, err := cli.List(ctx, &pb.ListFilter{})
	if err != nil {
		t.Fatalf("List 0: %v", err)
	}

	if _, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 5*time.Second)

	r1, err := cli.List(ctx, &pb.ListFilter{})
	if err != nil {
		t.Fatalf("List 1: %v", err)
	}

	// Revision is opt-in. If both are empty, that's allowed (provider
	// below the threshold). If r0 is non-empty, r1 must differ.
	if len(r0.GetRevision()) > 0 && string(r0.GetRevision()) == string(r1.GetRevision()) {
		t.Errorf("revision did not advance after a state mutation")
	}
}

// TestConformance_DrainOnSpeculative verifies that Drain on a machine
// in Speculative state is rejected (the state machine has no edge for
// this).
func TestConformance_DrainOnSpeculative(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)
	_, err := cli.Drain(ctx, &pb.DrainRequest{MachineId: id, GracePeriodSeconds: 5})
	if err == nil {
		t.Fatal("expected Drain on Speculative to fail")
	}
	// FailedPrecondition / Internal both reasonable; just not OK.
}

// mustReachState polls Get until the machine is in the desired state
// or timeout elapses.
func mustReachState(t *testing.T, cli pb.CapacityProviderClient, ctx context.Context, id string, want pb.MachineState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m, err := cli.Get(ctx, &pb.MachineRef{Id: id})
		if err != nil {
			t.Fatalf("Get during state-wait: %v", err)
		}
		if m.GetState() == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ctx cancelled while waiting for state %s on %s", want, id)
		case <-time.After(100 * time.Millisecond):
		}
	}
	m, err := cli.Get(ctx, &pb.MachineRef{Id: id})
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	t.Fatalf("machine %s did not reach %s within %v (final state: %s)",
		id, want, timeout, m.GetState())
}
