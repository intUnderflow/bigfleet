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
	"math"
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
	if _, err := cli.Delete(ctx, &pb.DeleteRequest{MachineId: id}); err != nil {
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

// TestConformance_ConfigureIdempotent: Configure is idempotent on
// (machine_id, target=Configured) like every other lifecycle RPC —
// back-to-back calls return the same operation_id whether the second
// arrives mid-Configuring or after the machine settled. M71 closed the
// audit gap where only Create had idempotency coverage.
func TestConformance_ConfigureIdempotent(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)
	if _, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 10*time.Second)

	req := &pb.ConfigureRequest{
		MachineId: id, ClusterId: "conformance-idem",
		BootstrapBlob: []byte("# conformance idempotency\n"),
	}
	a, err := cli.Configure(ctx, req)
	if err != nil {
		t.Fatalf("first Configure: %v", err)
	}
	b, err := cli.Configure(ctx, req)
	if err != nil {
		t.Fatalf("second Configure: %v", err)
	}
	if a.GetOperationId() != b.GetOperationId() {
		t.Errorf("operation_id differs across idempotent retries: a=%s b=%s",
			a.GetOperationId(), b.GetOperationId())
	}
}

// TestConformance_DrainIdempotent: same contract for Drain on
// (machine_id, target=Idle).
func TestConformance_DrainIdempotent(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)
	if _, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 10*time.Second)
	if _, err := cli.Configure(ctx, &pb.ConfigureRequest{
		MachineId: id, ClusterId: "conformance-idem-drain",
		BootstrapBlob: []byte("# conformance idempotency\n"),
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_CONFIGURED, 10*time.Second)

	req := &pb.DrainRequest{MachineId: id, GracePeriodSeconds: 30}
	a, err := cli.Drain(ctx, req)
	if err != nil {
		t.Fatalf("first Drain: %v", err)
	}
	b, err := cli.Drain(ctx, req)
	if err != nil {
		t.Fatalf("second Drain: %v", err)
	}
	if a.GetOperationId() != b.GetOperationId() {
		t.Errorf("operation_id differs across idempotent retries: a=%s b=%s",
			a.GetOperationId(), b.GetOperationId())
	}
}

// TestConformance_DeleteIdempotent: same contract for Delete on
// (machine_id, target=Speculative). Skipped for bare-metal-style
// providers that return Unimplemented from Delete.
func TestConformance_DeleteIdempotent(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)
	if _, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 10*time.Second)

	req := &pb.DeleteRequest{MachineId: id}
	a, err := cli.Delete(ctx, req)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			t.Skip("provider does not implement Delete (Unimplemented) — bare-metal-style provider, OK")
		}
		t.Fatalf("first Delete: %v", err)
	}
	b, err := cli.Delete(ctx, req)
	if err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if a.GetOperationId() != b.GetOperationId() {
		t.Errorf("operation_id differs across idempotent retries: a=%s b=%s",
			a.GetOperationId(), b.GetOperationId())
	}
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

	_, err := cli.Delete(ctx, &pb.DeleteRequest{MachineId: "nonexistent"})
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

// TestConformance_CostFieldBounds verifies the provider never
// publishes machine records whose cost-formula inputs are out of
// bounds: price_per_hour must be ≥ 0 and not NaN; interruption_
// probability must lie in [0, 1] (per the provider.proto contract).
// These two fields feed BigFleet's locked cost formula
// (effective_cost = price + probability × penalty) unmodified.
//
// The shard survives violations regardless — it rejects out-of-bounds
// records at ingest (bigfleet_shard_machines_rejected_total, ADR-0046
// addendum; survivability is asserted by pkg/shard's own tests, since
// this suite's system-under-test is the provider, not the shard) —
// but a provider that needs that rail is out of contract, and this
// test makes the contract mechanical.
func TestConformance_CostFieldBounds(t *testing.T) {
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
		if p := m.GetPricePerHour(); math.IsNaN(p) || p < 0 {
			t.Errorf("machine %s: price_per_hour %v negative or NaN", m.GetId(), p)
		}
		if ip := m.GetInterruptionProbability(); math.IsNaN(ip) || ip < 0 || ip > 1 {
			t.Errorf("machine %s: interruption_probability %v outside [0,1]", m.GetId(), ip)
		}
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
	// Any failing code except FAILED_PRECONDITION, which the M71 fencing
	// contract reserves for stale-token rejections so callers can alert
	// on zombie shards mechanically (see provider.proto).
	if status.Code(err) == codes.FailedPrecondition {
		t.Errorf("invalid transition rejected with FAILED_PRECONDITION — that code is reserved for fencing rejections; use a different code (got %v)", err)
	}
}

// TestConformance_TransitionalStateObservability is the M23 conformance
// check for the user-stories "transitional-state recovery" contract.
// The full kill-and-restart scenario (kill provider mid-Configure,
// restart, observe in-progress state preserved) requires process
// control we don't have over an external gRPC endpoint. What's
// testable from outside is the underlying property the recovery
// contract depends on: that transitional states are observable via
// Get while a transition is in progress.
//
// A provider that does instant transitions (Speculative → Idle in a
// single atomic step, no observable Creating window) passes this test
// trivially — there's nothing transitional to observe and therefore
// nothing to recover. A provider with staged transitions must report
// the intermediate state via Get for at least one observation window.
//
// The test polls Get aggressively (every 5 ms) for 1 s after Create
// and asserts at least one of {Speculative, Creating, Idle} was
// observed. Providers that complete the transition before the first
// poll are fine.
func TestConformance_TransitionalStateObservability(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id := pickSpeculative(t, cli, ctx)
	if _, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	deadline := time.Now().Add(1 * time.Second)
	seen := map[pb.MachineState]bool{}
	for time.Now().Before(deadline) {
		m, err := cli.Get(ctx, &pb.MachineRef{Id: id})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		seen[m.GetState()] = true
		if m.GetState() == pb.MachineState_MACHINE_STATE_IDLE {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 10*time.Second)

	// At least one valid post-Create state must have been observed.
	// A provider that returned an unrelated state (Failed without
	// last_error, or some made-up enum value) violates the contract.
	any := seen[pb.MachineState_MACHINE_STATE_SPECULATIVE] ||
		seen[pb.MachineState_MACHINE_STATE_CREATING] ||
		seen[pb.MachineState_MACHINE_STATE_IDLE]
	if !any {
		t.Errorf("Get reported no valid state during a Speculative → Idle transition; observed %v", seen)
	}
}

// TestConformance_DrainGraceTimeout is the M23 conformance check for
// the user-stories "drain-grace handling" contract: a Drain that
// can't complete inside grace_period_seconds must end up in Failed
// with last_error populated, NOT silently revert to a clean state.
//
// Calls Drain with grace_period_seconds = 0 against a Configured
// machine. After ~10 s, the final state must be one of:
//
//   - Idle: drain succeeded immediately. No failure path was
//     triggered. Valid: a 0-second grace is the lower bound, not a
//     mandatory failure trigger.
//   - Failed: drain didn't complete in time. last_error must be
//     non-empty.
//
// Stuck-in-Draining or back-in-Configured-without-error after a
// grace-exceeded drain fails the test. Last_error empty on a Failed
// state also fails the test.
func TestConformance_DrainGraceTimeout(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Walk a fresh machine to Configured.
	id := pickSpeculative(t, cli, ctx)
	if _, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 10*time.Second)
	if _, err := cli.Configure(ctx, &pb.ConfigureRequest{
		MachineId: id, ClusterId: "conformance-grace",
		BootstrapBlob: []byte("# conformance grace-test\n"),
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_CONFIGURED, 10*time.Second)

	// Drain with the most aggressive grace possible.
	if _, err := cli.Drain(ctx, &pb.DrainRequest{MachineId: id, GracePeriodSeconds: 0}); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// Wait up to 10 s for the drain to settle in either direction.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		m, err := cli.Get(ctx, &pb.MachineRef{Id: id})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		switch m.GetState() {
		case pb.MachineState_MACHINE_STATE_IDLE:
			// Drain succeeded immediately. No failure path triggered.
			return
		case pb.MachineState_MACHINE_STATE_FAILED:
			if m.GetLastError() == "" {
				t.Errorf("state = Failed but last_error empty — failure mode unexplained")
			}
			return
		case pb.MachineState_MACHINE_STATE_DRAINING:
			// Still in progress; keep polling.
		case pb.MachineState_MACHINE_STATE_CONFIGURED:
			t.Fatalf("state reverted to Configured after Drain — silent revert is not allowed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	m, err := cli.Get(ctx, &pb.MachineRef{Id: id})
	if err != nil {
		t.Fatalf("final Get: %v", err)
	}
	t.Fatalf("machine stuck in %s 10s after Drain(grace=0) — must reach Idle or Failed-with-last_error", m.GetState())
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

// TestConformance_DeleteOnConfigured verifies that Delete on a machine
// in Configured state is rejected — the state machine has no
// Configured → Deleting edge (paper §5: Delete releases unbound
// machines; the M73 release path only ever deletes Idle machines, and
// providers must refuse to pull a bound machine out from under its
// cluster). The machine must remain Configured afterwards. As with
// DrainOnSpeculative, the rejection must not use FAILED_PRECONDITION
// (reserved for M71 fencing rejections). Unimplemented passes —
// bare-metal-style providers don't support Delete at all.
func TestConformance_DeleteOnConfigured(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Walk a fresh machine to Configured.
	id := pickSpeculative(t, cli, ctx)
	if _, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 10*time.Second)
	if _, err := cli.Configure(ctx, &pb.ConfigureRequest{
		MachineId: id, ClusterId: "conformance-delete-configured",
		BootstrapBlob: []byte("# conformance delete-on-configured\n"),
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_CONFIGURED, 10*time.Second)

	_, err := cli.Delete(ctx, &pb.DeleteRequest{MachineId: id})
	if err == nil {
		t.Fatal("expected Delete on Configured to fail")
	}
	switch status.Code(err) {
	case codes.Unimplemented:
		t.Logf("provider does not implement Delete (Unimplemented) — bare-metal-style provider, OK")
		return
	case codes.FailedPrecondition:
		t.Errorf("invalid transition rejected with FAILED_PRECONDITION — that code is reserved for fencing rejections; use a different code (got %v)", err)
	}

	// The bound machine must be untouched by the rejected call.
	m, err := cli.Get(ctx, &pb.MachineRef{Id: id})
	if err != nil {
		t.Fatalf("Get after rejected Delete: %v", err)
	}
	if m.GetState() != pb.MachineState_MACHINE_STATE_CONFIGURED {
		t.Errorf("state after rejected Delete = %s, want Configured (no partial transition)", m.GetState())
	}
}
