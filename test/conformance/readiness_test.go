//go:build conformance

package conformance_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	providerfake "github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/provider/grpcadapter"
)

// TestConformance_NodeReadiness_ADR0056 is the ADR-0056 (Option A) check: a
// provider MUST NOT report a machine CONFIGURED until it has observed the node
// Ready on its target cluster. Until then the machine stays CONFIGURING; on
// timeout it goes FAILED. Reporting CONFIGURED on VM-boot, before the node
// joins, produces phantom capacity — the shard credits coverage that isn't
// schedulable and shortfalls read zero.
//
// IMPORTANT — why this is a reference-fake test, not a black-box -target test:
// the six-RPC contract carries no node-readiness ground-truth signal, so over
// the wire a provider that reports CONFIGURED on boot is indistinguishable from
// one that waits for Ready. The property therefore CANNOT be enforced black-box
// against an arbitrary -target provider. What is verifiable in-tree is that the
// conformance reference implementation (pkg/provider/fake) honours the gate: it
// holds CONFIGURING until a readiness signal and only then reports CONFIGURED.
// The fake models "provider observed the node Ready" as CompleteStaged on a
// ConfigureStaged machine. Full verification of a real provider is that
// provider's own against-a-cluster integration test; providers built on
// providerkit inherit the gate centrally (providerkit ReadinessChecker).
//
// Skips when -target is set: it builds and drives its own in-process fake, so
// it is meaningless against an external provider, and we must not run it in the
// child (-target) invocation that TestConformance_SelfTest_OnFake spawns.
func TestConformance_NodeReadiness_ADR0056(t *testing.T) {
	if *targetFlag != "" {
		t.Skip("ADR-0056 readiness gate: reference-fake test; not black-box verifiable against -target (see test doc)")
	}

	// ConfigureStaged holds the machine at Configuring after Configure,
	// modelling a provider that has actuated the host but has not yet
	// observed the node Ready on its target cluster.
	prov := providerfake.New(providerfake.Options{InstantTransitions: true, ConfigureStaged: true})
	const id = machine.ID("adr0056-m0")
	prov.AddSpeculative(id, machine.Profile{
		InstanceType: "conformance-instance",
		Zone:         "us-east-1a",
		Resources:    map[string]string{"nvidia.com/gpu": "1"},
	}, machine.CapacityTypeOnDemand, 1.0, 0.0)

	srv := grpc.NewServer()
	pb.RegisterCapacityProviderServer(srv, grpcadapter.New(prov))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	cli := pb.NewCapacityProviderClient(conn)

	// Speculative → Idle (instant).
	if _, err := cli.Create(ctx, &pb.CreateRequest{MachineId: string(id)}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	mustReachState(t, cli, ctx, string(id), pb.MachineState_MACHINE_STATE_IDLE, 5*time.Second)

	// Configure: the host is actuated but the node has not been observed Ready,
	// so the machine must land at — and stay at — CONFIGURING.
	if _, err := cli.Configure(ctx, &pb.ConfigureRequest{
		MachineId:     string(id),
		ClusterId:     "adr0056-cluster",
		BootstrapBlob: []byte("# adr-0056 readiness gate\n"),
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}

	// Gate assertion: while readiness is unobserved, CONFIGURED must never
	// appear. CONFIGURED here would be the phantom-capacity bug ADR-0056 closes.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		m, err := cli.Get(ctx, &pb.MachineRef{Id: string(id)})
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		switch m.GetState() {
		case pb.MachineState_MACHINE_STATE_CONFIGURED:
			t.Fatalf("ADR-0056 violated: machine reported CONFIGURED before the node was observed Ready")
		case pb.MachineState_MACHINE_STATE_CONFIGURING:
			// correct — still waiting on readiness
		default:
			t.Fatalf("expected CONFIGURING while readiness unobserved, got %s", m.GetState())
		}
		time.Sleep(50 * time.Millisecond)
	}

	// The provider observes the node Ready on its target cluster → only now may
	// the machine advance to CONFIGURED.
	if !prov.CompleteStaged(id) {
		t.Fatalf("CompleteStaged: expected the staged Configuring machine to advance on readiness")
	}
	mustReachState(t, cli, ctx, string(id), pb.MachineState_MACHINE_STATE_CONFIGURED, 5*time.Second)
}
