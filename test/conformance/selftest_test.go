//go:build conformance

package conformance_test

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	providerfake "github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/provider/grpcadapter"
)

func insecureCreds() credentials.TransportCredentials { return insecure.NewCredentials() }

// TestConformance_SelfTest_OnFake spins up pkg/provider/fake behind
// the gRPC adapter on a random localhost port and runs the
// conformance suite against it via a child `go test` invocation.
//
// This proves the conformance suite is itself self-consistent and
// keeps the fake honest against the contract.
func TestConformance_SelfTest_OnFake(t *testing.T) {
	if testing.Short() {
		t.Skip("conformance self-test: skipped under -short")
	}
	// When the suite is invoked with an explicit -target (e.g. by the
	// child process this test spawns, or by the user pointing at a
	// real provider), don't recurse — let the child run only the
	// contract tests that consume -target.
	if *targetFlag != "" {
		t.Skip("self-test: skipping when -target is set (child invocation or external provider run)")
	}
	prov := providerfake.New(providerfake.Options{InstantTransitions: true})
	// Seed enough Speculative slots for every conformance test that
	// asks for one (lifecycle, idempotency ×4, fencing ×5, drain-on-
	// spec, revision-advances). Each subtest asks for one and may
	// consume it.
	for i := 0; i < 32; i++ {
		prov.AddSpeculative(
			machine.ID("conf-spec-"+strconv.Itoa(i)),
			machine.Profile{
				InstanceType: "conformance-instance",
				Zone:         "us-east-1a",
				Resources:    map[string]string{"nvidia.com/gpu": "1"},
			},
			machine.CapacityTypeOnDemand, 1.0, 0.0,
		)
	}

	srv := grpc.NewServer()
	pb.RegisterCapacityProviderServer(srv, grpcadapter.New(prov))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	addr := lis.Addr().String()

	// Sanity: the suite can talk to the adapter.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecureCreds()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	cli := pb.NewCapacityProviderClient(conn)
	if _, err := cli.List(ctx, &pb.ListFilter{}); err != nil {
		t.Fatalf("List on self-test fake: %v", err)
	}

	// Run the conformance suite as a child go test process pointed at
	// our local server. Skips itself (otherwise we'd recurse).
	// SelfTest skips itself when -target is set (above), so we can
	// safely run all TestConformance_ tests in the child. -v
	// surfaces the per-test verdicts so a flaky/skipped test is
	// visible in the parent's logs rather than hidden behind a
	// blanket "ok".
	cmd := exec.Command("go", "test",
		"-tags=conformance",
		"-count=1",
		"-v",
		"-run", "^TestConformance_",
		"-target="+addr,
		"./test/conformance/...",
	)
	out, err := cmd.CombinedOutput()
	t.Logf("child output:\n%s", out)
	if err != nil {
		t.Fatalf("conformance suite against fake failed: %v", err)
	}
}
