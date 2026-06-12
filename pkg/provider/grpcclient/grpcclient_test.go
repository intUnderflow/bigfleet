package grpcclient_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/provider/grpcadapter"
	"github.com/intUnderflow/bigfleet/pkg/provider/grpcclient"
)

// startServer exposes a fake provider over a real gRPC listener — the
// same topology as production (client in the shard process, provider out
// of process), minus the network.
func startServer(t *testing.T) (string, *fake.Provider) {
	t.Helper()
	prov := fake.New(fake.Options{InstantTransitions: true})
	srv := grpc.NewServer()
	pb.RegisterCapacityProviderServer(srv, grpcadapter.New(prov))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), prov
}

func newEpoch(t *testing.T, restarts int) *fencing.Epoch {
	t.Helper()
	path := filepath.Join(t.TempDir(), "epoch")
	var e *fencing.Epoch
	for i := 0; i < restarts; i++ {
		var err error
		e, err = fencing.LoadEpoch(path)
		if err != nil {
			t.Fatalf("LoadEpoch: %v", err)
		}
	}
	return e
}

func newClient(t *testing.T, addr, shardID string, epoch *fencing.Epoch) *grpcclient.Client {
	t.Helper()
	c, err := grpcclient.New(addr, grpcclient.Identity{ShardID: shardID, Epoch: epoch}, grpcutil.TLSConfig{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestClient_LifecycleRoundTrip drives all six RPCs through the wire and
// checks the fencing stamps don't trip the fake's own enforcement (every
// call mints a fresh sequence number, so consecutive mutations from one
// client are always strictly newer).
func TestClient_LifecycleRoundTrip(t *testing.T) {
	addr, prov := startServer(t)
	prov.AddSpeculative("m-1", machine.Profile{InstanceType: "p5", Zone: "z-a"}, machine.CapacityTypeOnDemand, 6.0, 0.1)
	c := newClient(t, addr, "shard-0", newEpoch(t, 1))
	ctx := context.Background()

	ack, err := c.Create(ctx, provider.CreateRequest{MachineID: "m-1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if ack.OperationID == "" || ack.Machine.State != machine.StateIdle {
		t.Fatalf("Create ack = %+v, want op id + Idle", ack)
	}
	if ack.Machine.PricePerHour != 6.0 || ack.Machine.InterruptionProbability != 0.1 {
		t.Errorf("cost fields lost on the wire: %+v", ack.Machine)
	}

	if _, err := c.Configure(ctx, provider.ConfigureRequest{MachineID: "m-1", ClusterID: "c-1", BootstrapBlob: []byte("blob")}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	got, err := c.Get(ctx, "m-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != machine.StateConfigured {
		t.Errorf("Get state = %s, want Configured", got.State)
	}

	if _, err := c.Drain(ctx, provider.DrainRequest{MachineID: "m-1", GracePeriod: 30}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if _, err := c.Delete(ctx, provider.DeleteRequest{MachineID: "m-1"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	list, err := c.List(ctx, provider.ListFilter{States: []machine.State{machine.StateSpeculative}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Machines) != 1 || list.Machines[0].ID != "m-1" {
		t.Errorf("List = %+v, want m-1 back in Speculative", list.Machines)
	}
}

// TestClient_ZombieEpochFenced is the paper §11 scenario end-to-end: a
// client whose epoch predates the one the provider has already seen for
// the same shard_id is rejected with ErrFenced, and the original gRPC
// status survives the wrap.
func TestClient_ZombieEpochFenced(t *testing.T) {
	addr, prov := startServer(t)
	prov.AddSpeculative("m-1", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 6.0, 0.0)
	prov.AddSpeculative("m-2", machine.Profile{InstanceType: "p5"}, machine.CapacityTypeOnDemand, 6.0, 0.0)
	ctx := context.Background()

	current := newClient(t, addr, "shard-0", newEpoch(t, 2)) // epoch 2
	zombie := newClient(t, addr, "shard-0", newEpoch(t, 1))  // epoch 1

	if _, err := current.Create(ctx, provider.CreateRequest{MachineID: "m-1"}); err != nil {
		t.Fatalf("current epoch Create: %v", err)
	}
	_, err := zombie.Create(ctx, provider.CreateRequest{MachineID: "m-2"})
	if !errors.Is(err, provider.ErrFenced) {
		t.Fatalf("zombie Create: got %v, want ErrFenced", err)
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("gRPC status lost in wrap: code=%s, want FailedPrecondition", status.Code(err))
	}
	// The zombie's stale view must not have actuated anything.
	if got, err := current.Get(ctx, "m-2"); err != nil || got.State != machine.StateSpeculative {
		t.Errorf("m-2 = %+v (err=%v), want untouched Speculative", got, err)
	}
}

// TestClient_ErrorMapping: the sentinel errors pkg/shard matches on are
// re-attached client-side, with the provider's message preserved for the
// string-reading execute classifier.
func TestClient_ErrorMapping(t *testing.T) {
	addr, _ := startServer(t)
	c := newClient(t, addr, "shard-0", newEpoch(t, 1))
	ctx := context.Background()

	if _, err := c.Get(ctx, "missing"); !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("Get(missing): got %v, want ErrNotFound", err)
	}
	_, err := c.Delete(ctx, provider.DeleteRequest{MachineID: "missing"})
	if !errors.Is(err, provider.ErrNotFound) {
		t.Errorf("Delete(missing): got %v, want ErrNotFound", err)
	}
	if status.Code(err) != codes.NotFound {
		t.Errorf("gRPC status lost in wrap: code=%s, want NotFound", status.Code(err))
	}
}

// TestClient_IdentityValidation: construction fails fast on a missing
// fencing identity — a shard that can't fence must not reach a provider.
func TestClient_IdentityValidation(t *testing.T) {
	epoch := newEpoch(t, 1)
	if _, err := grpcclient.New("", grpcclient.Identity{ShardID: "s", Epoch: epoch}, grpcutil.TLSConfig{}); err == nil {
		t.Error("empty addr accepted")
	}
	if _, err := grpcclient.New("127.0.0.1:1", grpcclient.Identity{Epoch: epoch}, grpcutil.TLSConfig{}); err == nil {
		t.Error("empty ShardID accepted")
	}
	if _, err := grpcclient.New("127.0.0.1:1", grpcclient.Identity{ShardID: "s"}, grpcutil.TLSConfig{}); err == nil {
		t.Error("nil Epoch accepted")
	}
}
