//go:build integration

// Real async-provider integration: a real shard drives a real provider OVER
// gRPC through the full provision -> configure -> drain lifecycle, with the
// provider behaving ASYNCHRONOUSLY (every mutating RPC returns a transitional
// ack; the terminal state is reached out-of-band and observed only via the
// shard's reconcile loop). This is the path the synchronous in-process fake
// masks — and the path that surfaced three production bugs one ADR at a time:
//
//   - ADR-0057: the shard must learn an async-provider's terminal Configured
//     (reached via reconcile, not the worker) — here, provisioned machines
//     reach Configured only because reconcile finalizes the async transition.
//   - ADR-0058: a shard's concurrent execute pool stamps monotonic fence
//     sequence numbers but races the sends; a per-shard fence high-water mark
//     would brick machines as false zombies. Run with ExecuteConcurrency > 1
//     over real gRPC; assert no machine lands in Failed.
//   - ADR-0059: an async drain returns a Draining ack; the terminal Idle is
//     finalized via reconcile, releasing capacity. Drop demand and assert the
//     surplus actually drains.
//
// One test that would have caught all three, and guards the class going
// forward. It deliberately uses the real gRPC transport (grpcclient ->
// grpcadapter) and the fake's staged async modes, NOT the synchronous
// in-process path.
package integration_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	providerfake "github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/provider/grpcadapter"
	"github.com/intUnderflow/bigfleet/pkg/provider/grpcclient"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

func asyncProfile() machine.Profile {
	return machine.Profile{
		InstanceType: "async-it",
		Zone:         "zone-a",
		CapacityType: machine.CapacityTypeOnDemand,
		Resources:    map[string]string{"cpu": "4"},
	}
}

// asyncNeed demands `machines` async-it machines' worth of cpu (one Need row,
// aggregated; MinUnit one machine).
func asyncNeed(cluster machine.ClusterID, machines int, prio int32) needs.Need {
	return needs.Need{
		ClusterID: cluster,
		Profile: needs.NewProfile([]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"async-it"},
		}}, nil, prio, needs.PenaltyBucket8, needs.PenaltyBucket8),
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: strconv.Itoa(machines * 4)}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
	}
}

func countClusterState(sh *shard.Shard, cluster machine.ClusterID, state machine.State) int {
	return len(sh.Inventory().Snapshot().ListByClusterState(cluster, state))
}

func countState(sh *shard.Shard, state machine.State) int {
	return len(sh.Inventory().Snapshot().ListByState(state))
}

// waitFor polls cond until true or the deadline; fails with msg + the last
// observed snapshot summary on timeout.
func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string, dump func() string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s (after %s); state: %s", msg, d, dump())
}

func TestAsyncProvider_ProvisionConfigureDrainOverGRPC(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		cluster = machine.ClusterID("cluster-a")
		want    = 12 // machines to provision + configure
		keep    = 2  // demand after the drop; want-keep drain
	)

	// A REAL async provider: every mutating RPC returns a transitional ack
	// (Creating/Configuring/Draining) and reaches its terminal state only
	// when the out-of-band completer fires. Seed Speculative slots with slack
	// for Phase 1 to provision + bootstrap; the shard discovers them via
	// reconcile (List over gRPC), not SeedInventory.
	prov := providerfake.New(providerfake.Options{
		InstantTransitions: true,
		CreateStaged:       true,
		ConfigureStaged:    true,
		DrainStaged:        true,
	})
	for i := 0; i < want*2; i++ {
		prov.AddSpeculative(machine.ID(fmt.Sprintf("spec-%d", i)), asyncProfile(), machine.CapacityTypeOnDemand, 1.0, 0.05)
	}

	// Serve the provider over real gRPC.
	gsrv := grpc.NewServer()
	pb.RegisterCapacityProviderServer(gsrv, grpcadapter.New(prov))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(gsrv.Stop)

	// The shard dials the provider over gRPC (this is the path that stamps
	// fence tokens), with ExecuteConcurrency > 1 so the sends race.
	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	cli, err := grpcclient.New(lis.Addr().String(), grpcclient.Identity{ShardID: "shard-async", Epoch: epoch}, grpcutil.TLSConfig{})
	if err != nil {
		t.Fatalf("grpcclient.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	sh, err := shard.New(shard.Config{
		ID:                 "shard-async",
		Epoch:              epoch,
		Provider:           cli,
		CycleInterval:      50 * time.Millisecond,
		BootstrapTimeout:   2 * time.Second,
		ExecuteConcurrency: 8, // the ADR-0058 concurrency that raced the fence
		LocalBootstrap: func(context.Context, machine.ClusterID, []needs.Requirement) ([]byte, error) {
			return []byte("# async integration test\n"), nil
		},
	})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	go func() { _ = sh.Run(ctx) }()

	// The provider's async work completes out-of-band, on its own ticker —
	// the shard's reconcile loop is the only thing that observes it.
	go func() {
		tk := time.NewTicker(20 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tk.C:
				prov.CompleteAllStaged()
			}
		}
	}()

	dump := func() string {
		s := sh.Inventory().Snapshot()
		return fmt.Sprintf("configured=%d idle=%d speculative=%d draining=%d creating=%d configuring=%d failed=%d",
			countClusterState(sh, cluster, machine.StateConfigured),
			countState(sh, machine.StateIdle), countState(sh, machine.StateSpeculative),
			countState(sh, machine.StateDraining), countState(sh, machine.StateCreating),
			countState(sh, machine.StateConfiguring), len(s.ListByState(machine.StateFailed)))
	}

	// --- Provision + async configure (ADR-0057 reconcile-finalize; ADR-0058
	// concurrency must not self-fence). ---
	sh.ApplyRollup(cluster, []needs.Need{asyncNeed(cluster, want, 1000)})
	waitFor(t, 30*time.Second, func() bool {
		return countClusterState(sh, cluster, machine.StateConfigured) >= want
	}, fmt.Sprintf("demand for %d machines did not reach Configured via async provision+configure over gRPC", want), dump)

	// No machine may have Failed: a per-shard fence (ADR-0058 pre-fix) would
	// brick concurrent out-of-order arrivals as false zombies; any provider
	// transition error also lands here.
	if n := countState(sh, machine.StateFailed); n != 0 {
		t.Fatalf("ADR-0058: %d machines Failed during concurrent async provision (false-zombie fencing or transition error); state: %s", n, dump())
	}

	// --- Async drain (ADR-0059): drop demand; the surplus must drain to a
	// terminal unbound state via the reconcile-finalized Draining->Idle path,
	// releasing capacity. Wait for the drain to FULLY finalize — Configured
	// down to the new demand AND nothing left mid-drain — so the assertion
	// can't race the in-flight Draining->Idle completion.
	sh.ApplyRollup(cluster, []needs.Need{asyncNeed(cluster, keep, 1000)})
	waitFor(t, 30*time.Second, func() bool {
		return countClusterState(sh, cluster, machine.StateConfigured) <= keep &&
			countState(sh, machine.StateDraining) == 0
	}, fmt.Sprintf("surplus capacity did not finish draining after demand dropped to %d (async drain stuck in Draining?)", keep), dump)

	// The drain must not have bricked anything (a post-Drain transition that
	// tripped the Draining-must-have-a-cluster invariant — the ADR-0059 bug —
	// would leave Failed machines or strand them in Draining, both caught above).
	if n := countState(sh, machine.StateFailed); n != 0 {
		t.Fatalf("ADR-0059: %d machines Failed during async drain; state: %s", n, dump())
	}
}
