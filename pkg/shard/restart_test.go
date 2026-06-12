package shard

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/provider/grpcadapter"
	"github.com/intUnderflow/bigfleet/pkg/provider/grpcclient"
)

// TestShardRestart_RebuildsProtectionStateOverWire is the M72
// end-to-end: a shard restart against a real (out-of-process) provider
// rebuilds inventory from List+Get, and the rebuilt Configured machines
// keep their cluster binding and Assigned* protection state instead of
// being rejected as structural (the M70b tripwire) or silently zeroed
// (production-readiness audit, arc 2: "restart zeroes all preemption
// attribution"). Runs the real wire path — fake behind grpcadapter,
// shard on grpcclient — because the in-process path never lost anything;
// the proto round trip was the gap.
func TestShardRestart_RebuildsProtectionStateOverWire(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	prov := fake.New(fake.Options{InstantTransitions: true})
	prov.AddIdle("m-1", machine.Profile{
		InstanceType: "p5.48xlarge",
		Zone:         "us-east-1a",
		Resources:    map[string]string{"cpu": "96"},
	}, machine.CapacityTypeOnDemand, 6.0, 0.05)

	srv := grpc.NewServer()
	pb.RegisterCapacityProviderServer(srv, grpcadapter.New(prov))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	epochPath := filepath.Join(t.TempDir(), "epoch")
	newShard := func() *Shard {
		epoch, err := fencing.LoadEpoch(epochPath) // each load = one process restart
		if err != nil {
			t.Fatalf("LoadEpoch: %v", err)
		}
		cli, err := grpcclient.New(lis.Addr().String(), grpcclient.Identity{ShardID: "shard-restart", Epoch: epoch}, grpcutil.TLSConfig{})
		if err != nil {
			t.Fatalf("grpcclient.New: %v", err)
		}
		t.Cleanup(func() { _ = cli.Close() })
		s, err := New(Config{
			ID:               "shard-restart",
			Epoch:            epoch,
			Provider:         cli,
			CycleInterval:    50 * time.Millisecond,
			BootstrapTimeout: 2 * time.Second,
			LocalBootstrap: func(context.Context, machine.ClusterID, []needs.Requirement) ([]byte, error) {
				return []byte("# test bootstrap\n"), nil
			},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return s
	}

	// Process 1: ingest the Idle machine over the wire and configure it.
	s1 := newShard()
	if err := s1.reconcile(ctx); err != nil {
		t.Fatalf("s1 reconcile: %v", err)
	}
	profile := needs.NewProfile(nil, nil, 900_000, needs.PenaltyBucket8192, needs.PenaltyBucket64)
	if err := s1.execute(ctx, decision.Action{
		Kind:          decision.ActionKindBootstrap,
		MachineID:     "m-1",
		Cluster:       "c-1",
		SourceProfile: &profile,
	}); err != nil {
		t.Fatalf("s1 execute bootstrap: %v", err)
	}
	before, err := s1.inv.Get("m-1")
	if err != nil {
		t.Fatalf("s1 inventory get: %v", err)
	}
	if before.State != machine.StateConfigured || before.AssignedPriority != 900_000 {
		t.Fatalf("precondition: s1 machine = %+v, want Configured @ priority 900000", before)
	}

	// Process 2: fresh shard (empty memory, bumped epoch), same provider.
	s2 := newShard()
	if err := s2.reconcile(ctx); err != nil {
		t.Fatalf("s2 reconcile: %v", err)
	}
	got, err := s2.inv.Get("m-1")
	if err != nil {
		// Pre-M72 failure mode: the wire-borne Configured record lost its
		// cluster, failed machine.Invariant, and was rejected at ingest.
		t.Fatalf("rebuilt inventory is missing the Configured machine (M70b structural rejection?): %v", err)
	}
	if got.State != machine.StateConfigured {
		t.Errorf("State = %s, want Configured", got.State)
	}
	if got.Cluster != "c-1" {
		t.Errorf("Cluster = %q, want c-1", got.Cluster)
	}
	if got.AssignedPriority != before.AssignedPriority {
		t.Errorf("AssignedPriority = %d, want %d", got.AssignedPriority, before.AssignedPriority)
	}
	if got.AssignedInterruptionPenaltyDollars != before.AssignedInterruptionPenaltyDollars {
		t.Errorf("AssignedInterruptionPenaltyDollars = %v, want %v",
			got.AssignedInterruptionPenaltyDollars, before.AssignedInterruptionPenaltyDollars)
	}
	if got.AssignedReclamationPenaltyDollars != before.AssignedReclamationPenaltyDollars {
		t.Errorf("AssignedReclamationPenaltyDollars = %v, want %v",
			got.AssignedReclamationPenaltyDollars, before.AssignedReclamationPenaltyDollars)
	}
	if got.AssignedNeedFingerprint != profile.Fingerprint() {
		t.Errorf("AssignedNeedFingerprint = %q, want %q", got.AssignedNeedFingerprint, profile.Fingerprint())
	}
	// The verbatim echo is decoded then dropped — the hot-path record
	// must not carry the map (see Machine.ShardMetadata).
	if got.ShardMetadata != nil {
		t.Errorf("inventory retained the shard_metadata map: %v", got.ShardMetadata)
	}
}

// TestApplyReconciledMachine_RejectsClusterlessConfigured pins the M70b
// tripwire's surviving half: M72 makes healthy wire-borne Configured
// records pass ingest, but a genuinely cluster-less Configured record is
// still provider garbage and must still be rejected, not repaired.
func TestApplyReconciledMachine_RejectsClusterlessConfigured(t *testing.T) {
	t.Parallel()
	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	s, err := New(Config{
		ID:       "shard-test",
		Epoch:    epoch,
		Provider: fake.New(fake.Options{}),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.applyReconciledMachine(machine.Machine{
		ID:      "m-bad",
		State:   machine.StateConfigured,
		Host:    machine.HostRef{Provider: "fake", Ref: "m-bad"},
		Cluster: "", // the structural violation
		Profile: machine.Profile{InstanceType: "p5"},
	})
	if _, err := s.inv.Get("m-bad"); err == nil {
		t.Error("cluster-less Configured record was ingested; it must stay rejected")
	}
}
