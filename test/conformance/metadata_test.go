//go:build conformance

package conformance_test

import (
	"context"
	"maps"
	"testing"
	"time"

	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// The M72 contract under test in this file: ConfigureRequest.shard_metadata
// is STORE AND ECHO, NEVER INTERPRET. The provider keeps the map verbatim
// with the machine, echoes it on every Get/List snapshot for as long as the
// binding exists, and clears it — together with `cluster` — when a Drain
// completes back to Idle. The map is the only durable copy of BigFleet's
// assignment state (a restarted shard rebuilds preemption protection from
// it), so a provider that drops, rewrites, or filters keys silently
// removes workload protection fleet-wide.

// configureToConfigured walks a fresh Speculative machine to Configured
// for the given cluster with the given metadata, returning its id.
func configureToConfigured(t *testing.T, cli pb.CapacityProviderClient, ctx context.Context, cluster string, md map[string]string) string {
	t.Helper()
	id := pickSpeculative(t, cli, ctx)
	if _, err := cli.Create(ctx, &pb.CreateRequest{MachineId: id}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 10*time.Second)
	if _, err := cli.Configure(ctx, &pb.ConfigureRequest{
		MachineId:     id,
		ClusterId:     cluster,
		BootstrapBlob: []byte("# conformance metadata\n"),
		ShardMetadata: md,
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_CONFIGURED, 10*time.Second)
	return id
}

// TestConformance_MetadataEchoedOnGetAndList: shard_metadata sent on
// Configure comes back byte-for-byte on both read RPCs, and the Machine's
// `cluster` field reports the binding Configure established.
func TestConformance_MetadataEchoedOnGetAndList(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	md := map[string]string{
		"bigfleet.lucy.sh/assigned-priority": "900000",
		// ADR-0051 (M77g): the gang attribution rides as a store-and-echo
		// key alongside the rest; a provider must echo it verbatim like any
		// other so a restarted shard rebuilds the Same-domain attribution.
		"bigfleet.lucy.sh/assigned-group": "topology.bigfleet/rack\x00gang-7",
		"x-conformance/opaque":            "echo-me",
	}
	id := configureToConfigured(t, cli, ctx, "conformance-md", md)

	got, err := cli.Get(ctx, &pb.MachineRef{Id: id})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.GetCluster() != "conformance-md" {
		t.Errorf("Get cluster = %q, want conformance-md", got.GetCluster())
	}
	if !maps.Equal(got.GetShardMetadata(), md) {
		t.Errorf("Get shard_metadata = %v, want verbatim %v", got.GetShardMetadata(), md)
	}

	resp, err := cli.List(ctx, &pb.ListFilter{
		States: []pb.MachineState{pb.MachineState_MACHINE_STATE_CONFIGURED},
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	found := false
	for _, m := range resp.GetMachines() {
		if m.GetId() != id {
			continue
		}
		found = true
		if m.GetCluster() != "conformance-md" {
			t.Errorf("List cluster = %q, want conformance-md", m.GetCluster())
		}
		if !maps.Equal(m.GetShardMetadata(), md) {
			t.Errorf("List shard_metadata = %v, want verbatim %v", m.GetShardMetadata(), md)
		}
	}
	if !found {
		t.Errorf("List(CONFIGURED) did not return machine %s", id)
	}
}

// TestConformance_MetadataAndBindingClearOnDrain pins the M72 lifecycle
// decision: shard_metadata is per-assignment state, attached to the
// binding Configure creates — so when Drain completes and the machine
// returns to Idle unbound (paper §5), the metadata clears WITH the
// cluster. A provider that lets it linger would hand a dead workload's
// priority/penalties to whatever shard rebuilds inventory before the
// machine's next assignment, resurrecting stale preemption protection.
func TestConformance_MetadataAndBindingClearOnDrain(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	id := configureToConfigured(t, cli, ctx, "conformance-md-drain", map[string]string{
		"bigfleet.lucy.sh/assigned-priority": "1000000",
	})
	if _, err := cli.Drain(ctx, &pb.DrainRequest{MachineId: id, GracePeriodSeconds: 5}); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	mustReachState(t, cli, ctx, id, pb.MachineState_MACHINE_STATE_IDLE, 30*time.Second)

	got, err := cli.Get(ctx, &pb.MachineRef{Id: id})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.GetCluster() != "" {
		t.Errorf("cluster survived Drain: %q (Idle must be unbound)", got.GetCluster())
	}
	if len(got.GetShardMetadata()) != 0 {
		t.Errorf("shard_metadata survived Drain: %v (per-assignment state clears with the binding)", got.GetShardMetadata())
	}
}

// TestConformance_MetadataUnknownKeysPreservedVerbatim: the provider must
// not whitelist keys it recognises — every entry, including ones no
// BigFleet version it knows about ever wrote, comes back byte-for-byte.
// Empty values are values too.
func TestConformance_MetadataUnknownKeysPreservedVerbatim(t *testing.T) {
	cli, close := dial(t)
	defer close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	md := map[string]string{
		"x-conformance/definitely-unknown": "παντα ρει  \t|0xDEADBEEF",
		"x-conformance/empty-value":        "",
	}
	id := configureToConfigured(t, cli, ctx, "conformance-md-unknown", md)

	got, err := cli.Get(ctx, &pb.MachineRef{Id: id})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !maps.Equal(got.GetShardMetadata(), md) {
		t.Errorf("unknown keys not preserved verbatim: got %v, want %v", got.GetShardMetadata(), md)
	}
}
