// Command bigfleetctl is the M15 admin CLI for the BigFleet
// coordinator. It wraps the unary admin RPCs defined in
// api/proto/.../coordinator.proto so a platform engineer can do
// the Mode-2 work in docs/user-stories.md without writing one-off
// Go code or restarting the coordinator with new bootstrap state.
//
// Usage:
//
//	bigfleetctl --coordinator=host:7790 list-shards
//	bigfleetctl --coordinator=host:7790 list-domain-assignments
//	bigfleetctl --coordinator=host:7790 assign-domain --key=rack --value=r-1 --shard=shard-2
//	bigfleetctl --coordinator=host:7790 unassign-domain --key=rack --value=r-1
//	bigfleetctl --coordinator=host:7790 remove-shard --shard=shard-2
//	bigfleetctl --coordinator=host:7790 snapshot save backup.snap
//	bigfleetctl snapshot restore --data-dir=/var/lib/bigfleet --node-id=bigfleet-coordinator-0 \
//	    --raft-advertise=bigfleet-coordinator-0.bigfleet-coordinator.bigfleet-system.svc:7791 backup.snap
//
// v1 ships unauthenticated — the coordinator is a cluster-internal
// service. Exposing it externally requires a sidecar (mTLS / OIDC).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/intUnderflow/bigfleet/pkg/coordinator"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "bigfleetctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	root := flag.NewFlagSet("bigfleetctl", flag.ContinueOnError)
	root.SetOutput(os.Stderr)
	addr := root.String("coordinator", "127.0.0.1:7790", "host:port of the coordinator's gRPC service")
	timeout := root.Duration("timeout", 10*time.Second, "per-RPC timeout")
	if err := root.Parse(args); err != nil {
		return err
	}
	rest := root.Args()
	if len(rest) == 0 {
		return fmt.Errorf("subcommand required: list-shards | list-domain-assignments | list-quotas | assign-domain | unassign-domain | remove-shard | snapshot")
	}

	// `snapshot restore` is offline — it rewrites a STOPPED
	// coordinator's data dir and must not require a reachable
	// coordinator (the whole point is that there isn't one).
	if rest[0] == "snapshot" && len(rest) > 1 && rest[1] == "restore" {
		return cmdSnapshotRestore(rest[2:])
	}

	conn, err := grpc.NewClient(*addr,
		append(grpcutil.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		return fmt.Errorf("dial coordinator: %w", err)
	}
	defer func() { _ = conn.Close() }()
	cli := pb.NewCoordinatorClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	sub := rest[0]
	subArgs := rest[1:]
	switch sub {
	case "list-shards":
		return cmdListShards(ctx, cli)
	case "list-domain-assignments":
		return cmdListDomainAssignments(ctx, cli)
	case "list-quotas":
		return cmdListQuotas(ctx, cli)
	case "assign-domain":
		return cmdAssignDomain(ctx, cli, subArgs)
	case "unassign-domain":
		return cmdUnassignDomain(ctx, cli, subArgs)
	case "remove-shard":
		return cmdRemoveShard(ctx, cli, subArgs)
	case "snapshot":
		if len(subArgs) == 0 {
			return fmt.Errorf("snapshot subcommand required: save | restore")
		}
		switch subArgs[0] {
		case "save":
			// Deliberately not bound to --timeout: streaming the
			// snapshot takes as long as the state is large.
			return cmdSnapshotSave(context.Background(), cli, subArgs[1:])
		default:
			return fmt.Errorf("unknown snapshot subcommand %q (want save | restore)", subArgs[0])
		}
	default:
		return fmt.Errorf("unknown subcommand %q", sub)
	}
}

// cmdSnapshotSave streams the leader's freshest Raft snapshot and
// writes it as a single-file archive (M75 DR, ADR-0047).
func cmdSnapshotSave(ctx context.Context, cli pb.CoordinatorClient, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: snapshot save <file>")
	}
	out := args[0]

	stream, err := cli.SnapshotSave(ctx, &pb.SnapshotSaveRequest{})
	if err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("snapshot save: %w", err)
	}
	metaJSON := first.GetMetaJson()
	if len(metaJSON) == 0 {
		return fmt.Errorf("snapshot save: server did not send meta as the first frame")
	}

	// Spool state chunks to a temp file: the tar header needs the
	// payload size before the payload.
	tmp, err := os.CreateTemp(filepath.Dir(out), ".bigfleet-snapshot-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("snapshot save: %w", err)
		}
		if _, err := tmp.Write(chunk.GetData()); err != nil {
			return err
		}
	}
	if err := coordinator.WriteSnapshotArchive(out, metaJSON, tmp.Name()); err != nil {
		return err
	}

	var meta raft.SnapshotMeta
	if err := json.Unmarshal(metaJSON, &meta); err != nil {
		return fmt.Errorf("snapshot save: decode meta: %w", err)
	}
	fmt.Printf("saved snapshot index=%d term=%d to %s\n", meta.Index, meta.Term, out)
	return nil
}

// cmdSnapshotRestore rebuilds a STOPPED coordinator's data dir from a
// saved archive (or a snapshot-export directory). Offline by design:
// see pkg/coordinator.RestoreSnapshot for the hashicorp/raft restore
// semantics (single-voter configuration; peers re-join afterwards).
func cmdSnapshotRestore(args []string) error {
	fs := flag.NewFlagSet("snapshot restore", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dataDir := fs.String("data-dir", "", "the stopped coordinator's data dir to rebuild")
	nodeID := fs.String("node-id", "", "Raft server ID of the replica being restored (its pod name, e.g. bigfleet-coordinator-0)")
	raftAdvertise := fs.String("raft-advertise", "", "Raft transport address the restored replica advertises, e.g. bigfleet-coordinator-0.bigfleet-coordinator.<ns>.svc:7791")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: snapshot restore --data-dir=<dir> --node-id=<id> --raft-advertise=<host:port> <file-or-export-dir>")
	}
	if *dataDir == "" || *nodeID == "" || *raftAdvertise == "" {
		return fmt.Errorf("--data-dir, --node-id, and --raft-advertise are all required")
	}

	meta, state, err := coordinator.OpenSnapshotArchive(fs.Arg(0))
	if err != nil {
		return err
	}
	defer func() { _ = state.Close() }()
	if err := coordinator.RestoreSnapshot(*dataDir, *nodeID, *raftAdvertise, meta, state); err != nil {
		return err
	}
	fmt.Printf("restored snapshot index=%d term=%d into %s as single-voter %s\n", meta.Index, meta.Term, *dataDir, *nodeID)
	fmt.Println("start this replica first; wipe the other replicas' data dirs so they re-join the new cluster (docs/operator-guide.md → Disaster recovery)")
	return nil
}

func cmdListShards(ctx context.Context, cli pb.CoordinatorClient) error {
	resp, err := cli.ListShards(ctx, &pb.ListShardsRequest{})
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "SHARD\tADDRESS\tREGISTERED\tLAST_HEARTBEAT")
	for _, s := range resp.GetShards() {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			s.GetShardId(),
			s.GetAddress(),
			fmtUnix(s.GetRegisteredAtUnixNs()),
			fmtUnix(s.GetLastHeartbeatUnixNs()),
		)
	}
	return tw.Flush()
}

func cmdListDomainAssignments(ctx context.Context, cli pb.CoordinatorClient) error {
	resp, err := cli.ListDomainAssignments(ctx, &pb.ListDomainAssignmentsRequest{})
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TOPOLOGY_KEY\tTOPOLOGY_VALUE\tSHARD")
	for _, a := range resp.GetAssignments() {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", a.GetTopologyKey(), a.GetTopologyValue(), a.GetShardId())
	}
	return tw.Flush()
}

func cmdListQuotas(ctx context.Context, cli pb.CoordinatorClient) error {
	resp, err := cli.ListQuotas(ctx, &pb.ListQuotasRequest{})
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "PROVIDER\tREGION\tSHARD\tSLICE")
	for _, a := range resp.GetAllocations() {
		// Stable per-shard ordering for human-readable output.
		shards := make([]string, 0, len(a.GetPerShard()))
		for sh := range a.GetPerShard() {
			shards = append(shards, sh)
		}
		sort.Strings(shards)
		for _, sh := range shards {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n", a.GetProvider(), a.GetRegion(), sh, a.GetPerShard()[sh])
		}
	}
	return tw.Flush()
}

func cmdAssignDomain(ctx context.Context, cli pb.CoordinatorClient, args []string) error {
	fs := flag.NewFlagSet("assign-domain", flag.ContinueOnError)
	key := fs.String("key", "", "topology key (e.g. rack, zone, region)")
	value := fs.String("value", "", "topology value (e.g. r-1, us-east-1a)")
	shard := fs.String("shard", "", "shard id to bind the domain to")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *key == "" || *value == "" || *shard == "" {
		return fmt.Errorf("--key, --value, and --shard are all required")
	}
	if _, err := cli.AssignDomain(ctx, &pb.AssignDomainRequest{
		TopologyKey:   *key,
		TopologyValue: *value,
		ShardId:       *shard,
	}); err != nil {
		return err
	}
	fmt.Printf("assigned %s=%s → %s\n", *key, *value, *shard)
	return nil
}

func cmdUnassignDomain(ctx context.Context, cli pb.CoordinatorClient, args []string) error {
	fs := flag.NewFlagSet("unassign-domain", flag.ContinueOnError)
	key := fs.String("key", "", "topology key")
	value := fs.String("value", "", "topology value")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *key == "" || *value == "" {
		return fmt.Errorf("--key and --value are required")
	}
	if _, err := cli.UnassignDomain(ctx, &pb.UnassignDomainRequest{
		TopologyKey:   *key,
		TopologyValue: *value,
	}); err != nil {
		return err
	}
	fmt.Printf("unassigned %s=%s\n", *key, *value)
	return nil
}

func cmdRemoveShard(ctx context.Context, cli pb.CoordinatorClient, args []string) error {
	fs := flag.NewFlagSet("remove-shard", flag.ContinueOnError)
	shard := fs.String("shard", "", "shard id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *shard == "" {
		return fmt.Errorf("--shard is required")
	}
	if _, err := cli.RemoveShard(ctx, &pb.RemoveShardRequest{ShardId: *shard}); err != nil {
		return err
	}
	fmt.Printf("removed shard %s\n", *shard)
	return nil
}

func fmtUnix(ns int64) string {
	if ns == 0 {
		return "—"
	}
	return time.Unix(0, ns).UTC().Format(time.RFC3339)
}
