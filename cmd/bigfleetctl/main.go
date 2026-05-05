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
//
// v1 ships unauthenticated — the coordinator is a cluster-internal
// service. Exposing it externally requires a sidecar (mTLS / OIDC).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
		return fmt.Errorf("subcommand required: list-shards | list-domain-assignments | list-quotas | assign-domain | unassign-domain | remove-shard")
	}

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	default:
		return fmt.Errorf("unknown subcommand %q", sub)
	}
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
