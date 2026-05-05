package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"math/rand"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
	"github.com/intUnderflow/bigfleet/pkg/shard"
	"github.com/intUnderflow/bigfleet/pkg/shard/coordclient"
)

// parseStatefulSetOrdinal extracts the numeric suffix from a
// StatefulSet pod name (e.g. "bigfleet-shard-2" → 2). Used by the
// Configured-seed path to know which slice of the kwok cluster space
// this shard owns under the harness's `c % shardReplicas` mapping.
func parseStatefulSetOrdinal(podName string) (int, error) {
	for i := len(podName) - 1; i >= 0; i-- {
		if podName[i] == '-' {
			suffix := podName[i+1:]
			if suffix == "" {
				return 0, fmt.Errorf("empty ordinal suffix")
			}
			n, err := strconv.Atoi(suffix)
			if err != nil {
				return 0, fmt.Errorf("ordinal suffix %q: %w", suffix, err)
			}
			return n, nil
		}
	}
	return 0, fmt.Errorf("no '-' separator in pod name")
}

// seedFakeInventory mints n synthetic idle machines into the in-process
// fake provider AND seeds them into the shard's inventory so Phase 1
// can pick from them on the first cycle. Used by the scale-test
// harness to populate the shard with a realistic-shape pool without a
// real provider.
//
// Spread: round-robin across 5 instance types × 3 zones × bare-metal
// capacity type. Profile fingerprints are stable so the per-fingerprint
// pool cache (M11.16) sees real diversity instead of one giant bucket.
//
// nIdle is added as Speculative-then-Idle (the default Phase-1
// candidate pool — fresh headroom).
//
// Configured seed (M28): if both clusterStride > 0 and
// nConfiguredPerCluster > 0, the seed enumerates only the kwok
// cluster IDs the harness's deterministic
// `kwok-cluster-N → bigfleet-shard-(N % shardReplicas)` mapping
// routes to THIS shard. shardOrdinal is the shard's index within its
// StatefulSet (parsed from --id, e.g. "bigfleet-shard-2" → 2);
// clusterStride is the total shardReplicas count. The seed iterates
// cluster IDs c ∈ [0, totalClusters) where c % clusterStride ==
// shardOrdinal — the same set the kwok pods would dial. Configured
// machines created this way have demand-bearing cluster bindings
// the harness's load-driver actually targets, so Phase 3 sees them
// as "wanted" machines (no reclaim) and Phase 2 sees them as
// potential preemption victims if a higher-priority demand arrives.
//
// totalClusters is the harness's kwok.clusterCount (NOT divided by
// shardReplicas). Setting any of {nConfiguredPerCluster, clusterStride,
// totalClusters, shardOrdinal} to its zero value disables the
// Configured seed.
func seedFakeInventory(prov *fake.Provider, sh *shard.Shard, nIdle, nConfiguredPerCluster, totalClusters, clusterStride, shardOrdinal int, archetypes []archetype.Archetype, logger *slog.Logger) {
	types := []string{"a3-highgpu-8g", "m6i.large", "c6i.4xlarge", "n2-standard-32", "r6i.xlarge"}
	zones := []string{"zone-a", "zone-b", "zone-c"}
	resources := map[string]map[string]string{
		"a3-highgpu-8g":  {"nvidia.com/gpu": "8"},
		"m6i.large":      {"cpu": "2", "memory": "8Gi"},
		"c6i.4xlarge":    {"cpu": "16", "memory": "32Gi"},
		"n2-standard-32": {"cpu": "32", "memory": "128Gi"},
		"r6i.xlarge":     {"cpu": "4", "memory": "32Gi"},
	}

	logger.Info("seeding fake inventory",
		"idle", nIdle,
		"configured_per_cluster", nConfiguredPerCluster,
		"total_clusters", totalClusters,
		"cluster_stride", clusterStride,
		"shard_ordinal", shardOrdinal,
	)
	for i := 0; i < nIdle; i++ {
		t := types[i%len(types)]
		z := zones[i%len(zones)]
		profile := machine.Profile{
			InstanceType: t,
			Zone:         z,
			CapacityType: machine.CapacityTypeBareMetal,
			Resources:    resources[t],
		}
		id := machine.ID("idle-" + strconv.Itoa(i))
		prov.AddIdle(id, profile, machine.CapacityTypeBareMetal, 0, 0)
		_ = sh.SeedInventory(machine.Machine{
			ID:      id,
			State:   machine.StateIdle,
			Profile: profile,
		})
	}

	configuredSeeded := 0
	if nConfiguredPerCluster > 0 && totalClusters > 0 && clusterStride > 0 {
		// Configured-seed: bound to "kwok-cluster-N" cluster IDs that
		// the harness's `N % shardReplicas` mapping routes to THIS
		// shard. Each owned cluster gets nConfiguredPerCluster machines.
		//
		// M31: when an archetype catalog is provided, machines are
		// distributed across archetypes proportional to weight. Profile
		// (instance-type, zone, resources) and assigned penalties come
		// from the archetype; AssignedPriority is the top of the
		// archetype's PriorityClasses (the seed represents established
		// workloads at the top of their priority tier — burst demand
		// at lower priorities can't preempt them, only equal-tier
		// demand competes for capacity). When the catalog is empty,
		// fall back to pre-M31 behaviour: every machine is
		// a3-highgpu-8g at priority 1000000. This keeps existing
		// scaleway-1m / scaleway-5m profiles working without
		// modification.
		picker := archetype.NewPicker(archetypes)
		rng := rand.New(rand.NewSource(int64(shardOrdinal) + 1))
		idx := 0
		for c := shardOrdinal; c < totalClusters; c += clusterStride {
			cluster := machine.ClusterID("kwok-cluster-" + strconv.Itoa(c))
			for i := 0; i < nConfiguredPerCluster; i++ {
				var profile machine.Profile
				var assignedPriority int32
				var interruptionPenalty, reclamationPenalty float64
				if a := picker.Pick(rng); a != nil {
					it := a.InstanceTypes[idx%len(a.InstanceTypes)]
					z := "zone-a"
					if len(a.Zones) > 0 {
						z = a.Zones[idx%len(a.Zones)]
					}
					profile = machine.Profile{
						InstanceType: it,
						Zone:         z,
						CapacityType: machine.CapacityTypeBareMetal,
						Resources:    a.Resources,
					}
					assignedPriority = a.MaxPriority()
					interruptionPenalty = a.InterruptionPenalty
					reclamationPenalty = a.ReclamationPenalty
				} else {
					const it = "a3-highgpu-8g"
					profile = machine.Profile{
						InstanceType: it,
						Zone:         zones[idx%len(zones)],
						CapacityType: machine.CapacityTypeBareMetal,
						Resources:    resources[it],
					}
					assignedPriority = 1000000
					interruptionPenalty = 8192
					reclamationPenalty = 65536
				}
				id := machine.ID(fmt.Sprintf("conf-s%d-c%d-i%d", shardOrdinal, c, i))
				prov.AddConfigured(id, profile, machine.CapacityTypeBareMetal, 0, 0, cluster, assignedPriority, interruptionPenalty, reclamationPenalty)
				_ = sh.SeedInventory(machine.Machine{
					ID:                                 id,
					State:                              machine.StateConfigured,
					Cluster:                            cluster,
					Profile:                            profile,
					AssignedPriority:                   assignedPriority,
					AssignedInterruptionPenaltyDollars: interruptionPenalty,
					AssignedReclamationPenaltyDollars:  reclamationPenalty,
				})
				idx++
				configuredSeeded++
			}
		}
	}
	logger.Info("seed complete", "idle", nIdle, "configured", configuredSeeded, "archetypes", len(archetypes))
}

// runShard runs the shard controller. The in-process fake provider
// lives at pkg/provider/fake and is used here for laptop / kind / dev
// installs only; production deployments dial an out-of-tree provider
// over gRPC via pkg/provider/grpcadapter (see provider-author-guide).
func runShard(args []string) error {
	fs := flag.NewFlagSet("shard", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	listen := fs.String("listen", ":7780", "address to listen on for the Shard.Session gRPC service")
	metricsAddr := fs.String("metrics-addr", ":8780", "address for the Prometheus /metrics endpoint (\"0\" disables)")
	shardID := fs.String("id", "shard-0", "this shard's stable identifier")
	dataDir := fs.String("data-dir", "./data", "directory for shard-local persistent state (epoch counter)")
	seedMachines := fs.Int("seed-machines", 0, "scaletest: pre-seed the in-process fake provider with N synthetic idle machines spread across instance types and zones; 0 disables")
	seedConfiguredPerCluster := fs.Int("seed-configured-per-cluster", 0, "scaletest M29: pre-seed the in-process fake provider with N synthetic Configured machines per kwok cluster owned by this shard (cluster IDs of the form kwok-cluster-{c} where c % --seed-cluster-stride == this shard's ordinal). Models the production-realistic shape where most fleet inventory is running workloads. Combined with --seed-cluster-total + --seed-cluster-stride.")
	seedClusterTotal := fs.Int("seed-cluster-total", 0, "scaletest M29: total number of kwok clusters across the whole harness (i.e. kwok.clusterCount). Used by the Configured-seed loop along with --seed-cluster-stride to pick the cluster IDs this shard owns.")
	seedClusterStride := fs.Int("seed-cluster-stride", 0, "scaletest M29: total number of shard replicas in the harness (i.e. shard.replicas). The seed enumerates clusters c where c % stride == this shard's ordinal. 0 disables the Configured seed.")
	archetypesPath := fs.String("archetypes", "", "scaletest M31: path to a workload-archetype catalog YAML. When set, the Configured seed distributes machines across archetypes weighted by Archetype.Weight (instance-type, zone, resources, priority and penalties from each archetype). When empty, the seed falls back to a single a3-highgpu-8g GPU shape (the legacy M29 behaviour). Both this flag and the load-driver's archetypes reference must point at the same file so demand and Configured match.")
	maxActionsPerCycle := fs.Int("max-actions-per-cycle", 0, "cap total decision actions executed per cycle so a ramp burst doesn't blow past the cycle SLO; 0 = unlimited (production default). Surplus actions roll into the next cycle.")
	executeConcurrency := fs.Int("execute-concurrency", 1, "max parallel action executors per cycle. 1 = serial (historical default). Bootstrap actions wait on per-cluster gRPC RTTs; raise for ramp-burst workloads.")
	localBootstrap := fs.Bool("local-bootstrap", false, "scaletest: render bootstrap blobs locally instead of round-tripping through the operator stream. Decouples shard cycle benchmarks from cluster-stream RTT. Production must leave this false.")
	incrementalReconcile := fs.Bool("incremental-reconcile", false, "opt into delta-only provider.List polling using the SinceRevision cursor. Off = full List every cycle (works for any provider). On = only enable for providers that honour since_revision (plan §10.6 above-conformance-threshold).")
	availableCapacityInterval := fs.Duration("available-capacity-interval", 0, "minimum interval between AvailableCapacityUpdate emits per (cluster, fingerprint). 0 = use the shard's default (5s). Below the cycle interval is wasteful (operator-side apiserver writes); much above 30s starts to feel stale to humans watching `kubectl get availablecapacity`.")
	metricsWarmupCycles := fs.Int("metrics-warmup-cycles", 0, "skip cycle-duration + per-phase histogram observations for the first N cycles. Cycle 1 of any shard does a one-time full provider.List that is not representative of steady-state cycle cost; skipping it lets p99 reflect what the SLO actually measures. Counters are not affected.")
	coordinatorAddr := fs.String("coordinator-addr", "", "host:port of the coordinator's gRPC service. When set, the shard heartbeats to ReportShard so it appears in coordinator membership and can have domains assigned to it. Empty disables — single-shard / dev runs without a coordinator stay unaffected.")
	advertiseAddr := fs.String("advertise-addr", "", "host:port the coordinator should record as this shard's dial address. Defaults to --listen; in StatefulSet deploys, set to the per-pod headless-Service DNS, e.g. bigfleet-shard-0.bigfleet-shard-headless:7780.")
	heartbeatInterval := fs.Duration("coordinator-heartbeat-interval", 10*time.Second, "how often to send ReportShard to the coordinator. Below ~5s starts spamming Raft applies on registration churn; above ~30s makes coordinator-side LastHeartbeat staleness alarms fire on healthy shards.")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errFlagParse, err)
	}
	if *listen == "" {
		return errors.New("--listen is required")
	}
	if *advertiseAddr == "" {
		*advertiseAddr = *listen
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("data-dir: %w", err)
	}
	epoch, err := fencing.LoadEpoch(filepath.Join(*dataDir, "epoch"))
	if err != nil {
		return err
	}

	// In-memory fake provider. M5 swaps this for a real gRPC client
	// adapter once an out-of-tree provider is available.
	prov := fake.New(fake.Options{InstantTransitions: true})

	cfg := shard.Config{
		ID:                        *shardID,
		Epoch:                     epoch,
		Provider:                  prov,
		Logger:                    logger,
		MaxActionsPerCycle:        *maxActionsPerCycle,
		ExecuteConcurrency:        *executeConcurrency,
		IncrementalReconcile:      *incrementalReconcile,
		AvailableCapacityInterval: *availableCapacityInterval,
		MetricsWarmupCycles:       *metricsWarmupCycles,
	}
	if *localBootstrap {
		cfg.LocalBootstrap = func(_ context.Context, cluster machine.ClusterID, _ []needs.Requirement) ([]byte, error) {
			return []byte("# bigfleet scaletest stub bootstrap for " + string(cluster) + "\n"), nil
		}
	}
	sh, err := shard.New(cfg)
	if err != nil {
		return err
	}

	configuredEnabled := *seedConfiguredPerCluster > 0 && *seedClusterTotal > 0 && *seedClusterStride > 0
	if *seedMachines > 0 || configuredEnabled {
		shardOrdinal := 0
		if configuredEnabled {
			ord, err := parseStatefulSetOrdinal(*shardID)
			if err != nil {
				return fmt.Errorf("--seed-configured-per-cluster requires --id ending in -N (got %q): %w", *shardID, err)
			}
			shardOrdinal = ord
		}
		var arches []archetype.Archetype
		if *archetypesPath != "" {
			cat, err := archetype.LoadCatalog(*archetypesPath)
			if err != nil {
				return fmt.Errorf("--archetypes: %w", err)
			}
			arches = cat.Archetypes
			logger.Info("archetype catalog loaded", "path", *archetypesPath, "count", len(arches))
		}
		seedFakeInventory(prov, sh, *seedMachines, *seedConfiguredPerCluster, *seedClusterTotal, *seedClusterStride, shardOrdinal, arches, logger)
	}

	srv := grpc.NewServer()
	pb.RegisterShardServer(srv, sh)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	logger.Info("listening", "addr", lis.Addr().String(), "shard_id", *shardID, "epoch", epoch.Value())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelSig, sigs := signalContext()
	defer cancelSig()

	errCh := make(chan error, 3)
	go func() { errCh <- sh.Run(ctx) }()
	go func() { errCh <- srv.Serve(lis) }()

	// Coordinator client: registers this shard with the coordinator
	// (carrying advertise-addr) and receives instructions back. Empty
	// --coordinator-addr disables; the shard keeps running cycles
	// against its existing inventory either way (static-stability
	// hard rule). Owned by coordclient — pkg/shard does not import
	// pkg/coordinator.
	if *coordinatorAddr != "" {
		cc, err := coordclient.New(coordclient.Config{
			CoordinatorAddress: *coordinatorAddr,
			AdvertiseAddress:   *advertiseAddr,
			View:               coordclient.ViewFromShard(sh),
			CoordinatorTerm:    sh.CoordinatorTerm(),
			ReportInterval:     *heartbeatInterval,
			Logger:             logger,
		})
		if err != nil {
			return fmt.Errorf("coordclient: %w", err)
		}
		go func() { errCh <- cc.Run(ctx) }()
	}
	if *metricsAddr != "0" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		metricsSrv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		logger.Info("metrics serving", "addr", *metricsAddr)
		go func() { errCh <- metricsSrv.ListenAndServe() }()
		defer func() { _ = metricsSrv.Shutdown(context.Background()) }()
	}

	select {
	case <-sigs:
		logger.Info("signal received, shutting down")
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("component exited", "err", err)
		}
	}

	cancel()
	srv.GracefulStop()
	return nil
}
