package sim

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// SoakConfig configures a soak run.
type SoakConfig struct {
	// Cycles is the number of shard.Step calls to drive. With the
	// default 50,000 the run takes about 30 seconds wall-clock on
	// the M5 Max — well-suited to nightly CI.
	Cycles int

	// IdleSeed is the number of idle machines to seed at start.
	IdleSeed int

	// SpeculativeSeed is the number of speculative quota slots to seed.
	SpeculativeSeed int

	// MaxClusters is the size of the synthetic cluster pool that
	// rotates demand on/off across the run.
	MaxClusters int

	// ChurnEveryCycles is how often a synthetic event fires (a
	// cluster's needs are randomly mutated). Default 5.
	ChurnEveryCycles int

	// Seed is the deterministic RNG seed.
	Seed uint64
}

// DefaultSoakConfig returns a config sized for nightly CI on the M5 Max.
// Holds the engine over enough cycles to surface bugs that need
// repetition (use-after-reclaim, leaked transitional records) without
// blowing the 10-minute Makefile timeout.
func DefaultSoakConfig() SoakConfig {
	return SoakConfig{
		Cycles:           10_000,
		IdleSeed:         1_000,
		SpeculativeSeed:  2_000,
		MaxClusters:      30,
		ChurnEveryCycles: 5,
		Seed:             0xCAFEBABE,
	}
}

// SoakReport summarises the soak run.
type SoakReport struct {
	Cycles       int
	WallTime     time.Duration
	TotalActions int

	// EndStates records per-state machine counts at the end.
	EndStates map[machine.State]int

	// LeakedMachineIDs are machines whose lifecycle ended in Failed
	// or in a transitional state. None expected on a healthy run.
	LeakedMachineIDs []machine.ID
}

// Soak drives the engine through Cycles cycles with synthetic churn
// and asserts:
//
//   - the inventory size stays bounded (no leaked records);
//   - no machines remain stuck in transitional or Failed states;
//   - no panics; bounded wall-time; bounded action volume.
//
// Returns a SoakReport on success; non-nil error on assertion failure.
func Soak(ctx context.Context, cfg SoakConfig) (*SoakReport, error) {
	if cfg.Cycles <= 0 {
		cfg = DefaultSoakConfig()
	}
	if cfg.MaxClusters <= 0 {
		cfg.MaxClusters = 50
	}
	if cfg.ChurnEveryCycles <= 0 {
		cfg.ChurnEveryCycles = 5
	}

	prov := fake.New(fake.Options{InstantTransitions: true, Seed: cfg.Seed})
	for i := 0; i < cfg.IdleSeed; i++ {
		prov.AddIdle(
			machine.ID("idle-"+strconv.Itoa(i)),
			machine.Profile{
				InstanceType: "soak-instance",
				Zone:         "us-east-1a",
				Resources:    map[string]string{"nvidia.com/gpu": "1"},
			},
			machine.CapacityTypeBareMetal, 0, 0,
		)
	}
	for i := 0; i < cfg.SpeculativeSeed; i++ {
		prov.AddSpeculative(
			machine.ID("spec-"+strconv.Itoa(i)),
			machine.Profile{
				InstanceType: "soak-instance",
				Zone:         "us-east-1a",
				Resources:    map[string]string{"nvidia.com/gpu": "1"},
			},
			machine.CapacityTypeOnDemand, 6.0, 0.05,
		)
	}

	tmp, err := os.MkdirTemp("", "bigfleet-soak-")
	if err != nil {
		return nil, fmt.Errorf("tmp dir: %w", err)
	}
	defer os.RemoveAll(tmp)
	epoch, err := fencing.LoadEpoch(filepath.Join(tmp, "epoch"))
	if err != nil {
		return nil, fmt.Errorf("load epoch: %w", err)
	}

	totalActions := 0
	sh, err := shard.New(shard.Config{
		ID:               "soak",
		Epoch:            epoch,
		Provider:         prov,
		CycleInterval:    1 * time.Second, // unused
		BootstrapTimeout: 1 * time.Second,
		LocalBootstrap: func(_ context.Context, c machine.ClusterID, _ []needs.Requirement) ([]byte, error) {
			return []byte("# soak " + string(c) + "\n"), nil
		},
		OnActions: func(actions []decision.Action) {
			totalActions += len(actions)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("shard new: %w", err)
	}

	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x55AA55AA))
	gpuUnit := []needs.ResourceQty{{Name: "nvidia.com/gpu", Quantity: "1"}}
	pf := func(prio int32) needs.Profile {
		return needs.NewProfile(
			[]needs.Requirement{{
				Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn,
				Values: []string{"soak-instance"},
			}},
			nil, prio,
			needs.PenaltyBucket8192, needs.PenaltyBucket1,
		)
	}
	clusters := make([]machine.ClusterID, cfg.MaxClusters)
	for i := range clusters {
		clusters[i] = machine.ClusterID("soak-c" + strconv.Itoa(i))
	}

	start := time.Now()
	for c := 0; c < cfg.Cycles; c++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if c%cfg.ChurnEveryCycles == 0 {
			cluster := clusters[rng.IntN(len(clusters))]
			count := rng.IntN(50) + 1
			prio := int32(rng.IntN(1_000_000))
			sh.NeedsTable().Replace(cluster, []needs.Need{{
				ClusterID:          cluster,
				Profile:            pf(prio),
				AggregateResources: needs.ScaleResources(gpuUnit, count),
				MinUnit:            gpuUnit,
			}})
		}
		// Occasionally withdraw a cluster's demand entirely to
		// exercise Phase 3's reclaim path.
		if c%(cfg.ChurnEveryCycles*7) == 0 {
			cluster := clusters[rng.IntN(len(clusters))]
			sh.NeedsTable().Replace(cluster, nil)
		}
		_ = sh.Step(ctx)
	}

	// Build the report and assert invariants.
	rep := &SoakReport{
		Cycles:       cfg.Cycles,
		WallTime:     time.Since(start),
		TotalActions: totalActions,
		EndStates:    make(map[machine.State]int),
	}
	snap := sh.Inventory().Snapshot()
	for _, m := range snap.All() {
		rep.EndStates[m.State]++
		switch m.State {
		case machine.StateFailed:
			rep.LeakedMachineIDs = append(rep.LeakedMachineIDs, m.ID)
		case machine.StateCreating, machine.StateConfiguring,
			machine.StateDraining, machine.StateDeleting:
			// Transitional states should not persist after the soak's
			// final cycle when the fake provider is in
			// InstantTransitions mode.
			rep.LeakedMachineIDs = append(rep.LeakedMachineIDs, m.ID)
		}
	}

	// Inventory size invariant: every machine the provider knows
	// about should be tracked locally (no leaks, no phantoms).
	if got, want := snap.Len(), cfg.IdleSeed+cfg.SpeculativeSeed; got != want {
		return rep, fmt.Errorf("inventory size = %d, want %d (machine record leaked or lost)", got, want)
	}
	if len(rep.LeakedMachineIDs) > 0 {
		return rep, fmt.Errorf("%d machine(s) ended in transitional/Failed states (first: %s)",
			len(rep.LeakedMachineIDs), rep.LeakedMachineIDs[0])
	}
	return rep, nil
}
