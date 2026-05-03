package shard

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
)

// TestMetricsWarmup_SkipsCyclesUntilCount runs N+M cycles with
// MetricsWarmupCycles=N and asserts the cycle-duration histogram
// recorded only the last M observations.
//
// Uses the same setup pattern as the phase-dump diagnostic but is a
// regular -short-safe test so warmup behaviour stays under regression
// guard.
func TestMetricsWarmup_SkipsCyclesUntilCount(t *testing.T) {
	// Snapshot the current count for cycle_duration so the assertion
	// is delta-aware (other tests in the same binary may have
	// observed it).
	before := histogramCount(t, "bigfleet_shard_cycle_duration_seconds")

	const warmup = 3
	const total = 5
	s := setupSmallShard(t, warmup)
	ctx := t.Context()
	for i := 0; i < total; i++ {
		_ = s.Step(ctx)
	}

	after := histogramCount(t, "bigfleet_shard_cycle_duration_seconds")
	got := after - before
	want := uint64(total - warmup)
	if got != want {
		t.Errorf("cycle_duration observations = %d, want %d (warmup=%d, total=%d)",
			got, want, warmup, total)
	}
}

func setupSmallShard(t *testing.T, warmupCycles int) *Shard {
	t.Helper()
	dir := t.TempDir()
	epoch, err := fencing.LoadEpoch(filepath.Join(dir, "epoch"))
	if err != nil {
		t.Fatalf("epoch: %v", err)
	}
	prov := fake.New(fake.Options{InstantTransitions: true})
	s, err := New(Config{
		ID:                  "warmup-test",
		Epoch:               epoch,
		Provider:            prov,
		CycleInterval:       1 * time.Second,
		BootstrapTimeout:    1 * time.Second,
		MetricsWarmupCycles: warmupCycles,
		LocalBootstrap: func(ctx context.Context, _ machine.ClusterID, _ []needs.Requirement) ([]byte, error) {
			return []byte("# warmup\n"), nil
		},
	})
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	return s
}

func histogramCount(t *testing.T, name string) uint64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var sum uint64
		for _, m := range mf.Metric {
			sum += m.GetHistogram().GetSampleCount()
		}
		return sum
	}
	return 0
}
