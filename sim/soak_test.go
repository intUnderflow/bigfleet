//go:build soak

package sim_test

import (
	"context"
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/sim"
)

// TestSoak_DefaultConfig exercises the engine through the
// DefaultSoakConfig (50K cycles, 5K idle + 10K speculative seeds, 50
// rotating clusters, ChurnEveryCycles=5). On healthy code it should
// finish in under 60 seconds with no leaked machines and no panics.
//
// Build tag soak — not part of PR CI; runs in nightly.
func TestSoak_DefaultConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	rep, err := sim.Soak(ctx, sim.DefaultSoakConfig())
	if err != nil {
		t.Fatalf("Soak: %v\nreport: %+v", err, rep)
	}
	t.Logf("soak: %d cycles in %v, %d actions emitted; end states: %v",
		rep.Cycles, rep.WallTime, rep.TotalActions, rep.EndStates)
}
