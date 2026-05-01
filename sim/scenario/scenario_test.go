package scenario_test

import (
	"context"
	"testing"

	"github.com/intUnderflow/bigfleet/sim"
	"github.com/intUnderflow/bigfleet/sim/scenario"
)

// TestAllScenariosPass runs every registered scenario through the
// simulator and asserts every assertion passes. This is the
// regression suite — any scenario that fails here means the engine's
// behaviour changed in a way that broke a paper example.
func TestAllScenariosPass(t *testing.T) {
	for _, name := range scenario.Names() {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sc := scenario.Must(name)
			res, err := sim.Run(context.Background(), sc)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			for _, a := range res.Assertions {
				if !a.Pass {
					t.Errorf("assertion %q failed: %v", a.Name, a.Err)
				}
			}
		})
	}
}
