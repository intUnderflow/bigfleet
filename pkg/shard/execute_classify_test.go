package shard

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestClassifyExecuteError_RacedToIdle covers the bigfleet-uber #23
// finding: the post-Configure transition can fire while the machine
// has been raced back to Idle by a parallel actor. The error gets a
// distinct outcome label so the rate is visible without polluting
// the legitimate transition_error counter.
func TestClassifyExecuteError_RacedToIdle(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("sentinel directly", func(t *testing.T) {
		got := classifyExecuteError(ctx, errProvisionRacedToIdle)
		if got != "transition_raced_to_idle" {
			t.Errorf("classify(errProvisionRacedToIdle) = %q, want transition_raced_to_idle", got)
		}
	})

	t.Run("wrapped sentinel", func(t *testing.T) {
		wrapped := fmt.Errorf("bootstrap: %w", errProvisionRacedToIdle)
		got := classifyExecuteError(ctx, wrapped)
		if got != "transition_raced_to_idle" {
			t.Errorf("classify(wrapped) = %q, want transition_raced_to_idle", got)
		}
	})

	t.Run("legitimate post-Configure transition error keeps old label", func(t *testing.T) {
		// Real state-machine violation (not the race). Should still
		// classify as transition_error so alerting on real bugs
		// keeps working.
		err := errors.New("bootstrap: post-Configure transition: inventory: invalid state transition: Configuring → Speculative")
		got := classifyExecuteError(ctx, err)
		if got != "transition_error" {
			t.Errorf("classify(real transition error) = %q, want transition_error", got)
		}
	})
}
