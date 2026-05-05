package main

import (
	"context"
	"testing"
	"time"
)

// TestAssertRunnerActionOutcome_NoFire: an action with a fire error
// asserts false and surfaces a message — no stale-state pass.
func TestAssertRunnerActionOutcome_NoFire(t *testing.T) {
	r := &runnerActionResult{
		Action:    "kill-coordinator-leader",
		AtSeconds: 600,
		FireError: "kubectl exec failed",
	}
	assertRunnerActionOutcome(context.Background(), "", "", r, time.Time{})
	if r.Asserted {
		t.Errorf("Asserted = true, want false (action didn't fire)")
	}
	if r.AssertError == "" {
		t.Errorf("AssertError empty, want a message")
	}
}

// TestAssertRunnerActionOutcome_UnrecognisedAction: an action the
// runner doesn't know how to assert is reported as a failure.
func TestAssertRunnerActionOutcome_UnrecognisedAction(t *testing.T) {
	r := &runnerActionResult{Action: "do-something-weird", FiredAt: "2026-05-05T12:00:00Z"}
	assertRunnerActionOutcome(context.Background(), "", "", r, time.Time{})
	if r.Asserted {
		t.Errorf("Asserted = true, want false")
	}
	if r.AssertError == "" {
		t.Errorf("AssertError empty for unrecognised action")
	}
}
