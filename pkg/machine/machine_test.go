package machine_test

import (
	"errors"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/machine"
)

func TestCanTransition_AllowedPaths(t *testing.T) {
	t.Parallel()
	allowed := []struct {
		from, to machine.State
	}{
		{machine.StateSpeculative, machine.StateCreating},
		{machine.StateCreating, machine.StateIdle},
		{machine.StateCreating, machine.StateFailed},
		{machine.StateIdle, machine.StateConfiguring},
		{machine.StateIdle, machine.StateDeleting},
		{machine.StateConfiguring, machine.StateConfigured},
		{machine.StateConfiguring, machine.StateFailed},
		{machine.StateConfigured, machine.StateDraining},
		{machine.StateDraining, machine.StateIdle},
		{machine.StateDraining, machine.StateFailed},
		{machine.StateDeleting, machine.StateSpeculative},
		{machine.StateDeleting, machine.StateFailed},
	}
	for _, tc := range allowed {
		if !machine.CanTransition(tc.from, tc.to) {
			t.Errorf("expected %s → %s to be allowed", tc.from, tc.to)
		}
	}
}

func TestCanTransition_DisallowedPaths(t *testing.T) {
	t.Parallel()
	// A handful of transitions that are explicitly forbidden by the
	// state machine. These would represent state-machine corruption if
	// they ever fired.
	forbidden := []struct {
		from, to machine.State
	}{
		// Skipping the host-attach step.
		{machine.StateSpeculative, machine.StateIdle},
		// Cross-cluster reuse must route through Idle.
		{machine.StateConfigured, machine.StateConfiguring},
		// Bare-metal-style "Idle resets to Speculative" without going via Deleting.
		{machine.StateIdle, machine.StateSpeculative},
		// No coming back from Failed automatically.
		{machine.StateFailed, machine.StateIdle},
	}
	for _, tc := range forbidden {
		if machine.CanTransition(tc.from, tc.to) {
			t.Errorf("expected %s → %s to be forbidden", tc.from, tc.to)
		}
	}
}

func TestCheckTransition_WrapsSentinel(t *testing.T) {
	t.Parallel()
	err := machine.CheckTransition(machine.StateConfigured, machine.StateConfiguring)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, machine.ErrInvalidTransition) {
		t.Errorf("expected error to wrap ErrInvalidTransition, got %v", err)
	}
}

func TestState_IsStable(t *testing.T) {
	t.Parallel()
	for _, s := range []machine.State{machine.StateSpeculative, machine.StateIdle, machine.StateConfigured} {
		if !s.IsStable() {
			t.Errorf("expected %s to be stable", s)
		}
	}
	for _, s := range []machine.State{machine.StateCreating, machine.StateConfiguring, machine.StateDraining, machine.StateDeleting, machine.StateFailed} {
		if s.IsStable() {
			t.Errorf("expected %s to NOT be stable", s)
		}
	}
}

func TestInvariant_StableStates(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		machine machine.Machine
		wantErr bool
	}{
		{
			name: "speculative without host: ok",
			machine: machine.Machine{
				ID:    "m-1",
				State: machine.StateSpeculative,
			},
		},
		{
			name: "speculative with host: invalid",
			machine: machine.Machine{
				ID:    "m-1",
				State: machine.StateSpeculative,
				Host:  machine.HostRef{Provider: "aws", Ref: "i-1"},
			},
			wantErr: true,
		},
		{
			name: "idle with host: ok",
			machine: machine.Machine{
				ID:    "m-1",
				State: machine.StateIdle,
				Host:  machine.HostRef{Provider: "aws", Ref: "i-1"},
			},
		},
		{
			name: "idle without host: invalid",
			machine: machine.Machine{
				ID:    "m-1",
				State: machine.StateIdle,
			},
			wantErr: true,
		},
		{
			name: "configured with host and cluster: ok",
			machine: machine.Machine{
				ID:      "m-1",
				State:   machine.StateConfigured,
				Host:    machine.HostRef{Provider: "aws", Ref: "i-1"},
				Cluster: "cluster-1",
			},
		},
		{
			name: "configured without cluster: invalid",
			machine: machine.Machine{
				ID:    "m-1",
				State: machine.StateConfigured,
				Host:  machine.HostRef{Provider: "aws", Ref: "i-1"},
			},
			wantErr: true,
		},
		{
			name: "failed without last_error: invalid",
			machine: machine.Machine{
				ID:    "m-1",
				State: machine.StateFailed,
			},
			wantErr: true,
		},
		{
			name: "failed with last_error: ok",
			machine: machine.Machine{
				ID:        "m-1",
				State:     machine.StateFailed,
				LastError: "configure timed out",
			},
		},
		{
			name: "interruption probability out of range: invalid",
			machine: machine.Machine{
				ID:                      "m-1",
				State:                   machine.StateIdle,
				Host:                    machine.HostRef{Provider: "aws", Ref: "i-1"},
				InterruptionProbability: 1.5,
			},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.machine.Invariant()
			if (err != nil) != tc.wantErr {
				t.Errorf("Invariant() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}
