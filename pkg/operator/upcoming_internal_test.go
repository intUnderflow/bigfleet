package operator

import (
	"testing"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// TestUpcomingNodePhase_MachineStateMapping locks in the M14 contract
// that MACHINE_STATE_DRAINING surfaces as UpcomingNodeDraining. Other
// states are checked alongside as a regression guard since the table
// is small enough.
func TestUpcomingNodePhase_MachineStateMapping(t *testing.T) {
	cases := []struct {
		state pb.MachineState
		want  bfv1alpha1.UpcomingNodePhase
	}{
		{pb.MachineState_MACHINE_STATE_SPECULATIVE, bfv1alpha1.UpcomingNodeProvisioning},
		{pb.MachineState_MACHINE_STATE_CREATING, bfv1alpha1.UpcomingNodeProvisioning},
		{pb.MachineState_MACHINE_STATE_IDLE, bfv1alpha1.UpcomingNodeLaunched},
		{pb.MachineState_MACHINE_STATE_CONFIGURING, bfv1alpha1.UpcomingNodeRegistered},
		{pb.MachineState_MACHINE_STATE_CONFIGURED, bfv1alpha1.UpcomingNodeReady},
		{pb.MachineState_MACHINE_STATE_DRAINING, bfv1alpha1.UpcomingNodeDraining},
		{pb.MachineState_MACHINE_STATE_FAILED, bfv1alpha1.UpcomingNodeFailed},
	}
	for _, c := range cases {
		got := upcomingNodePhase(c.state)
		if got != c.want {
			t.Errorf("upcomingNodePhase(%v) = %q, want %q", c.state, got, c.want)
		}
	}
}
