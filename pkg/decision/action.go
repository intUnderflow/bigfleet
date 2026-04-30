package decision

import (
	"time"

	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// ActionKind discriminates the four kinds of action the decision engine
// can emit. Each cycle produces a slice of these for the shard to
// execute asynchronously against the provider and the operator stream.
type ActionKind uint8

const (
	ActionKindUnspecified ActionKind = iota
	// ActionKindBootstrap promotes an Idle machine to Configured for a
	// specific cluster. Emitted by Phase 1.
	ActionKindBootstrap
	// ActionKindProvision turns a Speculative machine into a Configured
	// machine for a cluster. Combines a provider Create followed by a
	// Configure once the host is up. Emitted by Phase 1 when no Idle
	// candidate is available.
	ActionKindProvision
	// ActionKindReclaim drains a Configured machine back to Idle.
	// Emitted by Phase 3 (excess release).
	ActionKindReclaim
	// ActionKindPreempt drains a Configured machine for a higher-priority
	// preemptor. Emitted by Phase 2. The grace period is set by the
	// priority gap.
	ActionKindPreempt
)

// String returns the canonical name.
func (k ActionKind) String() string {
	switch k {
	case ActionKindBootstrap:
		return "Bootstrap"
	case ActionKindProvision:
		return "Provision"
	case ActionKindReclaim:
		return "Reclaim"
	case ActionKindPreempt:
		return "Preempt"
	}
	return "Unspecified"
}

// Action describes one decision-engine output. A slice of Actions is the
// total decision for a cycle.
type Action struct {
	Kind      ActionKind
	MachineID machine.ID

	// Cluster the machine is being directed to. For Bootstrap and
	// Provision, this is the *destination* cluster. For Reclaim and
	// Preempt, this is the source cluster the machine is leaving.
	Cluster machine.ClusterID

	// GracePeriod is set on Reclaim and Preempt actions.
	GracePeriod time.Duration

	// Reason is a human-readable note attached for telemetry. Not used
	// for decision logic; safe to drop without affecting semantics.
	Reason string

	// PreemptorPriority is set on Preempt actions for surface-area in
	// telemetry / operator-side logging.
	PreemptorPriority int32
}
