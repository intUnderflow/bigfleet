package decision

import (
	"time"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// ActionKind discriminates the five kinds of action the decision engine
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
	// ActionKindDelete releases an Idle machine back to Speculative via
	// provider.Delete — the paper §8 Idle → Speculative lazy release
	// (M73). Emitted by Phase 3 for unclaimed Idle machines whose
	// per-CapacityType idle hold (ReleasePolicy) has expired. The
	// machine is unbound (ADR-0045: it counts for no cluster), so
	// Cluster is empty and no grace applies.
	ActionKindDelete
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
	case ActionKindDelete:
		return "Delete"
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
	// Empty for Delete (the machine is unbound).
	Cluster machine.ClusterID

	// SourceProfile is the profile of the originating Need (set for
	// Bootstrap and Provision). Carried through so the shard can produce
	// a BootstrapRequest with the right requirements and stamp the
	// configured machine with the workload's priority + penalties for
	// Phase 2 victim scoring. Nil for Reclaim and Preempt.
	SourceProfile *needs.Profile

	// GracePeriod is set on Reclaim and Preempt actions.
	GracePeriod time.Duration

	// Reason is a short, stable tag for why the engine emitted this
	// action ("phase1.idle", "phase3.excess", ...). Not used for
	// decision logic — but no longer droppable telemetry: the shard's
	// decision audit log (ADR-0046 addendum) records it per
	// executed/suppressed/dry-run action, so incident replay depends
	// on it surviving to the actuation boundary.
	Reason string

	// PreemptorPriority is set on Preempt actions for surface-area in
	// telemetry / operator-side logging.
	PreemptorPriority int32
}
