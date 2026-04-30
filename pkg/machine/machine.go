// Package machine defines the domain types used on the shard's hot path:
// the Machine record, its state machine, and the Profile that aggregates
// machines by their capacity-relevant attributes.
//
// These are deliberately plain Go structs rather than proto-generated
// types. The shard's hot path runs over millions of records; we want
// value semantics, no proto runtime overhead, and small struct footprints.
// Conversion to/from the wire protos happens at the gRPC boundary in
// pkg/api/conv (added in M3 when the shard speaks gRPC).
package machine

import (
	"errors"
	"fmt"
)

// ID is BigFleet's internal machine identifier. Stable across the entire
// lifecycle: a Speculative slot that becomes a real Idle host keeps the
// same ID; only the host reference fills in.
type ID string

// ClusterID identifies a cluster in BigFleet's world. Empty means the
// machine is not currently bound to a cluster.
type ClusterID string

// State is one of the eight machine states described in the BigFleet
// paper §5: three stable, four transitional, one terminal-pending-cleanup.
type State uint8

const (
	StateUnspecified State = iota
	StateSpeculative       // host=nil, cluster=""
	StateCreating          // Speculative → Idle in progress
	StateIdle              // host=set, cluster=""
	StateConfiguring       // Idle → Configured in progress
	StateConfigured        // host=set, cluster=set
	StateDraining          // Configured → Idle in progress
	StateDeleting          // Idle → Speculative in progress (cloud only)
	StateFailed            // last transition timed out
)

// String returns the canonical name of the state.
func (s State) String() string {
	switch s {
	case StateSpeculative:
		return "Speculative"
	case StateCreating:
		return "Creating"
	case StateIdle:
		return "Idle"
	case StateConfiguring:
		return "Configuring"
	case StateConfigured:
		return "Configured"
	case StateDraining:
		return "Draining"
	case StateDeleting:
		return "Deleting"
	case StateFailed:
		return "Failed"
	}
	return fmt.Sprintf("State(%d)", uint8(s))
}

// IsStable reports whether the state is one of the three stable states
// (machines in transitional or failed states are not available for
// allocation by the decision engine).
func (s State) IsStable() bool {
	return s == StateSpeculative || s == StateIdle || s == StateConfigured
}

// CapacityType is the cost-of-holding category. Drives idle-hold policy.
type CapacityType uint8

const (
	CapacityTypeUnspecified CapacityType = iota
	CapacityTypeBareMetal
	CapacityTypeReserved
	CapacityTypeOnDemand
	CapacityTypeSpot
)

// String returns the canonical name of the capacity type.
func (c CapacityType) String() string {
	switch c {
	case CapacityTypeBareMetal:
		return "BareMetal"
	case CapacityTypeReserved:
		return "Reserved"
	case CapacityTypeOnDemand:
		return "OnDemand"
	case CapacityTypeSpot:
		return "Spot"
	}
	return fmt.Sprintf("CapacityType(%d)", uint8(c))
}

// HostRef is the provider's identifier for a real host. Empty for
// machines in the Speculative state.
type HostRef struct {
	Provider string // e.g. "aws-eu-west-1", "bare-metal-amsterdam"
	Ref      string // provider-scoped ID
}

// Empty reports whether the host ref is unset.
func (h HostRef) Empty() bool {
	return h.Provider == "" && h.Ref == ""
}

// Profile is the bundle of attributes that make two machines functionally
// interchangeable for assignment purposes. Two machines with the same
// Profile are equivalent inputs to a Phase 1 satisfaction check.
type Profile struct {
	InstanceType string
	Zone         string
	CapacityType CapacityType
	// Resources are stored as canonical strings (e.g., "96", "768Gi") to
	// match the proto wire format. Comparison is exact-string for now;
	// quantity-aware comparison happens at the operator boundary when CRs
	// are aggregated into needs.
	Resources map[string]string
	// Labels surfaces provider-supplied labels needed for matching against
	// node-selector requirements (e.g., accelerator-type).
	Labels map[string]string
}

// Machine is the unit of inventory the autoscaler reasons about. Same
// shape regardless of state.
type Machine struct {
	ID      ID
	State   State
	Host    HostRef
	Cluster ClusterID
	Profile Profile

	// PricePerHour in USD. Zero for bare metal.
	PricePerHour float64

	// InterruptionProbability is the provider-declared hourly interruption
	// probability in [0, 1]. No cluster-side override (per design memory).
	InterruptionProbability float64

	// TransitionStartedUnixNanos is when the current transitional state
	// began. Zero when the state is stable.
	TransitionStartedUnixNanos int64

	// LastError is populated when State == StateFailed.
	LastError string

	// AssignedPriority is the priority of the workload this machine is
	// currently serving. Meaningful only when State == StateConfigured
	// (or the transitional states that surround it). Set by the shard
	// when it Applies a Phase 1 Bootstrap/Provision action; cleared on
	// drain. Phase 2 victim scoring reads this to compute the priority
	// gap.
	AssignedPriority int32

	// AssignedInterruptionPenaltyDollars is the dollar value (already
	// derived from the bucket) of the workload's interruption penalty.
	// Used by Phase 2 victim scoring. Meaningful when State ==
	// StateConfigured.
	AssignedInterruptionPenaltyDollars float64

	// AssignedReclamationPenaltyDollars is the dollar value of the
	// workload's reclamation penalty (the value tied to *this specific*
	// machine, e.g. burned-in GPUs). Used by Phase 2 victim scoring and
	// Phase 3 release tiebreaks.
	AssignedReclamationPenaltyDollars float64
}

// validTransitions encodes the legal state machine. The decision engine
// only emits transitions that begin in a stable state and target the
// next stable state in sequence; the table is consulted by both the
// inventory and provider implementations to validate reconciler output.
var validTransitions = map[State]map[State]bool{
	StateSpeculative: {StateCreating: true},
	StateCreating:    {StateIdle: true, StateFailed: true},
	StateIdle: {
		StateConfiguring: true,
		StateDeleting:    true, // cloud only; the call is provider-rejected for bare metal
	},
	StateConfiguring: {StateConfigured: true, StateFailed: true},
	StateConfigured:  {StateDraining: true},
	StateDraining:    {StateIdle: true, StateFailed: true},
	StateDeleting:    {StateSpeculative: true, StateFailed: true},
	// StateFailed is terminal-pending-cleanup; no automatic transitions out.
}

// CanTransition reports whether a transition from `from` to `to` is
// permitted by the state machine.
func CanTransition(from, to State) bool {
	if to == StateFailed {
		// Any transitional state can fail.
		return from == StateCreating || from == StateConfiguring ||
			from == StateDraining || from == StateDeleting
	}
	allowed, ok := validTransitions[from]
	return ok && allowed[to]
}

// ErrInvalidTransition is returned by callers that validate transitions
// before attempting them.
var ErrInvalidTransition = errors.New("invalid state transition")

// CheckTransition returns ErrInvalidTransition wrapped with from/to detail
// if the transition is not permitted; nil otherwise. Convenience wrapper
// over CanTransition for callers that want an error.
func CheckTransition(from, to State) error {
	if CanTransition(from, to) {
		return nil
	}
	return fmt.Errorf("%w: %s → %s", ErrInvalidTransition, from, to)
}

// Invariant validates the structural invariants of a Machine. Returns the
// first invariant violated, or nil. Used by tests and reconciliation
// code to assert state-machine consistency.
func (m *Machine) Invariant() error {
	switch m.State {
	case StateSpeculative, StateCreating:
		if !m.Host.Empty() {
			return fmt.Errorf("machine %s: %s state must have empty host", m.ID, m.State)
		}
		if m.Cluster != "" {
			return fmt.Errorf("machine %s: %s state must have empty cluster", m.ID, m.State)
		}
	case StateIdle, StateConfiguring, StateDeleting:
		if m.Host.Empty() {
			return fmt.Errorf("machine %s: %s state must have a host", m.ID, m.State)
		}
		// Cluster may be set during Configuring (we know the destination).
	case StateConfigured, StateDraining:
		if m.Host.Empty() {
			return fmt.Errorf("machine %s: %s state must have a host", m.ID, m.State)
		}
		if m.Cluster == "" {
			return fmt.Errorf("machine %s: %s state must have a cluster", m.ID, m.State)
		}
	case StateFailed:
		// No structural invariants beyond having a non-empty LastError.
		if m.LastError == "" {
			return fmt.Errorf("machine %s: Failed state must have last_error populated", m.ID)
		}
	case StateUnspecified:
		return fmt.Errorf("machine %s: state is Unspecified", m.ID)
	}
	if m.InterruptionProbability < 0 || m.InterruptionProbability > 1 {
		return fmt.Errorf("machine %s: interruption_probability %f outside [0,1]", m.ID, m.InterruptionProbability)
	}
	return nil
}
