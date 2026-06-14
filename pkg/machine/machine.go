// Package machine defines the domain types used on the shard's hot path:
// the Machine record, its state machine, and the Profile that aggregates
// machines by their capacity-relevant attributes.
//
// These are deliberately plain Go structs rather than proto-generated
// types. The shard's hot path runs over millions of records; we want
// value semantics, no proto runtime overhead, and small struct footprints.
// Conversion to/from the wire protos happens at the gRPC boundary in
// pkg/conv.
package machine

import (
	"errors"
	"fmt"
	"math"
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
	// Resources is this machine's base resource shape — what the hardware
	// *is*, stored as canonical quantity strings (e.g., "96", "768Gi") to
	// match the proto wire format.
	//
	// ADR-0027: demand no longer carries a per-replica shape, so this is
	// purely machine-descriptive. Per-machine capacity for Phase 1's
	// resource-vector diff is Machine.Allocatable; EffectiveAllocatable()
	// falls back to this field when Allocatable is unset.
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

	// AssignedNeedFingerprint is the needs.Profile.Fingerprint() of the
	// Need this machine was bootstrapped to satisfy. Set when the shard
	// transitions Configuring → Configured; cleared when the machine is
	// drained back to Idle.
	//
	// Why not derive from machine.Profile: needs.Profile holds requirements
	// (selectors), machine.Profile holds resolved attributes. A single
	// machine satisfies many needs.Profiles' requirements via subset-match,
	// so MatchProfile-based counting over-counts whenever multiple Needs
	// in the same cluster have nested label selectors. Phase 1's deficit
	// math needs exact 1:1 attribution: how many machines are *currently
	// assigned to this Need*, not how many *could match* it.
	AssignedNeedFingerprint string

	// AssignedGroup is the co-location Group (needs.Need.Group) of the
	// gang this machine was bootstrapped to serve. Set alongside
	// AssignedNeedFingerprint at Configure-time; cleared on drain.
	//
	// ADR-0051 (M77g): AssignedNeedFingerprint carries only the
	// Profile.Fingerprint, so two same-profile gangs are indistinguishable
	// to the Same-domain tiebreak — the cluster-granular CreditableTotal
	// can't tell "this domain holds *my gang's* machines" from "this
	// domain holds an equal number of *unrelated* same-class machines."
	// Recording the gang lets ChooseSameBucket break a capped-coverage tie
	// on *this gang's own* bound machines, so a served gang's Same-domain
	// choice is a fixed point through the in-flight bootstrap dwell that is
	// the #64 perturbation. Read as current state only — no cross-cycle
	// memory of past domain choices (ADR-0045 "no second ledger").
	AssignedGroup string

	// Allocatable is the per-machine capacity this hardware actually
	// provides. Phase 1's aggregate-demand-vs-aggregate-supply math
	// (ADR-0027) sums Allocatable across matching machines and diffs it,
	// in resource-vector space, against the Need's AggregateResources.
	//
	// Migration: when Allocatable is empty, EffectiveAllocatable() falls
	// back to Profile.Resources. Providers populating this field describe
	// a machine whose real capacity differs from its base Resources shape.
	Allocatable map[string]string

	// ShardMetadata is the provider-echoed copy of the opaque
	// shard_metadata map (provider.proto, M72): the assignment state the
	// shard writes at Configure time and recovers from List/Get after a
	// restart. It lives on the provider side of the Go contract —
	// pkg/provider/fake stores it verbatim, the conv boundary carries it
	// across the wire. The shard's inventory does NOT retain it:
	// reconcile decodes the well-known keys into the Assigned* fields at
	// ingest and drops the map, keeping the hot-path record small
	// (paper §9 budgets ~30–55 bytes per machine).
	ShardMetadata map[string]string
}

// validTransitions encodes the legal state machine. The decision engine
// only emits transitions that begin in a stable state and target the
// next stable state in sequence; the table is consulted by both the
// inventory and provider implementations to validate reconciler output.
// M44.4 Drop B: Configuring → Idle is a "rollback" transition for the
// case where the shard abandons a bootstrap attempt before the provider
// is touched (e.g., the operator-side BootstrapRequest RPC times out
// under the cycle ctx). Distinct from Configuring → Failed, which is
// for a real provider-side failure that consumes the machine. Rollback
// returns the machine to the Idle pool for retry on the next cycle —
// without it, every orchestration timeout permanently shrank the Idle
// pool (StateFailed is terminal).
var validTransitions = map[State]map[State]bool{
	StateSpeculative: {StateCreating: true},
	StateCreating:    {StateIdle: true, StateFailed: true},
	StateIdle: {
		StateConfiguring: true,
		StateDeleting:    true, // cloud only; the call is provider-rejected for bare metal
	},
	StateConfiguring: {StateConfigured: true, StateFailed: true, StateIdle: true /* rollback, see comment above */},
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

// EffectiveAllocatable returns the per-machine allocatable resource map
// callers should use for Phase 1's aggregate-supply math. Returns
// m.Allocatable when set; otherwise falls back to m.Profile.Resources.
// Tests and construction sites that don't set Allocatable explicitly
// keep working — the machine's base Resources shape is used as its
// capacity until a provider reports a distinct Allocatable.
func (m *Machine) EffectiveAllocatable() map[string]string {
	if len(m.Allocatable) > 0 {
		return m.Allocatable
	}
	return m.Profile.Resources
}

// EffectiveCost is the locked cost formula (CLAUDE.md, ADR-0029,
// paper §16):
//
//	effective_cost = price + (interruption_probability × interruption_penalty)
//
// price is per-hour, penalty is dollars. Dimensionally inconsistent
// on paper but the formula is the locked design choice; the result
// is a per-hour expected cost comparable to another machine's
// price. Not pluggable.
func (m Machine) EffectiveCost(interruptionPenaltyDollars float64) float64 {
	if interruptionPenaltyDollars < 0 {
		interruptionPenaltyDollars = 0
	}
	return m.PricePerHour + (m.InterruptionProbability * interruptionPenaltyDollars)
}

// Invariant validates the structural invariants of a Machine plus the
// bounds on the provider-declared cost-formula inputs: PricePerHour
// must be ≥ 0 and not NaN, InterruptionProbability must lie in [0, 1].
// Returns the first invariant violated, or nil.
//
// Where it runs: every inventory write (inventory.Insert / Apply) and,
// loudly, the shard's provider-ingest boundary (reconcile List results
// and Create acks — ADR-0046 addendum / M70). The production-readiness
// audit (arc 3) found the cost bounds unenforced at ingest: a provider
// returning a negative price or probability > 1 fed straight into
// EffectiveCost, or was silently dropped by a discarded Insert error.
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
	if m.InterruptionProbability < 0 || m.InterruptionProbability > 1 || math.IsNaN(m.InterruptionProbability) {
		return fmt.Errorf("machine %s: interruption_probability %f outside [0,1]", m.ID, m.InterruptionProbability)
	}
	if m.PricePerHour < 0 || math.IsNaN(m.PricePerHour) {
		return fmt.Errorf("machine %s: price_per_hour %f negative or NaN", m.ID, m.PricePerHour)
	}
	return nil
}
