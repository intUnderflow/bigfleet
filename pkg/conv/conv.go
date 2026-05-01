// Package conv translates between proto wire types
// (pkg/proto/bigfleet/v1alpha1) and domain types (pkg/needs, pkg/machine).
//
// The shard's hot path uses domain types — small Go structs without proto
// runtime overhead. Conversion happens at the gRPC boundary: incoming
// messages are translated once on the way in, outgoing actions translated
// once on the way out. This isolates the rest of the codebase from
// proto-generated noise (oneof wrappers, *_Payload structs, enum
// stringers) and lets the proto definitions evolve independently of the
// engine.
package conv

import (
	"fmt"

	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// NeedsFromRollup converts a ClusterCapacityNeeds proto into a slice of
// domain needs.Need values. The proto's full-replacement semantics are
// preserved by the caller (which Replace()s the cluster's contribution).
func NeedsFromRollup(in *pb.ClusterCapacityNeeds) ([]needs.Need, error) {
	if in == nil {
		return nil, nil
	}
	cluster := machine.ClusterID(in.GetClusterId())
	out := make([]needs.Need, 0, len(in.GetNeeds()))
	for i, n := range in.GetNeeds() {
		profile, err := profileFromProto(n)
		if err != nil {
			return nil, fmt.Errorf("need %d: %w", i, err)
		}
		out = append(out, needs.Need{
			ClusterID:        cluster,
			Profile:          profile,
			Count:            int(n.GetCount()),
			ArrivalUnixNanos: in.GetTimestampUnixNanos(),
		})
	}
	return out, nil
}

func profileFromProto(n *pb.CapacityNeed) (needs.Profile, error) {
	reqs := make([]needs.Requirement, 0, len(n.GetRequirements()))
	for _, r := range n.GetRequirements() {
		op, err := operatorFromProto(r.GetOperator())
		if err != nil {
			return needs.Profile{}, err
		}
		reqs = append(reqs, needs.Requirement{
			Key:      r.GetKey(),
			Operator: op,
			Values:   append([]string(nil), r.GetValues()...),
		})
	}
	res := make([]needs.ResourceQty, 0, len(n.GetResources()))
	for k, v := range n.GetResources() {
		res = append(res, needs.ResourceQty{Name: k, Quantity: v})
	}
	spread := make([]needs.TopologySpread, 0, len(n.GetSpread()))
	for _, s := range n.GetSpread() {
		spread = append(spread, needs.TopologySpread{
			TopologyKey:       s.GetTopologyKey(),
			MaxSkew:           s.GetMaxSkew(),
			WhenUnsatisfiable: whenUnsatisfiableFromProto(s.GetWhenUnsatisfiable()),
		})
	}
	return needs.NewProfile(
		reqs, res, spread,
		n.GetPriority(),
		penaltyBucketFromProto(n.GetInterruptionPenaltyBucket()),
		penaltyBucketFromProto(n.GetReclamationPenaltyBucket()),
	), nil
}

func operatorFromProto(op pb.NodeSelectorRequirement_Operator) (needs.Operator, error) {
	switch op {
	case pb.NodeSelectorRequirement_OPERATOR_UNSPECIFIED:
		return needs.OperatorUnspecified, nil
	case pb.NodeSelectorRequirement_OPERATOR_IN:
		return needs.OperatorIn, nil
	case pb.NodeSelectorRequirement_OPERATOR_NOT_IN:
		return needs.OperatorNotIn, nil
	case pb.NodeSelectorRequirement_OPERATOR_EXISTS:
		return needs.OperatorExists, nil
	case pb.NodeSelectorRequirement_OPERATOR_DOES_NOT_EXIST:
		return needs.OperatorDoesNotExist, nil
	case pb.NodeSelectorRequirement_OPERATOR_SAME:
		return needs.OperatorSame, nil
	}
	return 0, fmt.Errorf("unknown operator: %v", op)
}

func operatorToProto(op needs.Operator) pb.NodeSelectorRequirement_Operator {
	switch op {
	case needs.OperatorIn:
		return pb.NodeSelectorRequirement_OPERATOR_IN
	case needs.OperatorNotIn:
		return pb.NodeSelectorRequirement_OPERATOR_NOT_IN
	case needs.OperatorExists:
		return pb.NodeSelectorRequirement_OPERATOR_EXISTS
	case needs.OperatorDoesNotExist:
		return pb.NodeSelectorRequirement_OPERATOR_DOES_NOT_EXIST
	case needs.OperatorSame:
		return pb.NodeSelectorRequirement_OPERATOR_SAME
	}
	return pb.NodeSelectorRequirement_OPERATOR_UNSPECIFIED
}

// RequirementsToProto translates a slice of needs.Requirement into the
// wire form. Used by the shard when sending BootstrapRequest frames
// down the operator stream.
func RequirementsToProto(in []needs.Requirement) []*pb.NodeSelectorRequirement {
	if len(in) == 0 {
		return nil
	}
	out := make([]*pb.NodeSelectorRequirement, 0, len(in))
	for _, r := range in {
		out = append(out, &pb.NodeSelectorRequirement{
			Key:      r.Key,
			Operator: operatorToProto(r.Operator),
			Values:   append([]string(nil), r.Values...),
		})
	}
	return out
}

func whenUnsatisfiableFromProto(w pb.TopologySpread_WhenUnsatisfiable) needs.WhenUnsatisfiable {
	switch w {
	case pb.TopologySpread_WHEN_UNSATISFIABLE_DO_NOT_SCHEDULE:
		return needs.WhenUnsatisfiableDoNotSchedule
	case pb.TopologySpread_WHEN_UNSATISFIABLE_SCHEDULE_ANYWAY:
		return needs.WhenUnsatisfiableScheduleAnyway
	}
	return needs.WhenUnsatisfiableUnspecified
}

func penaltyBucketFromProto(b pb.PenaltyBucket) needs.PenaltyBucket {
	// PenaltyBucket enums are deliberately numeric-aligned between proto
	// and domain (see pkg/needs).
	return needs.PenaltyBucket(b)
}

// MachineFromProto converts a wire-format Machine into the domain
// representation used by the inventory.
func MachineFromProto(m *pb.Machine) (machine.Machine, error) {
	if m == nil {
		return machine.Machine{}, fmt.Errorf("nil machine")
	}
	state, err := stateFromProto(m.GetState())
	if err != nil {
		return machine.Machine{}, err
	}
	out := machine.Machine{
		ID:                         machine.ID(m.GetId()),
		State:                      state,
		PricePerHour:               m.GetPricePerHour(),
		InterruptionProbability:    m.GetInterruptionProbability(),
		TransitionStartedUnixNanos: m.GetTransitionStartedUnixNanos(),
		LastError:                  m.GetLastError(),
	}
	if h := m.GetHost(); h != nil {
		out.Host = machine.HostRef{Provider: h.GetProvider(), Ref: h.GetRef()}
	}
	out.Profile = machine.Profile{
		InstanceType: m.GetInstanceType(),
		Zone:         m.GetZone(),
		CapacityType: capacityTypeFromProto(m.GetCapacityType()),
		Labels:       cloneStringMap(m.GetLabels()),
	}
	if r := m.GetResources(); r != nil {
		out.Profile.Resources = cloneStringMap(r.GetResources())
	}
	return out, nil
}

// MachineToProto produces the wire form of a domain machine. Callers
// that build provider-side responses from in-memory state use this.
func MachineToProto(m machine.Machine) *pb.Machine {
	out := &pb.Machine{
		Id:                         string(m.ID),
		State:                      MachineStateToProto(m.State),
		InstanceType:               m.Profile.InstanceType,
		Zone:                       m.Profile.Zone,
		CapacityType:               capacityTypeToProto(m.Profile.CapacityType),
		PricePerHour:               m.PricePerHour,
		InterruptionProbability:    m.InterruptionProbability,
		Labels:                     cloneStringMap(m.Profile.Labels),
		TransitionStartedUnixNanos: m.TransitionStartedUnixNanos,
		LastError:                  m.LastError,
	}
	if !m.Host.Empty() {
		out.Host = &pb.HostRef{Provider: m.Host.Provider, Ref: m.Host.Ref}
	}
	if len(m.Profile.Resources) > 0 {
		out.Resources = &pb.Resources{Resources: cloneStringMap(m.Profile.Resources)}
	}
	return out
}

func stateFromProto(s pb.MachineState) (machine.State, error) {
	switch s {
	case pb.MachineState_MACHINE_STATE_UNSPECIFIED:
		return machine.StateUnspecified, nil
	case pb.MachineState_MACHINE_STATE_SPECULATIVE:
		return machine.StateSpeculative, nil
	case pb.MachineState_MACHINE_STATE_CREATING:
		return machine.StateCreating, nil
	case pb.MachineState_MACHINE_STATE_IDLE:
		return machine.StateIdle, nil
	case pb.MachineState_MACHINE_STATE_CONFIGURING:
		return machine.StateConfiguring, nil
	case pb.MachineState_MACHINE_STATE_CONFIGURED:
		return machine.StateConfigured, nil
	case pb.MachineState_MACHINE_STATE_DRAINING:
		return machine.StateDraining, nil
	case pb.MachineState_MACHINE_STATE_DELETING:
		return machine.StateDeleting, nil
	case pb.MachineState_MACHINE_STATE_FAILED:
		return machine.StateFailed, nil
	}
	return machine.StateUnspecified, fmt.Errorf("unknown MachineState: %v", s)
}

func MachineStateToProto(s machine.State) pb.MachineState {
	switch s {
	case machine.StateSpeculative:
		return pb.MachineState_MACHINE_STATE_SPECULATIVE
	case machine.StateCreating:
		return pb.MachineState_MACHINE_STATE_CREATING
	case machine.StateIdle:
		return pb.MachineState_MACHINE_STATE_IDLE
	case machine.StateConfiguring:
		return pb.MachineState_MACHINE_STATE_CONFIGURING
	case machine.StateConfigured:
		return pb.MachineState_MACHINE_STATE_CONFIGURED
	case machine.StateDraining:
		return pb.MachineState_MACHINE_STATE_DRAINING
	case machine.StateDeleting:
		return pb.MachineState_MACHINE_STATE_DELETING
	case machine.StateFailed:
		return pb.MachineState_MACHINE_STATE_FAILED
	}
	return pb.MachineState_MACHINE_STATE_UNSPECIFIED
}

func capacityTypeFromProto(c pb.CapacityType) machine.CapacityType {
	switch c {
	case pb.CapacityType_CAPACITY_TYPE_BARE_METAL:
		return machine.CapacityTypeBareMetal
	case pb.CapacityType_CAPACITY_TYPE_RESERVED:
		return machine.CapacityTypeReserved
	case pb.CapacityType_CAPACITY_TYPE_ON_DEMAND:
		return machine.CapacityTypeOnDemand
	case pb.CapacityType_CAPACITY_TYPE_SPOT:
		return machine.CapacityTypeSpot
	}
	return machine.CapacityTypeUnspecified
}

func capacityTypeToProto(c machine.CapacityType) pb.CapacityType {
	switch c {
	case machine.CapacityTypeBareMetal:
		return pb.CapacityType_CAPACITY_TYPE_BARE_METAL
	case machine.CapacityTypeReserved:
		return pb.CapacityType_CAPACITY_TYPE_RESERVED
	case machine.CapacityTypeOnDemand:
		return pb.CapacityType_CAPACITY_TYPE_ON_DEMAND
	case machine.CapacityTypeSpot:
		return pb.CapacityType_CAPACITY_TYPE_SPOT
	}
	return pb.CapacityType_CAPACITY_TYPE_UNSPECIFIED
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
