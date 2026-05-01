// Package grpcadapter wraps a Go pkg/provider.Provider as a proto
// pb.CapacityProviderServer. Used by tests and by the conformance
// suite's self-test to expose pkg/provider/fake over a real gRPC
// port; production out-of-tree providers implement
// pb.CapacityProviderServer directly and don't need this.
package grpcadapter

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/intUnderflow/bigfleet/pkg/conv"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider"
)

// Server adapts a Go provider.Provider to the gRPC service surface.
type Server struct {
	pb.UnimplementedCapacityProviderServer

	p provider.Provider
}

// New constructs a Server. The supplied Provider receives all RPC
// calls; conversion between proto types and pkg/machine domain types
// happens here.
func New(p provider.Provider) *Server {
	return &Server{p: p}
}

// Create implements pb.CapacityProviderServer.
func (s *Server) Create(ctx context.Context, req *pb.CreateRequest) (*pb.TransitionAck, error) {
	if req.GetMachineId() == "" {
		return nil, status.Error(codes.InvalidArgument, "machine_id required")
	}
	ack, err := s.p.Create(ctx, provider.CreateRequest{
		MachineID: machine.ID(req.GetMachineId()),
		Labels:    cloneStringMap(req.GetLabels()),
	})
	return ackToProto(ack, err)
}

// Configure implements pb.CapacityProviderServer.
func (s *Server) Configure(ctx context.Context, req *pb.ConfigureRequest) (*pb.TransitionAck, error) {
	if req.GetMachineId() == "" {
		return nil, status.Error(codes.InvalidArgument, "machine_id required")
	}
	ack, err := s.p.Configure(ctx, provider.ConfigureRequest{
		MachineID:     machine.ID(req.GetMachineId()),
		ClusterID:     machine.ClusterID(req.GetClusterId()),
		BootstrapBlob: req.GetBootstrapBlob(),
	})
	return ackToProto(ack, err)
}

// Drain implements pb.CapacityProviderServer.
func (s *Server) Drain(ctx context.Context, req *pb.DrainRequest) (*pb.TransitionAck, error) {
	if req.GetMachineId() == "" {
		return nil, status.Error(codes.InvalidArgument, "machine_id required")
	}
	ack, err := s.p.Drain(ctx, provider.DrainRequest{
		MachineID:   machine.ID(req.GetMachineId()),
		GracePeriod: provider.GracePeriod(req.GetGracePeriodSeconds()),
	})
	return ackToProto(ack, err)
}

// Delete implements pb.CapacityProviderServer.
func (s *Server) Delete(ctx context.Context, req *pb.MachineRef) (*pb.TransitionAck, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	ack, err := s.p.Delete(ctx, machine.ID(req.GetId()))
	return ackToProto(ack, err)
}

// Get implements pb.CapacityProviderServer.
func (s *Server) Get(ctx context.Context, req *pb.MachineRef) (*pb.Machine, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	m, err := s.p.Get(ctx, machine.ID(req.GetId()))
	if err != nil {
		return nil, mapErr(err)
	}
	return conv.MachineToProto(m), nil
}

// List implements pb.CapacityProviderServer.
func (s *Server) List(ctx context.Context, req *pb.ListFilter) (*pb.MachineList, error) {
	filter := provider.ListFilter{
		Zone:          req.GetZone(),
		InstanceType:  req.GetInstanceType(),
		MaxResults:    int(req.GetMaxResults()),
		SinceRevision: req.GetSinceRevision(),
	}
	for _, st := range req.GetStates() {
		ds, err := stateFromProto(st)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "states[]: %v", err)
		}
		filter.States = append(filter.States, ds)
	}
	resp, err := s.p.List(ctx, filter)
	if err != nil {
		return nil, mapErr(err)
	}
	out := &pb.MachineList{Revision: resp.Revision}
	out.Machines = make([]*pb.Machine, 0, len(resp.Machines))
	for _, m := range resp.Machines {
		out.Machines = append(out.Machines, conv.MachineToProto(m))
	}
	return out, nil
}

// ackToProto turns a Go-side TransitionAck into the wire form.
func ackToProto(ack provider.TransitionAck, err error) (*pb.TransitionAck, error) {
	if err != nil {
		return nil, mapErr(err)
	}
	return &pb.TransitionAck{
		OperationId: ack.OperationID,
		Machine:     conv.MachineToProto(ack.Machine),
	}, nil
}

// mapErr translates package-level provider errors to grpc statuses.
func mapErr(err error) error {
	switch {
	case errors.Is(err, provider.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, provider.ErrNotSupported):
		return status.Error(codes.Unimplemented, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// stateFromProto translates a proto MachineState into the domain enum.
// Mirrors pkg/conv but kept local so this package doesn't drag in the
// full proto-domain conversion surface.
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

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
