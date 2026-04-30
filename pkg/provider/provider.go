// Package provider defines the Go interface every CapacityProvider
// implements, plus the dial-out registry the shard uses to talk to
// out-of-tree provider processes.
//
// The interface mirrors the gRPC service contract but uses domain types
// (pkg/machine) rather than proto-generated structs. This keeps the
// shard's hot path free of proto runtime overhead and makes it easy to
// write in-process fakes (pkg/provider/fake) without standing up gRPC.
//
// Real providers live in *separate repositories*. They register a gRPC
// server implementing the protobuf CapacityProvider service; an adapter
// in the shard wraps that gRPC client to satisfy this Go interface.
// The repo never ships in-tree real providers (Kubernetes spent years
// undoing CCM/CSI in-tree providers; we don't repeat that mistake).
package provider

import (
	"context"
	"errors"

	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// Provider is the Go interface a shard talks to. Every method except
// List is *asynchronous*: it returns immediately with a TransitionAck
// describing the operation and a snapshot of the machine in (typically)
// the corresponding transitional state. Subsequent Get / List calls
// observe progress.
//
// All four lifecycle methods are *idempotent* on (machine_id, target_state):
// repeated calls toward the same target reuse the existing operation
// rather than starting a new one.
type Provider interface {
	// Create moves Speculative → Creating → Idle. Caller picks the
	// machine_id from a previously-listed Speculative slot.
	Create(ctx context.Context, req CreateRequest) (TransitionAck, error)

	// Configure moves Idle → Configuring → Configured for the given
	// cluster, applying the bootstrap blob (cloud-init, ignition, PXE).
	Configure(ctx context.Context, req ConfigureRequest) (TransitionAck, error)

	// Drain moves Configured → Draining → Idle, respecting the grace
	// period for kubelet-side graceful shutdown.
	Drain(ctx context.Context, req DrainRequest) (TransitionAck, error)

	// Delete moves Idle → Deleting → Speculative. Cloud only — bare
	// metal providers should return ErrNotSupported.
	Delete(ctx context.Context, machineID machine.ID) (TransitionAck, error)

	// Get returns the current state of one machine.
	Get(ctx context.Context, machineID machine.ID) (machine.Machine, error)

	// List returns machines matching the filter. Providers above the
	// conformance threshold support an opaque since_revision cursor;
	// providers below the threshold may ignore the cursor and return
	// full state. The returned cursor (Revision in the response) is
	// echoed by the next caller.
	List(ctx context.Context, filter ListFilter) (ListResponse, error)
}

// ErrNotFound is returned by Get/Delete when the machine is unknown.
var ErrNotFound = errors.New("provider: machine not found")

// ErrNotSupported is returned by methods the provider doesn't implement
// (e.g., bare metal returning ErrNotSupported from Delete).
var ErrNotSupported = errors.New("provider: operation not supported")

// CreateRequest is the input to Create.
type CreateRequest struct {
	MachineID machine.ID
	Labels    map[string]string
}

// ConfigureRequest is the input to Configure.
type ConfigureRequest struct {
	MachineID     machine.ID
	ClusterID     machine.ClusterID
	BootstrapBlob []byte
}

// DrainRequest is the input to Drain.
type DrainRequest struct {
	MachineID   machine.ID
	GracePeriod GracePeriod
}

// GracePeriod is a typed integer-second duration used by Drain.
type GracePeriod int64

// TransitionAck is the response to any of Create/Configure/Drain/Delete.
type TransitionAck struct {
	// OperationID for idempotent retry. Repeated calls with the same
	// (machine_id, target_state) reuse the same OperationID.
	OperationID string

	// Machine snapshot at acceptance time — typically already in the
	// corresponding transitional state.
	Machine machine.Machine
}

// ListFilter restricts a List call. Empty fields = no restriction.
type ListFilter struct {
	States        []machine.State
	Zone          string
	InstanceType  string
	MaxResults    int
	SinceRevision []byte
}

// ListResponse holds the matched machines plus an opaque cursor.
type ListResponse struct {
	Machines []machine.Machine
	Revision []byte
}
