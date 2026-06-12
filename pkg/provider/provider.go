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
// server implementing the protobuf CapacityProvider service;
// pkg/provider/grpcclient is the shard-side adapter that wraps that gRPC
// surface to satisfy this Go interface (M71).
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
	Delete(ctx context.Context, req DeleteRequest) (TransitionAck, error)

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

// ErrFenced is returned by a mutating call whose fencing token is not
// strictly newer than the provider's high-water mark for this shard_id
// (paper §11; wire code FAILED_PRECONDITION). It means a newer epoch of
// this shard identity has already contacted the provider — the caller is
// a zombie. That is an incident (duplicate shard identity / split brain),
// not a transient failure: do not blind-retry.
var ErrFenced = errors.New("provider: fencing token rejected; this shard is stale (zombie)")

// Fence is the paper §11 shard→provider fencing token carried by every
// mutating call. The zero value means "no token" — in-process callers
// (the scaletest harness constructing pkg/provider/fake directly) don't
// fence, and fenced implementations let those calls through.
//
// Server-side only: implementations validate it. The shard never sets it
// on requests — pkg/provider/grpcclient stamps the wire fields itself
// from its injected identity, minting a fresh SequenceNumber per call
// attempt so transport retries are never rejected as replays.
type Fence struct {
	ShardID        string
	ShardEpoch     int64
	SequenceNumber int64
}

// Zero reports whether the token is absent.
func (f Fence) Zero() bool { return f.ShardID == "" }

// CreateRequest is the input to Create.
type CreateRequest struct {
	MachineID machine.ID
	Fence     Fence
}

// ConfigureRequest is the input to Configure.
type ConfigureRequest struct {
	MachineID     machine.ID
	ClusterID     machine.ClusterID
	BootstrapBlob []byte
	Fence         Fence

	// ShardMetadata is opaque to the provider: stored verbatim with the
	// machine and echoed on Get/List until the binding ends (Drain
	// completes back to Idle). Never interpreted. M72 — this is the
	// durable copy of the assignment state a restarted shard decodes to
	// rebuild preemption protection; see pb.Machine.shard_metadata for
	// the full contract.
	ShardMetadata map[string]string
}

// DrainRequest is the input to Drain.
type DrainRequest struct {
	MachineID   machine.ID
	GracePeriod GracePeriod
	Fence       Fence
}

// DeleteRequest is the input to Delete. M71: Delete grew a request struct
// (it took a bare machine.ID before) because mutating calls carry the
// fencing token and reads don't.
type DeleteRequest struct {
	MachineID machine.ID
	Fence     Fence
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
