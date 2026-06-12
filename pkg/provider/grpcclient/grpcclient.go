// Package grpcclient is the shard-side dial-out adapter to an
// out-of-tree CapacityProvider: it implements pkg/provider.Provider over
// the bigfleet.v1alpha1.CapacityProvider gRPC service (M71, plan §12).
//
// Real providers live in separate repositories and run as separate
// processes; this client is the only in-tree code that talks to them.
// It stamps the paper §11 fencing token (shard_id, shard_epoch,
// sequence_number) on every mutating call from the identity injected at
// construction. A fresh sequence number is minted per call attempt, so
// transport-level retries are never rejected as replays; idempotency is
// the provider's job, keyed on (machine_id, target_state).
package grpcclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/intUnderflow/bigfleet/pkg/conv"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider"
)

// Default per-call deadlines, applied only when the caller's context has
// none. Lifecycle RPCs are async-accept (the provider returns a
// TransitionAck immediately and does the work in the background, per the
// provider contract), so a short deadline is correct even though the
// underlying transition may take hours. List scales with inventory size,
// so it gets more room.
const (
	defaultCallTimeout = 30 * time.Second
	defaultListTimeout = 2 * time.Minute
)

// Identity is the fencing identity stamped on every mutating call
// (paper §11): the shard's stable ID, its persisted restart epoch, and
// the per-process sequence counter.
type Identity struct {
	ShardID string
	Epoch   *fencing.Epoch
	// Seq may be nil; a fresh counter is created. Inject only when a
	// test needs to observe or pre-advance the sequence.
	Seq *fencing.Sequence
}

// Client implements provider.Provider over gRPC.
type Client struct {
	conn *grpc.ClientConn
	cli  pb.CapacityProviderClient
	id   Identity
}

// New dials addr (lazily — gRPC connects on first call) and returns a
// Client that fences as identity. Transport is plaintext, matching every
// other BigFleet edge (ADR-0008 trust-the-network reference-impl stance).
func New(addr string, identity Identity) (*Client, error) {
	if addr == "" {
		return nil, errors.New("grpcclient: provider address is empty")
	}
	if identity.ShardID == "" {
		return nil, errors.New("grpcclient: Identity.ShardID is required")
	}
	if identity.Epoch == nil {
		return nil, errors.New("grpcclient: Identity.Epoch is required")
	}
	if identity.Seq == nil {
		identity.Seq = &fencing.Sequence{}
	}
	conn, err := grpc.NewClient(addr,
		append(grpcutil.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		return nil, fmt.Errorf("grpcclient: dial %s: %w", addr, err)
	}
	return &Client{conn: conn, cli: pb.NewCapacityProviderClient(conn), id: identity}, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error { return c.conn.Close() }

// Create implements provider.Provider.
func (c *Client) Create(ctx context.Context, req provider.CreateRequest) (provider.TransitionAck, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultCallTimeout)
	defer cancel()
	ack, err := c.cli.Create(ctx, &pb.CreateRequest{
		MachineId:      string(req.MachineID),
		ShardId:        c.id.ShardID,
		ShardEpoch:     c.id.Epoch.Value(),
		SequenceNumber: c.id.Seq.Next(),
	})
	return ackFromProto(ack, err)
}

// Configure implements provider.Provider.
func (c *Client) Configure(ctx context.Context, req provider.ConfigureRequest) (provider.TransitionAck, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultCallTimeout)
	defer cancel()
	ack, err := c.cli.Configure(ctx, &pb.ConfigureRequest{
		MachineId:      string(req.MachineID),
		ClusterId:      string(req.ClusterID),
		BootstrapBlob:  req.BootstrapBlob,
		ShardId:        c.id.ShardID,
		ShardEpoch:     c.id.Epoch.Value(),
		SequenceNumber: c.id.Seq.Next(),
		ShardMetadata:  req.ShardMetadata,
	})
	return ackFromProto(ack, err)
}

// Drain implements provider.Provider.
func (c *Client) Drain(ctx context.Context, req provider.DrainRequest) (provider.TransitionAck, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultCallTimeout)
	defer cancel()
	ack, err := c.cli.Drain(ctx, &pb.DrainRequest{
		MachineId:          string(req.MachineID),
		GracePeriodSeconds: int64(req.GracePeriod),
		ShardId:            c.id.ShardID,
		ShardEpoch:         c.id.Epoch.Value(),
		SequenceNumber:     c.id.Seq.Next(),
	})
	return ackFromProto(ack, err)
}

// Delete implements provider.Provider.
func (c *Client) Delete(ctx context.Context, req provider.DeleteRequest) (provider.TransitionAck, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultCallTimeout)
	defer cancel()
	ack, err := c.cli.Delete(ctx, &pb.DeleteRequest{
		MachineId:      string(req.MachineID),
		ShardId:        c.id.ShardID,
		ShardEpoch:     c.id.Epoch.Value(),
		SequenceNumber: c.id.Seq.Next(),
	})
	return ackFromProto(ack, err)
}

// Get implements provider.Provider. Reads carry no fencing token.
func (c *Client) Get(ctx context.Context, machineID machine.ID) (machine.Machine, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultCallTimeout)
	defer cancel()
	m, err := c.cli.Get(ctx, &pb.MachineRef{Id: string(machineID)})
	if err != nil {
		return machine.Machine{}, mapStatusErr(err)
	}
	out, convErr := conv.MachineFromProto(m)
	if convErr != nil {
		return machine.Machine{}, fmt.Errorf("grpcclient: decode machine: %w", convErr)
	}
	return out, nil
}

// List implements provider.Provider. Reads carry no fencing token.
func (c *Client) List(ctx context.Context, filter provider.ListFilter) (provider.ListResponse, error) {
	ctx, cancel := withDefaultTimeout(ctx, defaultListTimeout)
	defer cancel()
	req := &pb.ListFilter{
		MaxResults:    int32(filter.MaxResults), //nolint:gosec // caps, not arbitrary input
		SinceRevision: filter.SinceRevision,
	}
	for _, st := range filter.States {
		req.States = append(req.States, conv.MachineStateToProto(st))
	}
	resp, err := c.cli.List(ctx, req)
	if err != nil {
		return provider.ListResponse{}, mapStatusErr(err)
	}
	out := provider.ListResponse{
		Machines: make([]machine.Machine, 0, len(resp.GetMachines())),
		Revision: resp.GetRevision(),
	}
	for _, m := range resp.GetMachines() {
		dm, convErr := conv.MachineFromProto(m)
		if convErr != nil {
			return provider.ListResponse{}, fmt.Errorf("grpcclient: decode machine: %w", convErr)
		}
		out.Machines = append(out.Machines, dm)
	}
	return out, nil
}

// ackFromProto decodes a TransitionAck, mapping transport errors first.
func ackFromProto(ack *pb.TransitionAck, err error) (provider.TransitionAck, error) {
	if err != nil {
		return provider.TransitionAck{}, mapStatusErr(err)
	}
	m, convErr := conv.MachineFromProto(ack.GetMachine())
	if convErr != nil {
		return provider.TransitionAck{}, fmt.Errorf("grpcclient: decode ack machine: %w", convErr)
	}
	return provider.TransitionAck{OperationID: ack.GetOperationId(), Machine: m}, nil
}

// mapStatusErr is the inverse of grpcadapter's mapErr: it re-attaches the
// pkg/provider sentinel errors callers match on (errors.Is), while the
// original gRPC status stays in the chain (status.FromError unwraps), so
// pkg/shard's string-reading error classifier still sees the provider's
// message verbatim.
//
// FAILED_PRECONDITION is reserved by the proto contract for fencing
// rejections — that mapping is what lets the shard treat a fenced
// mutation as a zombie-shard incident rather than a retryable error.
func mapStatusErr(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("%w: %w", provider.ErrNotFound, err)
	case codes.Unimplemented:
		return fmt.Errorf("%w: %w", provider.ErrNotSupported, err)
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %w", provider.ErrFenced, err)
	default:
		return err
	}
}

// withDefaultTimeout bounds a call that arrived without a deadline. The
// shard's execute path usually carries a cycle deadline already; this is
// the backstop for paths (and tests) that don't.
func withDefaultTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// Compile-time check that Client implements the interface.
var _ provider.Provider = (*Client)(nil)
