package shard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/intUnderflow/bigfleet/pkg/conv"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// Session implements pb.ShardServer.Session — the operator-initiated
// bidirectional stream that carries every piece of cluster ↔ shard
// traffic.
//
// Operators dial this endpoint, send a Hello, and then stay connected.
// The shard issues BootstrapRequest / ReclaimInstruction / NodeStateUpdate /
// AvailableCapacityUpdate frames; the operator answers with
// BootstrapBlobResponse / ReclaimAck.
//
// At most one session per cluster is active. A new connection from the
// same cluster replaces (and closes) the prior session.
func (s *Shard) Session(stream pb.Shard_SessionServer) error {
	// First frame must be Hello.
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "session: recv hello: %v", err)
	}
	hello := first.GetHello()
	if hello == nil || hello.GetClusterId() == "" {
		return status.Error(codes.InvalidArgument, "session: first frame must be Hello with cluster_id")
	}
	cluster := machine.ClusterID(hello.GetClusterId())
	sess := newOperatorSession(cluster, stream)
	s.installSession(cluster, sess)
	defer s.removeSession(cluster, sess)

	s.log.Info("operator session opened", "cluster", cluster, "protocol", hello.GetProtocolVersion())

	// Acknowledge the hello so operators have a positive signal that
	// the shard is processing their stream.
	_ = sess.send(&pb.ShardMessage{
		Payload: &pb.ShardMessage_Ack{Ack: &pb.Acknowledgement{
			Echo:            "hello",
			CoordinatorTerm: s.term.HighWaterMark(),
			ShardEpoch:      s.cfg.Epoch.Value(),
		}},
	})

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.log.Info("operator session closed", "cluster", cluster)
				return nil
			}
			s.log.Info("operator session recv error", "cluster", cluster, "err", err)
			return err
		}
		if err := s.handleOperatorMessage(stream.Context(), sess, msg); err != nil {
			s.log.Warn("operator message handling", "cluster", cluster, "err", err)
		}
	}
}

func (s *Shard) handleOperatorMessage(ctx context.Context, sess *operatorSession, msg *pb.OperatorMessage) error {
	switch p := msg.GetPayload().(type) {
	case *pb.OperatorMessage_Hello:
		// Operators may send Hello again after reconnect (rare). Ack it.
		return sess.send(&pb.ShardMessage{
			Payload: &pb.ShardMessage_Ack{Ack: &pb.Acknowledgement{
				Echo: "hello", CoordinatorTerm: s.term.HighWaterMark(), ShardEpoch: s.cfg.Epoch.Value(),
			}},
		})
	case *pb.OperatorMessage_Rollup:
		domainNeeds, err := conv.NeedsFromRollup(p.Rollup)
		if err != nil {
			return fmt.Errorf("rollup: %w", err)
		}
		s.needs.Replace(sess.cluster, domainNeeds)
		s.triggerCycle()
		_ = ctx
		return sess.send(&pb.ShardMessage{
			Payload: &pb.ShardMessage_Ack{Ack: &pb.Acknowledgement{
				Echo: "rollup", CoordinatorTerm: s.term.HighWaterMark(), ShardEpoch: s.cfg.Epoch.Value(),
			}},
		})
	case *pb.OperatorMessage_BootstrapResponse:
		sess.deliverBootstrapResponse(p.BootstrapResponse)
		return nil
	case *pb.OperatorMessage_ReclaimAck:
		sess.deliverReclaimAck(p.ReclaimAck)
		return nil
	}
	return fmt.Errorf("unknown OperatorMessage payload")
}

// installSession registers a new session, replacing (and closing) any
// prior session for the same cluster.
func (s *Shard) installSession(cluster machine.ClusterID, sess *operatorSession) {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	if prev, ok := s.sessionsByCluster[cluster]; ok {
		prev.close()
	}
	s.sessionsByCluster[cluster] = sess
}

// removeSession unregisters a session. The argument check ensures we
// don't accidentally remove a session that has already been replaced.
func (s *Shard) removeSession(cluster machine.ClusterID, sess *operatorSession) {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	if cur, ok := s.sessionsByCluster[cluster]; ok && cur == sess {
		delete(s.sessionsByCluster, cluster)
	}
	sess.close()
	// Forget AvailableCapacity dedup state so a reconnecting operator
	// (which may have lost its CRs across an API-server restart)
	// receives a fresh emit on the first cycle after reconnect rather
	// than silently waiting for the next genuine state change.
	if s.acCache != nil {
		s.acCache.forget(cluster)
	}
}

// lookupSession returns the active session for cluster, or nil if none
// is connected.
func (s *Shard) lookupSession(cluster machine.ClusterID) *operatorSession {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	return s.sessionsByCluster[cluster]
}

// operatorSession is the per-stream state. Lives for the lifetime of
// one Shard.Session RPC.
type operatorSession struct {
	cluster machine.ClusterID
	stream  pb.Shard_SessionServer

	// sendMu serialises all writes to the stream. gRPC streams require
	// that a single goroutine writes at a time.
	sendMu sync.Mutex
	closed atomic.Bool

	pendingBootstrap sync.Map // request_id (string) → chan *pb.BootstrapBlobResponse
	pendingReclaim   sync.Map // instruction_id → chan *pb.ReclaimAck
}

func newOperatorSession(cluster machine.ClusterID, stream pb.Shard_SessionServer) *operatorSession {
	return &operatorSession{
		cluster: cluster,
		stream:  stream,
	}
}

// send writes a message to the stream.
func (sess *operatorSession) send(msg *pb.ShardMessage) error {
	if sess.closed.Load() {
		return errors.New("session: closed")
	}
	sess.sendMu.Lock()
	defer sess.sendMu.Unlock()
	if sess.closed.Load() {
		return errors.New("session: closed")
	}
	return sess.stream.Send(msg)
}

func (sess *operatorSession) close() {
	if sess.closed.Swap(true) {
		return
	}
	// Wake up any waiters by closing pending channels — but they all
	// return ctx.Done() naturally on session-close; close is a soft
	// signal only.
}

// requestBootstrap sends a BootstrapRequest down the stream and waits
// for the matching BootstrapBlobResponse. ctx caps the wait.
func (sess *operatorSession) requestBootstrap(ctx context.Context, cluster machine.ClusterID, requirements []needs.Requirement) ([]byte, error) {
	id := mintID()
	ch := make(chan *pb.BootstrapBlobResponse, 1)
	sess.pendingBootstrap.Store(id, ch)
	defer sess.pendingBootstrap.Delete(id)

	req := &pb.BootstrapRequest{
		RequestId:    id,
		ClusterId:    string(cluster),
		Requirements: conv.RequirementsToProto(requirements),
	}
	if err := sess.send(&pb.ShardMessage{
		Payload: &pb.ShardMessage_BootstrapRequest{BootstrapRequest: req},
	}); err != nil {
		return nil, fmt.Errorf("send bootstrap request: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		if resp.GetError() != "" {
			return nil, fmt.Errorf("operator: %s", resp.GetError())
		}
		return resp.GetUserData(), nil
	}
}

// deliverBootstrapResponse routes an inbound response to the waiter
// registered under the same request_id, if any. Late responses are
// dropped silently.
func (sess *operatorSession) deliverBootstrapResponse(r *pb.BootstrapBlobResponse) {
	if r == nil || r.GetRequestId() == "" {
		return
	}
	v, ok := sess.pendingBootstrap.Load(r.GetRequestId())
	if !ok {
		return
	}
	ch := v.(chan *pb.BootstrapBlobResponse)
	select {
	case ch <- r:
	default:
	}
}

// sendReclaimInstruction emits a ReclaimInstruction frame to the
// operator and tracks the instruction_id so future ReclaimAcks can be
// matched. Fire-and-forget at the call site; ack tracking is best-effort
// for telemetry only.
func (sess *operatorSession) sendReclaimInstruction(machineID machine.ID, grace time.Duration, preemptorPriority int32) {
	id := mintID()
	ch := make(chan *pb.ReclaimAck, 1)
	sess.pendingReclaim.Store(id, ch)
	// We don't currently consume the ack beyond logging; reap stale
	// entries on session close. For M3 the map is small.
	_ = sess.send(&pb.ShardMessage{
		Payload: &pb.ShardMessage_ReclaimInstruction{ReclaimInstruction: &pb.ReclaimInstruction{
			InstructionId:      id,
			Nodes:              []string{string(machineID)},
			GracePeriodSeconds: int64(grace.Seconds()),
			PreemptorPriority:  preemptorPriority,
		}},
	})
}

func (sess *operatorSession) deliverReclaimAck(ack *pb.ReclaimAck) {
	if ack == nil || ack.GetInstructionId() == "" {
		return
	}
	v, ok := sess.pendingReclaim.LoadAndDelete(ack.GetInstructionId())
	if !ok {
		return
	}
	ch := v.(chan *pb.ReclaimAck)
	select {
	case ch <- ack:
	default:
	}
}

// SendNodeStateUpdate pushes a coalescing NodeStateUpdate frame down
// the stream. The receiver's supersedes_key is set to "node:<id>" so a
// stale update for the same machine is dropped on reconnect. Called
// by the shard's transition observer for every state change on a
// cluster-bound machine.
func (sess *operatorSession) SendNodeStateUpdate(u *pb.NodeStateUpdate) error {
	if u.SupersedesKey == "" {
		u.SupersedesKey = "node:" + u.GetMachineId()
	}
	return sess.send(&pb.ShardMessage{Payload: &pb.ShardMessage_NodeStateUpdate{NodeStateUpdate: u}})
}

// SendAvailableCapacityUpdate pushes a coalescing AvailableCapacityUpdate
// frame down the stream. supersedes_key is conventionally
// "available:<profile-fingerprint>" so successive updates for the same
// profile dedup at the operator. Called by the shard's per-cycle
// AvailableCapacity emission (paper §6.2).
func (sess *operatorSession) SendAvailableCapacityUpdate(u *pb.AvailableCapacityUpdate) error {
	return sess.send(&pb.ShardMessage{Payload: &pb.ShardMessage_AvailableCapacity{AvailableCapacity: u}})
}

// mintID returns a 16-character hex string suitable for a request_id /
// instruction_id. Crypto-random; no collisions in practice.
func mintID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
