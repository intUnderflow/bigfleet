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
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
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
	// ADR-0048 identity binding: on an mTLS transport the client
	// certificate must assert the cluster it claims to be — the
	// Hello.cluster_id is otherwise a free-text impersonation vector
	// (receive another cluster's reclaim instructions, or zero its
	// capacity with a forged full-replacement roll-up). Plaintext
	// transports skip the check: identity is only as strong as the
	// transport, and the plaintext posture is documented in the ADR.
	if uri, mtls, idErr := grpcutil.PeerIdentity(stream.Context()); mtls {
		if want := grpcutil.ClusterURI(string(cluster)); idErr != nil || uri != want {
			metrics.ShardSessionIdentityRejected.Inc()
			s.log.Error("operator session identity rejected",
				"cluster", cluster, "presented_identity", uri, "err", idErr)
			return status.Errorf(codes.PermissionDenied,
				"session: client certificate identity %q does not authorize cluster_id %q (want URI SAN %q)",
				uri, cluster, want)
		}
	}
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

	// M44.4 Drop B: split the recv pump from message handling. Slow
	// handlers (Rollup → needs.Replace + triggerCycle) used to run
	// inline and block delivery of every later message — including the
	// BootstrapBlobResponses the shard's executeBootstrap was waiting on
	// under the cycle ctx (10 s). One slow rollup could push every
	// in-flight executeBootstrap past its deadline, marking machines
	// Failed for an orchestration timeout that has nothing to do with
	// the machines.
	//
	// Fast paths (BootstrapResponse, ReclaimAck, Hello-Ack) stay inline:
	// they're channel sends or trivial Sends that complete in
	// microseconds. Slow paths (Rollup) are handed to a per-session
	// goroutine via sess.rollupChan so the read pump can keep draining
	// the stream and delivering responses immediately.
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
		if err := s.routeOperatorMessage(stream.Context(), sess, msg); err != nil {
			s.log.Warn("operator message handling", "cluster", cluster, "err", err)
		}
	}
}

// routeOperatorMessage dispatches an inbound frame either inline (fast
// paths whose handlers complete in microseconds) or to a per-session
// background worker (slow paths whose handlers do real work).
//
// Per-session ordering is preserved within each lane: a session has at
// most one rollup goroutine, so rollups for the same cluster apply in
// arrival order. Cross-lane ordering is intentionally relaxed —
// BootstrapResponses do not need to wait for an in-flight Rollup to
// complete before being delivered to their waiter.
func (s *Shard) routeOperatorMessage(ctx context.Context, sess *operatorSession, msg *pb.OperatorMessage) error {
	switch p := msg.GetPayload().(type) {
	case *pb.OperatorMessage_Hello:
		// Operators may send Hello again after reconnect (rare). Ack it
		// inline; the response is a single Send.
		return sess.send(&pb.ShardMessage{
			Payload: &pb.ShardMessage_Ack{Ack: &pb.Acknowledgement{
				Echo: "hello", CoordinatorTerm: s.term.HighWaterMark(), ShardEpoch: s.cfg.Epoch.Value(),
			}},
		})
	case *pb.OperatorMessage_Rollup:
		// Rollup is the slow path: NeedsFromRollup decode, needs.Replace
		// over the cluster's full demand, plus a cycle trigger. Hand it
		// off so the read pump returns immediately.
		sess.enqueueRollup(ctx, s, p.Rollup)
		return nil
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
	if prev, ok := s.sessionsByCluster[cluster]; ok {
		prev.close()
		metrics.ShardSessionLifecycle.WithLabelValues("replaced").Inc()
	} else {
		metrics.ShardSessionLifecycle.WithLabelValues("installed").Inc()
	}
	s.sessionsByCluster[cluster] = sess
	metrics.ShardActiveSessions.Set(float64(len(s.sessionsByCluster)))
	s.sessionsMu.Unlock()
}

// removeSession unregisters a session. The argument check ensures we
// don't accidentally remove a session that has already been replaced.
func (s *Shard) removeSession(cluster machine.ClusterID, sess *operatorSession) {
	s.sessionsMu.Lock()
	if cur, ok := s.sessionsByCluster[cluster]; ok && cur == sess {
		delete(s.sessionsByCluster, cluster)
		metrics.ShardSessionLifecycle.WithLabelValues("removed").Inc()
	}
	metrics.ShardActiveSessions.Set(float64(len(s.sessionsByCluster)))
	s.sessionsMu.Unlock()
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

	// rollupChan and rollupOnce wire the session to a single per-session
	// goroutine that processes rollups in arrival order. Slow rollup
	// handling no longer blocks the recv pump (M44.4 Drop B). Buffer
	// size 1 because rollups are full-replacement: we only ever need to
	// process the latest, and one in flight + one queued is enough.
	// If a third arrives while we have one queued, we drop the queued
	// one (the newer arrival supersedes — paper §3.1).
	rollupChan chan *pb.ClusterCapacityNeeds
	rollupOnce sync.Once
}

func newOperatorSession(cluster machine.ClusterID, stream pb.Shard_SessionServer) *operatorSession {
	return &operatorSession{
		cluster:    cluster,
		stream:     stream,
		rollupChan: make(chan *pb.ClusterCapacityNeeds, 1),
	}
}

// enqueueRollup hands the rollup proto to the per-session rollup worker
// (spawned lazily on first call). Buffer size 1: if a queued rollup is
// already pending, replace it with the newer one — rollups are full
// replacements per paper §3.1, so the older arrival is obsolete the
// moment a newer one is in hand.
func (sess *operatorSession) enqueueRollup(ctx context.Context, sh *Shard, rollup *pb.ClusterCapacityNeeds) {
	sess.rollupOnce.Do(func() {
		go sess.rollupWorker(ctx, sh)
	})
	for {
		select {
		case sess.rollupChan <- rollup:
			return
		default:
			// Queue already has a rollup waiting. Drain the stale one
			// and try again. The non-blocking drain may race with the
			// worker, which is fine — either way the next send succeeds.
			select {
			case <-sess.rollupChan:
			default:
			}
		}
	}
}

// rollupWorker drains the per-session rollup queue, processing one at
// a time so per-cluster rollup ordering is preserved. Runs for the
// lifetime of the session; exits when the stream context cancels or
// the session closes.
func (sess *operatorSession) rollupWorker(ctx context.Context, sh *Shard) {
	for {
		select {
		case <-ctx.Done():
			return
		case rollup, ok := <-sess.rollupChan:
			if !ok {
				return
			}
			if sess.closed.Load() {
				return
			}
			domainNeeds, err := conv.NeedsFromRollup(rollup)
			if err != nil {
				// M68b: reject-the-rollup-loudly — same posture as the
				// provider-ingest gate (validateProviderMachine). The
				// NeedsTable keeps the cluster's last-known-good demand;
				// nothing from the bad roll-up is applied.
				metrics.ShardRollupsRejected.WithLabelValues(string(sess.cluster)).Inc()
				sh.log.Error("rollup rejected at ingest validation; keeping last-known-good demand (M68b)",
					"cluster", sess.cluster, "err", err)
				continue
			}
			// ApplyRollup replaces the NeedsTable slice and marks the
			// ADR-0036 first-rollup gate (before triggerCycle, so the
			// next Phase 3 cycle sees it cleared). It may instead
			// quarantine the rollup (ADR-0046 empty-roll-up guard) —
			// the previous accepted demand stays active, so there is
			// no new cycle work and demandObservedAt must keep
			// tracking the accepted fingerprints, not the held ones.
			if sh.ApplyRollup(sess.cluster, domainNeeds) {
				sh.observeRolledUpDemand(sess.cluster, domainNeeds)
				sh.triggerCycle()
			}
			_ = sess.send(&pb.ShardMessage{
				Payload: &pb.ShardMessage_Ack{Ack: &pb.Acknowledgement{
					Echo: "rollup", CoordinatorTerm: sh.term.HighWaterMark(), ShardEpoch: sh.cfg.Epoch.Value(),
				}},
			})
		}
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
