package operator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// runOnce dials the shard, holds the bidi Session stream, runs all four
// goroutines (rollup loop, recv loop, send loop, lifecycle wait), and
// returns when any one of them errors out. The caller (Run) handles the
// reconnect backoff.
func (o *Operator) runOnce(ctx context.Context) error {
	conn, err := grpc.NewClient(o.cfg.ShardAddress,
		append(grpcutil.DialOptions(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		return fmt.Errorf("dial shard: %w", err)
	}
	defer func() { _ = conn.Close() }()

	cli := pb.NewShardClient(conn)
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	stream, err := cli.Session(streamCtx)
	if err != nil {
		return fmt.Errorf("open session: %w", err)
	}

	// Send Hello first.
	if err := stream.Send(&pb.OperatorMessage{
		Payload: &pb.OperatorMessage_Hello{Hello: &pb.Hello{
			ClusterId:       string(o.cfg.ClusterID),
			ProtocolVersion: "v1alpha1",
		}},
	}); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}

	sess := newSession(stream, o)

	// Goroutines:
	//  - rollupLoop ticks every RollupInterval, computes a rollup,
	//    enqueues it on the send channel.
	//  - recvLoop reads from the stream, dispatches to handlers.
	//  - sendLoop drains the send channel and writes to the stream.
	// Any one returning aborts the others via streamCtx cancel.

	g := newErrGroup(streamCtx, streamCancel)
	g.Go(func(ctx context.Context) error { return sess.sendLoop(ctx) })
	g.Go(func(ctx context.Context) error { return sess.recvLoop(ctx) })
	g.Go(func(ctx context.Context) error { return o.rollupLoop(ctx, sess) })

	return g.Wait()
}

// session is the per-connection state held by runOnce. The send path
// has two queues with different drop policies (paper §10.5 — bound the
// per-cluster outbox so a slow stream can't accumulate unbounded
// memory):
//
//   - pendingRollup: a single-slot atomic.Pointer for ClusterCapacityNeeds
//     rollups. Each rollup is full replacement (paper §3.1), so writing
//     a fresh rollup atomically replaces and DROPS any pending older
//     one. Coalesce-by-replace.
//
//   - outbox: a bounded channel for non-coalescing messages
//     (BootstrapBlobResponse, ReclaimAck). Drops the new message with a
//     metric when full — these are RPC responses tied to a shard
//     request; the shard will re-issue on timeout, so drop-newest is
//     correct (a queued-up response would only deliver after the
//     shard has timed out anyway).
type session struct {
	op     *Operator
	stream pb.Shard_SessionClient

	pendingRollup atomic.Pointer[pb.OperatorMessage]
	rollupSignal  chan struct{}
	outbox        chan *pb.OperatorMessage
	// dispatchSem caps in-flight goroutines for apiserver-bound
	// handlers (NodeStateUpdate, ReclaimInstruction, AvailableCapacity).
	// BootstrapRequest bypasses the sem entirely (see recvLoop).
	dispatchSem chan struct{}

	// nodeStatePending coalesces NodeStateUpdate frames per machine.
	// The shard emits a NodeStateUpdate for every transition (Idle →
	// Configuring → Configured for a binding); under burst the handler
	// runs against an apiserver write each time even though only the
	// latest state is observable. nodeStatePending stores the latest
	// inbound update per machine; nodeStateInFlight tracks which
	// machines have a worker active so we never spawn two for the
	// same ID. M44.4 Drop B: this halves operator-side apiserver
	// writes during a burst.
	nodeStateMu       sync.Mutex
	nodeStatePending  map[string]*pb.NodeStateUpdate
	nodeStateInFlight map[string]bool
}

const (
	// outboxCap bounds queued response messages per session.
	// Generous: above this the shard's RPC timeouts kick in
	// regardless. Drop-newest under load is the documented behaviour
	// (paper §10.5).
	outboxCap = 256

	// dispatchConcurrency caps in-flight goroutines for apiserver-bound
	// message handlers (NodeStateUpdate, ReclaimInstruction,
	// AvailableCapacityUpdate). M44.4 Drop B: 256 was actively
	// counterproductive — handlers contended on the controller-runtime
	// client's internal locks (cache RWMutex, write-rate-limiter token
	// bucket) and the per-handler tail blew up to 32 s p99 even though
	// apiserver throughput was at <1 % of budget. Dropped back to 32:
	// matches what each operator actually needs to drive its slice of
	// fleet-wide write traffic. Coalescing (see coalesceNodeStateUpdate)
	// further reduces the per-machine write count, so the lower bound
	// doesn't cap useful concurrency.
	dispatchConcurrency = 32
)

func newSession(stream pb.Shard_SessionClient, op *Operator) *session {
	return &session{
		op:                op,
		stream:            stream,
		rollupSignal:      make(chan struct{}, 1),
		outbox:            make(chan *pb.OperatorMessage, outboxCap),
		nodeStatePending:  make(map[string]*pb.NodeStateUpdate),
		nodeStateInFlight: make(map[string]bool),
	}
}

// coalesceNodeStateUpdate stores update as the latest pending state for
// its machine, returning true if a worker should be spawned to process
// it (no in-flight worker for that machine yet). When the worker
// finishes, it loops if a new update arrived in the meantime — so
// rapid Idle→Configuring→Configured sequences for the same machine
// collapse to a single apiserver write of the terminal state.
func (s *session) coalesceNodeStateUpdate(update *pb.NodeStateUpdate) bool {
	s.nodeStateMu.Lock()
	defer s.nodeStateMu.Unlock()
	id := update.GetMachineId()
	s.nodeStatePending[id] = update
	if s.nodeStateInFlight[id] {
		return false
	}
	s.nodeStateInFlight[id] = true
	return true
}

// takeNextNodeStateUpdate returns the latest pending update for the
// given machine and clears the slot. If nothing is pending, drops the
// in-flight flag and returns nil — the next coalesce call will spawn
// a fresh worker.
func (s *session) takeNextNodeStateUpdate(id string) *pb.NodeStateUpdate {
	s.nodeStateMu.Lock()
	defer s.nodeStateMu.Unlock()
	update, ok := s.nodeStatePending[id]
	if !ok {
		delete(s.nodeStateInFlight, id)
		return nil
	}
	delete(s.nodeStatePending, id)
	return update
}

// enqueueRollup replaces the pending rollup atomically. Older pending
// rollups are dropped — they're superseded by the new full-replacement
// state.
func (s *session) enqueueRollup(msg *pb.OperatorMessage) {
	s.pendingRollup.Store(msg)
	select {
	case s.rollupSignal <- struct{}{}:
	default:
		// Signal already pending; sendLoop will pick up the latest
		// rollup on its next iteration.
	}
}

// enqueue places a non-rollup frame (BootstrapBlobResponse, ReclaimAck)
// on the bounded outbox. Drops with a metric if the queue is full —
// see the session-level doc for why drop-newest is correct.
func (s *session) enqueue(ctx context.Context, msg *pb.OperatorMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.outbox <- msg:
		return nil
	default:
		metrics.OperatorOutboxDropped.Inc()
		return errOutboxFull
	}
}

var errOutboxFull = errors.New("operator: outbox full; dropped (shard will re-issue on timeout)")

// sendLoop drains both the pending-rollup slot and the outbox into the
// stream. Returns when the stream errors out or ctx is cancelled.
func (s *session) sendLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.rollupSignal:
			if msg := s.pendingRollup.Swap(nil); msg != nil {
				if err := s.stream.Send(msg); err != nil {
					return fmt.Errorf("stream.Send: %w", err)
				}
			}
		case msg, ok := <-s.outbox:
			if !ok {
				return nil
			}
			if err := s.stream.Send(msg); err != nil {
				return fmt.Errorf("stream.Send: %w", err)
			}
		}
	}
}

// recvLoop reads frames from the stream and dispatches them. The
// pool of in-flight dispatchers is split by message kind:
//
//   - Slow / apiserver-bound messages (NodeStateUpdate,
//     AvailableCapacityUpdate) go through a bounded semaphore. Without
//     a bound the burst NodeStateUpdate fan-out spawns 1000+ in-flight
//     handlers per operator, all queueing behind controller-runtime's
//     cache layer (M44.4 Drop B: 8 s+ handler p99 from this).
//
//   - Fast / shard-blocking messages (BootstrapRequest) bypass the
//     semaphore and run in their own goroutine. The shard's
//     executeBootstrap blocks on requestBootstrap with a one-cycle
//     deadline (10 s default); if those queue behind a slow
//     NodeStateUpdate at the bounded pool's gate, the shard cancels
//     and the machine ends up in Failed. Fast handlers don't write
//     to the apiserver — the unbounded path is safe.
func (s *session) recvLoop(ctx context.Context) error {
	if s.dispatchSem == nil {
		s.dispatchSem = make(chan struct{}, dispatchConcurrency)
	}
	for {
		msg, err := s.stream.Recv()
		if err != nil {
			return err
		}
		// NodeStateUpdate goes through the coalescer rather than a
		// per-message goroutine: under burst the shard emits multiple
		// state transitions per machine in rapid succession (Idle →
		// Configuring → Configured) and the operator only needs to
		// reflect the latest. coalesceNodeStateUpdate atomically stores
		// the latest pending update for the machine; if no worker is
		// already in flight for it we spawn one (taking a sem slot).
		// The worker loops, picking up any newer update that arrived
		// while it was processing.
		if u := msg.GetNodeStateUpdate(); u != nil {
			if !s.coalesceNodeStateUpdate(u) {
				continue
			}
			select {
			case s.dispatchSem <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			go func(machineID string) {
				metrics.OperatorDispatchInflight.Inc()
				defer func() {
					metrics.OperatorDispatchInflight.Dec()
					<-s.dispatchSem
				}()
				for {
					update := s.takeNextNodeStateUpdate(machineID)
					if update == nil {
						return
					}
					if err := s.op.handleNodeStateUpdate(ctx, update); err != nil {
						s.op.log.Warn("dispatch failed", "err", err)
					}
				}
			}(u.GetMachineId())
			continue
		}
		bounded := needsBoundedDispatch(msg)
		if bounded {
			select {
			case s.dispatchSem <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		go func(msg *pb.ShardMessage, bounded bool) {
			metrics.OperatorDispatchInflight.Inc()
			defer func() {
				metrics.OperatorDispatchInflight.Dec()
				if bounded {
					<-s.dispatchSem
				}
			}()
			if err := s.dispatch(ctx, msg); err != nil {
				s.op.log.Warn("dispatch failed", "err", err)
			}
		}(msg, bounded)
	}
}

// needsBoundedDispatch returns true for message kinds whose handler
// does apiserver writes — those are the ones whose unbounded
// concurrency caused the M44.4 Drop B back-pressure. Fast handlers
// (BootstrapRequest = CPU-only blob render, Hello/Ack = no work)
// bypass the bound so they never get blocked behind a slow
// NodeStateUpdate.
func needsBoundedDispatch(msg *pb.ShardMessage) bool {
	// NodeStateUpdate is handled separately via the coalescer in
	// recvLoop, so it doesn't appear here.
	switch msg.GetPayload().(type) {
	case *pb.ShardMessage_AvailableCapacity,
		*pb.ShardMessage_ReclaimInstruction:
		return true
	}
	return false
}

// dispatch routes one inbound frame to the appropriate handler.
func (s *session) dispatch(ctx context.Context, msg *pb.ShardMessage) error {
	switch p := msg.GetPayload().(type) {
	case *pb.ShardMessage_Ack:
		// Acks are observability only.
		return nil
	case *pb.ShardMessage_BootstrapRequest:
		return s.op.handleBootstrapRequest(ctx, s, p.BootstrapRequest)
	case *pb.ShardMessage_ReclaimInstruction:
		return s.op.handleReclaimInstruction(ctx, s, p.ReclaimInstruction)
	case *pb.ShardMessage_NodeStateUpdate:
		return s.op.handleNodeStateUpdate(ctx, p.NodeStateUpdate)
	case *pb.ShardMessage_AvailableCapacity:
		return s.op.handleAvailableCapacityUpdate(ctx, p.AvailableCapacity)
	}
	return errors.New("unknown ShardMessage payload")
}

// errGroup is a small wrapper around sync.WaitGroup + ctx-cancel that
// returns the first non-nil error from any goroutine. Avoids pulling in
// golang.org/x/sync just for this; the standard library has us covered.
type errGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	wg     sync.WaitGroup
	err    error
}

func newErrGroup(ctx context.Context, cancel context.CancelFunc) *errGroup {
	return &errGroup{ctx: ctx, cancel: cancel}
}

func (g *errGroup) Go(fn func(context.Context) error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := fn(g.ctx); err != nil {
			g.mu.Lock()
			if g.err == nil {
				g.err = err
			}
			g.mu.Unlock()
			g.cancel()
		}
	}()
}

func (g *errGroup) Wait() error {
	g.wg.Wait()
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}
