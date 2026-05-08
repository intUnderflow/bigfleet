package operator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/intUnderflow/bigfleet/pkg/metrics"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// runOnce dials the shard, holds the bidi Session stream, runs all four
// goroutines (rollup loop, recv loop, send loop, lifecycle wait), and
// returns when any one of them errors out. The caller (Run) handles the
// reconnect backoff.
func (o *Operator) runOnce(ctx context.Context) error {
	conn, err := grpc.NewClient(o.cfg.ShardAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	// dispatchSem caps in-flight dispatch goroutines (see recvLoop).
	dispatchSem chan struct{}
}

const (
	// outboxCap bounds queued response messages per session.
	// Generous: above this the shard's RPC timeouts kick in
	// regardless. Drop-newest under load is the documented behaviour
	// (paper §10.5).
	outboxCap = 256

	// dispatchConcurrency caps the number of in-flight dispatch
	// goroutines per session. M44.4 Drop B: under burst, recvLoop was
	// spawning 1000-1500 goroutines per operator (each handling a
	// NodeStateUpdate that does 2-3 apiserver writes), causing 8 s+
	// handler p99 and 63 % Create-conflict races. 32 is well under the
	// per-cluster apiserver QPS budget (200) and large enough to keep
	// the RTT-bound writes pipelined; the bound itself is what limits
	// heap pressure, GC overhead, and cache races.
	dispatchConcurrency = 32
)

func newSession(stream pb.Shard_SessionClient, op *Operator) *session {
	return &session{
		op:           op,
		stream:       stream,
		rollupSignal: make(chan struct{}, 1),
		outbox:       make(chan *pb.OperatorMessage, outboxCap),
	}
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
	switch msg.GetPayload().(type) {
	case *pb.ShardMessage_NodeStateUpdate,
		*pb.ShardMessage_AvailableCapacity,
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
