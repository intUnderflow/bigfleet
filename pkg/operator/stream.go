package operator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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

// session is the per-connection state held by runOnce.
type session struct {
	op     *Operator
	stream pb.Shard_SessionClient

	// outbox is drained by sendLoop. Buffered so the rollup loop can
	// enqueue without blocking; if the send side falls behind, the
	// channel backpressures the producers (acceptable for M4).
	outbox chan *pb.OperatorMessage
}

func newSession(stream pb.Shard_SessionClient, op *Operator) *session {
	return &session{
		op:     op,
		stream: stream,
		outbox: make(chan *pb.OperatorMessage, 64),
	}
}

// enqueue places a frame on the outbox. Blocks if the outbox is full.
func (s *session) enqueue(ctx context.Context, msg *pb.OperatorMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.outbox <- msg:
		return nil
	}
}

// sendLoop drains the outbox into the stream. Returns when the stream
// errors out or ctx is cancelled.
func (s *session) sendLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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

// recvLoop reads frames from the stream and dispatches them. Dispatch
// runs in a goroutine per frame so a slow handler (CRD write,
// kubelet-template render) doesn't block the stream's read pump.
// Without this, the shard's parallel-execute (M11.18) just queues
// behind the operator's serial recv loop and the cycle SLO blows
// regardless of shard concurrency.
func (s *session) recvLoop(ctx context.Context) error {
	for {
		msg, err := s.stream.Recv()
		if err != nil {
			return err
		}
		go func(msg *pb.ShardMessage) {
			if err := s.dispatch(ctx, msg); err != nil {
				s.op.log.Warn("dispatch failed", "err", err)
			}
		}(msg)
	}
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
