package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/intUnderflow/bigfleet/pkg/metrics"
)

// Config configures a Coordinator.
type Config struct {
	// NodeID is the per-replica unique identifier.
	NodeID string

	// DataDir is the local directory where Raft logs + snapshots +
	// the BoltDB stable store live.
	DataDir string

	// RaftBindAddress is the host:port the Raft transport listens on
	// (and is advertised to peers as).
	RaftBindAddress string

	// AdvertiseAddress overrides RaftBindAddress when advertising to
	// peers (e.g., when binding to 0.0.0.0). Optional.
	AdvertiseAddress string

	// Bootstrap, if true, bootstraps a fresh single-node cluster.
	// Use only on the very first Coordinator's first run; subsequent
	// replicas join via Join().
	Bootstrap bool

	// SnapshotRetention is how many snapshot files Raft retains.
	// Defaults to 2.
	SnapshotRetention int

	// Logger receives structured events.
	Logger *slog.Logger
}

// Coordinator is the running global coordinator. Construct via New
// and Run.
type Coordinator struct {
	cfg Config
	log *slog.Logger

	fsm  *FSM
	raft *raft.Raft

	// transport is held so we can close it during shutdown.
	transport raft.Transport

	// Backing stores held so we can close them during shutdown.
	logStore      raft.LogStore
	stableStore   raft.StableStore
	snapshotStore raft.SnapshotStore

	closeMu sync.Mutex
	closed  bool
}

// New constructs a Coordinator and brings up its Raft node
// synchronously. Returns once the local Raft replica is initialised
// (transport listening, BoltDB stores opened, NewRaft completed). The
// returned Coordinator is fully usable — tests and callers can read
// state, call Apply (after waiting for a leader), and so on.
//
// Use Run for the blocking lifecycle that closes everything on context
// cancellation.
func New(cfg Config) (*Coordinator, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("coordinator: Config.NodeID required")
	}
	if cfg.DataDir == "" {
		return nil, errors.New("coordinator: Config.DataDir required")
	}
	if cfg.RaftBindAddress == "" {
		return nil, errors.New("coordinator: Config.RaftBindAddress required")
	}
	if cfg.SnapshotRetention == 0 {
		cfg.SnapshotRetention = 2
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	c := &Coordinator{
		cfg: cfg,
		log: log.With("component", "coordinator", "node", cfg.NodeID),
		fsm: NewFSM(),
	}
	if err := c.start(); err != nil {
		c.close()
		return nil, err
	}
	return c, nil
}

// FSM returns the FSM (read-only access to State for callers).
func (c *Coordinator) FSM() *FSM { return c.fsm }

// State is a convenience pass-through.
func (c *Coordinator) State() *State { return c.fsm.State() }

// Raft returns the underlying Raft node. Used in tests; production
// callers should use the Apply / IsLeader helpers.
func (c *Coordinator) Raft() *raft.Raft { return c.raft }

// Run blocks until ctx is cancelled and closes Raft on exit. Raft is
// already running when Run is called (started by New); this is just
// the lifecycle hook that ties shutdown to ctx.
func (c *Coordinator) Run(ctx context.Context) error {
	defer c.close()
	c.log.Info("coordinator started", "raft_bind", c.cfg.RaftBindAddress, "data_dir", c.cfg.DataDir)
	<-ctx.Done()
	return ctx.Err()
}

// Close shuts the coordinator down without an outer Run loop. Useful
// in tests that don't want the goroutine.
func (c *Coordinator) Close() { c.close() }

// start brings up Raft synchronously. Separated from Run for tests
// that want to drive the node without an outer Run loop.
func (c *Coordinator) start() error {
	if err := os.MkdirAll(c.cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("coordinator: data dir: %w", err)
	}

	rcfg := raft.DefaultConfig()
	rcfg.LocalID = raft.ServerID(c.cfg.NodeID)
	rcfg.SnapshotInterval = 30 * time.Second
	rcfg.SnapshotThreshold = 1024
	// hashicorp/raft uses hclog; we don't bridge slog for now and let
	// the default stderr writer handle Raft's own logging. Re-evaluate
	// when production observability lands.

	addr := c.cfg.AdvertiseAddress
	if addr == "" {
		addr = c.cfg.RaftBindAddress
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return fmt.Errorf("coordinator: resolve advertise %q: %w", addr, err)
	}
	transport, err := raft.NewTCPTransport(c.cfg.RaftBindAddress, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("coordinator: tcp transport: %w", err)
	}
	c.transport = transport

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(c.cfg.DataDir, "raft-log.db"))
	if err != nil {
		return fmt.Errorf("coordinator: log store: %w", err)
	}
	c.logStore = logStore

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(c.cfg.DataDir, "raft-stable.db"))
	if err != nil {
		return fmt.Errorf("coordinator: stable store: %w", err)
	}
	c.stableStore = stableStore

	snapStore, err := raft.NewFileSnapshotStore(c.cfg.DataDir, c.cfg.SnapshotRetention, os.Stderr)
	if err != nil {
		return fmt.Errorf("coordinator: snapshot store: %w", err)
	}
	c.snapshotStore = snapStore

	r, err := raft.NewRaft(rcfg, c.fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return fmt.Errorf("coordinator: raft.NewRaft: %w", err)
	}
	c.raft = r

	if c.cfg.Bootstrap {
		hasState, herr := raft.HasExistingState(logStore, stableStore, snapStore)
		if herr != nil {
			return fmt.Errorf("coordinator: probe existing state: %w", herr)
		}
		if !hasState {
			cfg := raft.Configuration{
				Servers: []raft.Server{{
					ID:      raft.ServerID(c.cfg.NodeID),
					Address: transport.LocalAddr(),
				}},
			}
			if err := r.BootstrapCluster(cfg).Error(); err != nil {
				return fmt.Errorf("coordinator: bootstrap: %w", err)
			}
		}
	}

	return nil
}

// close shuts Raft down and closes backing stores. Safe to call
// multiple times.
func (c *Coordinator) close() {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	if c.raft != nil {
		_ = c.raft.Shutdown().Error()
	}
	if closer, ok := c.logStore.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
	if closer, ok := c.stableStore.(interface{ Close() error }); ok {
		_ = closer.Close()
	}
}

// Apply submits a Command through Raft. Only callable on the leader;
// returns raft.ErrNotLeader otherwise.
func (c *Coordinator) Apply(ctx context.Context, cmd Command) error {
	if c.raft == nil {
		return errors.New("coordinator: not started")
	}
	data, err := cmd.Encode()
	if err != nil {
		return fmt.Errorf("coordinator: encode command: %w", err)
	}
	timeout := 5 * time.Second
	if d, ok := ctx.Deadline(); ok {
		if rem := time.Until(d); rem > 0 && rem < timeout {
			timeout = rem
		}
	}
	f := c.raft.Apply(data, timeout)
	if err := f.Error(); err != nil {
		metrics.CoordinatorApplyTotal.WithLabelValues("error").Inc()
		return err
	}
	if resp := f.Response(); resp != nil {
		if e, ok := resp.(error); ok && e != nil {
			metrics.CoordinatorApplyTotal.WithLabelValues("fsm_error").Inc()
			return e
		}
	}
	metrics.CoordinatorApplyTotal.WithLabelValues("success").Inc()
	return nil
}

// IsLeader reports whether this node is the current Raft leader.
func (c *Coordinator) IsLeader() bool {
	return c.raft != nil && c.raft.State() == raft.Leader
}

// RaftTerm returns the current Raft term as known to this replica.
// Used by the gRPC server to stamp ReportAcks.
func (c *Coordinator) RaftTerm() int64 {
	if c.raft == nil {
		return 0
	}
	stats := c.raft.Stats()
	if v, ok := stats["term"]; ok {
		var n int64
		// stats values are decimal strings.
		for i := 0; i < len(v); i++ {
			if v[i] < '0' || v[i] > '9' {
				return 0
			}
			n = n*10 + int64(v[i]-'0')
		}
		return n
	}
	return 0
}

// LeaderAddress returns the current leader's advertised address, if
// known. Empty string when no leader is currently elected.
func (c *Coordinator) LeaderAddress() string {
	if c.raft == nil {
		return ""
	}
	addr, _ := c.raft.LeaderWithID()
	return string(addr)
}

// WaitForLeader blocks until a leader is elected or ctx expires.
func (c *Coordinator) WaitForLeader(ctx context.Context) error {
	for {
		if addr := c.LeaderAddress(); addr != "" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// AddVoter adds a new voter to the Raft cluster. Only callable on the
// leader. Used during cluster bring-up to join replicas.
func (c *Coordinator) AddVoter(id, addr string) error {
	if c.raft == nil {
		return errors.New("coordinator: not started")
	}
	f := c.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 5*time.Second)
	return f.Error()
}

// RemoveServer removes a peer from the Raft cluster.
func (c *Coordinator) RemoveServer(id string) error {
	if c.raft == nil {
		return errors.New("coordinator: not started")
	}
	f := c.raft.RemoveServer(raft.ServerID(id), 0, 5*time.Second)
	return f.Error()
}
