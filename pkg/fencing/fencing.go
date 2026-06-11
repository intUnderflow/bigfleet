// Package fencing implements the term / epoch / sequence helpers BigFleet
// uses to detect zombie senders.
//
// Two directions:
//
//   - Coordinator → shard: every CoordinatorInstruction carries
//     (coordinator_term, sequence_number). Shards keep a high-water mark of
//     the highest term they've seen and reject any instruction with a stale
//     term. CoordinatorTerm exposes that high-water mark and Validate
//     answers the rejection question.
//
//   - Shard → provider: every mutating provider RPC (Create / Configure /
//     Drain / Delete — reads don't fence) carries (shard_id, shard_epoch,
//     sequence_number), stamped by pkg/provider/grpcclient from this
//     package's Epoch and Sequence (M71). Providers keep a per-shard_id
//     high-water mark and reject non-monotonic tokens with
//     FAILED_PRECONDITION — the contract lives in
//     api/proto/bigfleet/v1alpha1/provider.proto. Epoch increments on
//     every restart and is persisted to local disk, so a fresh process
//     always fences higher than any prior process and a zombie can't
//     issue stale-state mutations once its successor has made contact.
//
// The package is intentionally tiny: it owns just enough state to make
// fencing correct, no policy.
package fencing

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ErrStaleTerm is returned by CoordinatorTerm.Validate when an
// instruction's term is older than the shard's current high-water mark.
var ErrStaleTerm = errors.New("fencing: stale coordinator term")

// CoordinatorTerm tracks the highest coordinator term the shard has
// observed. Safe for concurrent use.
type CoordinatorTerm struct {
	mu  sync.RWMutex
	hwm int64
}

// NewCoordinatorTerm returns a fresh tracker initialised at term 0.
func NewCoordinatorTerm() *CoordinatorTerm { return &CoordinatorTerm{} }

// HighWaterMark returns the highest term observed so far.
func (c *CoordinatorTerm) HighWaterMark() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hwm
}

// Validate accepts a term observed in an inbound instruction. Returns
// ErrStaleTerm if the term is older than the high-water mark; otherwise
// updates the high-water mark (if the term advanced) and returns nil.
//
// Equal terms are accepted: the same elected coordinator may legitimately
// issue many instructions at the same term.
func (c *CoordinatorTerm) Validate(term int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if term < c.hwm {
		return fmt.Errorf("%w: got %d, hwm %d", ErrStaleTerm, term, c.hwm)
	}
	if term > c.hwm {
		c.hwm = term
	}
	return nil
}

// Epoch is the shard's per-process epoch. It is monotonic across
// restarts of the same shard ID. Persisted to a small file under the
// shard's data directory; loaded on startup, incremented, written back,
// then handed out as the shard's identity.
type Epoch struct {
	value int64
}

// LoadEpoch reads the persisted epoch from path, increments it, and
// writes the new value back. If path doesn't exist, starts at 1. The
// returned Epoch's value is what the shard advertises to providers and
// the coordinator until the next restart.
func LoadEpoch(path string) (*Epoch, error) {
	if path == "" {
		return nil, errors.New("fencing: epoch path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("fencing: prepare epoch dir: %w", err)
	}

	var prev int64
	if data, err := os.ReadFile(path); err == nil {
		v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("fencing: parse epoch %q: %w", path, err)
		}
		prev = v
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("fencing: read epoch: %w", err)
	}

	next := prev + 1
	if err := os.WriteFile(path, []byte(strconv.FormatInt(next, 10)), 0o600); err != nil {
		return nil, fmt.Errorf("fencing: write epoch: %w", err)
	}
	return &Epoch{value: next}, nil
}

// Value returns the epoch number this shard is currently at.
func (e *Epoch) Value() int64 { return e.value }

// Sequence is a monotonic per-shard counter for outbound mutating
// provider RPCs, carried over the wire alongside the shard epoch by
// pkg/provider/grpcclient. One fresh value per call attempt — retries
// re-stamp, so a provider's strictly-newer check never mistakes a
// transport retry for a replay.
type Sequence struct {
	mu  sync.Mutex
	seq int64
}

// Next returns the next sequence number. Always non-zero.
func (s *Sequence) Next() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	return s.seq
}
