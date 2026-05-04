// Package fake is an in-memory CapacityProvider implementation used as
// a test fixture by the decision engine, the shard, and the simulator.
//
// This is NOT a deployable artifact. It has no Helm chart, no published
// image, no gRPC surface. Real providers live in separate repositories
// (per CLAUDE.md). The fake exists solely so we can exercise the engine
// without standing up a real provider.
package fake

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/provider"
)

// Provider is an in-memory CapacityProvider. Configurable via Options.
//
// Concurrency: an RWMutex protects all state. List and Get are readers
// (RLock), so the shard's reconcile can run concurrently with itself
// or with Get RPCs. Mutating RPCs (Create/Configure/Drain/Delete) and
// the Add* / FailNext seed methods are writers (Lock); they serialise
// with each other and with readers.
type Provider struct {
	mu sync.RWMutex

	machines map[machine.ID]*machine.Machine

	// lastModRev is the value of `rev` at the most recent mutation of
	// each machine. Used to honour ListFilter.SinceRevision: a machine
	// is included in a delta List iff its lastModRev is strictly greater
	// than the cursor. The fake provider satisfies the §0.1 C "above
	// conformance threshold" contract — `since_revision` is honoured and
	// the response Revision is monotonically advancing.
	lastModRev map[machine.ID]int

	// revLog is an append-only sequence of (rev, id) entries written
	// on every mutation. Used by List(SinceRevision=R) to find which
	// machines have changed since R without scanning all of
	// p.machines: binary-search for the first rev > R, walk forward,
	// dedup IDs (a single machine may appear multiple times if mutated
	// repeatedly), and emit the current state of each unique ID.
	//
	// At steady state with k changes/cycle and N inventory, this drops
	// per-cycle reconcile cost from O(N) to O(k + uniqueIDs).
	//
	// Memory grows monotonically with mutation count; for a long-lived
	// process this would need compaction, but the harness recreates
	// the fake on each run.
	revLog []revEntry

	// ops maps (machine_id, kind) → operation_id for idempotency. An
	// operation is "active" while the machine is at-or-progressing-to
	// the kind's target state. Once the machine moves on to a different
	// stable state, a future call of the same kind mints a fresh op.
	ops map[opKey]string

	// nextOp mints fresh operation IDs.
	nextOp int

	// rev is incremented on every state change; List returns the
	// current value as Revision so callers can track deltas.
	rev int

	// failNext queues an error to be returned by the next applicable
	// RPC. Test hook; cleared after firing.
	failNext map[failKey]error

	// rand is a deterministic source for any randomised behaviour
	// (currently unused beyond stable fingerprinting).
	rand *rand.Rand //nolint:unused

	// instantTransitions: if true, transitions complete immediately
	// (Speculative→Idle on Create returns a machine in Idle state, not
	// Creating). Used by tests that want to skip the staged transitions.
	instantTransitions bool
}

// revEntry is one append to the revision log: which machine's
// mutation produced this rev.
type revEntry struct {
	rev int
	id  machine.ID
}

// Options configures a fake provider.
type Options struct {
	InstantTransitions bool
	Seed               uint64
}

// New constructs a fake provider with no inventory. Seed via AddSpeculative
// or AddIdle.
func New(opts Options) *Provider {
	seed := opts.Seed
	if seed == 0 {
		seed = 0xDEADBEEFCAFEBABE
	}
	return &Provider{
		machines:           make(map[machine.ID]*machine.Machine),
		lastModRev:         make(map[machine.ID]int),
		ops:                make(map[opKey]string),
		failNext:           make(map[failKey]error),
		rand:               rand.New(rand.NewPCG(seed, seed^0xA5A5A5A5)),
		instantTransitions: opts.InstantTransitions,
	}
}

// AddSpeculative inserts a speculative quota slot.
func (p *Provider) AddSpeculative(id machine.ID, profile machine.Profile, capType machine.CapacityType, pricePerHour, interruptionProb float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	prof := profile
	if prof.CapacityType == machine.CapacityTypeUnspecified {
		prof.CapacityType = capType
	}
	p.machines[id] = &machine.Machine{
		ID:                      id,
		State:                   machine.StateSpeculative,
		Profile:                 prof,
		PricePerHour:            pricePerHour,
		InterruptionProbability: interruptionProb,
	}
	p.rev++
	p.lastModRev[id] = p.rev
	p.revLog = append(p.revLog, revEntry{rev: p.rev, id: id})
}

// AddIdle inserts an already-running, unbound machine. Used to model
// fixed inventory (bare metal) that exists regardless of any Create call.
func (p *Provider) AddIdle(id machine.ID, profile machine.Profile, capType machine.CapacityType, pricePerHour, interruptionProb float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	prof := profile
	if prof.CapacityType == machine.CapacityTypeUnspecified {
		prof.CapacityType = capType
	}
	p.machines[id] = &machine.Machine{
		ID:                      id,
		State:                   machine.StateIdle,
		Host:                    machine.HostRef{Provider: "fake", Ref: string(id)},
		Profile:                 prof,
		PricePerHour:            pricePerHour,
		InterruptionProbability: interruptionProb,
	}
	p.rev++
	p.lastModRev[id] = p.rev
	p.revLog = append(p.revLog, revEntry{rev: p.rev, id: id})
}

// FailNext queues a single error to be returned the next time the
// matching (machine_id, target_state) RPC is called. Useful for testing
// the shard's retry / Failed-state handling.
func (p *Provider) FailNext(id machine.ID, target machine.State, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failNext[failKey{id, target}] = err
}

// Create implements provider.Provider.
func (p *Provider) Create(_ context.Context, req provider.CreateRequest) (provider.TransitionAck, error) {
	return p.applyTransition(req.MachineID, opCreate, func(m *machine.Machine) {
		m.Host = machine.HostRef{Provider: "fake", Ref: string(req.MachineID)}
		for k, v := range req.Labels {
			if m.Profile.Labels == nil {
				m.Profile.Labels = make(map[string]string)
			}
			m.Profile.Labels[k] = v
		}
	})
}

// Configure implements provider.Provider.
func (p *Provider) Configure(_ context.Context, req provider.ConfigureRequest) (provider.TransitionAck, error) {
	return p.applyTransition(req.MachineID, opConfigure, func(m *machine.Machine) {
		m.Cluster = req.ClusterID
	})
}

// Drain implements provider.Provider.
func (p *Provider) Drain(_ context.Context, req provider.DrainRequest) (provider.TransitionAck, error) {
	return p.applyTransition(req.MachineID, opDrain, func(m *machine.Machine) {
		m.Cluster = ""
		m.AssignedPriority = 0
		m.AssignedInterruptionPenaltyDollars = 0
		m.AssignedReclamationPenaltyDollars = 0
	})
}

// Delete implements provider.Provider. Bare-metal-style providers should
// return ErrNotSupported instead.
func (p *Provider) Delete(_ context.Context, id machine.ID) (provider.TransitionAck, error) {
	return p.applyTransition(id, opDelete, func(m *machine.Machine) {
		m.Host = machine.HostRef{}
	})
}

// applyTransition is the shared implementation of all four lifecycle
// methods. It handles idempotent retry, failure injection, transition
// validation, and the instant-vs-staged transition mode.
func (p *Provider) applyTransition(id machine.ID, kind opKind, postEffect func(*machine.Machine)) (provider.TransitionAck, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	m, ok := p.machines[id]
	if !ok {
		return provider.TransitionAck{}, fmt.Errorf("%w: %s", provider.ErrNotFound, id)
	}
	transitional, stable := kind.targets()

	// Idempotent retry: if we've already started this kind on this
	// machine and the state is consistent (transitional or already at
	// the stable target), return the same op_id.
	if op, exists := p.ops[opKey{id, kind}]; exists && (m.State == transitional || m.State == stable) {
		return provider.TransitionAck{OperationID: op, Machine: *m}, nil
	}

	if err := p.consumeFail(id, stable); err != nil {
		return provider.TransitionAck{}, err
	}
	if !machine.CanTransition(m.State, transitional) {
		return provider.TransitionAck{}, fmt.Errorf("%s: %w (current=%s)", kind, machine.ErrInvalidTransition, m.State)
	}

	op := p.mintOp()
	p.ops[opKey{id, kind}] = op

	if p.instantTransitions {
		m.State = stable
	} else {
		m.State = transitional
	}
	if postEffect != nil {
		postEffect(m)
	}
	p.rev++
	p.lastModRev[id] = p.rev
	p.revLog = append(p.revLog, revEntry{rev: p.rev, id: id})
	return provider.TransitionAck{OperationID: op, Machine: *m}, nil
}

// Get implements provider.Provider. Reader.
func (p *Provider) Get(_ context.Context, id machine.ID) (machine.Machine, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	m, ok := p.machines[id]
	if !ok {
		return machine.Machine{}, fmt.Errorf("%w: %s", provider.ErrNotFound, id)
	}
	return *m, nil
}

// List implements provider.Provider. Honours SinceRevision: when set
// to the cursor returned by a prior call, only machines mutated after
// that cursor are included via the per-mutation revLog index — O(k +
// uniqueIDs) instead of O(N). Cold start (empty cursor) walks the
// full inventory map.
//
// Reader (RLock); concurrent Lists do not block each other and do not
// block Gets.
func (p *Provider) List(_ context.Context, filter provider.ListFilter) (provider.ListResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	since, hasSince := decodeRevision(filter.SinceRevision)
	rev := []byte(strconv.Itoa(p.rev))

	if !hasSince {
		// Cold start: full scan.
		out := make([]machine.Machine, 0, len(p.machines))
		for _, m := range p.machines {
			if !matchesFilter(*m, filter) {
				continue
			}
			out = append(out, *m)
			if filter.MaxResults > 0 && len(out) >= filter.MaxResults {
				break
			}
		}
		return provider.ListResponse{Machines: out, Revision: rev}, nil
	}

	// Delta: walk the revLog from the first entry with rev > since.
	// Dedup IDs (a single machine may appear multiple times if mutated
	// repeatedly within the cursor window) and emit current state.
	start := sort.Search(len(p.revLog), func(i int) bool {
		return p.revLog[i].rev > since
	})
	if start >= len(p.revLog) {
		return provider.ListResponse{Revision: rev}, nil
	}
	seen := make(map[machine.ID]struct{}, len(p.revLog)-start)
	out := make([]machine.Machine, 0, len(p.revLog)-start)
	for i := start; i < len(p.revLog); i++ {
		id := p.revLog[i].id
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		m, ok := p.machines[id]
		if !ok {
			continue // genuinely deleted (no fake path triggers this today)
		}
		if !matchesFilter(*m, filter) {
			continue
		}
		out = append(out, *m)
		if filter.MaxResults > 0 && len(out) >= filter.MaxResults {
			break
		}
	}
	return provider.ListResponse{Machines: out, Revision: rev}, nil
}

// decodeRevision parses an opaque cursor previously emitted by List. An
// empty cursor (cold start) returns hasSince=false; the caller treats
// that as "include everything". A malformed cursor is also treated as
// cold start — defensive against ever-changing wire encodings on the
// real-provider side.
func decodeRevision(b []byte) (int, bool) {
	if len(b) == 0 {
		return 0, false
	}
	n, err := strconv.Atoi(string(b))
	if err != nil {
		return 0, false
	}
	return n, true
}

func matchesFilter(m machine.Machine, f provider.ListFilter) bool {
	if len(f.States) > 0 {
		match := false
		for _, s := range f.States {
			if s == m.State {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	if f.Zone != "" && f.Zone != m.Profile.Zone {
		return false
	}
	if f.InstanceType != "" && f.InstanceType != m.Profile.InstanceType {
		return false
	}
	return true
}

func (p *Provider) consumeFail(id machine.ID, target machine.State) error {
	k := failKey{id, target}
	if err, ok := p.failNext[k]; ok {
		delete(p.failNext, k)
		return err
	}
	return nil
}

func (p *Provider) mintOp() string {
	p.nextOp++
	return "op-" + strconv.Itoa(p.nextOp)
}

type opKind uint8

const (
	opCreate opKind = iota
	opConfigure
	opDrain
	opDelete
)

func (k opKind) String() string {
	switch k {
	case opCreate:
		return "create"
	case opConfigure:
		return "configure"
	case opDrain:
		return "drain"
	case opDelete:
		return "delete"
	}
	return "unspecified"
}

// targets maps an opKind → (transitional state, stable target state).
func (k opKind) targets() (machine.State, machine.State) {
	switch k {
	case opCreate:
		return machine.StateCreating, machine.StateIdle
	case opConfigure:
		return machine.StateConfiguring, machine.StateConfigured
	case opDrain:
		return machine.StateDraining, machine.StateIdle
	case opDelete:
		return machine.StateDeleting, machine.StateSpeculative
	}
	return machine.StateUnspecified, machine.StateUnspecified
}

// opKey identifies the most-recent transition of the given kind on the
// given machine.
type opKey struct {
	id   machine.ID
	kind opKind
}

type failKey struct {
	id     machine.ID
	target machine.State
}

// Compile-time check that Provider implements the interface.
var _ provider.Provider = (*Provider)(nil)

// SentinelErrors are exported so tests can match on them when injecting
// failures via FailNext.
var (
	ErrSyntheticFailure = errors.New("fake provider: synthetic failure")
)
