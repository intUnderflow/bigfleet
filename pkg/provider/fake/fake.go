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

	// fenceHWM is the per-shard_id high-water mark of accepted fencing
	// tokens (paper §11, M71). Mutating calls that carry a token must be
	// strictly newer than this mark or they're rejected with ErrFenced;
	// calls with a zero token (in-process harness construction) bypass
	// fencing entirely. The fake enforces this because it is what the
	// conformance suite's self-test runs against — it has to model the
	// contract real providers are held to.
	fenceHWM map[string]fenceMark

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

	// configureStaged: if true, Configure returns the machine at
	// Configuring (the transitional state) even when instantTransitions
	// is set — modelling a provider whose kubelet bootstrap genuinely
	// takes time. The caller (the closed-loop sim) completes the
	// Configuring → Configured transition after its bootstrap-dwell
	// budget elapses, so the engine observes the machine in Configuring
	// for N cycles (ADR-0051 / M77g: the in-flight dwell is the #64
	// perturbation). Create / Drain / Delete still honour
	// instantTransitions.
	configureStaged bool

	// createStaged: the pre-Configuring (provision) twin of
	// configureStaged. If true, Create returns the machine at Creating
	// (the transitional Speculative→Idle state) even under
	// instantTransitions — modelling a provider whose host provisioning
	// genuinely takes time (boot, image pull, join). The closed-loop sim
	// completes the Creating → Idle transition after its provision-dwell
	// budget elapses, so the engine observes the machine in Creating for
	// N cycles. Creating is a state the Phase 1 Same-domain coverage walk
	// (seed.go: Configured+Configuring only) does NOT count, and which
	// executeProvision leaves with NO gang attribution (AssignedGroup is
	// stamped only at the later Idle→Configuring) — the invisible runway
	// the over-acquire hypothesis turns on. Configure / Drain / Delete
	// still honour instantTransitions.
	createStaged bool
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
	// ConfigureStaged makes Configure leave the machine at Configuring
	// even under InstantTransitions (see Provider.configureStaged). The
	// closed-loop sim's bootstrap-dwell model drives the completion.
	ConfigureStaged bool
	// CreateStaged makes Create leave the machine at Creating even under
	// InstantTransitions (see Provider.createStaged). The closed-loop
	// sim's provision-dwell model drives the Creating→Idle completion.
	CreateStaged bool
	Seed         uint64
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
		fenceHWM:           make(map[string]fenceMark),
		failNext:           make(map[failKey]error),
		rand:               rand.New(rand.NewPCG(seed, seed^0xA5A5A5A5)),
		instantTransitions: opts.InstantTransitions,
		configureStaged:    opts.ConfigureStaged,
		createStaged:       opts.CreateStaged,
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

// AddConfigured inserts a machine in the Configured state, bound to
// the given cluster + workload identity. Used by the scaletest harness
// to model production-realistic seed shapes — most fleet inventory
// is running workloads (Configured), not Idle headroom. The
// AssignedPriority + AssignedInterruptionPenaltyDollars +
// AssignedReclamationPenaltyDollars feed Phase 2 victim scoring; pass
// representative values for the workload class this machine simulates.
func (p *Provider) AddConfigured(
	id machine.ID,
	profile machine.Profile,
	capType machine.CapacityType,
	pricePerHour, interruptionProb float64,
	cluster machine.ClusterID,
	priority int32,
	interruptionPenalty, reclamationPenalty float64,
) {
	p.mu.Lock()
	defer p.mu.Unlock()
	prof := profile
	if prof.CapacityType == machine.CapacityTypeUnspecified {
		prof.CapacityType = capType
	}
	p.machines[id] = &machine.Machine{
		ID:                                 id,
		State:                              machine.StateConfigured,
		Host:                               machine.HostRef{Provider: "fake", Ref: string(id)},
		Cluster:                            cluster,
		Profile:                            prof,
		PricePerHour:                       pricePerHour,
		InterruptionProbability:            interruptionProb,
		AssignedPriority:                   priority,
		AssignedInterruptionPenaltyDollars: interruptionPenalty,
		AssignedReclamationPenaltyDollars:  reclamationPenalty,
		// M72: a real provider holding a Configured machine would also be
		// holding the shard_metadata some shard's Configure stored.
		// Seeding it keeps harness-seeded fleets restart-rebuildable over
		// the wire, where only the echo survives — the Assigned* fields
		// above only reach the shard on the in-process path.
		// ADR-0051 adds the assigned-group key (empty: AddConfigured does
		// not model a specific gang).
		ShardMetadata: machine.EncodeShardMetadata(priority, interruptionPenalty, reclamationPenalty, "", ""),
	}
	p.rev++
	p.lastModRev[id] = p.rev
	p.revLog = append(p.revLog, revEntry{rev: p.rev, id: id})
}

// SetAllocatable overrides the named machine's Allocatable map. Used by
// the scaletest seed (ADR-0022 / M45.4) to make a single machine cover
// N replicas of its Profile.Resources — modelling the production-realistic
// per-machine Pod density. No-op when the machine doesn't exist or
// allocatable is empty (the consumer falls back to Profile.Resources
// via Machine.EffectiveAllocatable()).
func (p *Provider) SetAllocatable(id machine.ID, allocatable map[string]string) {
	if len(allocatable) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	m, ok := p.machines[id]
	if !ok {
		return
	}
	out := make(map[string]string, len(allocatable))
	for k, v := range allocatable {
		out[k] = v
	}
	m.Allocatable = out
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

// FailMachine forcibly transitions the named machine to StateFailed,
// regardless of its current state. Models an unsolicited provider
// notification — spot reclaim, hardware fault, kernel panic — that
// the autoscaler discovers via the next provider.List(). M38 / Item 5.
//
// Returns false if the machine doesn't exist; true otherwise. Already-
// failed machines stay failed (idempotent).
func (p *Provider) FailMachine(id machine.ID, reason string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	m, ok := p.machines[id]
	if !ok {
		return false
	}
	if m.State == machine.StateFailed {
		return true
	}
	m.State = machine.StateFailed
	m.LastError = reason
	p.rev++
	p.lastModRev[id] = p.rev
	p.revLog = append(p.revLog, revEntry{rev: p.rev, id: id})
	return true
}

// RemoveMachine deletes a machine from the provider entirely, modelling a
// hard host loss the autoscaler discovers via the next List (the machine
// vanishes — a terminated / spot-reclaimed node whose Configured→Failed the
// FSM does not model as an in-place transition). The shard's full reconcile
// removes the absent machine from inventory; the cluster whose Pods it
// hosted then sees them evicted and the gang re-derives its deficit.
// Returns false if the machine was already absent. (M38 FailMachine keeps
// the record present-but-Failed, which the reconcile transition check
// cannot apply onto a Configured machine — so a clean removal is the
// ingestible incumbent-loss model.)
func (p *Provider) RemoveMachine(id machine.ID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.machines[id]; !ok {
		return false
	}
	delete(p.machines, id)
	p.rev++
	// No revLog append for a deletion: the incremental delta path keys on
	// present records, and the closed-loop sim reconciles full (removal is
	// detected by the snapshot-walk, not the delta). lastModRev is left as
	// the last mutation; a fresh List simply omits the id.
	delete(p.lastModRev, id)
	return true
}

// CompleteStaged advances a machine the provider is holding in a staged
// transitional state (Creating under CreateStaged, Configuring under
// ConfigureStaged) to that op's stable target, applying the same
// post-effect the instant completion would have — most importantly the
// host that comes into being when provisioning settles at Idle. The
// closed-loop sim's dwell models call this when a per-machine dwell budget
// elapses, so the provider (the host-lifecycle source of truth) advances
// first and the shard's reconcile then pulls the new stable state into
// inventory through the normal forward transition. Returns false (no-op)
// if the machine is absent or not in the expected staged state — the dwell
// record is then simply dropped.
//
// Only the two staged forward transitions are modelled (Creating→Idle,
// Configuring→Configured); a real provider's Drain/Delete completion does
// not ride this hook.
func (p *Provider) CompleteStaged(id machine.ID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	m, ok := p.machines[id]
	if !ok {
		return false
	}
	switch m.State {
	case machine.StateCreating:
		m.State = machine.StateIdle
		m.Host = machine.HostRef{Provider: "fake", Ref: string(id)}
	case machine.StateConfiguring:
		m.State = machine.StateConfigured
	default:
		return false
	}
	p.rev++
	p.lastModRev[id] = p.rev
	p.revLog = append(p.revLog, revEntry{rev: p.rev, id: id})
	return true
}

// RandomConfiguredMachine returns the ID of a randomly-selected machine
// in StateConfigured, or "" if none exist. Used by the M38 failure
// injector to pick victims. Iteration order over the map is random by
// Go's map semantics; we accept that and don't try to be fancy with
// reservoir sampling.
func (p *Provider) RandomConfiguredMachine() machine.ID {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, m := range p.machines {
		if m.State == machine.StateConfigured {
			return id
		}
	}
	return ""
}

// ConfiguredCount returns the number of machines currently in
// StateConfigured. Used by the M38 failure injector to scale its
// per-tick draw correctly: expected_failures_per_tick = ratePerSec
// × configuredCount. ADR-0019 (M44.4) rewrote the injector to use
// this method after the previous 32-sample heuristic was found
// unable to fire at all under the 1.16e-8/sec/machine real-fleet
// rate.
func (p *Provider) ConfiguredCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, m := range p.machines {
		if m.State == machine.StateConfigured {
			n++
		}
	}
	return n
}

// Create implements provider.Provider. The host comes into being only
// when provisioning settles at Idle: a machine still Creating (the
// CreateStaged path) has no host yet, so the post-effect sets the host
// only once the record has reached the stable Idle target — mirroring
// Delete's symmetric "clear host at Speculative" guard, and keeping the
// staged Creating record's empty-host invariant intact (machine.go).
func (p *Provider) Create(_ context.Context, req provider.CreateRequest) (provider.TransitionAck, error) {
	return p.applyTransition(req.MachineID, opCreate, req.Fence, func(m *machine.Machine) {
		if m.State == machine.StateIdle {
			m.Host = machine.HostRef{Provider: "fake", Ref: string(req.MachineID)}
		}
	})
}

// Configure implements provider.Provider.
func (p *Provider) Configure(_ context.Context, req provider.ConfigureRequest) (provider.TransitionAck, error) {
	return p.applyTransition(req.MachineID, opConfigure, req.Fence, func(m *machine.Machine) {
		m.Cluster = req.ClusterID
		// M72: store-and-echo, never interpret. The fake is the
		// conformance reference, so it deliberately does NOT decode the
		// well-known keys into the Assigned* fields — a copy of the
		// verbatim map is the whole obligation, unknown keys included.
		m.ShardMetadata = cloneStringMap(req.ShardMetadata)
	})
}

// Drain implements provider.Provider.
func (p *Provider) Drain(_ context.Context, req provider.DrainRequest) (provider.TransitionAck, error) {
	return p.applyTransition(req.MachineID, opDrain, req.Fence, func(m *machine.Machine) {
		m.Cluster = ""
		m.AssignedPriority = 0
		m.AssignedInterruptionPenaltyDollars = 0
		m.AssignedReclamationPenaltyDollars = 0
		// M72: shard_metadata is per-assignment state established by
		// Configure; it clears with the binding, not with the machine.
		m.ShardMetadata = nil
	})
}

// Delete implements provider.Provider. Bare-metal-style providers should
// return ErrNotSupported instead.
func (p *Provider) Delete(_ context.Context, req provider.DeleteRequest) (provider.TransitionAck, error) {
	return p.applyTransition(req.MachineID, opDelete, req.Fence, func(m *machine.Machine) {
		// §7: the host is released when the machine settles back at
		// Speculative. In the staged-transition mode the Deleting record
		// keeps its host ref — the real host exists until teardown
		// finishes, and machine.Invariant requires Deleting to carry one
		// (M73: the shard ingests these records at reconcile).
		if m.State == machine.StateSpeculative {
			m.Host = machine.HostRef{}
		}
	})
}

// checkFence enforces the paper §11 fencing contract. Caller holds p.mu.
//
// The check runs before everything else in applyTransition — before the
// not-found check, before idempotent-retry short-circuiting — per the
// proto contract: a zombie's request must not be applied, must not be
// answered with a cached operation_id, and must not learn whether the
// machine exists. A token that passes advances the high-water mark even
// if the operation itself then fails (the mark records "newest shard
// process seen", not "operations that succeeded").
func (p *Provider) checkFence(f provider.Fence) error {
	if f.Zero() {
		return nil // unfenced in-process caller; see fenceHWM doc
	}
	hwm, known := p.fenceHWM[f.ShardID]
	newer := f.ShardEpoch > hwm.epoch ||
		(f.ShardEpoch == hwm.epoch && f.SequenceNumber > hwm.seq)
	if known && !newer {
		return fmt.Errorf("%w: shard %q sent (epoch=%d, seq=%d), high-water mark is (epoch=%d, seq=%d)",
			provider.ErrFenced, f.ShardID, f.ShardEpoch, f.SequenceNumber, hwm.epoch, hwm.seq)
	}
	p.fenceHWM[f.ShardID] = fenceMark{epoch: f.ShardEpoch, seq: f.SequenceNumber}
	return nil
}

// fenceMark is the highest (epoch, seq) accepted for one shard_id.
type fenceMark struct {
	epoch, seq int64
}

// applyTransition is the shared implementation of all four lifecycle
// methods. It handles fencing, idempotent retry, failure injection,
// transition validation, and the instant-vs-staged transition mode.
func (p *Provider) applyTransition(id machine.ID, kind opKind, fence provider.Fence, postEffect func(*machine.Machine)) (provider.TransitionAck, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.checkFence(fence); err != nil {
		return provider.TransitionAck{}, err
	}
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

	// configureStaged / createStaged override instant completion for their
	// respective op: the machine reaches the transitional state and stays
	// there until the sim's dwell budget drives it to the stable target.
	// configureStaged: Configuring (bootstrap dwell, ADR-0051 / M77g).
	// createStaged: Creating (provision/pre-Configuring dwell — the
	// invisible runway seed.go's coverage walk does not count).
	staged := (p.configureStaged && kind == opConfigure) ||
		(p.createStaged && kind == opCreate)
	if p.instantTransitions && !staged {
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

// cloneStringMap copies the caller's map so post-Configure mutations on
// the caller's side can't reach the stored record (echo must be exactly
// what was sent at Configure time).
func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
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
