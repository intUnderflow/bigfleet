package occ

import (
	"runtime"
	"sync"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// CycleResult is what RunCycle hands back to the caller. Results is
// in the same order as the allNeeds slice passed in; element i
// corresponds to allNeeds[i]. Workers populate
// BootstrapMachines / ProvisionMachines / Deficit / Unsatisfied based
// on the final post-barrier claimed-set.
//
// Claimed is that final claimed-set flattened to machine IDs: every
// machine the cycle attributed to some Need — Configured/Configuring
// credited by the pre-pass, Idle/Speculative committed by workers.
// ADR-0045: this is the engine's only supply-attribution arithmetic;
// Phase 3 reclaims exactly the Configured machines absent from it.
type CycleResult struct {
	Results []NeedResult
	Claimed map[machine.ID]struct{}
}

// Config tunes the cycle's behaviour. Workers and Retries are the
// only knobs surfaced today; reasonable defaults come from ADR-0029
// (GOMAXPROCS workers, 10 retries).
type Config struct {
	Workers int
	Retries int
}

// Option configures one Config field.
type Option func(*Config)

// WithWorkers overrides the default worker pool size (GOMAXPROCS).
// Values ≤ 0 fall back to the default.
func WithWorkers(n int) Option {
	return func(c *Config) { c.Workers = n }
}

// WithRetries overrides the default per-Need retry budget (10).
// Values ≤ 0 fall back to the default.
func WithRetries(n int) Option {
	return func(c *Config) { c.Retries = n }
}

const (
	defaultRetries = 10
)

func defaultConfig() Config {
	return Config{
		Workers: runtime.GOMAXPROCS(0),
		Retries: defaultRetries,
	}
}

// RunCycle is the cycle entry point. It owns SharedState, Broker,
// and PoolCache lifetimes; orchestrates the priority-sorted pre-
// pass that credits existing supply; dispatches the OCC worker
// pool over Idle + Speculative; barrier-waits; and reconstructs
// per-Need outcomes from the final claimed-set.
//
// The returned CycleResult has the same length and order as
// allNeeds — the caller can correlate by index.
func RunCycle(snap *inventory.Snapshot, allNeeds []needs.Need, opts ...Option) CycleResult {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.Retries <= 0 {
		cfg.Retries = defaultRetries
	}

	state := NewSharedState(snap)
	broker := NewBroker(state)
	cache := NewPoolCache(snap)

	// Build a stable []*needs.Need pointing into allNeeds. Workers
	// reference these pointers; barrier post-processing correlates them
	// back to result indices by position (needPtrs[i] == &allNeeds[i]).
	needPtrs := make([]*needs.Need, len(allNeeds))
	for i := range allNeeds {
		needPtrs[i] = &allNeeds[i]
	}

	// Pre-pass: credit existing supply (Configured / Configuring) for
	// each Need in priority-descending order. Single-threaded.
	results := SeedConfiguredSupply(state, needPtrs, cfg.Retries)

	// Build the work queue. Channel capacity is bounded above by
	// initial-Needs × (retries + 2) — every Need can be re-queued at
	// most cfg.Retries times via displacement, plus the initial push.
	queueBuf := len(allNeeds) * (cfg.Retries + 2)
	if queueBuf < 64 {
		queueBuf = 64
	}
	work := make(chan QueuedNeed, queueBuf)
	var wg sync.WaitGroup

	push := func(qn QueuedNeed) {
		wg.Add(1)
		work <- qn
	}

	// Spawn workers. Each pulls from work, runs the per-Need flow,
	// and decrements wg. When wg counter reaches zero, the main
	// goroutine closes work and the workers exit.
	for i := 0; i < cfg.Workers; i++ {
		go func() {
			for qn := range work {
				processNeed(qn, state, broker, cache, push)
				wg.Done()
			}
		}()
	}

	// Seed initial queue with Needs whose post-pre-pass deficit is
	// nonzero. Zero-deficit Needs are fully covered by existing
	// supply; nothing to do.
	for i := range results {
		if needs.IsZero(results[i].Deficit) {
			continue
		}
		push(QueuedNeed{Need: results[i].Need, RetriesLeft: cfg.Retries})
	}

	wg.Wait()
	close(work)

	// Barrier post-processing: reconstruct per-Need BootstrapMachines /
	// ProvisionMachines / Deficit from the final claimed-set. Workers
	// don't touch results during the cycle — only here, single-
	// threaded, after the barrier.
	claimed := make(map[machine.ID]struct{})
	for i := range results {
		r := &results[i]
		r.BootstrapMachines = nil
		r.ProvisionMachines = nil
		r.ClaimedMachines = nil
		r.SameDomain = state.SameDomainFor(r.Need)
		r.SameSatisfiable = state.SameSatisfiableFor(r.Need)
		sumAlloc := []needs.ResourceQty(nil)
		for _, mid := range state.ClaimedFor(r.Need) {
			claimed[mid] = struct{}{}
			// #325: retain the per-Need full claimed-set (every state,
			// not just acquired Idle/Speculative) so the satisfied-gang
			// probe can watch a served Same-Need's machine set churn.
			r.ClaimedMachines = append(r.ClaimedMachines, mid)
			m, ok := snap.Get(mid)
			if !ok {
				continue
			}
			alloc := needs.ResourceQtysFromMap(m.EffectiveAllocatable())
			sumAlloc = needs.AddResources(sumAlloc, alloc)
			switch m.State {
			case machine.StateIdle:
				r.BootstrapMachines = append(r.BootstrapMachines, mid)
			case machine.StateSpeculative:
				r.ProvisionMachines = append(r.ProvisionMachines, mid)
			}
		}
		r.Deficit = needs.SubResources(r.Need.AggregateResources, sumAlloc)
		r.Unsatisfied = !needs.IsZero(r.Deficit)
		// ADR-0061: classify why an Unsatisfied Need is short. A Need that
		// claimed anything trivially has matching supply; otherwise scan
		// the inventory for any machine of the right shape (held by anyone,
		// at any priority). Observation-only, after the barrier — no claim
		// decision rides on it.
		if r.Unsatisfied {
			r.MatchingSupplyExists = len(r.ClaimedMachines) > 0 ||
				anyMatchingSupply(snap, r.Need.Profile, r.Need.MinUnit)
		}
	}

	return CycleResult{Results: results, Claimed: claimed}
}

// anyMatchingSupply reports whether the inventory holds at least one
// machine — in any of Idle / Speculative / Configured — whose shape
// MatchProfile-passes profile and covers minUnit, ignoring claims and
// priority. It reuses the snapshot's instance-type buckets (no sort, no
// copy) and early-returns on the first match, so the common pinned case is
// a small bucket walk; the unpinned fallback walks the full per-state slice
// but still early-returns. Read-only; called only for Unsatisfied Needs at
// the barrier (ADR-0061).
func anyMatchingSupply(snap *inventory.Snapshot, profile needs.Profile, minUnit []needs.ResourceQty) bool {
	types := pinnedInstanceTypes(profile)
	for _, st := range []machine.State{machine.StateIdle, machine.StateSpeculative, machine.StateConfigured} {
		var pool []machine.Machine
		if len(types) == 0 {
			pool = snap.ListByState(st)
		} else {
			for _, t := range types {
				pool = append(pool, snap.ListByStateInstanceType(st, t)...)
			}
		}
		for i := range pool {
			m := &pool[i]
			if !matchProfile(profile, *m) {
				continue
			}
			if needs.Covers(needs.ResourceQtysFromMap(m.EffectiveAllocatable()), minUnit) {
				return true
			}
		}
	}
	return false
}

// processNeed runs one Need through the broker. Tries Idle then
// Speculative; on each attempt builds candidates from the
// appropriate pool, proposes, and on conflict retries with a
// fresh observation up to the per-Need retry budget. Displaced
// incumbents are re-queued via push (with their own retry budgets
// pre-decremented by the broker).
//
// Topology-constrained Needs (Same or Spread) get one Find+Propose
// attempt per state — their candidate-finders' bucket selection
// (or skew accounting) restarts from a clean slate on each call,
// so a second call in the same state after a partial commit can
// re-pick differently and violate the constraint. The basic path
// retries-on-commit because each retry sees a fresh claimed-set
// and can pick the next-cheapest unclaimed machine.
//
// processNeed never writes to NeedResult — barrier-time post-
// processing in RunCycle reconstructs the outcomes from the
// final claimed-set.
func processNeed(qn QueuedNeed, state *SharedState, broker *Broker, cache *PoolCache, push func(QueuedNeed)) {
	prec := PrecedenceFromProfile(qn.Need.Profile)
	mode := modeFor(qn.Need)
	topologyConstrained := hasTopologyConstraint(qn.Need.Profile)
	startingDeficit := computeDeficit(qn.Need, state)
	committedAnything := false

	defer func() {
		// "Retries exhausted" only counts when the worker entered with
		// real demand AND finished without committing anything new
		// AND the budget hit zero. Catalog-bound zero-deficit Needs
		// (covered by pre-pass) don't count; per-cycle Needs that
		// committed and then ran out of budget on a follow-up state
		// don't count either.
		if qn.RetriesLeft <= 0 && !committedAnything && !needs.IsZero(startingDeficit) {
			metrics.ShardPhase1OCCRetriesExhaustedTotal.Inc()
		}
	}()

	for _, st := range []machine.State{machine.StateIdle, machine.StateSpeculative} {
		for qn.RetriesLeft > 0 {
			deficit := computeDeficit(qn.Need, state)
			if needs.IsZero(deficit) {
				return
			}
			pool := cache.Get(st, qn.Need.Profile)
			cands := findCandidatesFor(pool, state, st, prec, qn.Need, deficit)
			if len(cands.Machines) == 0 {
				break
			}

			seq := state.BucketSeq(cands.Bucket)
			r := broker.Propose(Proposal{
				Need:        qn.Need,
				Bucket:      cands.Bucket,
				Machines:    cands.Machines,
				ObservedSeq: seq,
				Precedence:  prec,
				Mode:        mode,
				RetriesLeft: qn.RetriesLeft,
			})

			for _, d := range r.Displaced {
				if d.RetriesLeft > 0 {
					push(d)
				}
			}

			if r.Status == StatusCommitted {
				committedAnything = true
				// Topology-constrained Needs: don't retry in this
				// state. The constraint's bucket / skew accounting
				// is per-call; a second call after partial commit
				// can re-pick into a different bucket or domain
				// distribution and violate the constraint.
				if topologyConstrained {
					break
				}
				continue
			}
			qn.RetriesLeft--
		}
	}
}

// hasTopologyConstraint reports whether p carries a Same operator or
// a DoNotSchedule TopologySpread — either makes Find* per-call state-
// sensitive and unsafe to retry within the same state after a partial
// commit.
func hasTopologyConstraint(p needs.Profile) bool {
	if _, ok := SameRequirementKey(p); ok {
		return true
	}
	if _, _, ok := StrictSpread(p); ok {
		return true
	}
	return false
}

// computeDeficit returns the residual demand vector for n after
// subtracting Σ Allocatable across machines currently claimed for n.
// O(claimsForN); the reverse index in SharedState makes this cheap.
func computeDeficit(n *needs.Need, state *SharedState) []needs.ResourceQty {
	snap := state.Snapshot()
	sumAlloc := []needs.ResourceQty(nil)
	for _, mid := range state.ClaimedFor(n) {
		m, ok := snap.Get(mid)
		if !ok {
			continue
		}
		sumAlloc = needs.AddResources(sumAlloc, needs.ResourceQtysFromMap(m.EffectiveAllocatable()))
	}
	return needs.SubResources(n.AggregateResources, sumAlloc)
}

// findCandidatesFor routes to FindSame / FindSpread / FindBasic
// based on the Profile's constraints. Same wins over Spread when
// both are present (Same is the stronger constraint and the
// pre-OCC allocator at pkg/decision/phase1_allocator.go:120-125
// applies the same precedence).
//
// Same Needs acquire inside the domain the pre-pass chose jointly
// over creditable + acquirable supply (ADR-0040 Addendum) — the
// recorded domain survives displacement re-queues because the
// QueuedNeed carries the same *needs.Need pointer the pre-pass keyed
// it under. An empty recorded domain (no bucket existed anywhere)
// leaves FindSame on its best-bucket fallback.
//
// Same wins over Spread when both are present — Same is the stronger
// constraint (ADR-0040, ADR-0041).
func findCandidatesFor(pool *Pool, state *SharedState, st machine.State, prec Precedence, n *needs.Need, deficit []needs.ResourceQty) Candidates {
	profile := n.Profile
	if sameKey, ok := SameRequirementKey(profile); ok {
		// ADR-0042 Addendum: a parked Need does not acquire. Its
		// creditable assembly was claimed by the pre-pass; the residual
		// ages in the shortfall buffer until the periodic re-probe (or
		// fresh supply) un-parks it.
		if n.AcquisitionParked {
			return Candidates{}
		}
		return pool.FindSame(state, st, prec, deficit, n.MinUnit, sameKey, state.SameDomainFor(n))
	}
	if topoKey, skew, ok := StrictSpread(profile); ok {
		return pool.FindSpread(state, st, prec, deficit, n.MinUnit, topoKey, skew)
	}
	return pool.FindBasic(state, st, prec, deficit, n.MinUnit)
}

// modeFor classifies a Need into ModeIncremental or ModeAllOrNothing
// based on its Profile and MinUnit. The classification is pure
// (function of the Need's static fields); no operator-side hint.
//
// A Need is gang-scheduled (ModeAllOrNothing) iff:
//  1. It carries a Same operator (workloads share a topology value),
//     AND
//  2. Its MinUnit doesn't cover its AggregateResources (multi-machine
//     gang — one machine alone can't satisfy the Need's atomic
//     unit).
//
// Everything else is ModeIncremental: partial fill is semantically
// fine, the shortfall buffer absorbs any residual deficit.
func modeFor(n *needs.Need) ProposalMode {
	hasSame := false
	for _, r := range n.Profile.Requirements() {
		if r.Operator == needs.OperatorSame {
			hasSame = true
			break
		}
	}
	if !hasSame {
		return ModeIncremental
	}
	if needs.Covers(n.MinUnit, n.AggregateResources) {
		return ModeIncremental
	}
	return ModeAllOrNothing
}
