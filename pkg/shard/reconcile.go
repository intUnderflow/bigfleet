package shard

import (
	"context"
	"errors"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/provider"
)

// reconcile pulls the provider's view of every machine and updates the
// in-memory inventory to match.
//
// Two modes, gated on Config.IncrementalReconcile:
//
//   - Off (default; works for any provider): full List every cycle.
//     Walks the entire response, then walks the inventory snapshot to
//     find removals.
//
//   - On (provider above the §10.6 conformance threshold): pumps an
//     opaque cursor (`SinceRevision`) across cycles. Only deltas are
//     returned; the snapshot-walk for removals is skipped because
//     deltas don't communicate "this id is gone" yet (tombstone wire
//     extension is deferred until a real provider needs it). On a
//     fresh cursor (cold start, or shard restart), the call still
//     returns the full inventory — same behaviour as the off path
//     for the first cycle.
func (s *Shard) reconcile(ctx context.Context) error {
	if s.cfg.IncrementalReconcile {
		return s.reconcileIncremental(ctx)
	}
	return s.reconcileFull(ctx)
}

// reconcileFull is the always-correct full-List path. Linear in the
// inventory size (one Get + state compare per returned machine, plus a
// snapshot walk to find removals). Suitable for any provider.
func (s *Shard) reconcileFull(ctx context.Context) error {
	resp, err := s.cfg.Provider.List(ctx, provider.ListFilter{})
	if err != nil {
		return err
	}

	seen := make(map[machine.ID]struct{}, len(resp.Machines))
	for _, pm := range resp.Machines {
		seen[pm.ID] = struct{}{}
		s.applyReconciledMachine(pm)
	}

	for _, m := range s.inv.Snapshot().All() {
		if _, ok := seen[m.ID]; ok {
			continue
		}
		if err := s.inv.Remove(m.ID); err != nil {
			if !equivalentErrNotFound(err) {
				s.log.Warn("inventory remove failed", "machine", m.ID, "err", err)
			}
		}
	}
	return nil
}

// reconcileIncremental pumps the cursor and processes only the deltas.
// At steady state with N machines and k changes per cycle this is
// O(k) instead of O(N), which is the difference between the cycle SLO
// being achievable and not (M11.21 phase dump showed reconcile is ~85
// % of the cycle at 500K).
func (s *Shard) reconcileIncremental(ctx context.Context) error {
	resp, err := s.cfg.Provider.List(ctx, provider.ListFilter{
		SinceRevision: s.reconcileCursor,
	})
	if err != nil {
		return err
	}
	for _, pm := range resp.Machines {
		s.applyReconciledMachine(pm)
	}
	s.reconcileCursor = resp.Revision
	return nil
}

// applyReconciledMachine merges one machine from the provider into the
// shard's inventory.
//
// Fast path (M11.24a): when the local inventory already has the
// machine in the same state as the provider returned, do nothing. This
// is the common case at steady state and *especially* in the cycle
// after execute fans out actions — execute already locally
// `applyTransition`-ed each machine to the provider's TransitionAck
// state, so the next reconcile sees state-match and skips. No proto
// round-trip, no Apply, no allocation.
//
// Slow paths: state diverged → apply the new state, preserving the
// locally-known Assigned* fields (shard memory is authoritative for a
// machine it already tracks); machine is new → Insert, restoring the
// Assigned* fields from the provider-echoed shard_metadata (M72 — the
// restart-rebuild path; the echo is the only durable copy).
//
// Validation (ADR-0046 addendum / M70): records taking a slow path are
// screened by machine.Invariant before they touch inventory — the
// production-readiness audit (arc 3) found nothing bounding the
// provider-declared price / interruption_probability on this path. An
// earlier comment here claimed the records arrived "pre-validated" by
// the fake or grpcadapter; that was false — grpcadapter checks only
// the state enum, and Insert/Apply's own Invariant errors were
// discarded below, so garbage was silently dropped (or, for fields
// Invariant didn't yet bound, silently accepted into the cost
// formula). A rejected record is logged + counted and the inventory
// keeps its last-known-good state; reconcileFull marks the ID seen
// first, so rejection never masquerades as removal. The state-match
// fast path is not screened: it ingests nothing. (The pre-M11.24a
// MachineToProto+MachineFromProto round-trip "to exercise the
// validation paths" stays dead — it validated the same enum twice and
// dominated post-burst cycles.)
func (s *Shard) applyReconciledMachine(dm machine.Machine) {
	// Skip in-flight machines (bigfleet-uber #23 fix). A worker is
	// driving this machine through its provider RPC(s); its local
	// `applyTransition`-ed state (e.g. Configuring while
	// provider.Configure is in flight) is the authoritative
	// snapshot for the duration of the action. The provider's
	// List view lags the in-flight RPC and will report a pre-RPC
	// state — applying it here overwrites Configuring → Idle and
	// causes the post-Configure transition to fail with
	// "invalid state transition: Idle → Configured" (the
	// errProvisionRacedToIdle path in execute.go). When the
	// worker completes, the next reconcile will see a state-match
	// (M11.24a fast path) and skip.
	if s.isPending(dm.ID) {
		return
	}
	existing, getErr := s.inv.Get(dm.ID)
	if getErr == nil && existing.State == dm.State {
		return
	}
	if s.validateProviderMachine(&dm) != nil {
		return
	}
	if getErr == nil {
		// ADR-0057: capture the binding before Apply so a binding-clearing
		// terminal transition observed here (e.g. Draining → Idle completed
		// async) still routes its update to the cluster that owned the
		// machine — exactly as applyTransition does on the worker path.
		prevCluster := existing.Cluster
		dm.AssignedPriority = existing.AssignedPriority
		dm.AssignedInterruptionPenaltyDollars = existing.AssignedInterruptionPenaltyDollars
		dm.AssignedReclamationPenaltyDollars = existing.AssignedReclamationPenaltyDollars
		dm.AssignedNeedFingerprint = existing.AssignedNeedFingerprint
		dm.AssignedGroup = existing.AssignedGroup // ADR-0051: gang attribution
		dm.ShardMetadata = nil                    // provider-domain echo; never retained in inventory (see Machine.ShardMetadata)
		if err := s.inv.Apply(dm); err != nil {
			return
		}
		// ADR-0057: an async provider drives a transition to completion
		// out-of-band — it returns a TransitionAck in the transitional state
		// and reaches the terminal state (e.g. Configuring → Configured)
		// only via this reconcile, never via the worker's applyTransition.
		// So this is the ONLY place the operator can learn the terminal
		// state. Emit the node-state update here, the symmetric counterpart
		// to applyTransition's notify; without it, every async out-of-tree
		// provider's Configured nodes are invisible to the operator and the
		// workload never schedules. (We only get here on a real state change
		// — the state-match fast path returned above — so this never floods,
		// and the update coalesces by supersedes_key=node:<id> regardless.)
		s.notifyNodeState(dm, prevCluster)
		return
	}
	// M72: a machine with no local record is either genuinely new or —
	// the case that matters — a record being rebuilt from List after a
	// process restart wiped shard memory. The provider-echoed
	// shard_metadata is the only durable copy of the assignment state
	// (AssignedPriority + penalties for Phase 2 victim scoring, the Need
	// fingerprint for Phase 1 attribution); decode it here so a restarted
	// shard regains preemption protection instead of zeroing it
	// fleet-wide (production-readiness audit, arc 2). Malformed entries
	// are skipped key-by-key and logged — partial protection on one
	// machine beats refusing the machine.
	if err := dm.DecodeShardMetadata(); err != nil {
		s.log.Warn("shard_metadata decode failed at ingest; protection state may be incomplete",
			"machine", dm.ID, "err", err)
	}
	dm.ShardMetadata = nil // decoded above; don't carry the map onto the hot path
	if err := s.inv.Insert(dm); err != nil {
		return
	}
	// ADR-0057: a machine first seen already in a bound state (e.g. a
	// restart-rebuilt Configured record, or an async provider whose first
	// observed state for a fresh id is already terminal) must reach the
	// operator too.
	s.notifyNodeState(dm, "")
}

func equivalentErrNotFound(err error) bool {
	return errors.Is(err, inventory.ErrNotFound)
}
