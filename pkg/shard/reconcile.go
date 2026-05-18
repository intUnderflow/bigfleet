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
// Slow paths: state diverged → apply the new state; machine is new →
// Insert. Both preserve the assigned-* fields (Priority,
// InterruptionPenalty, ReclamationPenalty) which the provider doesn't
// know about.
//
// The provider's domain types come pre-validated by either the fake
// (constructs valid Machines directly) or grpcadapter (validates at
// the proto-to-domain conversion). The pre-M11.24a code re-routed
// every reconciled machine through MachineToProto+MachineFromProto
// "to exercise the validation paths" — pure duplication that
// dominated the cycle whenever execute had just fanned out a burst.
// Trust the provider boundary; drop the round-trip.
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
	if existing, getErr := s.inv.Get(dm.ID); getErr == nil {
		if existing.State == dm.State {
			return
		}
		dm.AssignedPriority = existing.AssignedPriority
		dm.AssignedInterruptionPenaltyDollars = existing.AssignedInterruptionPenaltyDollars
		dm.AssignedReclamationPenaltyDollars = existing.AssignedReclamationPenaltyDollars
		dm.AssignedNeedFingerprint = existing.AssignedNeedFingerprint
		_ = s.inv.Apply(dm)
		return
	}
	_ = s.inv.Insert(dm)
}

func equivalentErrNotFound(err error) bool {
	return errors.Is(err, inventory.ErrNotFound)
}
