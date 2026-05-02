package shard

import (
	"context"
	"errors"

	"github.com/intUnderflow/bigfleet/pkg/conv"
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

// reconcileFull is the always-correct full-List path. Quadratic in the
// inventory size at large scale (proto round-trip per machine, plus
// snapshot walk for removals). Suitable for any provider.
func (s *Shard) reconcileFull(ctx context.Context) error {
	resp, err := s.cfg.Provider.List(ctx, provider.ListFilter{})
	if err != nil {
		return err
	}

	seen := make(map[machine.ID]struct{}, len(resp.Machines))
	for _, pm := range resp.Machines {
		dm, err := conv.MachineFromProto(conv.MachineToProto(pm))
		if err != nil {
			return err
		}
		seen[dm.ID] = struct{}{}
		if err := s.applyReconciledMachine(dm); err != nil {
			return err
		}
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
		dm, err := conv.MachineFromProto(conv.MachineToProto(pm))
		if err != nil {
			return err
		}
		if err := s.applyReconciledMachine(dm); err != nil {
			return err
		}
	}
	s.reconcileCursor = resp.Revision
	return nil
}

// applyReconciledMachine merges one machine from the provider into the
// shard's inventory: preserve the assigned-* fields the provider
// doesn't know about, skip the write when nothing changed, fall back to
// Insert when the machine is new. Shared by both reconcile paths so the
// merge logic stays in one place.
func (s *Shard) applyReconciledMachine(dm machine.Machine) error {
	if existing, getErr := s.inv.Get(dm.ID); getErr == nil {
		dm.AssignedPriority = existing.AssignedPriority
		dm.AssignedInterruptionPenaltyDollars = existing.AssignedInterruptionPenaltyDollars
		dm.AssignedReclamationPenaltyDollars = existing.AssignedReclamationPenaltyDollars
		if existing.State == dm.State {
			return nil
		}
		_ = s.inv.Apply(dm)
		return nil
	}
	_ = s.inv.Insert(dm)
	return nil
}

func equivalentErrNotFound(err error) bool {
	return errors.Is(err, inventory.ErrNotFound)
}
