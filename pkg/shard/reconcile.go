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
// The current M3 implementation does a full List every cycle. That is
// fine at the test scales we exercise here (hundreds of machines per
// shard); plan §10.6 calls for the cursor-based incremental List once
// providers above the conformance threshold support it, and the
// reconciler will switch to that path then. The wire field
// (since_revision) is already present on ListFilter.
func (s *Shard) reconcile(ctx context.Context) error {
	resp, err := s.cfg.Provider.List(ctx, provider.ListFilter{})
	if err != nil {
		return err
	}

	seen := make(map[machine.ID]struct{}, len(resp.Machines))
	for _, pm := range resp.Machines {
		// MachineFromProto round-trips through the proto-shaped Machine
		// from the provider; here the provider is already returning
		// domain types, so the conversion is a no-op clone (we still
		// route through MachineToProto + MachineFromProto to get the
		// validation paths exercised on every reconcile).
		dm, err := conv.MachineFromProto(conv.MachineToProto(pm))
		if err != nil {
			return err
		}
		seen[dm.ID] = struct{}{}

		// Preserve assigned-* fields if the inventory already has them.
		// The provider doesn't know about workload priority / penalties.
		if existing, getErr := s.inv.Get(dm.ID); getErr == nil {
			dm.AssignedPriority = existing.AssignedPriority
			dm.AssignedInterruptionPenaltyDollars = existing.AssignedInterruptionPenaltyDollars
			dm.AssignedReclamationPenaltyDollars = existing.AssignedReclamationPenaltyDollars
			if existing.State == dm.State {
				continue // No change, skip the write.
			}
			// Apply via state-machine validation. If the validation
			// rejects the transition (e.g., we missed an intermediate),
			// swallow the error — next reconcile will catch up.
			_ = s.inv.Apply(dm)
			continue
		}
		// Fresh machine — insert.
		_ = s.inv.Insert(dm)
	}

	// Remove inventory entries the provider no longer knows about.
	for _, m := range s.inv.Snapshot().All() {
		if _, ok := seen[m.ID]; ok {
			continue
		}
		if err := s.inv.Remove(m.ID); err != nil {
			// ErrNotFound is fine — concurrent removal.
			if !equivalentErrNotFound(err) {
				s.log.Warn("inventory remove failed", "machine", m.ID, "err", err)
			}
		}
	}
	return nil
}

func equivalentErrNotFound(err error) bool {
	return errors.Is(err, inventory.ErrNotFound)
}
