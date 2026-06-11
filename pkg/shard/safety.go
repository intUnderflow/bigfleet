// ADR-0046 actuation safety rails. All three rails live at the
// shard's actuation/ingest boundary — pkg/decision stays pure: the
// engine computes the paper-faithful answer every cycle, the rails
// govern what crosses from decision to execution (rail 1, rail 3) and
// what an inbound roll-up is allowed to do to the NeedsTable (rail 2).
// None of them ever reorders priority — see the ADR's §16-tension
// section.
//
// The ADR-0046 Addendum (second half of M70) adds three more surfaces
// at the same boundary, kept in this file: dry-run/shadow mode (the
// suppression branch lives in runCycleCapturing), the provider-ingest
// machine validator below, and the decision audit log.
package shard

import (
	"strings"
	"sync"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
)

// DefaultReclaimCapFraction is the production default for the
// per-cluster, per-cycle reclaim blast-radius cap (ADR-0046 rail 1):
// at most max(1, ⌊fraction × Configured⌋) Reclaims execute per
// cluster per cycle. 5% is an order of magnitude above healthy
// steady-state reclaim churn, and turns a full-fleet drain signal
// into ≥ 20 cycles of emission with ReclaimGrace (10 m) ahead of the
// first workload kill — operator-reaction time instead of one cycle.
// The default is applied at the binary boundary (shard subcommand,
// all-in-one, chart); shard.Config's zero value keeps the cap off so
// the sim canaries retain their diagnostic power.
const DefaultReclaimCapFraction = 0.05

// Empty-roll-up guard thresholds (ADR-0046 rail 2). Constants, not
// configuration — the ADR-0042-Addendum posture: tunables stay
// constants until evidence demands otherwise.
const (
	// rollupGuardMinRows is the floor: a roll-up is only eligible for
	// quarantine when the cluster's previously ACCEPTED demand spans
	// at least this many Need rows. Small clusters legitimately drop
	// to zero by deleting one workload; their blast radius is already
	// bounded in absolute terms by rail 1.
	rollupGuardMinRows = 10
	// rollupGuardRetainFraction: a roll-up retaining less than this
	// fraction of the accepted baseline's rows is the wipe signature
	// (failed CR List, emptied store, forged empty message).
	rollupGuardRetainFraction = 0.1
	// rollupGuardConsecutive consistent reports confirm intent; the
	// Nth is accepted. Three independent executions of the operator's
	// List+aggregate pipeline rule out one-shot truncation; genuine
	// mass scale-down is delayed ~2 roll-up intervals — invisible
	// next to the 10 m drain grace its reclaims then carry.
	rollupGuardConsecutive = 3
)

// rollupGuard holds the per-cluster quarantine state for rail 2.
// In-memory by design: after a shard restart the baseline is empty
// and the first roll-up is accepted whatever its size — the restart
// window belongs to ADR-0036's first-rollup gate.
type rollupGuard struct {
	mu        sync.Mutex
	byCluster map[machine.ClusterID]*rollupGuardEntry
}

type rollupGuardEntry struct {
	// acceptedRows is the Need-row count of the last accepted
	// roll-up — the baseline drops are measured against. It does NOT
	// move while roll-ups are being held, so every held roll-up is
	// compared to the same pre-anomaly demand.
	acceptedRows int
	// held counts consecutive roll-ups currently in quarantine.
	held int
}

// rollupGuardVerdict is admit's outcome, shaped for the caller's
// logging/metrics rather than re-deriving state.
type rollupGuardVerdict struct {
	accepted bool
	// held is the consecutive-quarantine count after this roll-up
	// (0 when accepted).
	held int
	// prevRows is the accepted baseline the comparison ran against.
	prevRows int
	// confirmed marks an acceptance that happened because the drop
	// persisted rollupGuardConsecutive times (vs. an ordinary
	// accept) — the caller logs the demand replacement loudly.
	confirmed bool
}

// admit decides whether a full-replacement roll-up with newRows Need
// rows may replace the cluster's demand. Row count is the demand-
// magnitude proxy (ADR-0046: the wipe class is list-shaped
// truncation; resource-vector magnitude has no honest scalar across
// heterogeneous resources).
func (g *rollupGuard) admit(c machine.ClusterID, newRows int) rollupGuardVerdict {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.byCluster == nil {
		g.byCluster = make(map[machine.ClusterID]*rollupGuardEntry)
	}
	e := g.byCluster[c]
	if e == nil {
		e = &rollupGuardEntry{}
		g.byCluster[c] = e
	}
	prev := e.acceptedRows
	suspicious := prev >= rollupGuardMinRows &&
		float64(newRows) < rollupGuardRetainFraction*float64(prev)
	if !suspicious {
		e.acceptedRows = newRows
		e.held = 0
		return rollupGuardVerdict{accepted: true, prevRows: prev}
	}
	e.held++
	if e.held >= rollupGuardConsecutive {
		e.acceptedRows = newRows
		e.held = 0
		return rollupGuardVerdict{accepted: true, prevRows: prev, confirmed: true}
	}
	return rollupGuardVerdict{held: e.held, prevRows: prev}
}

// reclaimCap is rail 1's arithmetic: max(1, ⌊fraction × configured⌋).
// The floor keeps small clusters draining (a cluster of 5 still
// releases 1/cycle); the base is the cycle snapshot's Configured
// count, so a full drain shrinks its own cap geometrically — extra
// margin, never starvation, because of the floor.
func reclaimCap(fraction float64, configured int) int {
	c := int(fraction * float64(configured))
	if c < 1 {
		c = 1
	}
	return c
}

// capReclaims enforces the per-cluster blast-radius cap over one
// cycle's collected actions. Only ActionKindReclaim is capped:
// Preempts are priority-driven capacity allocation (paper §16 — see
// the ADR), Bootstraps/Provisions are acquisitions. Within each
// cluster the kept reclaims are the head of the emission sequence,
// which is Phase 3's paper-§8 release order (cheapest-per-hour
// first) — the cap defers the tail, it doesn't reorder. Returns the
// filtered slice and the number of deferred reclaims; fraction <= 0
// disables.
func capReclaims(snap *inventory.Snapshot, actions []decision.Action, fraction float64) ([]decision.Action, int) {
	if fraction <= 0 {
		return actions, 0
	}
	var perCluster map[machine.ClusterID]int
	for i := range actions {
		if actions[i].Kind != decision.ActionKindReclaim {
			continue
		}
		if perCluster == nil {
			perCluster = make(map[machine.ClusterID]int)
		}
		perCluster[actions[i].Cluster]++
	}
	if perCluster == nil {
		return actions, 0
	}
	over := false
	allowed := make(map[machine.ClusterID]int, len(perCluster))
	for cl, n := range perCluster {
		c := reclaimCap(fraction, snap.CountByClusterState(cl, machine.StateConfigured))
		allowed[cl] = c
		if n > c {
			over = true
		}
	}
	if !over {
		return actions, 0
	}
	kept := make([]decision.Action, 0, len(actions))
	taken := make(map[machine.ClusterID]int, len(allowed))
	capped := 0
	for _, a := range actions {
		if a.Kind == decision.ActionKindReclaim {
			if taken[a.Cluster] >= allowed[a.Cluster] {
				capped++
				continue
			}
			taken[a.Cluster]++
		}
		kept = append(kept, a)
	}
	return kept, capped
}

// validateProviderMachine is the ingest gate for machine records
// arriving from the provider (reconcile List results, Create acks).
// machine.Invariant is the validator the production-readiness audit
// (arc 3) flagged as unwired: it bounds the provider-declared
// cost-formula inputs — price ≥ 0, interruption_probability ∈ [0, 1]
// — alongside the structural state invariants. Policy (ADR-0046
// addendum): reject the record — log + count, never crash, never
// silently accept — and let the inventory keep its last-known-good
// state.
func (s *Shard) validateProviderMachine(m *machine.Machine) error {
	err := m.Invariant()
	if err == nil {
		return nil
	}
	metrics.ShardMachinesRejected.WithLabelValues(machineRejectReason(err)).Inc()
	s.log.Warn("provider machine rejected at ingest (ADR-0046 addendum)",
		"machine", m.ID, "state", m.State.String(), "err", err)
	return err
}

// machineRejectReason buckets an Invariant violation into a bounded
// metric label. Same Contains-classification pattern as
// classifyExecuteError; the cost-formula bounds get their own labels
// because they are the money-path signal, everything else is shape.
func machineRejectReason(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "price_per_hour"):
		return "price"
	case strings.Contains(msg, "interruption_probability"):
		return "interruption_probability"
	}
	return "structural"
}

// auditAction appends one record to the decision audit log (ADR-0046
// addendum): cycle, kind, machine, cluster, reason, grace, and the
// disposition outcome — the classified execute result ("success",
// "provider_error", ...) for executed actions, "suppressed" for
// kill-switch drops, "dryrun" for shadow mode. The slog JSON handler
// supplies the timestamp. cycle is the shard's cycle counter at
// record time; for executed actions that is the executing cycle,
// which under the ADR-0021 async pool can trail the deciding cycle.
// No-op when no audit logger is configured.
func (s *Shard) auditAction(a *decision.Action, outcome string) {
	if s.audit == nil {
		return
	}
	s.audit.Info("action",
		"cycle", s.cycleCount.Load(),
		"kind", a.Kind.String(),
		"machine", string(a.MachineID),
		"cluster", string(a.Cluster),
		"reason", a.Reason,
		"grace_seconds", a.GracePeriod.Seconds(),
		"outcome", outcome,
	)
}
