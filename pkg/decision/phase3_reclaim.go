package decision

import (
	"sort"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// Phase3Result is the output of a Phase 3 pass: Reclaim actions for
// the Configured machines a cluster holds in excess of its demand,
// plus Delete actions for the unclaimed Idle machines whose
// per-CapacityType hold has expired (the §8 release half, M73).
type Phase3Result struct {
	Actions []Action
}

// ClusterReadyFn reports whether the named cluster has been heard from
// (first rollup received) since this shard process started.
// ADR-0036: Phase 3 must skip reclaim for clusters that haven't yet
// reported, because their NeedsTable slice is empty for the trivial
// "I haven't told you anything yet" reason, not for the meaningful
// "I have no demand right now" reason. Without the gate, the
// install / shard-restart window drains every Configured machine
// before the operator pipeline catches up.
//
// Callers in tests that don't care about the gate (most decision-
// package unit tests) pass `AlwaysReady`. Callers in tests that
// specifically exercise the gate's behaviour pass a stub.
type ClusterReadyFn func(machine.ClusterID) bool

// AlwaysReady is a ClusterReadyFn that always returns true. Used by
// tests that don't exercise the ADR-0036 gate.
func AlwaysReady(machine.ClusterID) bool { return true }

// Phase3 is shrinkage-only reclaim (ADR-0045): a Configured machine is
// excess iff this cycle's attribution walk — Phase 1's pre-pass credit
// plus worker commits, the engine's single supply arithmetic — left it
// unclaimed, i.e. the cluster's bound capacity exceeds its demand by
// at least that machine. claimed is that walk's final claimed-set
// (Phase1Result.Claimed). At steady demand Phase 1 acquires until the
// walk covers, so nothing is unclaimed and Phase 3 emits nothing;
// excess appears only when demand shrinks below what is bound (or
// supply was seeded above demand, which the contract cannot
// distinguish from a scale-down).
//
// Phase 3 does NOT re-derive a per-cluster keep-set, and it carries no
// satisfaction arithmetic of its own. The pre-ADR-0045 version walked
// Needs a second time (claimMatching / claimMatchingSame mirroring the
// seed pass, plus three rounds of ADR-0040/0041/0042 mirror patches);
// any drift between the two walks made Phase 1 provision what Phase 3
// reclaimed — the Bootstrap↔Reclaim oscillator class. Sharing the
// claimed-set removes it by construction. Same-constraint scoping
// (ADR-0040 domain confinement, ADR-0042 parked Needs keeping their
// incumbent assembly) lives in the shared walk, so off-domain scatter
// is still excess and a parked gang's demand still counts.
//
// The unclaimed remainder is emitted in SortedClusterStateBucket order
// — the same keep-priority order (price asc, reclamation_penalty desc,
// ID asc) the credit walk claims in — so what is released is the
// keep-priority tail, in the paper-§8 release order. The grace rides
// the ReclaimInstruction to the operator, which bounds the cordon +
// PDB-respecting eviction with it (ADR-0009 / M69).
//
// The second walk is §8's other half (M73 / ADR-0049): "Idle →
// Speculative lazily per provider". An Idle machine that this cycle's
// claimed-set did not count toward any Need — so Phase 1 is not about
// to bootstrap it — and that has sat Idle past its CapacityType's hold
// (policy, measured from the snapshot's IdleSince stamp against now)
// gets a Delete. The ADR-0036 cluster gate deliberately does NOT apply
// here: an Idle machine is bound to no cluster and counts for nothing
// (ADR-0045), so releasing it can never take capacity from anyone —
// the hold window plus the restart-resets-the-clock stamp semantics
// are the protection, and the worst case of a wrong release is one
// re-buy, not a workload death. A zero-value policy releases nothing.
func Phase3(snap *inventory.Snapshot, claimed map[machine.ID]struct{}, clusterReady ClusterReadyFn, policy ReleasePolicy, now time.Time) Phase3Result {
	out := Phase3Result{}

	for _, cluster := range clustersWithConfigured(snap) {
		// ADR-0036 gate: skip clusters that haven't yet reported.
		// Their empty NeedsTable slice is "haven't told me yet",
		// not "have no demand"; reclaiming on that signal would
		// drain seeded supply during install / restart windows.
		if !clusterReady(cluster) {
			continue
		}
		configured := snap.SortedClusterStateBucket(cluster, machine.StateConfigured)
		for i := range configured {
			m := &configured[i]
			if _, kept := claimed[m.ID]; kept {
				continue
			}
			out.Actions = append(out.Actions, Action{
				Kind:        ActionKindReclaim,
				MachineID:   m.ID,
				Cluster:     cluster,
				GracePeriod: ReclaimGrace,
				Reason:      "phase3.excess",
			})
		}
	}

	// §8 release walk (M73). Emission order is the snapshot's
	// deterministic ID order; no intra-cycle ordering matters because
	// every expired machine releases this cycle — the hold window, not
	// a cap, is what spreads releases over time (ADR-0049).
	for _, m := range snap.ListByState(machine.StateIdle) {
		if _, kept := claimed[m.ID]; kept {
			continue
		}
		hold, releasable := policy.Hold(m.Profile.CapacityType)
		if !releasable {
			continue
		}
		since, ok := snap.IdleSince(m.ID)
		if !ok {
			// No stamp = hold. Defensive: every inventory write path
			// stamps Idle entries, but an unstamped machine must err
			// toward a delayed release, never a premature delete.
			continue
		}
		if now.Sub(since) < hold {
			continue
		}
		out.Actions = append(out.Actions, Action{
			Kind:      ActionKindDelete,
			MachineID: m.ID,
			Reason:    "phase3.release",
		})
	}

	return out
}

// clustersWithConfigured returns the cluster IDs that have at least
// one Configured machine, sorted. M27: iterates the snapshot's per-
// cluster index keys instead of walking the full Configured slice
// (which at 500K configured was ~9 % of Phase3's hot path). Sorted so
// the cross-cluster emission order is deterministic.
func clustersWithConfigured(snap *inventory.Snapshot) []machine.ClusterID {
	out := make([]machine.ClusterID, 0, len(snap.ClusterIDs()))
	for cl := range snap.ClusterIDs() {
		if snap.CountByClusterState(cl, machine.StateConfigured) > 0 {
			out = append(out, cl)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
