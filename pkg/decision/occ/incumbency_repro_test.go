package occ_test

import (
	"sort"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// M77h — incumbency-stable machine SELECTION (ADR-0051 extended to the
// claim granularity). The domain tiebreak (M77g) pins WHICH domain a
// served gang chooses; this layer pins WHICH machines it claims within
// that domain. The driver #65 isolated: the credit/claim pass walks a
// domain's machines in keep-priority order (Configured before
// Configuring, then price asc / reclamation_penalty desc / ID asc) and
// claims under stop-when-covered, so as an in-flight non-incumbent
// matures (Configuring→Configured) it sorts ahead of a gang's incumbent
// and bumps it out of the first-N-covering claimed subset — the bumped
// incumbent goes unclaimed and Phase 3 reclaims it (ADR-0045
// shrinkage-only: unclaimed Configured = excess), then it re-bootstraps:
// the residual Bootstrap≈Reclaim lockstep the gate still showed after the
// domain was pinned. The fix claims this gang's own incumbents first.

func claimedSetFor(res occ.CycleResult, group string) []string {
	for i := range res.Results {
		r := &res.Results[i]
		if r.Need == nil || r.Need.Group != group {
			continue
		}
		out := make([]string, 0, len(r.ClaimedMachines))
		for _, mid := range r.ClaimedMachines {
			out = append(out, string(mid))
		}
		sort.Strings(out)
		return out
	}
	return nil
}

func contains(set []string, want string) bool {
	for _, s := range set {
		if s == want {
			return true
		}
	}
	return false
}

// TestSeedSameProfile_IncumbentKeptOverMaturedEquivalent is the M77h
// discriminator (fail-pre / pass-post). Single cycle, one gang, deficit
// = 2 machines. Its domain holds two of the gang's own incumbents
// (g-mmm, g-nnn — Configured, attributed) PLUS a non-incumbent
// equivalent (g-aaa — Configured, NO attribution) whose ID sorts FIRST,
// so under the bare keep-priority sort stop-when-covered would claim
// {g-aaa, g-mmm} and leave the incumbent g-nnn unclaimed. The fix
// prefers the gang's own machines, so the claim keeps {g-mmm, g-nnn} and
// g-aaa (genuine excess) is the unclaimed remainder Phase 3 may reclaim.
func TestSeedSameProfile_IncumbentKeptOverMaturedEquivalent(t *testing.T) {
	prof := gangRackProfile()
	fp := prof.Fingerprint()
	group := gangRackKey + "\x00c1-gangA"
	gpuQty := needs.ResourceQtysFromMap(gangGPU)

	inv := inventory.New()
	mustInsert(t, inv, gangCfgMachine("g-mmm", "c1", "rack-x", group, fp))
	mustInsert(t, inv, gangCfgMachine("g-nnn", "c1", "rack-x", group, fp))
	// Non-incumbent equivalent, sorts first by ID, in the same domain.
	mustInsert(t, inv, gangCfgMachine("g-aaa", "c1", "rack-x", "", ""))
	snap := inv.Snapshot()

	demand := []needs.Need{
		{ClusterID: "c1", Profile: prof, AggregateResources: needs.ScaleResources(gpuQty, 2), MinUnit: gpuQty, Group: group},
	}
	got := claimedSetFor(occ.RunCycle(snap, demand), group)
	t.Logf("claimed=%v", got)

	if len(got) != 2 {
		t.Fatalf("claimed %d machines, want 2: %v", len(got), got)
	}
	if !contains(got, "g-mmm") || !contains(got, "g-nnn") {
		t.Errorf("claimed %v, want both incumbents {g-mmm, g-nnn} kept (M77h: incumbents preferred over the sorts-ahead fresh g-aaa)", got)
	}
	if contains(got, "g-aaa") {
		t.Errorf("claimed the non-incumbent g-aaa over an incumbent — the bump M77h closes (got %v)", got)
	}
}

// TestSeedSameProfile_ClaimedSetStableAcrossMaturation is the offline
// pin M77f/M77g lacked at this granularity: the within-domain churn
// reproduced deterministically across one maturation. Pre-fix the gang's
// claimed set changes between the two cycles (the non-incumbent matures,
// re-sorts ahead, bumps an incumbent); post-fix it is stable. (The
// closed-loop sim self-damps and does not reproduce the SUSTAINED
// actuation — see sim/incumbency_repro_test.go's note — so this engine-
// granularity pin is the discriminating offline guard.)
func TestSeedSameProfile_ClaimedSetStableAcrossMaturation(t *testing.T) {
	prof := gangRackProfile()
	fp := prof.Fingerprint()
	group := gangRackKey + "\x00c1-gangA"
	gpuQty := needs.ResourceQtysFromMap(gangGPU)
	demand := []needs.Need{
		{ClusterID: "c1", Profile: prof, AggregateResources: needs.ScaleResources(gpuQty, 2), MinUnit: gpuQty, Group: group},
	}

	// The gang's domain holds its two incumbents plus one non-incumbent
	// machine (g-aaa, ID sorts first) that is in flight in cycle 1 and
	// matured in cycle 2 — the snapshot the claim ranks on moves exactly
	// as the field's bootstrap dwell makes it move.
	build := func(nonGangState machine.State) *inventory.Snapshot {
		inv := inventory.New()
		mustInsert(t, inv, gangCfgMachine("g-mmm", "c1", "rack-x", group, fp))
		mustInsert(t, inv, gangCfgMachine("g-nnn", "c1", "rack-x", group, fp))
		nm := gangCfgMachine("g-aaa", "c1", "rack-x", "", "")
		nm.State = nonGangState
		mustInsert(t, inv, nm)
		return inv.Snapshot()
	}

	set1 := claimedSetFor(occ.RunCycle(build(machine.StateConfiguring), demand), group)
	set2 := claimedSetFor(occ.RunCycle(build(machine.StateConfigured), demand), group)
	t.Logf("cycle1 (g-aaa Configuring) claimed=%v", set1)
	t.Logf("cycle2 (g-aaa Configured) claimed=%v", set2)

	if len(set1) != len(set2) {
		t.Fatalf("claimed-set size changed across maturation: %v -> %v", set1, set2)
	}
	for i := range set1 {
		if set1[i] != set2[i] {
			t.Fatalf("within-domain CHURN: claimed set changed across maturation: %v -> %v", set1, set2)
		}
	}
}

func mustInsert(t *testing.T, inv *inventory.Inventory, m machine.Machine) {
	t.Helper()
	if err := inv.Insert(m); err != nil {
		t.Fatalf("insert %s: %v", m.ID, err)
	}
}
