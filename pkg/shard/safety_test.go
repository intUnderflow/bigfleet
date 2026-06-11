package shard

// ADR-0046 safety-rail tests. The rails have their own test surface
// here because the sim canaries deliberately run rails-off (they pin
// engine pathologies the rails would dampen).

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
)

func newSafetyShard(t *testing.T, mut func(*Config)) (*Shard, *fake.Provider) {
	t.Helper()
	prov := fake.New(fake.Options{InstantTransitions: true})
	epoch, err := fencing.LoadEpoch(filepath.Join(t.TempDir(), "epoch"))
	if err != nil {
		t.Fatalf("LoadEpoch: %v", err)
	}
	cfg := Config{
		ID:               "safety-test",
		Epoch:            epoch,
		Provider:         prov,
		CycleInterval:    time.Second, // unused; cycles driven via Step
		BootstrapTimeout: time.Second,
		LocalBootstrap: func(context.Context, machine.ClusterID, []needs.Requirement) ([]byte, error) {
			return []byte("# safety test\n"), nil
		},
	}
	if mut != nil {
		mut(&cfg)
	}
	sh, err := New(cfg)
	if err != nil {
		t.Fatalf("shard.New: %v", err)
	}
	return sh, prov
}

func capTestProfile() machine.Profile {
	return machine.Profile{
		InstanceType: "cap-test",
		Zone:         "zone-a",
		CapacityType: machine.CapacityTypeBareMetal,
		Resources:    map[string]string{"cpu": "4"},
	}
}

// capTestNeed demands `machines` cap-test machines' worth of cpu.
func capTestNeed(cluster machine.ClusterID, machines int, prio int32) needs.Need {
	return needs.Need{
		ClusterID: cluster,
		Profile: needs.NewProfile([]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"cap-test"},
		}}, nil, prio, needs.PenaltyBucket8, needs.PenaltyBucket8),
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: strconv.Itoa(machines * 4)}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
	}
}

func seedConfigured(t *testing.T, sh *Shard, prov *fake.Provider, cluster machine.ClusterID, id machine.ID) {
	t.Helper()
	prov.AddConfigured(id, capTestProfile(), machine.CapacityTypeBareMetal, 0, 0, cluster, 1000, 8, 8)
	if err := sh.SeedInventory(machine.Machine{
		ID:      id,
		State:   machine.StateConfigured,
		Host:    machine.HostRef{Provider: "fake", Ref: string(id)},
		Cluster: cluster,
		Profile: capTestProfile(),
	}); err != nil {
		t.Fatalf("SeedInventory(%s): %v", id, err)
	}
}

// mkRows builds n NeedsTable rows (distinct priorities → distinct
// Profiles, so Replace stores n rows).
func mkRows(cluster machine.ClusterID, n int) []needs.Need {
	out := make([]needs.Need, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, capTestNeed(cluster, 1, int32(i+1)))
	}
	return out
}

func countKind(actions []decision.Action, kind decision.ActionKind) int {
	n := 0
	for _, a := range actions {
		if a.Kind == kind {
			n++
		}
	}
	return n
}

// --- Rail 1: reclaim blast-radius cap ------------------------------

func TestReclaimCap_Arithmetic(t *testing.T) {
	cases := []struct {
		fraction   float64
		configured int
		want       int
	}{
		{0.05, 0, 1},   // floor: progress even on a vanishing cluster
		{0.05, 1, 1},   // max(1, 0.05) = 1
		{0.05, 19, 1},  // 0.95 floors to 0 → floor lifts to 1
		{0.05, 21, 1},  // 1.05 floors to 1
		{0.05, 40, 2},  // 2.0
		{0.05, 100, 5}, // 5.0
		{0.25, 40, 10},
		{0.5, 7, 3},
	}
	for _, c := range cases {
		if got := reclaimCap(c.fraction, c.configured); got != c.want {
			t.Errorf("reclaimCap(%v, %d) = %d, want %d", c.fraction, c.configured, got, c.want)
		}
	}
}

// TestCapReclaims_PerClusterOrderAndExemptions pins capReclaims'
// contract: per-cluster caps from the snapshot's Configured count,
// the kept reclaims are the head of each cluster's emission sequence
// (Phase 3's paper-§8 release order), and Bootstrap / Provision /
// Preempt pass untouched — Preempt exempt by design (§16).
func TestCapReclaims_PerClusterOrderAndExemptions(t *testing.T) {
	inv := inventory.New()
	insert := func(cluster machine.ClusterID, prefix string, n int) {
		for i := 0; i < n; i++ {
			if err := inv.Insert(machine.Machine{
				ID:      machine.ID(fmt.Sprintf("%s-%d", prefix, i)),
				State:   machine.StateConfigured,
				Host:    machine.HostRef{Provider: "fake", Ref: prefix},
				Cluster: cluster,
				Profile: capTestProfile(),
			}); err != nil {
				t.Fatalf("Insert: %v", err)
			}
		}
	}
	insert("cl-a", "a", 40) // cap at 5% → 2
	insert("cl-b", "b", 10) // cap at 5% → 1
	snap := inv.Snapshot()

	var actions []decision.Action
	for i := 0; i < 3; i++ {
		actions = append(actions, decision.Action{Kind: decision.ActionKindBootstrap, MachineID: machine.ID("boot-" + strconv.Itoa(i)), Cluster: "cl-a"})
	}
	for i := 0; i < 10; i++ {
		actions = append(actions, decision.Action{Kind: decision.ActionKindReclaim, MachineID: machine.ID("a-" + strconv.Itoa(i)), Cluster: "cl-a"})
	}
	for i := 0; i < 3; i++ {
		actions = append(actions, decision.Action{Kind: decision.ActionKindReclaim, MachineID: machine.ID("b-" + strconv.Itoa(i)), Cluster: "cl-b"})
	}
	for i := 0; i < 2; i++ {
		actions = append(actions, decision.Action{Kind: decision.ActionKindPreempt, MachineID: machine.ID("p-" + strconv.Itoa(i)), Cluster: "cl-b"})
	}

	kept, capped := capReclaims(snap, actions, 0.05)
	if capped != (10-2)+(3-1) {
		t.Errorf("capped = %d, want %d", capped, 10)
	}
	if got := countKind(kept, decision.ActionKindBootstrap); got != 3 {
		t.Errorf("bootstraps kept = %d, want 3 (never capped)", got)
	}
	if got := countKind(kept, decision.ActionKindPreempt); got != 2 {
		t.Errorf("preempts kept = %d, want 2 (§16: never capped)", got)
	}
	var keptReclaims []string
	for _, a := range kept {
		if a.Kind == decision.ActionKindReclaim {
			keptReclaims = append(keptReclaims, string(a.MachineID))
		}
	}
	// Head of each cluster's emission order: §8 releases those first.
	want := []string{"a-0", "a-1", "b-0"}
	if len(keptReclaims) != len(want) {
		t.Fatalf("kept reclaims = %v, want %v", keptReclaims, want)
	}
	for i := range want {
		if keptReclaims[i] != want[i] {
			t.Fatalf("kept reclaims = %v, want %v", keptReclaims, want)
		}
	}

	// Disabled fraction passes everything through.
	if all, n := capReclaims(snap, actions, 0); n != 0 || len(all) != len(actions) {
		t.Errorf("fraction 0: capped %d / kept %d, want 0 / %d", n, len(all), len(actions))
	}
}

// TestReclaimCap_RollsOverAcrossCycles drives a real fleet-drain
// signal (empty demand over 40 Configured) through Step and asserts
// the cap meters the drain per cycle while every machine is still
// eventually reclaimed — roll-over, not drop.
func TestReclaimCap_RollsOverAcrossCycles(t *testing.T) {
	const frac = 0.25
	sh, prov := newSafetyShard(t, func(c *Config) { c.ReclaimCapFraction = frac })
	cluster := machine.ClusterID("drain-c")
	const total = 40
	for i := 0; i < total; i++ {
		seedConfigured(t, sh, prov, cluster, machine.ID("m-"+strconv.Itoa(i)))
	}
	cappedBefore := testutil.ToFloat64(metrics.ShardReclaimsCapped)

	// Empty full-replacement roll-up: "reclaim everything I hold".
	sh.ApplyRollup(cluster, nil)

	ctx := context.Background()
	executed := 0
	cycles := 0
	for ; cycles < 60; cycles++ {
		confStart := sh.Inventory().Snapshot().CountByClusterState(cluster, machine.StateConfigured)
		if confStart == 0 {
			break
		}
		acts := sh.Step(ctx)
		reclaims := countKind(acts, decision.ActionKindReclaim)
		if limit := reclaimCap(frac, confStart); reclaims > limit {
			t.Fatalf("cycle %d: %d reclaims > cap %d (configured %d)", cycles, reclaims, limit, confStart)
		}
		executed += reclaims
	}
	if executed != total {
		t.Errorf("total executed reclaims = %d, want %d (surplus must roll over, not drop)", executed, total)
	}
	if cycles < 2 {
		t.Errorf("drain completed in %d cycle(s); the cap should have spread it over several", cycles)
	}
	if got := sh.Inventory().Snapshot().CountByState(machine.StateIdle); got != total {
		t.Errorf("idle after drain = %d, want %d", got, total)
	}
	if delta := testutil.ToFloat64(metrics.ShardReclaimsCapped) - cappedBefore; delta <= 0 {
		t.Errorf("bigfleet_shard_reclaims_capped_total delta = %v, want > 0", delta)
	}
}

// TestReclaimCap_Property_SumBoundedOverCycles: with the cap on,
// Σ executed reclaims over k cycles ≤ k × cap regardless of demand
// swings — and per cycle, reclaims never exceed that cycle's own cap.
func TestReclaimCap_Property_SumBoundedOverCycles(t *testing.T) {
	const frac = 0.05
	sh, prov := newSafetyShard(t, func(c *Config) { c.ReclaimCapFraction = frac })
	cluster := machine.ClusterID("prop-c")
	const total = 60
	for i := 0; i < total; i++ {
		seedConfigured(t, sh, prov, cluster, machine.ID("p-"+strconv.Itoa(i)))
	}

	rng := rand.New(rand.NewSource(0x46)) // deterministic
	ctx := context.Background()
	const cycles = 50
	sumReclaims, sumCaps := 0, 0
	maxConfigured := total
	for c := 0; c < cycles; c++ {
		// Random full-replacement demand swing: everything, nothing,
		// or a random fraction of the fleet.
		switch rng.Intn(3) {
		case 0:
			sh.ApplyRollup(cluster, nil)
		case 1:
			sh.ApplyRollup(cluster, []needs.Need{capTestNeed(cluster, total, 1000)})
		default:
			sh.ApplyRollup(cluster, []needs.Need{capTestNeed(cluster, rng.Intn(total)+1, 1000)})
		}
		confStart := sh.Inventory().Snapshot().CountByClusterState(cluster, machine.StateConfigured)
		if confStart > maxConfigured {
			maxConfigured = confStart
		}
		limit := reclaimCap(frac, confStart)
		reclaims := countKind(sh.Step(ctx), decision.ActionKindReclaim)
		if reclaims > limit {
			t.Fatalf("cycle %d: %d reclaims > cap %d (configured %d)", c, reclaims, limit, confStart)
		}
		sumReclaims += reclaims
		sumCaps += limit
	}
	if kCap := cycles * reclaimCap(frac, maxConfigured); sumReclaims > kCap {
		t.Errorf("Σ reclaims = %d over %d cycles, want ≤ k × cap = %d", sumReclaims, cycles, kCap)
	}
	if sumReclaims > sumCaps {
		t.Errorf("Σ reclaims = %d exceeds Σ per-cycle caps = %d", sumReclaims, sumCaps)
	}
}

// --- Rail 2: empty-roll-up quarantine ------------------------------

func TestRollupGuard_QuarantineThenAcceptAfterConsecutive(t *testing.T) {
	sh, _ := newSafetyShard(t, func(c *Config) { c.EmptyRollupGuard = true })
	cluster := machine.ClusterID("guard-c")

	if !sh.ApplyRollup(cluster, mkRows(cluster, 20)) {
		t.Fatal("baseline roll-up must apply")
	}
	if got := sh.NeedsTable().Stats().Needs; got != 20 {
		t.Fatalf("baseline rows = %d, want 20", got)
	}

	// Wipe #1 and #2: held; previously accepted demand stays active;
	// the ADR-0036 gate still clears (the operator DID report).
	for i := 1; i < rollupGuardConsecutive; i++ {
		if sh.ApplyRollup(cluster, nil) {
			t.Fatalf("wipe #%d applied, want quarantined", i)
		}
		if got := sh.NeedsTable().Stats().Needs; got != 20 {
			t.Fatalf("after wipe #%d: rows = %d, want 20 (previous demand stays active)", i, got)
		}
		if !sh.FirstRollupReceived(cluster) {
			t.Fatalf("after wipe #%d: ADR-0036 gate not cleared", i)
		}
		if g := testutil.ToFloat64(metrics.ShardRollupQuarantined.WithLabelValues(string(cluster))); g != float64(i) {
			t.Fatalf("quarantine gauge after wipe #%d = %v, want %d", i, g, i)
		}
	}

	// Wipe #3: the drop persisted; intent confirmed; applied.
	if !sh.ApplyRollup(cluster, nil) {
		t.Fatalf("wipe #%d must apply (consistency confirms intent)", rollupGuardConsecutive)
	}
	if got := sh.NeedsTable().Stats().Needs; got != 0 {
		t.Fatalf("after confirmation: rows = %d, want 0", got)
	}
	if g := testutil.ToFloat64(metrics.ShardRollupQuarantined.WithLabelValues(string(cluster))); g != 0 {
		t.Fatalf("quarantine gauge after acceptance = %v, want 0", g)
	}
}

func TestRollupGuard_RecoveryResetsQuarantine(t *testing.T) {
	sh, _ := newSafetyShard(t, func(c *Config) { c.EmptyRollupGuard = true })
	cluster := machine.ClusterID("guard-r")

	sh.ApplyRollup(cluster, mkRows(cluster, 20))
	if sh.ApplyRollup(cluster, nil) {
		t.Fatal("wipe must quarantine")
	}
	// Demand comes back: accepted immediately, quarantine resets.
	if !sh.ApplyRollup(cluster, mkRows(cluster, 18)) {
		t.Fatal("recovered demand must apply immediately")
	}
	if got := sh.NeedsTable().Stats().Needs; got != 18 {
		t.Fatalf("rows = %d, want 18", got)
	}
	// A fresh wipe restarts the consecutive count from 1 — it must
	// not inherit the pre-recovery hold.
	if sh.ApplyRollup(cluster, nil) {
		t.Fatal("fresh wipe must quarantine again")
	}
	if sh.ApplyRollup(cluster, nil) {
		t.Fatal("second consecutive wipe still held")
	}
	if !sh.ApplyRollup(cluster, nil) {
		t.Fatal("third consecutive wipe must apply")
	}
}

func TestRollupGuard_FloorAndGradualScaleDown(t *testing.T) {
	sh, _ := newSafetyShard(t, func(c *Config) { c.EmptyRollupGuard = true })

	// Below the floor: a small cluster scaling to zero is never held.
	small := machine.ClusterID("guard-small")
	sh.ApplyRollup(small, mkRows(small, rollupGuardMinRows-1))
	if !sh.ApplyRollup(small, nil) {
		t.Fatal("below-floor cluster's empty roll-up must apply immediately")
	}

	// Gradual scale-down: each step retains ≥10% of the accepted
	// baseline, so the baseline tracks down and nothing quarantines.
	grad := machine.ClusterID("guard-grad")
	for _, rows := range []int{40, 20, 8, 0} {
		if !sh.ApplyRollup(grad, mkRows(grad, rows)) {
			t.Fatalf("gradual step to %d rows quarantined, want applied", rows)
		}
	}

	// First-ever roll-up (no baseline): always accepted, whatever its
	// size — the restart window is ADR-0036's, not the guard's.
	fresh := machine.ClusterID("guard-fresh")
	if !sh.ApplyRollup(fresh, nil) {
		t.Fatal("first roll-up with no baseline must apply")
	}
}

func TestRollupGuard_LogsLoudly(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	sh, _ := newSafetyShard(t, func(c *Config) {
		c.EmptyRollupGuard = true
		c.Logger = logger
	})
	cluster := machine.ClusterID("guard-log")
	sh.ApplyRollup(cluster, mkRows(cluster, 20))
	sh.ApplyRollup(cluster, nil)
	if !strings.Contains(buf.String(), "rollup quarantined") {
		t.Errorf("quarantine produced no loud log line; got: %s", buf.String())
	}
	sh.ApplyRollup(cluster, nil)
	sh.ApplyRollup(cluster, nil)
	if !strings.Contains(buf.String(), "quarantined rollup accepted") {
		t.Errorf("confirmed acceptance produced no log line; got: %s", buf.String())
	}
}

// --- Rail 3: kill switch -------------------------------------------

// TestActuationPaused_SuppressesExecution: the cycle keeps deciding
// (Step returns the engine's actions) but nothing executes — no
// machine leaves its state — and suppressed actions are counted.
func TestActuationPaused_SuppressesExecution(t *testing.T) {
	sh, prov := newSafetyShard(t, func(c *Config) { c.ActuationPaused = true })
	ctx := context.Background()

	// Acquisition side: demand over idle supply decides Bootstraps.
	for i := 0; i < 4; i++ {
		prov.AddIdle(machine.ID("idle-"+strconv.Itoa(i)), capTestProfile(), machine.CapacityTypeBareMetal, 0, 0)
	}
	// Release side: a Configured machine under an empty roll-up
	// decides a Reclaim.
	seedConfigured(t, sh, prov, "paused-r", "conf-0")
	sh.ApplyRollup("paused-r", nil)
	sh.ApplyRollup("paused-b", []needs.Need{capTestNeed("paused-b", 4, 1000)})

	suppressedBefore := testutil.ToFloat64(metrics.ShardActionsSuppressed.WithLabelValues("Bootstrap")) +
		testutil.ToFloat64(metrics.ShardActionsSuppressed.WithLabelValues("Reclaim"))

	for cycle := 0; cycle < 3; cycle++ {
		acts := sh.Step(ctx)
		if countKind(acts, decision.ActionKindBootstrap) == 0 {
			t.Fatalf("cycle %d: no Bootstrap decided — the engine must keep deciding while paused", cycle)
		}
		if countKind(acts, decision.ActionKindReclaim) == 0 {
			t.Fatalf("cycle %d: no Reclaim decided — the engine must keep deciding while paused", cycle)
		}
		snap := sh.Inventory().Snapshot()
		if got := snap.CountByState(machine.StateIdle); got != 4 {
			t.Fatalf("cycle %d: idle = %d, want 4 (no Bootstrap may execute)", cycle, got)
		}
		if got := snap.CountByState(machine.StateConfigured); got != 1 {
			t.Fatalf("cycle %d: configured = %d, want 1 (no Reclaim may execute)", cycle, got)
		}
	}

	suppressedAfter := testutil.ToFloat64(metrics.ShardActionsSuppressed.WithLabelValues("Bootstrap")) +
		testutil.ToFloat64(metrics.ShardActionsSuppressed.WithLabelValues("Reclaim"))
	if delta := suppressedAfter - suppressedBefore; delta < 5 {
		t.Errorf("suppressed-actions delta = %v, want ≥ 5 (4 Bootstraps + 1 Reclaim per cycle)", delta)
	}
	if got := testutil.ToFloat64(metrics.ShardActuationPaused); got != 1 {
		t.Errorf("bigfleet_shard_actuation_paused = %v, want 1", got)
	}
}

// TestActuationPaused_ReportingContinues: the pause must not blind
// the operator — shortfall tracking (and with it the coordinator
// report surface) keeps flowing from the still-running cycle.
func TestActuationPaused_ReportingContinues(t *testing.T) {
	sh, _ := newSafetyShard(t, func(c *Config) { c.ActuationPaused = true })
	cluster := machine.ClusterID("paused-sf")
	// Demand nothing in inventory can satisfy → a shortfall.
	sh.ApplyRollup(cluster, []needs.Need{{
		ClusterID: cluster,
		Profile: needs.NewProfile([]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"no-such-type"},
		}}, nil, 1000, needs.PenaltyBucket8, needs.PenaltyBucket8),
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
	}})
	sh.Step(context.Background())
	if got := sh.ShortfallCount(); got == 0 {
		t.Error("shortfall count = 0 while paused, want > 0 (reporting must continue)")
	}
}
