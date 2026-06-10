package decision_test

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// All needs deleted: every configured machine reclaimed.
func TestPhase3_AllExcessWhenNoNeeds(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 4; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-a", 100, 0, 0))
	}
	r := decision.Phase3(inv.Snapshot(), nil, decision.AlwaysReady)
	if got := len(r.Actions); got != 4 {
		t.Fatalf("reclaim actions = %d, want 4", got)
	}
	for _, a := range r.Actions {
		if a.Kind != decision.ActionKindReclaim {
			t.Errorf("expected Reclaim, got %s", a.Kind)
		}
	}
}

// Need is fully satisfied by the configured machines: nothing reclaimed.
func TestPhase3_NoOpWhenExactlyMet(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 3; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-a", 100, 0, 0))
	}
	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{gpuNeed("cluster-a", gpuProfile(100), 3)}, decision.AlwaysReady)
	if got := len(r.Actions); got != 0 {
		t.Errorf("expected zero reclaim actions, got %d", got)
	}
}

// Cluster has 5 configured but only 3 are needed: 2 reclaimed, cheapest
// per-hour first.
func TestPhase3_ReclaimsCheapestFirst(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// Mix of three on-demand ($6) and two bare-metal ($0). Cluster
	// needs 3. Phase 3 should release the three on-demand machines.
	for i := 0; i < 3; i++ {
		m := configuredVictim(idN(i), "cluster-a", 100, 0, 0)
		m.PricePerHour = 6.0
		m.Profile.CapacityType = machine.CapacityTypeOnDemand
		_ = inv.Insert(m)
	}
	for i := 3; i < 5; i++ {
		m := configuredVictim(idN(i), "cluster-a", 100, 0, 0)
		m.PricePerHour = 0
		m.Profile.CapacityType = machine.CapacityTypeBareMetal
		_ = inv.Insert(m)
	}
	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{gpuNeed("cluster-a", gpuProfile(100), 3)}, decision.AlwaysReady)
	if got := len(r.Actions); got != 2 {
		t.Fatalf("reclaim actions = %d, want 2", got)
	}
	for _, a := range r.Actions {
		// The two on-demand machines (priced higher) are reclaimed first.
		// Check by looking up the machine's price.
		m, _ := inv.Get(a.MachineID)
		if m.PricePerHour != 6.0 {
			t.Errorf("reclaimed cheap machine %s (price=%f); should reclaim expensive first", a.MachineID, m.PricePerHour)
		}
	}
}

// Tied prices: prefer reclaiming the lower reclamation_penalty machine.
func TestPhase3_TiebreakByReclamationPenalty(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	low := configuredVictim("v-low", "cluster-a", 100, 0, 0)
	low.PricePerHour = 6.0
	low.AssignedReclamationPenaltyDollars = 0
	high := configuredVictim("v-high", "cluster-a", 100, 0, 0)
	high.PricePerHour = 6.0
	high.AssignedReclamationPenaltyDollars = 5_000
	_ = inv.Insert(low)
	_ = inv.Insert(high)

	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{gpuNeed("cluster-a", gpuProfile(100), 1)}, decision.AlwaysReady)
	if got := len(r.Actions); got != 1 {
		t.Fatalf("reclaim actions = %d, want 1", got)
	}
	if r.Actions[0].MachineID != "v-low" {
		t.Errorf("reclaimed %s, want v-low (lower reclamation penalty)", r.Actions[0].MachineID)
	}
}

// ADR-0027: Phase 3 keeps a Configured machine when it MatchProfiles a
// live Need and helps cover that Need's demand — NOT by its (possibly
// stale) AssignedNeedFingerprint. This mirrors Phase 1's MatchProfile-
// based supply credit; keying the two phases differently (the
// pre-ADR-0027 "Drop F" keep-by-fingerprint semantics) made Phase 1
// provision a machine Phase 3 then reclaimed — a Bootstrap<->Reclaim
// thrash. The Drop F failure mode (inventory bloating with machines
// bound to dead fingerprints) is still prevented — see the second case:
// a machine no live Need claims is reclaimed regardless of its stamp.
func TestPhase3_KeepsByMatchProfileNotFingerprint(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	pfA := gpuProfile(100)
	// pfB is looser than pfA (no instance-type pin) — pfA's machine
	// satisfies pfB's requirements.
	pfB := needs.NewProfile(nil, nil, 100, needs.PenaltyBucket8192, needs.PenaltyBucketPinned)

	// Machine still stamped with the now-stale fp_A.
	m := configuredVictim("victim-stale", "cluster-a", 100, 0, 0)
	m.AssignedNeedFingerprint = pfA.Fingerprint()
	_ = inv.Insert(m)

	// The cluster's live Need is pfB and its demand needs this machine.
	// Pre-ADR-0027 keep-by-fingerprint would have reclaimed it (stale
	// stamp); the MatchProfile mirror keeps it — it genuinely serves pfB.
	kept := decision.Phase3(inv.Snapshot(),
		[]needs.Need{gpuNeed("cluster-a", pfB, 1)}, decision.AlwaysReady)
	if got := len(kept.Actions); got != 0 {
		t.Fatalf("reclaim actions = %d, want 0 (machine MatchProfiles the live Need)", got)
	}

	// With no live Need claiming it, the same machine is reclaimed —
	// the Drop F inventory-bloat failure mode is still prevented, just
	// by "claimed by a live Need" rather than fingerprint equality.
	gone := decision.Phase3(inv.Snapshot(), nil, decision.AlwaysReady)
	if got := len(gone.Actions); got != 1 || gone.Actions[0].MachineID != "victim-stale" {
		t.Fatalf("with no Needs: reclaim = %+v, want [victim-stale]", gone.Actions)
	}
}

// Different profiles: a configured machine that doesn't match any
// remaining need is reclaimed even when other configured machines still
// satisfy their respective needs.
func TestPhase3_PerProfileMatching(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// 4 GPU machines configured for cluster-a.
	for i := 0; i < 4; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-a", 100, 0, 0))
	}
	// Roll-up says: only 1 GPU need remains (training mostly done).
	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{gpuNeed("cluster-a", gpuProfile(100), 1)}, decision.AlwaysReady)
	if got := len(r.Actions); got != 3 {
		t.Errorf("reclaim actions = %d, want 3", got)
	}
}

// Conservation: total Configured count before Phase 3 = (machines kept)
// + (machines whose Reclaim action is emitted). Machines aren't
// double-counted.
func TestPhase3_Conservation(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 10; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-a", 100, 0, 0))
	}
	totalConfigured := 10
	r := decision.Phase3(inv.Snapshot(),
		[]needs.Need{gpuNeed("cluster-a", gpuProfile(100), 4)}, decision.AlwaysReady)
	reclaim := len(r.Actions)
	keep := totalConfigured - reclaim
	if keep != 4 {
		t.Errorf("keep = %d, want 4 (need.count); reclaim = %d", keep, reclaim)
	}
}

// TestPhase3_DenseMachine_OneCoversManyPodsOfDemand: ADR-0022 / M45.2.
// One Configured machine with density=8 (c6a.4xlarge shape, 4Gi per-
// replica Pods) covers up to 8 Pods of demand without being reclaimed.
// If demand is 8 the machine is fully utilised and kept; if demand is 0
// the machine is reclaimed. The pre-ADR-0022 budget math would have
// only "consumed" 1 Pod of budget per kept machine, so a Need.Count of
// 8 with one dense machine would have reclaimed 7 phantom machines.
func TestPhase3_DenseMachine_OneCoversManyPodsOfDemand(t *testing.T) {
	t.Parallel()

	unit := []needs.ResourceQty{
		{Name: "cpu", Quantity: "1"},
		{Name: "memory", Quantity: "4Gi"},
	}
	profile := needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"c6a.4xlarge"},
		}},
		nil,
		1000,
		needs.PenaltyBucket64,
		needs.PenaltyBucketPinned,
	)

	denseMachine := func(id machine.ID) machine.Machine {
		return machine.Machine{
			ID:    id,
			State: machine.StateConfigured,
			Host:  machine.HostRef{Provider: "fake", Ref: string(id)},
			Profile: machine.Profile{
				InstanceType: "c6a.4xlarge",
				Resources:    map[string]string{"cpu": "1", "memory": "4Gi"},
			},
			Cluster:                 "cluster-A",
			AssignedNeedFingerprint: profile.Fingerprint(),
			Allocatable:             map[string]string{"cpu": "16", "memory": "32Gi"},
		}
	}

	// Two dense machines (16 Pods total capacity) vs Need.Count=8.
	// Pre-ADR-0022: budget=8, kept-2-decremented-to-6, no reclaim because
	// both kept. Same result either way, but the test confirms the new
	// budget tracks density.
	inv := inventory.New()
	if err := inv.Insert(denseMachine("dense-1")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := inv.Insert(denseMachine("dense-2")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	snap := inv.Snapshot()

	// Demand = 8 Pods. First dense machine absorbs 8 (density). Second
	// has no remaining budget → Reclaim.
	need := needs.Need{
		ClusterID:          "cluster-A",
		Profile:            profile,
		AggregateResources: needs.ScaleResources(unit, 8),
		MinUnit:            unit,
	}
	res := decision.Phase3(snap, []needs.Need{need}, decision.AlwaysReady)

	if len(res.Actions) != 1 {
		t.Fatalf("expected 1 Reclaim (second dense machine has no Pod budget left after first absorbs 8), got %d actions: %#v", len(res.Actions), res.Actions)
	}
	if res.Actions[0].Kind != decision.ActionKindReclaim {
		t.Errorf("action kind = %v, want Reclaim", res.Actions[0].Kind)
	}
}

// configuredGpuInZone returns a Configured a3-highgpu-8g machine bound
// to cluster, in the given zone. Reuses the Phase 1 Same-test shape so
// the two phases' Same-domain tests describe the same fleet.
func configuredGpuInZone(id machine.ID, cluster machine.ClusterID, zone string) machine.Machine {
	m := gpuMachineInZone(id, zone, 1.0)
	m.State = machine.StateConfigured
	m.Cluster = cluster
	return m
}

// ADR-0040: Phase 3's claim for a Same-Profile Need is confined to the
// single best Same-domain bucket, so off-domain scatter is reclaimed.
// Pre-fix, the vacuous cross-domain claim kept scattered machines that
// Phase 1 (strict) could never count toward the co-located Need — the
// Bootstrap↔Reclaim equilibrium the ADR documents.
func TestPhase3_Same_KeepsChosenDomainReclaimsScatter(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// zone-a: 3 machines (satisfies the Need); zone-b: 2 (scatter).
	for i := 0; i < 3; i++ {
		_ = inv.Insert(configuredGpuInZone("a-"+idN(i), "cluster-x", "zone-a"))
	}
	for i := 0; i < 2; i++ {
		_ = inv.Insert(configuredGpuInZone("b-"+idN(i), "cluster-x", "zone-b"))
	}
	snap := inv.Snapshot()

	r := decision.Phase3(snap, []needs.Need{gpuNeed(
		"cluster-x",
		gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		3,
	)}, decision.AlwaysReady)

	if got := len(r.Actions); got != 2 {
		t.Fatalf("reclaim actions = %d, want 2 (the zone-b scatter)", got)
	}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		if m.Profile.Zone != "zone-b" {
			t.Errorf("reclaimed %s in %s; the chosen zone-a domain must be kept whole", a.MachineID, m.Profile.Zone)
		}
	}
}

// No single domain covers the Need: Phase 3 keeps exactly the chosen
// bucket and reclaims the rest — the residual is Phase 1's shortfall,
// not a reason to keep cross-domain scatter. ADR-0040 §2.
func TestPhase3_Same_NoSatisfiableDomainKeepsOneBucketOnly(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 2; i++ {
		_ = inv.Insert(configuredGpuInZone("a-"+idN(i), "cluster-x", "zone-a"))
		_ = inv.Insert(configuredGpuInZone("b-"+idN(i), "cluster-x", "zone-b"))
	}
	snap := inv.Snapshot()

	r := decision.Phase3(snap, []needs.Need{gpuNeed(
		"cluster-x",
		gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		3,
	)}, decision.AlwaysReady)

	if got := len(r.Actions); got != 2 {
		t.Fatalf("reclaim actions = %d, want 2 (one whole domain kept, the other reclaimed)", got)
	}
	zones := map[string]int{}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		zones[m.Profile.Zone]++
	}
	if len(zones) != 1 {
		t.Errorf("reclaims span %d zones, want 1 (claims must not split across domains): %+v", len(zones), zones)
	}
}

// In-flight bootstraps anchor the domain choice: Configuring supply is
// claimed first, and the residual's Configured claim lands in the
// domain that best covers what's left — so Phase 3 keeps the domain
// Phase 1 is building toward and reclaims the rest. This is the
// per-cycle convergence step of ADR-0040 §2.
func TestPhase3_Same_ConfiguringAnchorsTheDomain(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	// zone-a: 2 Configuring + 1 Configured. zone-b: 3 Configured.
	for i := 0; i < 2; i++ {
		m := configuredGpuInZone("a-cfg-"+idN(i), "cluster-x", "zone-a")
		m.State = machine.StateConfiguring
		_ = inv.Insert(m)
	}
	_ = inv.Insert(configuredGpuInZone("a-conf", "cluster-x", "zone-a"))
	for i := 0; i < 3; i++ {
		_ = inv.Insert(configuredGpuInZone("b-"+idN(i), "cluster-x", "zone-b"))
	}
	snap := inv.Snapshot()

	r := decision.Phase3(snap, []needs.Need{gpuNeed(
		"cluster-x",
		gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		3,
	)}, decision.AlwaysReady)

	// The 2 zone-a Configuring + 1 zone-a Configured cover the Need;
	// zone-b's 3 Configured are off-domain and reclaimed.
	if got := len(r.Actions); got != 3 {
		t.Fatalf("reclaim actions = %d, want 3 (all of zone-b)", got)
	}
	for _, a := range r.Actions {
		m, _ := snap.Get(a.MachineID)
		if m.Profile.Zone != "zone-b" {
			t.Errorf("reclaimed %s in %s; zone-a is anchored by its in-flight bootstraps", a.MachineID, m.Profile.Zone)
		}
	}
}

// ADR-0036: when the cluster hasn't yet reported (firstRollupReceived
// gate fails) Phase 3 must skip reclaim, even if the NeedsTable slice
// for that cluster is empty. Brief #36 traced the install-time
// fleet-drain bug here: seeded Configured supply with no rollup yet
// shouldn't be reclaimed.
func TestPhase3_GateSkipsReclaimWhenClusterNotReady(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 100; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-not-yet-reporting", 100, 0, 0))
	}
	// No Needs and the gate says the cluster hasn't reported.
	clusterReady := func(machine.ClusterID) bool { return false }
	r := decision.Phase3(inv.Snapshot(), nil, clusterReady)
	if got := len(r.Actions); got != 0 {
		t.Fatalf("Phase 3 reclaimed %d machines for a cluster that hasn't reported; ADR-0036 requires zero", got)
	}
}

// Once a cluster has reported (even with zero Needs in the rollup),
// Phase 3 should resume normal reclaim — empty rollup is now "I have
// no demand right now," not "I haven't told you yet."
func TestPhase3_GateAllowsReclaimAfterEmptyRollup(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 50; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-reported-empty", 100, 0, 0))
	}
	// Cluster has reported but its rollup is empty.
	clusterReady := func(c machine.ClusterID) bool {
		return c == "cluster-reported-empty"
	}
	r := decision.Phase3(inv.Snapshot(), nil, clusterReady)
	if got := len(r.Actions); got != 50 {
		t.Fatalf("Phase 3 reclaimed %d after empty rollup; want 50 (cluster has reported, supply is excess)", got)
	}
}

// Per-cluster isolation: only the cluster that has reported gets its
// excess reclaimed; another cluster pending its first rollup is
// untouched.
func TestPhase3_GateIsPerCluster(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 10; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-reported", 100, 0, 0))
	}
	for i := 10; i < 20; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-pending", 100, 0, 0))
	}
	clusterReady := func(c machine.ClusterID) bool {
		return c == "cluster-reported"
	}
	r := decision.Phase3(inv.Snapshot(), nil, clusterReady)
	if got := len(r.Actions); got != 10 {
		t.Fatalf("expected 10 reclaim actions (cluster-reported only); got %d", got)
	}
	for _, a := range r.Actions {
		if a.Cluster != "cluster-reported" {
			t.Errorf("Phase 3 reclaimed in %q; gate should have skipped non-reported clusters", a.Cluster)
		}
	}
}
