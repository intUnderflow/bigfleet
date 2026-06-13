package decision_test

import (
	"testing"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// runPhase3 runs the ADR-0045 shared-attribution path the shard uses:
// Phase 1 on the snapshot and demand produces the cycle's claimed-set
// (the single supply arithmetic), and Phase 3 diffs the Configured
// inventory against it. Phase 3 has no demand walk of its own, so
// every demand-shaped expectation below is really an expectation on
// the shared walk.
func runPhase3(t *testing.T, snap *inventory.Snapshot, demand []needs.Need, ready decision.ClusterReadyFn) decision.Phase3Result {
	t.Helper()
	p1 := decision.Phase1(snap, demand)
	// Zero release policy: the reclaim-shaped tests in this file stay
	// release-free; the M73 release walk has its own tests below.
	return decision.Phase3(snap, p1.Claimed, ready, decision.ReleasePolicy{}, time.Time{})
}

// runPhase3Release is runPhase3 with a live release policy and clock —
// the M73 §8 Idle→Speculative release path.
func runPhase3Release(t *testing.T, snap *inventory.Snapshot, demand []needs.Need, policy decision.ReleasePolicy, now time.Time) decision.Phase3Result {
	t.Helper()
	p1 := decision.Phase1(snap, demand)
	return decision.Phase3(snap, p1.Claimed, decision.AlwaysReady, policy, now)
}

// All needs deleted: every configured machine reclaimed.
func TestPhase3_AllExcessWhenNoNeeds(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 4; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-a", 100, 0, 0))
	}
	r := runPhase3(t, inv.Snapshot(), nil, decision.AlwaysReady)
	if got := len(r.Actions); got != 4 {
		t.Fatalf("reclaim actions = %d, want 4", got)
	}
	for _, a := range r.Actions {
		if a.Kind != decision.ActionKindReclaim {
			t.Errorf("expected Reclaim, got %s", a.Kind)
		}
		// M69: the grace rides the ReclaimInstruction to the operator;
		// a voluntary reclaim gets the full-graceful tier.
		if a.GracePeriod != decision.ReclaimGrace {
			t.Errorf("grace = %v, want %v", a.GracePeriod, decision.ReclaimGrace)
		}
		if a.PreemptorPriority != 0 {
			t.Errorf("preemptor priority = %d, want 0 (no preemptor on a voluntary reclaim)", a.PreemptorPriority)
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
	r := runPhase3(t, inv.Snapshot(),
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
	r := runPhase3(t, inv.Snapshot(),
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

	r := runPhase3(t, inv.Snapshot(),
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
	kept := runPhase3(t, inv.Snapshot(),
		[]needs.Need{gpuNeed("cluster-a", pfB, 1)}, decision.AlwaysReady)
	if got := len(kept.Actions); got != 0 {
		t.Fatalf("reclaim actions = %d, want 0 (machine MatchProfiles the live Need)", got)
	}

	// With no live Need claiming it, the same machine is reclaimed —
	// the Drop F inventory-bloat failure mode is still prevented, just
	// by "claimed by a live Need" rather than fingerprint equality.
	gone := runPhase3(t, inv.Snapshot(), nil, decision.AlwaysReady)
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
	r := runPhase3(t, inv.Snapshot(),
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
	r := runPhase3(t, inv.Snapshot(),
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
	res := runPhase3(t, snap, []needs.Need{need}, decision.AlwaysReady)

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

	r := runPhase3(t, snap, []needs.Need{gpuNeed(
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

	r := runPhase3(t, snap, []needs.Need{gpuNeed(
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

	r := runPhase3(t, snap, []needs.Need{gpuNeed(
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

// ADR-0041 rider (now living in the shared seed walk, ADR-0045): the
// acquirable fold consumes. Two same-fingerprint unfoldable gang
// Needs, each currently served by one Configured machine in its own
// zone, and one idle zone able to cover one whole gang: the first Need
// ranks the idle zone best (only satisfiable bucket) and virtually
// consumes its members, so the second Need's joint view no longer
// contains it and the second keeps a creditable zone. Pre-rider, BOTH
// Needs ranked the fresh idle zone best (nil claimed-view), claimed
// nothing creditable, and both serving machines were mass-reclaimed —
// 20 of 24 healthy bound gangs in the simulator's trace.
func TestPhase3_Same_AcquirableFoldConsumes(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(configuredGpuInZone("a-0", "cluster-x", "zone-a"))
	_ = inv.Insert(configuredGpuInZone("b-0", "cluster-x", "zone-b"))
	for i := 0; i < 2; i++ {
		_ = inv.Insert(gpuMachineInZone("c-"+idN(i), "zone-c", 1.0))
	}
	snap := inv.Snapshot()

	pf := gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone")
	// Two gangs of the same class, each 2 machines' worth — neither
	// serving zone (1 machine) satisfies one, the idle zone-c (2
	// machines) satisfies exactly one.
	demand := []needs.Need{gpuNeed("cluster-x", pf, 2), gpuNeed("cluster-x", pf, 2)}

	r := runPhase3(t, snap, demand, decision.AlwaysReady)
	if got := len(r.Actions); got != 1 {
		t.Fatalf("reclaim actions = %d, want 1 (no mass off-domain reclaim of both serving zones): %+v", got, r.Actions)
	}
	// The first Need consumed zone-c; the second kept the
	// lexicographically-first creditable zone (zone-a). Only zone-b's
	// machine is excess.
	if r.Actions[0].MachineID != "b-0" {
		t.Errorf("reclaimed %s, want b-0 (the one serving machine no Need kept)", r.Actions[0].MachineID)
	}
}

// ADR-0041 rider 3 at Phase 3 level: a satisfiable serving (creditable)
// domain beats a smaller satisfiable acquirable-only domain, so the
// Need stays put — and the serving domain's genuine excess is still
// reclaimed individually by the claim loop's stop-when-covered.
// Pre-rider the smallest-satisfiable rule chose the fresh 2-Idle
// zone-b and relocated the gang, reclaiming all three healthy zone-a
// machines.
func TestPhase3_Same_PrefersServingCreditableDomain(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 3; i++ {
		_ = inv.Insert(configuredGpuInZone("a-"+idN(i), "cluster-x", "zone-a"))
	}
	for i := 0; i < 2; i++ {
		_ = inv.Insert(gpuMachineInZone("b-"+idN(i), "zone-b", 1.0))
	}
	snap := inv.Snapshot()

	r := runPhase3(t, snap, []needs.Need{gpuNeed(
		"cluster-x",
		gpuProfileWithSame(1_000_000, "topology.kubernetes.io/zone"),
		2,
	)}, decision.AlwaysReady)

	if got := len(r.Actions); got != 1 {
		t.Fatalf("reclaim actions = %d, want 1 (the serving zone's excess third machine only): %+v", got, r.Actions)
	}
	m, _ := snap.Get(r.Actions[0].MachineID)
	if m.Profile.Zone != "zone-a" {
		t.Errorf("reclaimed %s in %s, want the excess inside the kept zone-a", r.Actions[0].MachineID, m.Profile.Zone)
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
	r := runPhase3(t, inv.Snapshot(), nil, clusterReady)
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
	r := runPhase3(t, inv.Snapshot(), nil, clusterReady)
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
	r := runPhase3(t, inv.Snapshot(), nil, clusterReady)
	if got := len(r.Actions); got != 10 {
		t.Fatalf("expected 10 reclaim actions (cluster-reported only); got %d", got)
	}
	for _, a := range r.Actions {
		if a.Cluster != "cluster-reported" {
			t.Errorf("Phase 3 reclaimed in %q; gate should have skipped non-reported clusters", a.Cluster)
		}
	}
}

// --- M73 / ADR-0049: the §8 Idle → Speculative release walk ---

// idleOfType returns an Idle a3-highgpu-8g machine of the given
// capacity type for the M73 release tests. Inserting it stamps its
// idle-since at ~now, so tests steer expiry through the `now` they
// pass to Phase 3 rather than by faking clocks.
func idleOfType(id machine.ID, ct machine.CapacityType, price float64) machine.Machine {
	m := gpuMachineInZone(id, "zone-a", price)
	m.Profile.CapacityType = ct
	return m
}

// deletedIDs extracts the Delete-action machine IDs from a result.
func deletedIDs(r decision.Phase3Result) map[machine.ID]bool {
	out := map[machine.ID]bool{}
	for _, a := range r.Actions {
		if a.Kind == decision.ActionKindDelete {
			out[a.MachineID] = true
		}
	}
	return out
}

// Paper §8 verbatim: bare metal holds forever, on-demand minutes,
// spot ~1m. At 2 minutes past idle entry only spot has expired; at 11
// minutes on-demand joins it; the fixed tiers (and an unknown capacity
// type) are never released at any age.
func TestPhase3_Release_HoldExpiresPerCapacityType(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(idleOfType("idle-bm", machine.CapacityTypeBareMetal, 0))
	_ = inv.Insert(idleOfType("idle-rsv", machine.CapacityTypeReserved, 0))
	_ = inv.Insert(idleOfType("idle-od", machine.CapacityTypeOnDemand, 1.0))
	_ = inv.Insert(idleOfType("idle-spot", machine.CapacityTypeSpot, 0.3))
	_ = inv.Insert(idleOfType("idle-unspec", machine.CapacityTypeUnspecified, 1.0))
	snap := inv.Snapshot()

	at2m := runPhase3Release(t, snap, nil, decision.DefaultReleasePolicy(), time.Now().Add(2*time.Minute))
	if got := deletedIDs(at2m); len(got) != 1 || !got["idle-spot"] {
		t.Fatalf("at +2m: deleted %v, want exactly {idle-spot} (spot ~1m expired, on-demand 10m not)", got)
	}
	for _, a := range at2m.Actions {
		if a.Kind != decision.ActionKindDelete {
			continue
		}
		if a.Cluster != "" || a.GracePeriod != 0 || a.Reason != "phase3.release" {
			t.Errorf("delete action shape = %+v; want unbound, no grace, reason phase3.release", a)
		}
	}

	at11m := runPhase3Release(t, snap, nil, decision.DefaultReleasePolicy(), time.Now().Add(11*time.Minute))
	got := deletedIDs(at11m)
	if len(got) != 2 || !got["idle-od"] || !got["idle-spot"] {
		t.Fatalf("at +11m: deleted %v, want exactly {idle-od, idle-spot}", got)
	}

	// Fixed capacity never releases — §8 "bare metal: forever";
	// reserved is paid-for regardless (paper §4); an unspecified
	// capacity type is held because its cost class is unknown.
	atForever := runPhase3Release(t, snap, nil, decision.DefaultReleasePolicy(), time.Now().Add(1000*time.Hour))
	for _, fixed := range []machine.ID{"idle-bm", "idle-rsv", "idle-unspec"} {
		if deletedIDs(atForever)[fixed] {
			t.Errorf("%s released; fixed/unknown capacity must hold forever", fixed)
		}
	}
}

// An Idle machine the cycle's claimed-set counts toward a Need is the
// machine Phase 1 is about to bootstrap; it must never be deleted out
// from under that commitment, however expired its hold.
func TestPhase3_Release_ClaimedIdleNeverReleased(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(idleOfType("idle-claimed", machine.CapacityTypeOnDemand, 1.0))
	snap := inv.Snapshot()

	r := runPhase3Release(t, snap,
		[]needs.Need{gpuNeed("cluster-a", gpuProfile(100), 1)},
		decision.DefaultReleasePolicy(), time.Now().Add(1000*time.Hour))
	if got := deletedIDs(r); len(got) != 0 {
		t.Fatalf("deleted %v; the claimed-set must shield Phase 1's bootstrap target", got)
	}
}

// The zero-value policy releases nothing — the repo's "zero value =
// historical behaviour" convention, which keeps every pre-M73 caller
// and canary byte-identical.
func TestPhase3_Release_ZeroPolicyReleasesNothing(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	_ = inv.Insert(idleOfType("idle-od", machine.CapacityTypeOnDemand, 1.0))
	_ = inv.Insert(idleOfType("idle-spot", machine.CapacityTypeSpot, 0.3))
	snap := inv.Snapshot()

	r := runPhase3Release(t, snap, nil, decision.ReleasePolicy{}, time.Now().Add(1000*time.Hour))
	if got := len(r.Actions); got != 0 {
		t.Fatalf("actions = %d, want 0 under the zero policy", got)
	}
}

// Reclaim and release coexist in one pass: shrinkage excess drains to
// Idle (Reclaim) while previously-idled elastic machines past their
// hold leave entirely (Delete) — §8's two halves in one Phase 3.
func TestPhase3_Release_CoexistsWithReclaim(t *testing.T) {
	t.Parallel()
	inv := inventory.New()
	for i := 0; i < 2; i++ {
		_ = inv.Insert(configuredVictim(idN(i), "cluster-a", 100, 0, 0))
	}
	_ = inv.Insert(idleOfType("idle-spot", machine.CapacityTypeSpot, 0.3))
	snap := inv.Snapshot()

	r := runPhase3Release(t, snap, nil, decision.DefaultReleasePolicy(), time.Now().Add(2*time.Minute))
	reclaims, deletes := 0, 0
	for _, a := range r.Actions {
		switch a.Kind {
		case decision.ActionKindReclaim:
			reclaims++
		case decision.ActionKindDelete:
			deletes++
		}
	}
	if reclaims != 2 || deletes != 1 {
		t.Fatalf("reclaims=%d deletes=%d, want 2 and 1: %+v", reclaims, deletes, r.Actions)
	}
}
