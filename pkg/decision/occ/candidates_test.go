package occ_test

import (
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// ---- fixtures ---------------------------------------------------------

func smallProfile(pri int32, instanceTypes ...string) needs.Profile {
	if len(instanceTypes) == 0 {
		instanceTypes = []string{"m5.large"}
	}
	return needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   instanceTypes,
		}},
		nil, pri,
		needs.PenaltyBucket1024,
		needs.PenaltyBucket1,
	)
}

func sameProfile(pri int32, sameKey string) needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{
			{
				Key:      "node.kubernetes.io/instance-type",
				Operator: needs.OperatorIn,
				Values:   []string{"m5.large"},
			},
			{Key: sameKey, Operator: needs.OperatorSame},
		},
		nil, pri,
		needs.PenaltyBucket1024,
		needs.PenaltyBucket1,
	)
}

func spreadProfile(pri int32, topoKey string, maxSkew int32) needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{{
			Key:      "node.kubernetes.io/instance-type",
			Operator: needs.OperatorIn,
			Values:   []string{"m5.large"},
		}},
		[]needs.TopologySpread{{
			TopologyKey:       topoKey,
			MaxSkew:           maxSkew,
			WhenUnsatisfiable: needs.WhenUnsatisfiableDoNotSchedule,
		}},
		pri,
		needs.PenaltyBucket1024,
		needs.PenaltyBucket1,
	)
}

// idleMachine constructs an Idle machine with a fixed (4 cpu, 8Gi) shape.
func idleMachine(id machine.ID, price float64, labels map[string]string) machine.Machine {
	mp := machine.Profile{
		InstanceType: "m5.large",
		Zone:         "us-east-1a",
		CapacityType: machine.CapacityTypeOnDemand,
		Resources:    map[string]string{"cpu": "4", "memory": "16Gi"},
		Labels:       labels,
	}
	return machine.Machine{
		ID:           id,
		State:        machine.StateIdle,
		Profile:      mp,
		PricePerHour: price,
		Host:         machine.HostRef{Provider: "fake", Ref: string(id)},
	}
}

func snapWith(machines ...machine.Machine) *inventory.Snapshot {
	inv := inventory.New()
	for _, m := range machines {
		_ = inv.Insert(m)
	}
	return inv.Snapshot()
}

func freshStateWith(machines ...machine.Machine) *occ.SharedState {
	return occ.NewSharedState(snapWith(machines...))
}

// ---- PoolCache --------------------------------------------------------

func TestPoolCache_GetCachesSameProfile(t *testing.T) {
	t.Parallel()
	snap := snapWith(idleMachine("m1", 1.0, nil), idleMachine("m2", 2.0, nil))
	cache := occ.NewPoolCache(snap)
	profile := smallProfile(100)

	p1 := cache.Get(machine.StateIdle, profile)
	p2 := cache.Get(machine.StateIdle, profile)
	if p1 != p2 {
		t.Fatalf("Get for same profile returned distinct pools (%p vs %p)", p1, p2)
	}
}

func TestPoolCache_ConcurrentGetIsSafe(t *testing.T) {
	t.Parallel()
	const N = 50
	var machines []machine.Machine
	for i := 0; i < N; i++ {
		machines = append(machines, idleMachine(machine.ID("m-"+strconv.Itoa(i)), float64(i), nil))
	}
	snap := snapWith(machines...)
	cache := occ.NewPoolCache(snap)
	profile := smallProfile(100)

	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	seen := make([]*occ.Pool, workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			seen[w] = cache.Get(machine.StateIdle, profile)
		}()
	}
	wg.Wait()
	for w := 1; w < workers; w++ {
		if seen[w] != seen[0] {
			t.Errorf("worker %d got distinct Pool %p vs %p", w, seen[w], seen[0])
		}
	}
}

// ---- FindBasic --------------------------------------------------------

func TestFindBasic_PicksCheapestUnclaimed(t *testing.T) {
	t.Parallel()
	state := freshStateWith(
		idleMachine("m-cheap", 1.0, nil),
		idleMachine("m-mid", 2.0, nil),
		idleMachine("m-exp", 3.0, nil),
	)
	cache := occ.NewPoolCache(state.Snapshot())
	profile := smallProfile(100)
	pool := cache.Get(machine.StateIdle, profile)

	deficit := []needs.ResourceQty{{Name: "cpu", Quantity: "4"}}
	minUnit := []needs.ResourceQty{{Name: "cpu", Quantity: "1"}}

	c := pool.FindBasic(state, machine.StateIdle, occ.Precedence{}, deficit, minUnit)
	if len(c.Machines) != 1 || c.Machines[0] != "m-cheap" {
		t.Fatalf("FindBasic = %v, want [m-cheap]", c.Machines)
	}
	if c.Bucket.State != machine.StateIdle {
		t.Errorf("Bucket.State = %v, want Idle", c.Bucket.State)
	}
	if c.Bucket.ProfileFP == "" {
		t.Error("Bucket.ProfileFP empty")
	}
}

func TestFindBasic_SkipsClaimedMachines(t *testing.T) {
	t.Parallel()
	state := freshStateWith(
		idleMachine("m-cheap", 1.0, nil),
		idleMachine("m-mid", 2.0, nil),
	)
	cache := occ.NewPoolCache(state.Snapshot())
	profile := smallProfile(100)
	pool := cache.Get(machine.StateIdle, profile)

	state.SeedClaim("m-cheap", &needs.Need{}, occ.Precedence{Priority: 50}, 10)

	deficit := []needs.ResourceQty{{Name: "cpu", Quantity: "4"}}
	minUnit := []needs.ResourceQty{{Name: "cpu", Quantity: "1"}}

	c := pool.FindBasic(state, machine.StateIdle, occ.Precedence{}, deficit, minUnit)
	if len(c.Machines) != 1 || c.Machines[0] != "m-mid" {
		t.Fatalf("FindBasic with m-cheap claimed = %v, want [m-mid]", c.Machines)
	}
}

func TestFindBasic_AccumulatesUntilCovered(t *testing.T) {
	t.Parallel()
	// 3 machines of cpu=4 each; deficit cpu=10 needs all three (12 ≥ 10).
	state := freshStateWith(
		idleMachine("m1", 1.0, nil),
		idleMachine("m2", 1.0, nil),
		idleMachine("m3", 1.0, nil),
	)
	cache := occ.NewPoolCache(state.Snapshot())
	profile := smallProfile(100)
	pool := cache.Get(machine.StateIdle, profile)

	deficit := []needs.ResourceQty{{Name: "cpu", Quantity: "10"}}
	minUnit := []needs.ResourceQty{{Name: "cpu", Quantity: "1"}}

	c := pool.FindBasic(state, machine.StateIdle, occ.Precedence{}, deficit, minUnit)
	if len(c.Machines) != 3 {
		t.Fatalf("FindBasic = %v, want all 3 machines", c.Machines)
	}
}

func TestFindBasic_ReturnsEmptyOnZeroDeficit(t *testing.T) {
	t.Parallel()
	state := freshStateWith(idleMachine("m1", 1.0, nil))
	cache := occ.NewPoolCache(state.Snapshot())
	pool := cache.Get(machine.StateIdle, smallProfile(100))

	c := pool.FindBasic(state, machine.StateIdle, occ.Precedence{}, nil, nil)
	if len(c.Machines) != 0 {
		t.Fatalf("FindBasic on zero deficit = %v, want empty", c.Machines)
	}
}

// ---- FindSame ---------------------------------------------------------

// Empty domain: the pre-pass found no bucket anywhere, so FindSame
// keeps its original best-bucket scoring as the fallback (ADR-0040
// Addendum).
func TestFindSame_PrefersAtomicSatisfiableBucket(t *testing.T) {
	t.Parallel()
	// Two racks. Rack A has 1 machine (capacity 4 cpu, can't atomically cover 8 cpu).
	// Rack B has 2 machines (capacity 8 cpu, atomically covers).
	// Atomic must win over partial.
	mA := idleMachine("a-1", 1.0, map[string]string{"topology.kubernetes.io/rack": "rack-a"})
	mB1 := idleMachine("b-1", 2.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"})
	mB2 := idleMachine("b-2", 2.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"})
	state := freshStateWith(mA, mB1, mB2)
	cache := occ.NewPoolCache(state.Snapshot())
	profile := sameProfile(100, "topology.kubernetes.io/rack")
	pool := cache.Get(machine.StateIdle, profile)

	deficit := []needs.ResourceQty{{Name: "cpu", Quantity: "8"}}
	minUnit := []needs.ResourceQty{{Name: "cpu", Quantity: "1"}}

	c := pool.FindSame(state, machine.StateIdle, occ.Precedence{}, deficit, minUnit, "topology.kubernetes.io/rack", "")
	if c.Bucket.SameValue != "rack-b" {
		t.Fatalf("FindSame chose %q, want rack-b (atomic)", c.Bucket.SameValue)
	}
	if len(c.Machines) != 2 {
		t.Fatalf("FindSame Machines = %v, want both rack-b machines", c.Machines)
	}
}

func TestFindSame_FallsBackToMostAvailableWhenNoAtomic(t *testing.T) {
	t.Parallel()
	// Neither rack can atomically cover deficit cpu=20; rack with more
	// machines wins (most-available tie-break).
	mA := idleMachine("a-1", 1.0, map[string]string{"topology.kubernetes.io/rack": "rack-a"})
	mB1 := idleMachine("b-1", 2.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"})
	mB2 := idleMachine("b-2", 2.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"})
	mB3 := idleMachine("b-3", 2.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"})
	state := freshStateWith(mA, mB1, mB2, mB3)
	cache := occ.NewPoolCache(state.Snapshot())
	profile := sameProfile(100, "topology.kubernetes.io/rack")
	pool := cache.Get(machine.StateIdle, profile)

	deficit := []needs.ResourceQty{{Name: "cpu", Quantity: "20"}}
	minUnit := []needs.ResourceQty{{Name: "cpu", Quantity: "1"}}

	c := pool.FindSame(state, machine.StateIdle, occ.Precedence{}, deficit, minUnit, "topology.kubernetes.io/rack", "")
	if c.Bucket.SameValue != "rack-b" {
		t.Fatalf("FindSame chose %q, want rack-b (most available for partial fill)", c.Bucket.SameValue)
	}
}

func TestFindSame_BucketKeyCarriesSameKeyAndValue(t *testing.T) {
	t.Parallel()
	m1 := idleMachine("m1", 1.0, map[string]string{"topology.kubernetes.io/rack": "rack-a"})
	state := freshStateWith(m1)
	cache := occ.NewPoolCache(state.Snapshot())
	profile := sameProfile(100, "topology.kubernetes.io/rack")
	pool := cache.Get(machine.StateIdle, profile)

	c := pool.FindSame(state, machine.StateIdle, occ.Precedence{},
		[]needs.ResourceQty{{Name: "cpu", Quantity: "1"}},
		[]needs.ResourceQty{{Name: "cpu", Quantity: "1"}},
		"topology.kubernetes.io/rack",
		"",
	)
	if c.Bucket.SameKey != "topology.kubernetes.io/rack" {
		t.Errorf("Bucket.SameKey = %q, want topology.kubernetes.io/rack", c.Bucket.SameKey)
	}
	if c.Bucket.SameValue != "rack-a" {
		t.Errorf("Bucket.SameValue = %q, want rack-a", c.Bucket.SameValue)
	}
}

// ADR-0040 Addendum: a non-empty domain skips bucket scoring entirely
// — acquisition is confined to the domain the pre-pass chose jointly,
// even when another bucket would score better.
func TestFindSame_DomainConfinesAcquisition(t *testing.T) {
	t.Parallel()
	// rack-b atomically covers the deficit and would win the scoring;
	// the anchored domain rack-a must be honoured anyway.
	mA := idleMachine("a-1", 1.0, map[string]string{"topology.kubernetes.io/rack": "rack-a"})
	mB1 := idleMachine("b-1", 2.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"})
	mB2 := idleMachine("b-2", 2.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"})
	state := freshStateWith(mA, mB1, mB2)
	cache := occ.NewPoolCache(state.Snapshot())
	profile := sameProfile(100, "topology.kubernetes.io/rack")
	pool := cache.Get(machine.StateIdle, profile)

	deficit := []needs.ResourceQty{{Name: "cpu", Quantity: "8"}}
	minUnit := []needs.ResourceQty{{Name: "cpu", Quantity: "1"}}

	c := pool.FindSame(state, machine.StateIdle, occ.Precedence{}, deficit, minUnit, "topology.kubernetes.io/rack", "rack-a")
	if c.Bucket.SameValue != "rack-a" {
		t.Fatalf("FindSame chose %q, want the anchored rack-a", c.Bucket.SameValue)
	}
	if len(c.Machines) != 1 || c.Machines[0] != "a-1" {
		t.Fatalf("FindSame Machines = %v, want [a-1] only (confined to rack-a)", c.Machines)
	}
}

// A domain with no eligible machines yields no candidates — never a
// silent re-pick of another domain (the off-domain bootstrap was the
// oscillation source the Addendum closes).
func TestFindSame_DomainWithNoCandidatesReturnsEmpty(t *testing.T) {
	t.Parallel()
	mB := idleMachine("b-1", 2.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"})
	state := freshStateWith(mB)
	cache := occ.NewPoolCache(state.Snapshot())
	profile := sameProfile(100, "topology.kubernetes.io/rack")
	pool := cache.Get(machine.StateIdle, profile)

	c := pool.FindSame(state, machine.StateIdle, occ.Precedence{},
		[]needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
		[]needs.ResourceQty{{Name: "cpu", Quantity: "1"}},
		"topology.kubernetes.io/rack",
		"rack-a",
	)
	if len(c.Machines) != 0 {
		t.Fatalf("FindSame = %v, want empty (rack-a has no machines; rack-b must not be re-picked)", c.Machines)
	}
}

// ---- FindSpread -------------------------------------------------------

func TestFindSpread_RespectsMaxSkew(t *testing.T) {
	t.Parallel()
	// Three racks (using a custom topology key so the Labels path is
	// exercised — the well-known Zone projection on machine.Profile is
	// hardcoded by idleMachine and would collapse all into one bucket).
	// Two machines per rack. With maxSkew=1 and deficit cpu=12 (1
	// machine = 4 cpu so 3 machines cover), we should pick one per
	// rack.
	rackKey := "topology.kubernetes.io/rack"
	mka := idleMachine("a-1", 1.0, map[string]string{rackKey: "rack-a"})
	mka2 := idleMachine("a-2", 1.0, map[string]string{rackKey: "rack-a"})
	mb := idleMachine("b-1", 2.0, map[string]string{rackKey: "rack-b"})
	mb2 := idleMachine("b-2", 2.0, map[string]string{rackKey: "rack-b"})
	mc := idleMachine("c-1", 3.0, map[string]string{rackKey: "rack-c"})
	mc2 := idleMachine("c-2", 3.0, map[string]string{rackKey: "rack-c"})
	state := freshStateWith(mka, mka2, mb, mb2, mc, mc2)
	cache := occ.NewPoolCache(state.Snapshot())
	profile := spreadProfile(100, rackKey, 1)
	pool := cache.Get(machine.StateIdle, profile)

	deficit := []needs.ResourceQty{{Name: "cpu", Quantity: "12"}}
	minUnit := []needs.ResourceQty{{Name: "cpu", Quantity: "1"}}

	c := pool.FindSpread(state, machine.StateIdle, occ.Precedence{}, deficit, minUnit, rackKey, 1)
	if len(c.Machines) != 3 {
		t.Fatalf("FindSpread = %v, want 3 machines", c.Machines)
	}
	racks := map[string]int{}
	for _, mid := range c.Machines {
		racks[string(mid)[:1]]++
	}
	for r, count := range racks {
		if count != 1 {
			t.Errorf("rack %s picked %d times under skew=1; want 1", r, count)
		}
	}
}

// ---- SeedConfiguredSupply --------------------------------------------

func TestSeedConfiguredSupply_CreditsHighPriFirst(t *testing.T) {
	t.Parallel()
	// One Configured machine in cluster c1; two Needs of the same
	// profile but different priorities. The high-pri Need must get the
	// claim; the low-pri Need carries the full deficit.
	configured := machine.Machine{
		ID:    "m-conf",
		State: machine.StateConfigured,
		Profile: machine.Profile{
			InstanceType: "m5.large",
			Resources:    map[string]string{"cpu": "4"},
		},
		Cluster: "c1",
		Host:    machine.HostRef{Provider: "fake", Ref: "m-conf"},
	}
	state := freshStateWith(configured)
	profile := smallProfile(100)
	highPriProfile := smallProfile(500)

	highPri := needs.Need{
		ClusterID:          "c1",
		Profile:            highPriProfile,
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "1"}},
	}
	lowPri := needs.Need{
		ClusterID:          "c1",
		Profile:            profile,
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "1"}},
	}

	results := occ.SeedConfiguredSupply(state, []*needs.Need{&lowPri, &highPri}, 10)
	if !state.IsClaimed("m-conf") {
		t.Fatal("Configured machine not claimed by pre-pass")
	}

	// Find each Need in results and check its deficit.
	for _, r := range results {
		switch r.Need.Profile.Priority() {
		case 500: // high-pri — should be covered
			if !needs.IsZero(r.Deficit) {
				t.Errorf("high-pri deficit = %v, want zero (Configured supply absorbed it)", r.Deficit)
			}
		case 100: // low-pri — should still carry full deficit
			if needs.IsZero(r.Deficit) {
				t.Error("low-pri deficit zero; pre-pass shouldn't have credited because m-conf went to high-pri")
			}
		}
	}
}

func TestSeedConfiguredSupply_OnlyCreditsMatchingMachines(t *testing.T) {
	t.Parallel()
	// Machine pinned to t3.large; Need pins m5.large. No credit.
	configured := machine.Machine{
		ID:    "m-conf",
		State: machine.StateConfigured,
		Profile: machine.Profile{
			InstanceType: "t3.large",
			Resources:    map[string]string{"cpu": "4"},
		},
		Cluster: "c1",
		Host:    machine.HostRef{Provider: "fake", Ref: "m-conf"},
	}
	state := freshStateWith(configured)
	profile := smallProfile(100) // pins m5.large

	n := needs.Need{
		ClusterID:          "c1",
		Profile:            profile,
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "1"}},
	}
	results := occ.SeedConfiguredSupply(state, []*needs.Need{&n}, 10)
	if state.IsClaimed("m-conf") {
		t.Error("non-matching Configured machine was claimed; MatchProfile filter broken")
	}
	if needs.IsZero(results[0].Deficit) {
		t.Error("deficit zero despite no matching supply credited")
	}
}

// creatingMachine constructs an unbound Speculative→Creating machine
// (no Host, no Cluster per machine.Invariant) carrying a fingerprint
// attribution, for the ADR-0052 (#66/#74) own-Creating credit tests.
// cpu=4, matches the smallProfile m5.large pin.
func creatingMachine(id machine.ID, fingerprint string) machine.Machine {
	return machine.Machine{
		ID:    id,
		State: machine.StateCreating,
		Profile: machine.Profile{
			InstanceType: "m5.large",
			Resources:    map[string]string{"cpu": "4"},
		},
		AssignedNeedFingerprint: fingerprint,
	}
}

// ADR-0052: the shard counts its OWN in-flight Creating machine — one it
// provisioned for this Need, carrying the Need's fingerprint — against the
// deficit, so it does not re-Provision the same runway every cycle. The
// non-Same arm credits by fingerprint; Creating is unbound, so it is
// credited shard-wide, not from the Need's cluster bucket.
func TestSeedConfiguredSupply_CreditsOwnCreating(t *testing.T) {
	t.Parallel()
	profile := smallProfile(100)
	state := freshStateWith(creatingMachine("m-creating", profile.Fingerprint()))

	n := needs.Need{
		ClusterID:          "c1",
		Profile:            profile,
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "1"}},
	}
	results := occ.SeedConfiguredSupply(state, []*needs.Need{&n}, 10)
	if !state.IsClaimed("m-creating") {
		t.Fatal("own-attributed Creating machine not credited by the pre-pass (ADR-0052)")
	}
	if !needs.IsZero(results[0].Deficit) {
		t.Errorf("deficit = %v, want zero (own in-flight Creating absorbs it)", results[0].Deficit)
	}
}

// A Creating machine that is not THIS Need's own commitment counts for
// nobody — the ADR-0052 own-only guard. Without it, crediting arbitrary
// in-flight supply would be the "in-flight discounting" of unattributed
// machines that ADR-0045 forbids by name. Both an unattributed Creating
// machine and one stamped for a different Need (here distinguished only by
// fingerprint — the machine's profile still matches, so the own-predicate
// is the sole rejecter) must be skipped.
func TestSeedConfiguredSupply_IgnoresNonOwnCreating(t *testing.T) {
	t.Parallel()
	profile := smallProfile(100)
	otherFP := smallProfile(100, "c5.large").Fingerprint()
	for _, tc := range []struct {
		name        string
		fingerprint string
	}{
		{"unattributed (no fingerprint)", ""},
		{"attributed to a different Need", otherFP},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := freshStateWith(creatingMachine("m-creating", tc.fingerprint))
			n := needs.Need{
				ClusterID:          "c1",
				Profile:            profile,
				AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
				MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "1"}},
			}
			results := occ.SeedConfiguredSupply(state, []*needs.Need{&n}, 10)
			if state.IsClaimed("m-creating") {
				t.Error("non-own Creating machine credited; ADR-0052 own-only guard broken")
			}
			if needs.IsZero(results[0].Deficit) {
				t.Error("deficit zero despite no own-attributed in-flight supply")
			}
		})
	}
}

// clusterSameMachine constructs a cluster-bound machine (Configured or
// Configuring) carrying a rack label, for the ADR-0040 Same-domain
// seed tests. cpu=4 per machine.
func clusterSameMachine(id machine.ID, st machine.State, cluster machine.ClusterID, rack string) machine.Machine {
	return machine.Machine{
		ID:    id,
		State: st,
		Profile: machine.Profile{
			InstanceType: "m5.large",
			Resources:    map[string]string{"cpu": "4"},
			Labels:       map[string]string{"topology.kubernetes.io/rack": rack},
		},
		Cluster: cluster,
		Host:    machine.HostRef{Provider: "fake", Ref: string(id)},
	}
}

func sameNeed(cluster machine.ClusterID, pf needs.Profile, cpu string) needs.Need {
	return needs.Need{
		ClusterID:          cluster,
		Profile:            pf,
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: cpu}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
	}
}

// ADR-0040: a Same-Profile's pre-pass credit is confined to the single
// best Same-domain bucket. Pre-fix the walk credited across domains,
// so Phase 1 believed scattered supply satisfied a co-located Need
// that FindSame (strict) could never finish — the Bootstrap↔Reclaim
// equilibrium the ADR documents.
func TestSeedConfiguredSupply_SameCreditsSingleDomain(t *testing.T) {
	t.Parallel()
	// rack-a: 2 machines (cpu 8); rack-b: 3 machines (cpu 12). Demand
	// cpu=12 — only rack-b is satisfiable.
	state := freshStateWith(
		clusterSameMachine("a-1", machine.StateConfigured, "c1", "rack-a"),
		clusterSameMachine("a-2", machine.StateConfigured, "c1", "rack-a"),
		clusterSameMachine("b-1", machine.StateConfigured, "c1", "rack-b"),
		clusterSameMachine("b-2", machine.StateConfigured, "c1", "rack-b"),
		clusterSameMachine("b-3", machine.StateConfigured, "c1", "rack-b"),
	)
	n := sameNeed("c1", sameProfile(100, "topology.kubernetes.io/rack"), "12")

	results := occ.SeedConfiguredSupply(state, []*needs.Need{&n}, 10)
	if !needs.IsZero(results[0].Deficit) {
		t.Errorf("deficit = %v, want zero (rack-b covers the demand)", results[0].Deficit)
	}
	for _, id := range []machine.ID{"b-1", "b-2", "b-3"} {
		if !state.IsClaimed(id) {
			t.Errorf("rack-b machine %s not claimed", id)
		}
	}
	for _, id := range []machine.ID{"a-1", "a-2"} {
		if state.IsClaimed(id) {
			t.Errorf("rack-a machine %s claimed; credit crossed the Same domain", id)
		}
	}
}

// When no single domain covers the demand, only the chosen (most-
// covering; here count+value tiebreak → rack-a) bucket is credited and
// the residual surfaces as the Need's deficit — a shortfall, not a
// cross-domain credit. ADR-0040 §2.
func TestSeedConfiguredSupply_SameUnsatisfiableCreditsOneDomainOnly(t *testing.T) {
	t.Parallel()
	state := freshStateWith(
		clusterSameMachine("a-1", machine.StateConfigured, "c1", "rack-a"),
		clusterSameMachine("a-2", machine.StateConfigured, "c1", "rack-a"),
		clusterSameMachine("b-1", machine.StateConfigured, "c1", "rack-b"),
		clusterSameMachine("b-2", machine.StateConfigured, "c1", "rack-b"),
	)
	n := sameNeed("c1", sameProfile(100, "topology.kubernetes.io/rack"), "12")

	results := occ.SeedConfiguredSupply(state, []*needs.Need{&n}, 10)
	claimed := 0
	for _, id := range []machine.ID{"a-1", "a-2", "b-1", "b-2"} {
		if state.IsClaimed(id) {
			claimed++
		}
	}
	if claimed != 2 {
		t.Errorf("claimed %d machines, want 2 (one domain's worth)", claimed)
	}
	if state.IsClaimed("a-1") != state.IsClaimed("a-2") || state.IsClaimed("b-1") != state.IsClaimed("b-2") {
		t.Error("claims split across domains; must stay within one bucket")
	}
	if needs.IsZero(results[0].Deficit) {
		t.Error("deficit zero despite no single domain covering the demand")
	}
}

// Within the chosen bucket the Configured machines are credited before
// Configuring ones — same preference order as the non-Same walk.
func TestSeedConfiguredSupply_SameClaimsConfiguredBeforeConfiguring(t *testing.T) {
	t.Parallel()
	state := freshStateWith(
		clusterSameMachine("m-conf", machine.StateConfigured, "c1", "rack-a"),
		clusterSameMachine("m-cfg-1", machine.StateConfiguring, "c1", "rack-a"),
		clusterSameMachine("m-cfg-2", machine.StateConfiguring, "c1", "rack-a"),
	)
	n := sameNeed("c1", sameProfile(100, "topology.kubernetes.io/rack"), "8")

	results := occ.SeedConfiguredSupply(state, []*needs.Need{&n}, 10)
	if !needs.IsZero(results[0].Deficit) {
		t.Errorf("deficit = %v, want zero", results[0].Deficit)
	}
	if !state.IsClaimed("m-conf") {
		t.Error("Configured machine skipped in favour of Configuring; order broken")
	}
	cfgClaims := 0
	for _, id := range []machine.ID{"m-cfg-1", "m-cfg-2"} {
		if state.IsClaimed(id) {
			cfgClaims++
		}
	}
	if cfgClaims != 1 {
		t.Errorf("claimed %d Configuring machines, want 1 (demand is 2 machines total)", cfgClaims)
	}
}

// ADR-0040 Addendum: the domain is ranked over JOINT potential. An
// acquirable-rich domain (shard-wide Idle, no cluster binding)
// outranks a creditable-only domain when it covers more — the credit
// walk then claims nothing (the chosen domain has no creditable
// members) and acquisition, confined to the recorded domain, fills
// the deficit. Pre-Addendum the creditable-only rank chose rack-a
// here while FindSame independently chose rack-b: a cross-domain
// group Phase 3 reclaimed half of next cycle.
func TestSeedConfiguredSupply_JointChoosesAcquirableRichDomain(t *testing.T) {
	t.Parallel()
	state := freshStateWith(
		clusterSameMachine("a-1", machine.StateConfigured, "c1", "rack-a"),
		idleMachine("b-1", 1.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"}),
		idleMachine("b-2", 1.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"}),
	)
	n := sameNeed("c1", sameProfile(100, "topology.kubernetes.io/rack"), "8")

	results := occ.SeedConfiguredSupply(state, []*needs.Need{&n}, 10)
	if got := state.SameDomainFor(&n); got != "rack-b" {
		t.Fatalf("recorded domain = %q, want rack-b (joint total 8 covers; creditable-only rack-a totals 4)", got)
	}
	if state.IsClaimed("a-1") {
		t.Error("rack-a Configured claimed; credit must be confined to the chosen rack-b domain")
	}
	for _, id := range []machine.ID{"b-1", "b-2"} {
		if state.IsClaimed(id) {
			t.Errorf("acquirable Idle %s claimed by the pre-pass; only creditable members are SeedClaimed", id)
		}
	}
	if needs.IsZero(results[0].Deficit) {
		t.Error("deficit zero; the chosen domain has no creditable supply so the full demand must remain for acquisition")
	}
}

// Claimed Idle is excluded from the acquirable half: with rack-b's
// Idle machines already claimed, rack-a's creditable supply is the
// only joint potential left and wins.
func TestSeedConfiguredSupply_JointExcludesClaimedIdle(t *testing.T) {
	t.Parallel()
	state := freshStateWith(
		clusterSameMachine("a-1", machine.StateConfigured, "c1", "rack-a"),
		idleMachine("b-1", 1.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"}),
		idleMachine("b-2", 1.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"}),
	)
	foreign := needs.Need{}
	state.SeedClaim("b-1", &foreign, occ.Precedence{Priority: 999}, 10)
	state.SeedClaim("b-2", &foreign, occ.Precedence{Priority: 999}, 10)

	n := sameNeed("c1", sameProfile(100, "topology.kubernetes.io/rack"), "8")
	occ.SeedConfiguredSupply(state, []*needs.Need{&n}, 10)

	if got := state.SameDomainFor(&n); got != "rack-a" {
		t.Fatalf("recorded domain = %q, want rack-a (rack-b's Idle is claimed and contributes nothing)", got)
	}
	if !state.IsClaimed("a-1") {
		t.Error("rack-a Configured not claimed despite rack-a being the chosen domain")
	}
}

// Successive same-fingerprint Needs distribute across domains as
// earlier choices claim supply: the first Need's SeedClaims empty a
// domain's creditable half, so the next Need's joint rank moves to
// the acquirable-rich domain.
func TestSeedConfiguredSupply_JointProgressionAcrossNeeds(t *testing.T) {
	t.Parallel()
	state := freshStateWith(
		clusterSameMachine("a-1", machine.StateConfigured, "c1", "rack-a"),
		clusterSameMachine("a-2", machine.StateConfigured, "c1", "rack-a"),
		idleMachine("b-1", 1.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"}),
		idleMachine("b-2", 1.0, map[string]string{"topology.kubernetes.io/rack": "rack-b"}),
	)
	pf := sameProfile(100, "topology.kubernetes.io/rack")
	n1 := sameNeed("c1", pf, "8")
	n1.Group = "owner-1"
	n2 := sameNeed("c1", pf, "8")
	n2.Group = "owner-2"

	results := occ.SeedConfiguredSupply(state, []*needs.Need{&n1, &n2}, 10)

	if got := state.SameDomainFor(&n1); got != "rack-a" {
		t.Fatalf("n1 domain = %q, want rack-a (satisfiable tie broken by smallest value)", got)
	}
	if got := state.SameDomainFor(&n2); got != "rack-b" {
		t.Fatalf("n2 domain = %q, want rack-b (rack-a's creditable supply was claimed by n1)", got)
	}
	if !needs.IsZero(results[0].Deficit) {
		t.Errorf("n1 deficit = %v, want zero (rack-a's 2 Configured cover it)", results[0].Deficit)
	}
	if needs.IsZero(results[1].Deficit) {
		t.Error("n2 deficit zero; rack-b is acquirable-only so the pre-pass credits nothing")
	}
}

// sortedIDs is shared with displacement_test.go — define a local copy
// because Go doesn't deduplicate across _test files of the same
// package and we want this self-contained. Different name to avoid
// clash.
func sortedIDsCand(in []machine.ID) []machine.ID {
	out := make([]machine.ID, len(in))
	copy(out, in)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

var _ = sortedIDsCand
