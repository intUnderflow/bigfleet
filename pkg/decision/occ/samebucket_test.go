package occ_test

import (
	"fmt"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

const gangRackKey = "topology.bigfleet/rack"

var gangGPU = map[string]string{"nvidia.com/gpu": "8"}

// gangRackProfile is a Same(rack) GPU profile (one whole-node Pod).
func gangRackProfile() needs.Profile {
	return needs.NewProfile([]needs.Requirement{
		{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"a3-highgpu-8g"}},
		{Key: gangRackKey, Operator: needs.OperatorSame},
	}, nil, 1000, needs.BucketForDollars(16384), needs.BucketForDollars(32768))
}

// gangCfgMachine is a Configured GPU machine on rack, optionally carrying
// the gang attribution (group + fingerprint) ADR-0051 reads.
func gangCfgMachine(id machine.ID, cluster machine.ClusterID, rack, group, fingerprint string) machine.Machine {
	return machine.Machine{
		ID:    id,
		State: machine.StateConfigured,
		Host:  machine.HostRef{Provider: "fake", Ref: string(id)},
		Profile: machine.Profile{
			InstanceType: "a3-highgpu-8g",
			Zone:         "zone-a",
			CapacityType: machine.CapacityTypeBareMetal,
			Resources:    gangGPU,
			Labels:       map[string]string{gangRackKey: rack},
		},
		Allocatable:             gangGPU,
		Cluster:                 cluster,
		AssignedPriority:        1000,
		AssignedNeedFingerprint: fingerprint,
		AssignedGroup:           group,
	}
}

// TestChooseSameBucket_GangOwnBreaksCoverageTie is the ADR-0051 (M77g)
// unit pin: the exact tie the offline diagnosis demonstrated. Two domains
// each fully cover the gang's deficit (both satisfiable, both at capped
// creditable coverage 1.0, so rule 2 ties). Domain X is the incumbent —
// it holds THIS gang's own bound machines plus some accumulated
// neighbour/stray supply, so its joint Total is LARGER. Domain Y holds an
// EQUAL-coverage count of an UNRELATED same-class gang's machines and
// nothing more, so its Total is SMALLER. Pre-fix the tie fell through to
// rule 3 (smallest joint Total), which picks Y — the gang flips off its
// own machines; the abandoned machines become strays Phase 3 reclaims,
// and with the acquirable snapshot moving each cycle (the bootstrap
// dwell) the argmin keeps flipping (the #64 lockstep). Rule 2b breaks the
// tie on gang-own coverage first, so X (the incumbent) wins and the
// choice is stable.
func TestChooseSameBucket_GangOwnBreaksCoverageTie(t *testing.T) {
	// One resource dimension (e.g. gpu); deficit = 3 units.
	deficit := []int64{3}

	// Domain X (incumbent): 3 of the gang's OWN machines + 3 neighbour
	// machines (all creditable; gang-own = 3). Total = 6 — the LARGER
	// joint total, so rule 3 disfavours it.
	x := occ.SameBucket{
		Value:              "rack-x",
		Count:              6,
		CreditableCount:    6,
		Total:              []int64{6},
		CreditableTotal:    []int64{6},
		CreditableOwnTotal: []int64{3}, // this gang's bound machines
	}
	// Domain Y: 3 of an UNRELATED same-class gang's machines (creditable
	// but NOT gang-own), nothing else. Total = 3 — the SMALLER joint
	// total, so pre-fix rule 3 picks it. Capped creditable coverage ties X
	// (3/3 = 1.0); gang-own coverage is 0.
	y := occ.SameBucket{
		Value:              "rack-y",
		Count:              3,
		CreditableCount:    3,
		Total:              []int64{3},
		CreditableTotal:    []int64{3},
		CreditableOwnTotal: nil, // unrelated gang — contributes nothing
	}

	// Order must not matter; assert both ways.
	for _, tc := range []struct {
		name    string
		buckets []occ.SameBucket
		wantVal string
	}{
		{"x-first", []occ.SameBucket{x, y}, "rack-x"},
		{"y-first", []occ.SameBucket{y, x}, "rack-x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := occ.ChooseSameBucket(tc.buckets, deficit)
			if got < 0 {
				t.Fatalf("ChooseSameBucket returned -1, want %s", tc.wantVal)
			}
			if tc.buckets[got].Value != tc.wantVal {
				t.Errorf("chose %q, want %q (gang must follow its own bindings, ADR-0051)",
					tc.buckets[got].Value, tc.wantVal)
			}
		})
	}
}

// TestChooseSameBucket_GangOwnDoesNotOverrideCreditable confirms rule 2b
// is strictly BELOW rule 2: a domain with greater cluster-creditable
// coverage still wins even if a rival holds more gang-own coverage but
// less creditable. (Gang-own is a sub-total of creditable, so this is a
// constructed/degenerate ordering, but the strict layering must hold —
// rule 2b only fires on a creditable TIE.)
func TestChooseSameBucket_GangOwnDoesNotOverrideCreditable(t *testing.T) {
	deficit := []int64{4}
	// Domain P: creditable 4 (full coverage), gang-own 0.
	p := occ.SameBucket{
		Value: "rack-p", Count: 4, CreditableCount: 4,
		Total: []int64{4}, CreditableTotal: []int64{4}, CreditableOwnTotal: nil,
	}
	// Domain Q: creditable 2 (partial — loses rule 2), gang-own 2.
	q := occ.SameBucket{
		Value: "rack-q", Count: 4, CreditableCount: 2,
		Total: []int64{4}, CreditableTotal: []int64{2}, CreditableOwnTotal: []int64{2},
	}
	got := occ.ChooseSameBucket([]occ.SameBucket{q, p}, deficit)
	if got < 0 || ([]occ.SameBucket{q, p})[got].Value != "rack-p" {
		t.Errorf("rule 2 (creditable coverage) must outrank rule 2b (gang-own); got index %d", got)
	}
}

// TestRunCycle_GangOwnAttributionPicksOwnDomain is the end-to-end wiring
// guard for ADR-0051's attribution path through seedSameProfile: two
// same-(cluster, profile) rack gangs, each fully bound on its own rack
// with the gang attribution stamped on the machines (AssignedGroup). It
// confirms the machine.AssignedGroup → CreditableOwnTotal → ChooseSameBucket
// chain selects each gang's own domain (the discriminating fail-pre /
// pass-post tie is TestChooseSameBucket_GangOwnBreaksCoverageTie; this
// symmetric case the lexicographic tiebreak already resolves, so it is a
// no-regression guard for the wiring, not the fix discriminator).
func TestRunCycle_GangOwnAttributionPicksOwnDomain(t *testing.T) {
	prof := gangRackProfile()
	fp := prof.Fingerprint()
	groupA := gangRackKey + "\x00c1-gangA"
	groupB := gangRackKey + "\x00c1-gangB"

	inv := inventory.New()
	// Gang A: 3 machines on rack-a, attributed to groupA.
	// Gang B: 3 machines on rack-b, attributed to groupB.
	for i := 0; i < 3; i++ {
		if err := inv.Insert(gangCfgMachine(machine.ID(fmt.Sprintf("a-%d", i)), "c1", "rack-a", groupA, fp)); err != nil {
			t.Fatal(err)
		}
		if err := inv.Insert(gangCfgMachine(machine.ID(fmt.Sprintf("b-%d", i)), "c1", "rack-b", groupB, fp)); err != nil {
			t.Fatal(err)
		}
	}
	snap := inv.Snapshot()

	gpuQty := needs.ResourceQtysFromMap(gangGPU)
	demand := []needs.Need{
		{ClusterID: "c1", Profile: prof, AggregateResources: needs.ScaleResources(gpuQty, 3), MinUnit: gpuQty, Group: groupA},
		{ClusterID: "c1", Profile: prof, AggregateResources: needs.ScaleResources(gpuQty, 3), MinUnit: gpuQty, Group: groupB},
	}
	res := occ.RunCycle(snap, demand)

	got := map[string]string{}
	for i := range res.Results {
		r := &res.Results[i]
		if r.Need == nil {
			continue
		}
		got[r.Need.Group] = r.SameDomain
	}
	if got[groupA] != "rack-a" {
		t.Errorf("gang A chose domain %q, want rack-a (its own machines)", got[groupA])
	}
	if got[groupB] != "rack-b" {
		t.Errorf("gang B chose domain %q, want rack-b (its own machines)", got[groupB])
	}
}

// TestChooseSameBucket_NoGangOwnFallsThroughToSlack pins the pre-fix
// behaviour for the no-attribution case: when NEITHER tied domain holds
// gang-own machines (CreditableOwnTotal empty everywhere — the production
// seed's limitation, since the harness can't stamp runtime gang IDs),
// rule 2b is inert and the existing rule 3 (smallest joint Total) decides.
// This is exactly why pre-seeded gangs are not fixed (see the report).
func TestChooseSameBucket_NoGangOwnFallsThroughToSlack(t *testing.T) {
	deficit := []int64{3}
	// Two satisfiable domains, equal cluster-creditable coverage, no
	// gang-own anywhere; the smaller joint Total wins (rule 3).
	small := occ.SameBucket{
		Value: "rack-small", Count: 3, CreditableCount: 3,
		Total: []int64{3}, CreditableTotal: []int64{3},
	}
	big := occ.SameBucket{
		Value: "rack-big", Count: 5, CreditableCount: 3,
		Total: []int64{5}, CreditableTotal: []int64{3},
	}
	got := occ.ChooseSameBucket([]occ.SameBucket{big, small}, deficit)
	if got < 0 || ([]occ.SameBucket{big, small})[got].Value != "rack-small" {
		t.Errorf("with no gang-own attribution, rule 3 (smallest joint Total) must decide; got index %d", got)
	}
}
