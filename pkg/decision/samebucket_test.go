package decision

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// synthSameMachine is one machine of a synthetic Same-domain pool: the
// domain value it carries and its EffectiveAllocatable vector.
type synthSameMachine struct {
	domain string
	alloc  []needs.ResourceQty
}

// foldSameMachines builds the per-domain bucket aggregates both
// choosers rank, from one shared machine list — the same fold the two
// crediting sites perform inline.
func foldSameMachines(ms []synthSameMachine) ([]sameBucket, []occ.SameBucket) {
	index := map[string]int{}
	var dec []sameBucket
	var oc []occ.SameBucket
	for _, m := range ms {
		i, ok := index[m.domain]
		if !ok {
			i = len(dec)
			index[m.domain] = i
			dec = append(dec, sameBucket{value: m.domain})
			oc = append(oc, occ.SameBucket{Value: m.domain})
		}
		dec[i].count++
		dec[i].total = needs.AddResources(dec[i].total, m.alloc)
		oc[i].Count++
		oc[i].Total = needs.AddResources(oc[i].Total, m.alloc)
	}
	return dec, oc
}

func cpus(n string) []needs.ResourceQty {
	return []needs.ResourceQty{{Name: "cpu", Quantity: n}}
}

func rackMachines(domain string, n int, alloc []needs.ResourceQty) []synthSameMachine {
	out := make([]synthSameMachine, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, synthSameMachine{domain: domain, alloc: alloc})
	}
	return out
}

// TestChooseSameBucketParity feeds the decision- and occ-side ADR-0040
// bucket choosers the same synthetic machine sets and asserts they
// pick the same bucket. The helper is duplicated across the two
// packages (occ must not import decision); this test is the guard that
// keeps the duplication aligned, and it pins the documented rule:
// satisfiable preferred, smallest satisfiable total, else most-
// covering, tiebreak larger count then smallest value.
//
// ADR-0040 Addendum: both callers now feed JOINT totals — creditable
// (cluster Configured/Configuring) plus acquirable (shard-wide Idle +
// Speculative) — folded into one SameBucket per domain. The chooser
// is agnostic to which half a member came from, so the synthetic
// machines here stand in for either; the "joint totals" cases mix
// the two shapes inside one domain.
func TestChooseSameBucketParity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		machines []synthSameMachine
		deficit  []needs.ResourceQty
		want     string // expected domain value; "" = no bucket (-1)
	}{
		{
			name: "satisfiable beats bigger unsatisfiable",
			machines: append(
				rackMachines("rack-a", 1, cpus("4")),
				rackMachines("rack-b", 2, cpus("4"))...),
			deficit: cpus("8"),
			want:    "rack-b",
		},
		{
			name: "smallest satisfiable total wins",
			machines: append(
				rackMachines("rack-a", 4, cpus("4")),     // total 16
				rackMachines("rack-b", 3, cpus("4"))...), // total 12
			deficit: cpus("10"),
			want:    "rack-b",
		},
		{
			name: "none satisfiable: most covering wins",
			machines: append(
				rackMachines("rack-a", 1, cpus("4")),
				rackMachines("rack-b", 3, cpus("4"))...),
			deficit: cpus("20"),
			want:    "rack-b",
		},
		{
			name: "score tie: larger machine count wins",
			machines: append(
				rackMachines("rack-a", 1, cpus("8")),
				rackMachines("rack-b", 2, cpus("4"))...), // both total 8
			deficit: cpus("6"),
			want:    "rack-b",
		},
		{
			name: "full tie: lexicographically smallest value",
			machines: append(
				rackMachines("rack-b", 2, cpus("4")),
				rackMachines("rack-a", 2, cpus("4"))...),
			deficit: cpus("8"),
			want:    "rack-a",
		},
		{
			name: "multi-dimension coverage is capped per dimension",
			machines: append(
				// rack-a overflows cpu 4× but covers no memory (capped
				// coverage 1.0); rack-b covers cpu fully and half the
				// memory (coverage 1.5). Uncapped, rack-a's cpu overflow
				// would mask its memory hole and win.
				rackMachines("rack-a", 2, []needs.ResourceQty{{Name: "cpu", Quantity: "32"}}),
				rackMachines("rack-b", 2, []needs.ResourceQty{
					{Name: "cpu", Quantity: "8"}, {Name: "memory", Quantity: "16Gi"},
				})...),
			deficit: []needs.ResourceQty{
				{Name: "cpu", Quantity: "16"}, {Name: "memory", Quantity: "64Gi"},
			},
			want: "rack-b",
		},
		{
			// ADR-0040 Addendum joint-total shape: rack-a is
			// creditable-only (2 Configured); rack-b's total merges 1
			// creditable machine with 2 acquirable Idle of a wider
			// resource shape. The joint fold must rank rack-b
			// satisfiable even though its creditable half alone is the
			// smallest bucket.
			name: "joint totals merge creditable and acquirable halves",
			machines: append(
				rackMachines("rack-a", 2, cpus("4")),
				append(
					rackMachines("rack-b", 1, cpus("4")),
					rackMachines("rack-b", 2, []needs.ResourceQty{
						{Name: "cpu", Quantity: "4"}, {Name: "memory", Quantity: "16Gi"},
					})...,
				)...),
			deficit: cpus("12"),
			want:    "rack-b",
		},
		{
			name:     "no candidates",
			machines: nil,
			deficit:  cpus("8"),
			want:     "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Both input orders: the rule is a strict total order over
			// domain values, so the pick must be order-independent.
			orders := [][]synthSameMachine{tc.machines, reversed(tc.machines)}
			for _, ms := range orders {
				decBuckets, occBuckets := foldSameMachines(ms)
				decIdx := chooseSameBucket(decBuckets, tc.deficit)
				occIdx := occ.ChooseSameBucket(occBuckets, tc.deficit)

				decVal, occVal := "", ""
				if decIdx >= 0 {
					decVal = decBuckets[decIdx].value
				}
				if occIdx >= 0 {
					occVal = occBuckets[occIdx].Value
				}
				if decVal != occVal {
					t.Fatalf("parity broken: decision chose %q, occ chose %q", decVal, occVal)
				}
				if decVal != tc.want {
					t.Errorf("chose %q, want %q", decVal, tc.want)
				}
			}
		})
	}
}

func reversed(in []synthSameMachine) []synthSameMachine {
	out := make([]synthSameMachine, len(in))
	for i, m := range in {
		out[len(in)-1-i] = m
	}
	return out
}

// TestSameDomainChoiceParity_Phase1VsPhase3 is the end-to-end mirror
// of the chooser parity above: on one snapshot, the domain Phase 1's
// pre-pass records for a Same Need must be the domain whose
// Configured machines Phase 3 keeps. Both rank by the joint potential
// (creditable + acquirable, ADR-0040 Addendum); if either side
// regressed to creditable-only the two phases would pick different
// domains for the same Need and resume the reclaim↔re-bootstrap
// fight.
//
// Shape: rack-a holds 1 Configured (creditable-only, total 4); rack-b
// holds 1 Configured + 2 Idle (joint total 12, satisfiable for the
// 12-cpu demand). Joint ranking picks rack-b on both sides;
// creditable-only ranking would have had Phase 3's Configured walk
// see {a: 4, b: 4} and tie-break to rack-a.
func TestSameDomainChoiceParity_Phase1VsPhase3(t *testing.T) {
	t.Parallel()
	const rackKey = "topology.bigfleet/rack"
	mk := func(id machine.ID, st machine.State, cluster machine.ClusterID, rack string) machine.Machine {
		return machine.Machine{
			ID:    id,
			State: st,
			Profile: machine.Profile{
				InstanceType: "m5.large",
				Resources:    map[string]string{"cpu": "4"},
				Labels:       map[string]string{rackKey: rack},
			},
			Cluster: cluster,
			Host:    machine.HostRef{Provider: "fake", Ref: string(id)},
		}
	}
	inv := inventory.New()
	if err := inv.Insert(mk("conf-a", machine.StateConfigured, "c1", "rack-a")); err != nil {
		t.Fatal(err)
	}
	if err := inv.Insert(mk("conf-b", machine.StateConfigured, "c1", "rack-b")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []machine.ID{"idle-b-1", "idle-b-2"} {
		m := mk(id, machine.StateIdle, "", "rack-b")
		if err := inv.Insert(m); err != nil {
			t.Fatal(err)
		}
	}
	snap := inv.Snapshot()

	profile := needs.NewProfile(
		[]needs.Requirement{
			{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"m5.large"}},
			{Key: rackKey, Operator: needs.OperatorSame},
		},
		nil, 1000,
		needs.PenaltyBucket1024, needs.PenaltyBucket1,
	)
	n := needs.Need{
		ClusterID:          "c1",
		Profile:            profile,
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: "12"}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "4"}},
	}

	state := occ.NewSharedState(snap)
	occ.SeedConfiguredSupply(state, []*needs.Need{&n}, 10)
	p1Domain := state.SameDomainFor(&n)
	if p1Domain != "rack-b" {
		t.Fatalf("Phase 1 pre-pass chose %q, want rack-b (joint 12 beats creditable-only 4)", p1Domain)
	}

	p3 := Phase3(snap, []needs.Need{n}, AlwaysReady)
	reclaimed := map[machine.ID]bool{}
	for _, a := range p3.Actions {
		reclaimed[a.MachineID] = true
	}
	for _, m := range snap.ListByClusterState("c1", machine.StateConfigured) {
		rack := m.Profile.Labels[rackKey]
		kept := !reclaimed[m.ID]
		if kept != (rack == p1Domain) {
			t.Errorf("machine %s (rack %s): kept=%v — Phase 3 must keep exactly Phase 1's chosen domain %q", m.ID, rack, kept, p1Domain)
		}
	}
}
