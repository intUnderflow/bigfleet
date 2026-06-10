package decision

import (
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// synthSameMachine is one machine of a synthetic Same-domain pool: the
// domain value it carries, its allocatable as a milli-unit vector
// (dimension order fixed by the test case — the chooser is agnostic),
// and which half of the joint fold it came from (creditable = the
// cluster's Configured/Configuring; default acquirable).
type synthSameMachine struct {
	domain     string
	alloc      []int64
	creditable bool
}

// foldSameMachines builds the per-domain bucket aggregates the chooser
// ranks, from one machine list — the same fold the two crediting sites
// (occ.seedSameProfile, claimMatchingSame) perform inline, including
// the ADR-0041 rider-3 rule that only creditable members bump
// CreditableCount.
func foldSameMachines(ms []synthSameMachine) []occ.SameBucket {
	index := map[string]int{}
	var out []occ.SameBucket
	for _, m := range ms {
		i, ok := index[m.domain]
		if !ok {
			i = len(out)
			index[m.domain] = i
			out = append(out, occ.SameBucket{Value: m.domain})
		}
		out[i].Count++
		if m.creditable {
			out[i].CreditableCount++
		}
		out[i].Total = occ.VecAdd(out[i].Total, m.alloc)
	}
	return out
}

// cpu quantities in milli-units; dimension 0 = cpu, dimension 1 =
// memory where a case uses it.
func cpus(n int64) []int64 { return []int64{n * 1000} }

func rackMachines(domain string, n int, alloc []int64) []synthSameMachine {
	out := make([]synthSameMachine, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, synthSameMachine{domain: domain, alloc: alloc})
	}
	return out
}

func creditableRackMachines(domain string, n int, alloc []int64) []synthSameMachine {
	out := rackMachines(domain, n, alloc)
	for i := range out {
		out[i].creditable = true
	}
	return out
}

// TestChooseSameBucket_Rule pins occ.ChooseSameBucket's documented
// total order — satisfiable preferred, smallest satisfiable total,
// else most-covering, tiebreak larger count then smallest value — from
// the decision side, where claimMatchingSame consumes it. (The former
// pkg/decision twin of the chooser is gone; Phase 3 imports occ's
// directly, so a parity test is no longer needed — this keeps the rule
// coverage the parity cases provided.)
//
// ADR-0040 Addendum: callers feed JOINT totals — creditable plus
// acquirable — folded into one SameBucket per domain. The chooser is
// agnostic to which half a member came from; the "joint totals" case
// mixes the two shapes inside one domain.
func TestChooseSameBucket_Rule(t *testing.T) {
	t.Parallel()

	gi := int64(1024 * 1024 * 1024) // 1Gi in bytes; ×1000 below for milli

	cases := []struct {
		name     string
		machines []synthSameMachine
		deficit  []int64
		want     string // expected domain value; "" = no bucket (-1)
	}{
		{
			name: "satisfiable beats bigger unsatisfiable",
			machines: append(
				rackMachines("rack-a", 1, cpus(4)),
				rackMachines("rack-b", 2, cpus(4))...),
			deficit: cpus(8),
			want:    "rack-b",
		},
		{
			name: "smallest satisfiable total wins",
			machines: append(
				rackMachines("rack-a", 4, cpus(4)),     // total 16
				rackMachines("rack-b", 3, cpus(4))...), // total 12
			deficit: cpus(10),
			want:    "rack-b",
		},
		{
			name: "none satisfiable: most covering wins",
			machines: append(
				rackMachines("rack-a", 1, cpus(4)),
				rackMachines("rack-b", 3, cpus(4))...),
			deficit: cpus(20),
			want:    "rack-b",
		},
		{
			name: "score tie: larger machine count wins",
			machines: append(
				rackMachines("rack-a", 1, cpus(8)),
				rackMachines("rack-b", 2, cpus(4))...), // both total 8
			deficit: cpus(6),
			want:    "rack-b",
		},
		{
			name: "full tie: lexicographically smallest value",
			machines: append(
				rackMachines("rack-b", 2, cpus(4)),
				rackMachines("rack-a", 2, cpus(4))...),
			deficit: cpus(8),
			want:    "rack-a",
		},
		{
			name: "multi-dimension coverage is capped per dimension",
			machines: append(
				// rack-a overflows cpu 4× but covers no memory (capped
				// coverage 1.0); rack-b covers cpu fully and half the
				// memory (coverage 1.5). Uncapped, rack-a's cpu overflow
				// would mask its memory hole and win.
				rackMachines("rack-a", 2, []int64{32 * 1000, 0}),
				rackMachines("rack-b", 2, []int64{8 * 1000, 16 * gi * 1000})...),
			deficit: []int64{16 * 1000, 64 * gi * 1000},
			want:    "rack-b",
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
				rackMachines("rack-a", 2, cpus(4)),
				append(
					creditableRackMachines("rack-b", 1, cpus(4)),
					rackMachines("rack-b", 2, []int64{4 * 1000, 16 * gi * 1000})...,
				)...),
			deficit: cpus(12),
			want:    "rack-b",
		},
		{
			name:     "no candidates",
			machines: nil,
			deficit:  cpus(8),
			want:     "",
		},
		{
			// ADR-0041 rider 3: among satisfiable buckets the serving
			// (creditable) domain wins BEFORE the smallest-total rule —
			// rack-a's smaller acquirable-only total must not relocate
			// a healthy gang.
			name: "satisfiable: creditable beats smaller acquirable-only",
			machines: append(
				rackMachines("rack-a", 2, cpus(4)),               // total 8, sat
				creditableRackMachines("rack-b", 3, cpus(4))...), // total 12, sat
			deficit: cpus(6),
			want:    "rack-b",
		},
		{
			// ADR-0041 rider 3: ... and before the lexicographic
			// tiebreak — a fresh idle domain that merely sorts lower
			// must not win a satisfiable tie.
			name: "satisfiable: creditable beats lexicographically-lower acquirable-only",
			machines: append(
				rackMachines("rack-a", 2, cpus(4)),
				creditableRackMachines("rack-b", 2, cpus(4))...),
			deficit: cpus(8),
			want:    "rack-b",
		},
		{
			// ADR-0041 rider 3 is confined to the satisfiable regime:
			// among unsatisfiable buckets coverage still outranks
			// creditable, preserving the ADR-0040 Addendum's
			// concentrate-then-park behaviour
			// (TestIntegration_SameDomain_NoOscillation's shape).
			name: "unsatisfiable: coverage still outranks creditable",
			machines: append(
				creditableRackMachines("rack-a", 2, cpus(4)), // covers 8 of 20
				rackMachines("rack-b", 3, cpus(4))...),       // covers 12 of 20
			deficit: cpus(20),
			want:    "rack-b",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Both input orders: the rule is a strict total order over
			// domain values, so the pick must be order-independent.
			orders := [][]synthSameMachine{tc.machines, reversed(tc.machines)}
			for _, ms := range orders {
				buckets := foldSameMachines(ms)
				got := ""
				if idx := occ.ChooseSameBucket(buckets, tc.deficit); idx >= 0 {
					got = buckets[idx].Value
				}
				if got != tc.want {
					t.Errorf("chose %q, want %q", got, tc.want)
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

// TestSameDomainChoiceParity_Phase1VsPhase3 is the end-to-end mirror:
// on one snapshot, the domain Phase 1's pre-pass records for a Same
// Need must be the domain whose Configured machines Phase 3 keeps.
// Both rank by the joint potential (creditable + acquirable, ADR-0040
// Addendum); if either side regressed to creditable-only the two
// phases would pick different domains for the same Need and resume the
// reclaim↔re-bootstrap fight.
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
