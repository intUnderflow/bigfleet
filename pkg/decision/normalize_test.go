package decision_test

import (
	"reflect"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// NormalizeDemand unit tests (ADR-0041). The shapes mirror the
// uber-5k sub-machine gang pathology at miniature scale: density-10
// machines (16 cpu / 128Gi) against 4-Pod gangs of 2 cpu / 16Gi Pods,
// so one machine hosts a whole gang and folding applies.

const nrmRackKey = "topology.bigfleet/rack"

// nrmGangProfile is a Same(rack) co-located Profile pinned to one
// instance type, with non-trivial spread/penalties so the strip-and-
// rebuild path is exercised.
func nrmGangProfile() needs.Profile {
	return needs.NewProfile(
		[]needs.Requirement{
			{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: []string{"r6i.2xlarge"}},
			{Key: nrmRackKey, Operator: needs.OperatorSame},
		},
		[]needs.TopologySpread{{TopologyKey: "topology.kubernetes.io/zone", MaxSkew: 1, WhenUnsatisfiable: needs.WhenUnsatisfiableDoNotSchedule}},
		1000,
		needs.PenaltyBucket4096, needs.PenaltyBucket32768,
	)
}

// nrmGangNeed is one gang's Need: aggregate = gangSize Pods of
// 2 cpu / 16Gi, MinUnit = one Pod (the pre-fold shape needs.Aggregate
// produces for a co-location group).
func nrmGangNeed(cluster machine.ClusterID, pf needs.Profile, gangSize int, group string, arrival int64) needs.Need {
	pod := []needs.ResourceQty{{Name: "cpu", Quantity: "2"}, {Name: "memory", Quantity: "16Gi"}}
	return needs.Need{
		ClusterID:          cluster,
		Profile:            pf,
		AggregateResources: needs.ScaleResources(pod, gangSize),
		MinUnit:            pod,
		Group:              group,
		ArrivalUnixNanos:   arrival,
	}
}

func nrmPlainNeed(cluster machine.ClusterID, cpu string) needs.Need {
	pf := needs.NewProfile(nil, nil, 100, needs.PenaltyBucket32, needs.PenaltyBucket64)
	return needs.Need{
		ClusterID:          cluster,
		Profile:            pf,
		AggregateResources: []needs.ResourceQty{{Name: "cpu", Quantity: cpu}},
		MinUnit:            []needs.ResourceQty{{Name: "cpu", Quantity: "200m"}},
	}
}

// nrmMachine is a density-10 gang-archetype machine: 16 cpu / 128Gi,
// instance-typed and racked so it both matches the gang Profile and
// covers a 4-Pod gang aggregate (8 cpu / 64Gi).
func nrmMachine(id machine.ID, st machine.State, cluster machine.ClusterID) machine.Machine {
	m := machine.Machine{
		ID:    id,
		State: st,
		Profile: machine.Profile{
			InstanceType: "r6i.2xlarge",
			Zone:         "zone-a",
			Resources:    map[string]string{"cpu": "16", "memory": "128Gi"},
			Labels:       map[string]string{nrmRackKey: "zone-a-rack-0"},
		},
		Cluster: cluster,
	}
	if st != machine.StateSpeculative {
		m.Host = machine.HostRef{Provider: "fake", Ref: string(id)}
	}
	return m
}

func nrmSnapshot(t *testing.T, machines ...machine.Machine) *inventory.Snapshot {
	t.Helper()
	inv := inventory.New()
	for _, m := range machines {
		if err := inv.Insert(m); err != nil {
			t.Fatalf("insert %s: %v", m.ID, err)
		}
	}
	return inv.Snapshot()
}

func hasSameReq(p needs.Profile) bool {
	_, ok := occ.SameRequirementKey(p)
	return ok
}

// Foldable gangs of one (cluster, Profile, size) class fold into one
// plain Need: Same stripped, aggregates summed, MinUnit = one gang's
// aggregate, spread/priority/penalties preserved, earliest arrival
// kept.
func TestNormalizeDemand_FoldsSameSizeGangs(t *testing.T) {
	t.Parallel()
	snap := nrmSnapshot(t, nrmMachine("cfg-0", machine.StateConfigured, "c1"))
	pf := nrmGangProfile()
	demand := []needs.Need{
		nrmGangNeed("c1", pf, 4, "g0", 300),
		nrmGangNeed("c1", pf, 4, "g1", 100),
		nrmGangNeed("c1", pf, 4, "g2", 200),
	}

	out := decision.NormalizeDemand(snap, demand)
	if len(out) != 1 {
		t.Fatalf("normalized rows = %d, want 1 folded Need: %+v", len(out), out)
	}
	f := out[0]
	if f.ClusterID != "c1" {
		t.Errorf("cluster = %s, want c1", f.ClusterID)
	}
	if hasSameReq(f.Profile) {
		t.Errorf("folded Profile still carries a Same requirement: %+v", f.Profile.Requirements())
	}
	wantAgg := []needs.ResourceQty{{Name: "cpu", Quantity: "24"}, {Name: "memory", Quantity: "192Gi"}}
	if !reflect.DeepEqual(f.AggregateResources, wantAgg) {
		t.Errorf("aggregate = %+v, want %+v (3 gangs summed)", f.AggregateResources, wantAgg)
	}
	wantMin := []needs.ResourceQty{{Name: "cpu", Quantity: "8"}, {Name: "memory", Quantity: "64Gi"}}
	if !reflect.DeepEqual(f.MinUnit, wantMin) {
		t.Errorf("MinUnit = %+v, want one gang's aggregate %+v (§7 atomicity floor)", f.MinUnit, wantMin)
	}
	if f.Profile.Priority() != pf.Priority() ||
		f.Profile.InterruptionPenaltyBucket() != pf.InterruptionPenaltyBucket() ||
		f.Profile.ReclamationPenaltyBucket() != pf.ReclamationPenaltyBucket() ||
		!reflect.DeepEqual(f.Profile.Spread(), pf.Spread()) {
		t.Errorf("folded Profile lost priority/penalties/spread: %+v", f.Profile)
	}
	if f.ArrivalUnixNanos != 100 {
		t.Errorf("arrival = %d, want earliest member's 100", f.ArrivalUnixNanos)
	}
}

// Gangs of different sizes are different atomic units: they fold into
// separate Needs, each with its own MinUnit floor.
func TestNormalizeDemand_DifferentSizesFoldSeparately(t *testing.T) {
	t.Parallel()
	snap := nrmSnapshot(t, nrmMachine("cfg-0", machine.StateConfigured, "c1"))
	pf := nrmGangProfile()
	demand := []needs.Need{
		nrmGangNeed("c1", pf, 4, "g0", 0),
		nrmGangNeed("c1", pf, 2, "g1", 0),
		nrmGangNeed("c1", pf, 4, "g2", 0),
	}

	out := decision.NormalizeDemand(snap, demand)
	if len(out) != 2 {
		t.Fatalf("normalized rows = %d, want 2 (one per gang size): %+v", len(out), out)
	}
	bySize := map[string]needs.Need{}
	for _, n := range out {
		bySize[n.MinUnit[0].Quantity] = n
	}
	if n, ok := bySize["8"]; !ok || n.AggregateResources[0].Quantity != "16" {
		t.Errorf("size-4 fold: got %+v, want cpu aggregate 16 with MinUnit cpu 8", bySize["8"])
	}
	if n, ok := bySize["4"]; !ok || n.AggregateResources[0].Quantity != "4" {
		t.Errorf("size-2 fold: got %+v, want cpu aggregate 4 with MinUnit cpu 4", bySize["4"])
	}
}

// A gang whose aggregate exceeds every matching machine keeps its
// per-gang Same Need — the genuinely cross-machine topology case.
func TestNormalizeDemand_UnfoldablePassesThroughWithSame(t *testing.T) {
	t.Parallel()
	snap := nrmSnapshot(t, nrmMachine("cfg-0", machine.StateConfigured, "c1"))
	pf := nrmGangProfile()
	// 12 Pods × 2 cpu = 24 cpu > the 16-cpu machine.
	demand := []needs.Need{nrmGangNeed("c1", pf, 12, "g0", 0)}

	out := decision.NormalizeDemand(snap, demand)
	if !reflect.DeepEqual(out, demand) {
		t.Fatalf("unfoldable gang must pass through unchanged:\n got %+v\nwant %+v", out, demand)
	}
	if !hasSameReq(out[0].Profile) {
		t.Errorf("Same requirement was stripped from an unfoldable Need")
	}
}

// Foldability looks at every serving tier: a shard-wide Idle (or
// Speculative) machine with room folds a gang even when the cluster's
// own Configured machines are too small — and the creditable half is
// per-cluster, so a same-class gang in a cluster with a big Configured
// machine folds while another cluster's (with neither a fitting
// Configured nor any fitting shard-wide machine) does not.
func TestNormalizeDemand_FoldabilityTiers(t *testing.T) {
	t.Parallel()
	pf := nrmGangProfile()

	t.Run("idle machine folds", func(t *testing.T) {
		t.Parallel()
		snap := nrmSnapshot(t, nrmMachine("idle-0", machine.StateIdle, ""))
		out := decision.NormalizeDemand(snap, []needs.Need{nrmGangNeed("c1", pf, 4, "g0", 0)})
		if len(out) != 1 || hasSameReq(out[0].Profile) {
			t.Fatalf("gang must fold against shard-wide Idle: %+v", out)
		}
	})
	t.Run("speculative machine folds", func(t *testing.T) {
		t.Parallel()
		snap := nrmSnapshot(t, nrmMachine("spec-0", machine.StateSpeculative, ""))
		out := decision.NormalizeDemand(snap, []needs.Need{nrmGangNeed("c1", pf, 4, "g0", 0)})
		if len(out) != 1 || hasSameReq(out[0].Profile) {
			t.Fatalf("gang must fold against shard-wide Speculative: %+v", out)
		}
	})
	t.Run("other cluster's Configured does not fold", func(t *testing.T) {
		t.Parallel()
		// c2's Configured machine fits, but it is creditable for c2
		// only and there is no shard-wide acquirable supply: c1's gang
		// stays a Same Need, c2's folds.
		snap := nrmSnapshot(t, nrmMachine("cfg-c2", machine.StateConfigured, "c2"))
		out := decision.NormalizeDemand(snap, []needs.Need{
			nrmGangNeed("c1", pf, 4, "g0", 0),
			nrmGangNeed("c2", pf, 4, "g0", 0),
		})
		if len(out) != 2 {
			t.Fatalf("normalized rows = %d, want 2: %+v", len(out), out)
		}
		if !hasSameReq(out[0].Profile) || out[0].ClusterID != "c1" {
			t.Errorf("c1's gang must pass through with Same intact: %+v", out[0])
		}
		if hasSameReq(out[1].Profile) || out[1].ClusterID != "c2" {
			t.Errorf("c2's gang must fold via its own Configured machine: %+v", out[1])
		}
	})
}

// Non-Same Needs pass through unchanged in input order; folded Needs
// append after them in sorted group-key order. Clusters never share a
// fold.
func TestNormalizeDemand_OrderingAndClusterSeparation(t *testing.T) {
	t.Parallel()
	snap := nrmSnapshot(t,
		nrmMachine("cfg-c1", machine.StateConfigured, "c1"),
		nrmMachine("cfg-c2", machine.StateConfigured, "c2"),
	)
	pf := nrmGangProfile()
	plainA := nrmPlainNeed("c2", "40")
	plainB := nrmPlainNeed("c1", "10")
	demand := []needs.Need{
		nrmGangNeed("c2", pf, 4, "g0", 0), // folds; c2 sorts after c1
		plainA,
		nrmGangNeed("c1", pf, 4, "g0", 0),
		plainB,
		nrmGangNeed("c1", pf, 4, "g1", 0),
	}

	out := decision.NormalizeDemand(snap, demand)
	if len(out) != 4 {
		t.Fatalf("normalized rows = %d, want 4 (2 passthrough + 2 folds): %+v", len(out), out)
	}
	if !reflect.DeepEqual(out[0], plainA) || !reflect.DeepEqual(out[1], plainB) {
		t.Errorf("non-Same Needs must pass through first, in input order: %+v", out[:2])
	}
	if out[2].ClusterID != "c1" || out[3].ClusterID != "c2" {
		t.Errorf("folded Needs must append in sorted group-key order (c1 before c2): %+v", out[2:])
	}
	if out[2].AggregateResources[0].Quantity != "16" {
		t.Errorf("c1 fold aggregate cpu = %s, want 16 (2 gangs)", out[2].AggregateResources[0].Quantity)
	}
	if out[3].AggregateResources[0].Quantity != "8" {
		t.Errorf("c2 fold aggregate cpu = %s, want 8 (1 gang — clusters never share a fold)", out[3].AggregateResources[0].Quantity)
	}

	// Deterministic: a second call over the same inputs reproduces the
	// output exactly (memoization must not be observable).
	again := decision.NormalizeDemand(snap, demand)
	if !reflect.DeepEqual(out, again) {
		t.Errorf("NormalizeDemand is not deterministic:\n first %+v\nsecond %+v", out, again)
	}
}

// A cycle with no Same-carrying Needs normalizes to itself.
func TestNormalizeDemand_NoSamePassthrough(t *testing.T) {
	t.Parallel()
	snap := nrmSnapshot(t, nrmMachine("cfg-0", machine.StateConfigured, "c1"))
	demand := []needs.Need{nrmPlainNeed("c1", "10"), nrmPlainNeed("c2", "20")}
	out := decision.NormalizeDemand(snap, demand)
	if !reflect.DeepEqual(out, demand) {
		t.Fatalf("non-Same demand must pass through unchanged:\n got %+v\nwant %+v", out, demand)
	}
}
