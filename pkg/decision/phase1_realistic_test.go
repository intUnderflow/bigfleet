package decision_test

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
)

// TestPhase1_RealisticCatalog_BindsIdleSeed reproduces the dev-5k
// scenario locally: load the realistic archetype catalog, seed an Idle
// pool from one archetype, build CR-style demand from the same
// archetype, and verify Phase 1 emits Bootstrap actions.
//
// If this test fails, the scaletest harness is structurally unable to
// match Idle seed against load-driver demand even though both come
// from the same catalog. (Surfaced in the kind dev-5k run: 6000 Idle,
// 5000 demand, 0 actions emitted.)
func TestPhase1_RealisticCatalog_BindsIdleSeed(t *testing.T) {
	t.Parallel()
	cat, err := archetype.LoadCatalog("../../test/scaletest/profiles/archetypes/realistic.yaml")
	if err != nil {
		t.Fatalf("load realistic catalog: %v", err)
	}
	arches := cat.Archetypes
	if len(arches) == 0 {
		t.Fatalf("empty catalog")
	}

	// Pick the cpu-service archetype (no sameRack — keeps the test
	// focused on resource/instance-type matching).
	var a *archetype.Archetype
	for i := range arches {
		if arches[i].Name == "cpu-service" {
			a = &arches[i]
			break
		}
	}
	if a == nil {
		t.Fatalf("cpu-service not in catalog")
	}

	// Seed 100 Idle machines using the same logic as seedFakeInventory.
	inv := inventory.New()
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 100; i++ {
		it := a.InstanceTypes[i%len(a.InstanceTypes)]
		z := a.Zones[i%len(a.Zones)]
		profile := machine.Profile{
			InstanceType: it,
			Zone:         z,
			CapacityType: machine.CapacityTypeBareMetal,
			Resources:    a.PickSize(rng),
		}
		id := machine.ID("idle-" + strconv.Itoa(i))
		if err := inv.Insert(machine.Machine{
			ID:      id,
			State:   machine.StateIdle,
			Host:    machine.HostRef{Provider: "fake", Ref: string(id)},
			Profile: profile,
		}); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Build 50 demand Needs using the same archetype's resources.
	// Mirrors what the load-driver emits: priority + bucketed penalties
	// + In requirements on instance-type and zone.
	allNeeds := make([]needs.Need, 0, 50)
	for i := 0; i < 50; i++ {
		// Pick a size from the same distribution.
		res := a.PickSize(rng)
		resQ := make([]needs.ResourceQty, 0, len(res))
		for k, v := range res {
			resQ = append(resQ, needs.ResourceQty{Name: k, Quantity: v})
		}
		profile := needs.NewProfile(
			[]needs.Requirement{
				{Key: "node.kubernetes.io/instance-type", Operator: needs.OperatorIn, Values: a.InstanceTypes},
				{Key: "topology.kubernetes.io/zone", Operator: needs.OperatorIn, Values: a.Zones},
			},
			resQ, nil,
			a.PriorityClasses[0],
			needs.PenaltyBucket1024,
			needs.PenaltyBucket8192,
		)
		allNeeds = append(allNeeds, needs.Need{
			ClusterID: "kwok-cluster-0",
			Profile:   profile,
			Count:     1,
			Group:     "pod-" + strconv.Itoa(i),
		})
	}

	r := decision.Phase1(inv.Snapshot(), allNeeds)
	t.Logf("emitted %d actions, %d unsatisfied (out of 50 needs, 100 idle seed)", len(r.Actions), len(r.Unsatisfied))

	// Probe MatchProfile directly to surface why take() returns nothing.
	if len(r.Actions) == 0 {
		t.Logf("---- DIAG ----")
		t.Logf("Need[0] profile.Requirements:")
		for _, req := range allNeeds[0].Profile.RequirementsRO() {
			t.Logf("  Key=%s Op=%v Values=%v", req.Key, req.Operator, req.Values)
		}
		t.Logf("Need[0] profile.Resources: %v", allNeeds[0].Profile.ResourcesRO())
		// Sample a few Idle machines
		idle := inv.Snapshot().ListByState(machine.StateIdle)
		t.Logf("Idle machines (showing 3 of %d):", len(idle))
		for i, m := range idle {
			if i >= 3 {
				break
			}
			t.Logf("  [%d] InstanceType=%s Zone=%s Resources=%v Labels=%v",
				i, m.Profile.InstanceType, m.Profile.Zone, m.Profile.Resources, m.Profile.Labels)
		}
		// Check matches
		matches := 0
		for _, m := range idle {
			if decision.MatchProfile(allNeeds[0].Profile, m) {
				matches++
			}
		}
		t.Logf("MatchProfile(Need[0]) matches %d / %d idle machines", matches, len(idle))
		t.Fatalf("Phase 1 emitted ZERO actions")
	}
}
