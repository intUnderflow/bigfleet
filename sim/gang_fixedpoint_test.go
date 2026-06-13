package sim_test

import (
	"fmt"
	"testing"

	"github.com/intUnderflow/bigfleet/sim"
)

// gangSweepCfg parameterizes the M77a field shape: two clusters whose
// gang demand rides ONE whole-machine instance type (a3-highgpu-8g),
// Same(rack) + Same(zone) gangs against a shared Idle/Speculative
// pool, static demand.
type gangSweepCfg struct {
	name string

	smallGangs   []int // Same(rack) gang sizes per cluster
	mediumGangs  []int // Same(zone) gang sizes per cluster
	smallCfg     int   // Configured seeded per cluster, small pool
	mediumCfg    int
	smallBlock   int // ContiguousRackBlock
	mediumBlock  int
	smallIdle    int
	mediumIdle   int
	smallSpec    int
	mediumSpec   int
	racksPerZone int
	mediumFirst  bool // declare medium workloads before small
	rollupStamps bool // per-rollup arrival stamps (wire-path fidelity)
}

func gangSweepScenario(cfg gangSweepCfg, cycles int) sim.ClosedLoopScenario {
	shapes := []sim.WorkloadShape{
		{
			Name:                       "gpu-small",
			PodResources:               map[string]string{"cpu": "64", "memory": "256Gi", "nvidia.com/gpu": "8"},
			InstanceTypes:              []string{"a3-highgpu-8g"},
			Zones:                      []string{"zone-a", "zone-b", "zone-c"},
			Priority:                   1000,
			InterruptionPenaltyDollars: 16384,
			ReclamationPenaltyDollars:  32768,
			SameRack:                   true,
		},
		{
			Name:                       "gpu-medium",
			PodResources:               map[string]string{"cpu": "64", "memory": "256Gi", "nvidia.com/gpu": "8"},
			InstanceTypes:              []string{"a3-highgpu-8g"},
			Zones:                      []string{"zone-a", "zone-b", "zone-c"},
			Priority:                   1000,
			InterruptionPenaltyDollars: 32768,
			ReclamationPenaltyDollars:  65536,
			SameZone:                   true,
		},
	}
	var workloads []sim.WorkloadSpec
	addSmall := func() {
		for _, n := range cfg.smallGangs {
			workloads = append(workloads, sim.WorkloadSpec{Shape: "gpu-small", Replicas: n})
		}
	}
	addMedium := func() {
		for _, n := range cfg.mediumGangs {
			workloads = append(workloads, sim.WorkloadSpec{Shape: "gpu-medium", Replicas: n})
		}
	}
	if cfg.mediumFirst {
		addMedium()
		addSmall()
	} else {
		addSmall()
		addMedium()
	}
	return sim.ClosedLoopScenario{
		Name:   cfg.name,
		Shapes: shapes,
		Clusters: []sim.ClusterSpec{
			{ID: "c1", Workloads: workloads},
			{ID: "c2", Workloads: workloads},
		},
		Seeds: []sim.SeedPool{
			{Shape: "gpu-small", Density: 1, ConfiguredPerCluster: cfg.smallCfg,
				Idle: cfg.smallIdle, Speculative: cfg.smallSpec,
				ContiguousRackBlock: cfg.smallBlock, RacksPerZone: cfg.racksPerZone},
			{Shape: "gpu-medium", Density: 1, ConfiguredPerCluster: cfg.mediumCfg,
				Idle: cfg.mediumIdle, Speculative: cfg.mediumSpec,
				ContiguousRackBlock: cfg.mediumBlock, RacksPerZone: cfg.racksPerZone},
		},
		ControllerManaged:   true,
		CRPerPod:            true,
		RollupArrivalStamps: cfg.rollupStamps,
		Cycles:              cycles,
	}
}

// TestGangFixedPointSweep is the diagnosis harness for the M77a gang
// oscillation: at static demand the trailing window must show zero
// Bootstrap and zero Reclaim growth. Sweeps candidate shapes.
func TestGangFixedPointSweep(t *testing.T) {
	const cycles, k = 120, 60
	base := gangSweepCfg{
		smallGangs:  []int{3, 3},
		mediumGangs: []int{6, 6},
		smallCfg:    6, mediumCfg: 12,
		smallBlock: 3, mediumBlock: 6,
		smallIdle: 3, mediumIdle: 3,
		smallSpec: 6, mediumSpec: 6,
		racksPerZone: 4,
	}

	cfgs := []gangSweepCfg{}
	add := func(name string, mut func(*gangSweepCfg)) {
		c := base
		c.name = name
		// deep-copy slices
		c.smallGangs = append([]int(nil), base.smallGangs...)
		c.mediumGangs = append([]int(nil), base.mediumGangs...)
		mut(&c)
		cfgs = append(cfgs, c)
	}

	add("base", func(c *gangSweepCfg) {})
	// Seed/demand mismatch per archetype (ADR-0044: seeds sized to
	// expected demand; draws differ).
	add("mismatch", func(c *gangSweepCfg) {
		c.smallGangs = []int{2, 4, 3}
		c.mediumGangs = []int{4, 7}
		c.smallCfg = 8
		c.mediumCfg = 12
		c.smallBlock = 4
		c.mediumBlock = 4
	})
	// Non-tiling blocks: rack blocks of 4 against gangs of 3.
	add("nontiling", func(c *gangSweepCfg) {
		c.smallGangs = []int{3, 3, 3}
		c.smallCfg = 9
		c.smallBlock = 4
		c.mediumGangs = []int{5, 6}
		c.mediumCfg = 11
		c.mediumBlock = 4
	})
	// Medium declared first (walk order).
	add("mediumfirst", func(c *gangSweepCfg) { c.mediumFirst = true })
	// More racks per zone: more domain choices.
	add("manyracks", func(c *gangSweepCfg) {
		c.racksPerZone = 8
		c.smallGangs = []int{2, 3, 4}
		c.smallCfg = 9
		c.smallBlock = 4
		c.mediumGangs = []int{4, 6, 8}
		c.mediumCfg = 18
		c.mediumBlock = 6
	})
	// Tight pool: idle/spec scarce relative to gang sizes.
	add("tightpool", func(c *gangSweepCfg) {
		c.smallIdle, c.mediumIdle = 2, 2
		c.smallSpec, c.mediumSpec = 2, 2
		c.smallGangs = []int{3, 4}
		c.smallCfg = 6
		c.smallBlock = 4
		c.mediumGangs = []int{5, 6}
		c.mediumCfg = 12
		c.mediumBlock = 4
	})
	// Big pool: idle 1.2x / spec 5x like dev-50.
	add("bigpool", func(c *gangSweepCfg) {
		c.smallGangs = []int{2, 4, 3}
		c.mediumGangs = []int{4, 7}
		c.smallCfg = 8
		c.mediumCfg = 12
		c.smallBlock = 4
		c.mediumBlock = 4
		c.smallIdle, c.mediumIdle = 5, 5
		c.smallSpec, c.mediumSpec = 20, 20
		c.racksPerZone = 6
	})
	// Field-faithful: Same(zone) Configured machines round-robin across
	// zones (seedZoneRack blocks only SameRack archetypes), Configured
	// excess over drawn demand (ADR-0044 sizes by expectation), big
	// idle/spec pools (1.2x headroom + 5x speculative + per-zone gang
	// floors), 10 racks per zone.
	add("field", func(c *gangSweepCfg) {
		c.smallGangs = []int{2, 4, 3, 2, 4, 3} // 18 machines
		c.mediumGangs = []int{4, 8, 6, 5}      // 23 machines
		c.smallCfg, c.smallBlock = 24, 4       // excess 6
		c.mediumCfg, c.mediumBlock = 27, 0     // excess 4, round-robin
		c.smallIdle, c.mediumIdle = 12, 24     // per-zone floors
		c.smallSpec, c.mediumSpec = 24, 40     // ~5x
		c.racksPerZone = 10
	})
	// Field + wire-path rollup arrival stamps: the cross-cluster walk
	// order flips cycle to cycle as the two operators' rollup
	// timestamps race (conv.NeedsFromRollup stamps every row).
	add("field-arrival", func(c *gangSweepCfg) {
		c.smallGangs = []int{2, 4, 3, 2, 4, 3}
		c.mediumGangs = []int{4, 8, 6, 5}
		c.smallCfg, c.smallBlock = 24, 4
		c.mediumCfg, c.mediumBlock = 27, 0
		c.smallIdle, c.mediumIdle = 12, 24
		c.smallSpec, c.mediumSpec = 24, 40
		c.racksPerZone = 10
		c.rollupStamps = true
	})
	// Arrival stamps on the plain aligned base shape: is order flip
	// alone enough, or does it need the field's loose supply?
	add("base-arrival", func(c *gangSweepCfg) { c.rollupStamps = true })
	// Full field-scale a3 economy, ADR-0044 arithmetic on the dev-50
	// numbers (305 Configured/cluster, 732 idle, 3050 spec; a3 shares:
	// small 24/182.6, medium 36/182.6): per cluster ~40 small / ~60
	// medium Configured vs ~36 / ~53 drawn; idle 96/144; spec 401/601.
	// Acquirable totals dominate creditable in every domain — the rule 3
	// knife edge.
	add("fieldscale", func(c *gangSweepCfg) {
		c.smallGangs = []int{2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3} // 36
		c.mediumGangs = []int{4, 8, 6, 5, 7, 4, 6, 8, 5}         // 53
		c.smallCfg, c.smallBlock = 40, 4
		c.mediumCfg, c.mediumBlock = 60, 0 // round-robin zones
		c.smallIdle, c.mediumIdle = 96, 144
		c.smallSpec, c.mediumSpec = 401, 601
		c.racksPerZone = 10
		c.rollupStamps = true
	})
	// Same, stamps off: isolates the cross-cluster order-flip term.
	add("fieldscale-nostamp", func(c *gangSweepCfg) {
		c.smallGangs = []int{2, 4, 3, 2, 4, 3, 2, 4, 3, 2, 4, 3}
		c.mediumGangs = []int{4, 8, 6, 5, 7, 4, 6, 8, 5}
		c.smallCfg, c.smallBlock = 40, 4
		c.mediumCfg, c.mediumBlock = 60, 0
		c.smallIdle, c.mediumIdle = 96, 144
		c.smallSpec, c.mediumSpec = 401, 601
		c.racksPerZone = 10
	})

	for _, cfg := range cfgs {
		cfg := cfg
		t.Run(cfg.name, func(t *testing.T) {
			res := runClosedLoop(t, gangSweepScenario(cfg, cycles))
			boots := res.SumLast(k, func(c sim.CycleStats) int { return c.Bootstraps + c.Provisions })
			recls := res.SumLast(k, func(c sim.CycleStats) int { return c.Reclaims })
			activeCycles := 0
			for _, c := range res.Last(k) {
				if c.Churn() > 0 {
					activeCycles++
				}
			}
			end := res.Last(1)[0]
			t.Logf("%s: trailing %d cycles: acq=%d recl=%d activeCycles=%d shortfalls=%d configured=%d",
				cfg.name, k, boots, recls, activeCycles, end.Shortfalls, end.Configured)
			if boots+recls > 0 {
				t.Logf("OSCILLATION reproduced in %q", cfg.name)
			}
			dumpTrace(t, res)
		})
	}
	_ = fmt.Sprintf
}
