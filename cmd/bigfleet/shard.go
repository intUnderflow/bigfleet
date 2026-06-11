package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/api/resource"

	"math"
	"math/rand"

	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/grpcutil"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
	"github.com/intUnderflow/bigfleet/pkg/scaletest/preflight"
	"github.com/intUnderflow/bigfleet/pkg/shard"
	"github.com/intUnderflow/bigfleet/pkg/shard/coordclient"
)

// runFailureInjector is M38's spot-reclaim / hardware-fault simulator.
//
// Each tick (1 s) computes expected_failures = ratePerSec ×
// ConfiguredCount(), draws Poisson(expected), and fails that many
// distinct random Configured machines.
//
// ADR-0019 (M44.4) rewrote this from a 32-sample heuristic that
// failed sampled machines with probability == ratePerSec — which at
// the realistic 1.16e-8 to 1.16e-7 per-second-per-machine rates
// would never fire (32 × 1.16e-8 × 1800 ≈ 6.7e-4 expected over a
// 30-min run). The Failed counts seen in the M44.3 / scaleway-50k
// runs (478, 611) were not from this injector; they were Bootstrap
// timeout transitions from pkg/shard/execute.go. Tracked separately.
//
// Exits when ctx is cancelled.
func runFailureInjector(ctx context.Context, prov *fake.Provider, ratePerSec float64, logger *slog.Logger) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	totalFailed := 0
	for {
		select {
		case <-ctx.Done():
			logger.Info("failure injector exiting", "total_failed", totalFailed)
			return
		case <-tick.C:
			n := prov.ConfiguredCount()
			if n == 0 {
				continue
			}
			expected := ratePerSec * float64(n)
			k := poissonDraw(rng, expected)
			for i := 0; i < k; i++ {
				id := prov.RandomConfiguredMachine()
				if id == "" {
					break
				}
				if prov.FailMachine(id, "scaletest M38: simulated spot reclaim") {
					totalFailed++
				}
			}
		}
	}
}

// poissonDraw returns a sample from Poisson(λ). Knuth's algorithm
// is fine for λ ≤ ~30 (each iteration draws one Float64). For the
// realistic-fleet rates × 30-min soaks × 60K Configured we expect
// λ in the 0.1–10 range; well inside the comfort zone.
func poissonDraw(rng *rand.Rand, lambda float64) int {
	if lambda <= 0 {
		return 0
	}
	L := math.Exp(-lambda)
	k := 0
	p := 1.0
	for {
		k++
		p *= rng.Float64()
		if p <= L {
			return k - 1
		}
	}
}

// parseStatefulSetOrdinal extracts the numeric suffix from a
// StatefulSet pod name (e.g. "bigfleet-shard-2" → 2). Used by the
// Configured-seed path to know which slice of the kwok cluster space
// this shard owns under the harness's `c % shardReplicas` mapping.
func parseStatefulSetOrdinal(podName string) (int, error) {
	for i := len(podName) - 1; i >= 0; i-- {
		if podName[i] == '-' {
			suffix := podName[i+1:]
			if suffix == "" {
				return 0, fmt.Errorf("empty ordinal suffix")
			}
			n, err := strconv.Atoi(suffix)
			if err != nil {
				return 0, fmt.Errorf("ordinal suffix %q: %w", suffix, err)
			}
			return n, nil
		}
	}
	return 0, fmt.Errorf("no '-' separator in pod name")
}

// seedFakeInventory mints n synthetic idle machines into the in-process
// fake provider AND seeds them into the shard's inventory so Phase 1
// can pick from them on the first cycle. Used by the scale-test
// harness to populate the shard with a realistic-shape pool without a
// real provider.
//
// Spread: round-robin across 5 instance types × 3 zones × bare-metal
// capacity type. Profile fingerprints are stable so the per-fingerprint
// pool cache (M11.16) sees real diversity instead of one giant bucket.
//
// Three tiers are seeded (ADR-0026 — the harness must model the whole
// capacity model, not just the owned half):
//   - nIdle Idle machines — real, owned hardware (CapacityType BareMetal,
//     price 0). Phase 1's preferred, cheapest candidate pool.
//   - nSpeculative Speculative slots — elastic procurement quota
//     (CapacityType OnDemand, non-zero price). Phase 1's "Create +
//     bootstrap" fallback when Idle can't satisfy a Need. Drawn from the
//     demand catalog so the elastic pool spans what workloads ask for.
//   - the Configured seed below — workloads already running.
//
// Configured seed (M28): if both clusterStride > 0 and
// nConfiguredPerCluster > 0, the seed enumerates only the kwok
// cluster IDs the harness's deterministic
// `kwok-cluster-N → bigfleet-shard-(N % shardReplicas)` mapping
// routes to THIS shard. shardOrdinal is the shard's index within its
// StatefulSet (parsed from --id, e.g. "bigfleet-shard-2" → 2);
// clusterStride is the total shardReplicas count. The seed iterates
// cluster IDs c ∈ [0, totalClusters) where c % clusterStride ==
// shardOrdinal — the same set the kwok pods would dial. Configured
// machines created this way have demand-bearing cluster bindings
// the harness's load-driver actually targets, so Phase 3 sees them
// as "wanted" machines (no reclaim) and Phase 2 sees them as
// potential preemption victims if a higher-priority demand arrives.
//
// totalClusters is the harness's kwok.clusterCount (NOT divided by
// shardReplicas). Setting any of {nConfiguredPerCluster, clusterStride,
// totalClusters, shardOrdinal} to its zero value disables the
// Configured seed.
// scaleResourceMap multiplies each quantity in `in` by `factor` using
// k8s resource.Quantity arithmetic so the canonical-string format the
// rest of the system expects is preserved. factor <= 1 returns a
// shallow copy unchanged (preserves the pre-ADR-0022 1:1 behaviour).
func scaleResourceMap(in map[string]string, factor int) map[string]string {
	if len(in) == 0 || factor <= 1 {
		return cloneResourceMap(in)
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		// M66.2 (complexity audit Action 2): only the compressible
		// core resources scale with density. Extended resources —
		// nvidia.com/gpu above all — are physical device counts: a
		// density-100 seed multiplying gpu:8 to gpu:800 manufactured
		// machines no hardware resembles, silently perturbed
		// ADR-0041's snapshot-dependent foldability (gang aggregates
		// "fit" phantom capacity), and contradicted the cloud
		// harness's empirically ~8-GPU nodes.
		if k != "cpu" && k != "memory" && k != "ephemeral-storage" {
			out[k] = v
			continue
		}
		q, err := resource.ParseQuantity(v)
		if err != nil {
			// Unparseable: leave the original — Phase 1's PodsPerMachine
			// will return 0 for that machine and the seed effectively
			// can't host the workload, which is the conservative answer.
			out[k] = v
			continue
		}
		scaled := q.DeepCopy()
		scaled.Mul(int64(factor))
		out[k] = scaled.String()
	}
	return out
}

func cloneResourceMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// seedZoneRack returns the zone and the topology.bigfleet/rack label
// for one seeded machine. a may be nil (the legacy no-catalog seed
// shape); idx is a per-archetype running counter within one seed loop
// for archetype machines, or the loop index for the nil-archetype
// fallback; zones is the candidate zone list (the archetype's, or the
// harness default), with "zone-a" as the empty-list fallback.
//
// Non-sameRack machines round-robin both zone and rack — the spread
// that gives the per-fingerprint pool cache real diversity.
//
// sameRack archetypes are placed in contiguous rack blocks instead
// (ADR-0040 Addendum §4): block size = the archetype's max group size
// (GroupSizeRange[1], fallback 8 when unset), and the whole block
// shares one (zone, rack) so a single rack can host a whole
// co-location gang. The zone is derived from the block — not the
// per-machine index — because the rack label embeds the zone;
// rotating zones per machine would fragment a block into per-zone
// slivers. Round-robin left ~1-3 co-located machines per rack against
// gangs of 3-8: demand that is physically unsatisfiable regardless of
// attribution, which real fleets avoid by procuring co-located
// capacity in rack units.
func seedZoneRack(a *archetype.Archetype, idx, racksPerZone int, zones []string) (string, string) {
	slot := idx
	if a != nil && a.SameRack {
		block := a.GroupSizeRange[1]
		if block <= 0 {
			block = 8
		}
		slot = idx / block
	}
	zone := "zone-a"
	if len(zones) > 0 {
		zone = zones[slot%len(zones)]
	}
	return zone, fmt.Sprintf("%s-rack-%d", zone, slot%racksPerZone)
}

// archetypeZones is the zone count MachineAllocation's per-zone gang
// floor multiplies by (ADR-0044 §3).
func archetypeZones(a *archetype.Archetype) int { return len(a.Zones) }

func sumInts(xs []int) int {
	total := 0
	for _, x := range xs {
		total += x
	}
	return total
}

func seedFakeInventory(prov *fake.Provider, sh *shard.Shard, nIdle, nSpeculative, nConfiguredPerCluster, totalClusters, clusterStride, shardOrdinal, densityMultiplier int, archetypes, demandArchetypes []archetype.Archetype, logger *slog.Logger) {
	// The no-catalog rotation tables live in pkg/scaletest/preflight so
	// the matching-capacity preflight models exactly the shapes seeded
	// here (M60: the dev-50 stall happened because these tables and the
	// load-driver's demand shape lived in two package mains nothing
	// could cross-check).
	types := preflight.LegacyInstanceTypes
	zones := []string{"zone-a", "zone-b", "zone-c"}
	resources := preflight.LegacyResources

	logger.Info("seeding fake inventory",
		"idle", nIdle,
		"configured_per_cluster", nConfiguredPerCluster,
		"total_clusters", totalClusters,
		"cluster_stride", clusterStride,
		"shard_ordinal", shardOrdinal,
	)
	// M42 (Item 9): Idle seed mirrors the archetype's PickSize
	// distribution so MatchProfile-with-exact-resource-equality has
	// machines that match what the load-driver requests. Without
	// this the idle pool declares only "instance-type maxed out"
	// resources (e.g. c6i.4xlarge → {cpu: 16}) and a CR asking
	// {cpu: 4} fails MatchProfile against every idle, leaving Phase 1
	// silently unable to bind.
	//
	// ADR-0044: per-archetype machine counts derive from machine-demand
	// shares (weight × E[replicas] / podsPerMachine), not raw weight —
	// a whole-machine (extended-resource) archetype needs ~density×
	// more machines per pod than a cpu-shaped one, and weight-
	// proportional pools starved it by exactly that factor. Gang
	// archetypes are floored at max(GroupSizeRange) per zone, on top of
	// the share split, so the largest drawable gang is satisfiable by
	// construction. The same allocation shapes all three tiers so Idle
	// and Configured see the same archetype mix at fold time.
	idleRng := rand.New(rand.NewSource(int64(shardOrdinal) + 17))
	// M44.4 Drop E: Idle seed carries a topology.bigfleet/rack label so
	// MatchProfile can satisfy the Same(rack) requirement the operator
	// emits for sameRack-archetype Needs. ADR-0024: Same is now derived
	// from the source pod's podAffinity, so only sameRack Pods carry it
	// — but rack-labelled Idle machines are still needed as their
	// candidates, else those Needs never emit a Bootstrap. 10 racks per
	// zone matches the Configured seed. Rack assignment is per-archetype
	// (seedZoneRack): sameRack archetypes in contiguous blocks, the
	// rest round-robin (ADR-0040 Addendum §4).
	const idleRacksPerZone = 10
	idleRackCounters := map[string]int{}
	idleSeeded := 0
	addIdle := func(profile machine.Profile) {
		id := machine.ID("idle-" + strconv.Itoa(idleSeeded))
		prov.AddIdle(id, profile, machine.CapacityTypeBareMetal, 0, 0)
		// ADR-0022 / M45.4: Allocatable = densityMultiplier × per-replica
		// Profile.Resources. EffectiveAllocatable() falls back to
		// Profile.Resources when this is nil, so densityMultiplier ≤ 1
		// is a no-op (preserves pre-ADR-0022 1 Pod = 1 machine math).
		allocatable := scaleResourceMap(profile.Resources, densityMultiplier)
		prov.SetAllocatable(id, allocatable)
		_ = sh.SeedInventory(machine.Machine{
			ID:          id,
			State:       machine.StateIdle,
			Profile:     profile,
			Allocatable: allocatable,
		})
		idleSeeded++
	}
	if len(archetypes) > 0 {
		alloc := archetype.MachineAllocation(archetypes, densityMultiplier, nIdle, archetypeZones)
		if total := sumInts(alloc); total > nIdle {
			logger.Info("seed: gang floors raised idle machines", "requested", nIdle, "seeding", total)
		}
		for ai := range archetypes {
			a := &archetypes[ai]
			for n := 0; n < alloc[ai]; n++ {
				it := a.InstanceTypes[idleSeeded%len(a.InstanceTypes)]
				z, rack := seedZoneRack(a, idleRackCounters[a.Name], idleRacksPerZone, a.Zones)
				idleRackCounters[a.Name]++
				labels := map[string]string{
					"topology.bigfleet/rack": rack,
				}
				for k, v := range a.PickLabels(idleRng) {
					labels[k] = v
				}
				addIdle(machine.Profile{
					InstanceType: it,
					Zone:         z,
					CapacityType: machine.CapacityTypeBareMetal,
					Resources:    a.PickSize(idleRng),
					Labels:       labels,
				})
			}
		}
	} else {
		for i := 0; i < nIdle; i++ {
			t := types[i%len(types)]
			z, rack := seedZoneRack(nil, i, idleRacksPerZone, zones)
			addIdle(machine.Profile{
				InstanceType: t,
				Zone:         z,
				CapacityType: machine.CapacityTypeBareMetal,
				Resources:    resources[t],
				Labels: map[string]string{
					"topology.bigfleet/rack": rack,
				},
			})
		}
	}

	// ADR-0026: Speculative tier — elastic procurement quota. Without
	// it, Phase 1's "fall back to Speculative (Create + bootstrap)"
	// path is dead in the harness and any demand the fixed Idle seed
	// can't directly satisfy becomes a permanent shortfall — which is
	// not how the paper's BigFleet behaves. Drawn from the *demand*
	// archetype catalog (a cloud's available capacity realistically
	// spans what workloads ask for, not what's already running);
	// CapacityType OnDemand with a non-zero price so effective_cost is
	// meaningful and Phase 1 correctly prefers the cheaper owned Idle
	// tier. Per-shape pricing is a future refinement; a flat price is
	// enough to make the tier exist and be priced.
	const (
		specPricePerHour  = 1.0
		specInterruptProb = 0.05
		specRacksPerZone  = 10
	)
	specArches := demandArchetypes
	if len(specArches) == 0 {
		// No separate demand catalog (profile didn't split seed/demand):
		// the seed catalog is also the demand catalog.
		specArches = archetypes
	}
	specRng := rand.New(rand.NewSource(int64(shardOrdinal) + 31))
	specRackCounters := map[string]int{}
	specSeeded := 0
	addSpeculative := func(profile machine.Profile) {
		id := machine.ID("spec-" + strconv.Itoa(specSeeded))
		prov.AddSpeculative(id, profile, machine.CapacityTypeOnDemand, specPricePerHour, specInterruptProb)
		// ADR-0022 / M45.4: Allocatable = densityMultiplier × per-replica
		// Profile.Resources, same as the Idle seed — a realized
		// Speculative slot becomes an Idle machine of this density.
		allocatable := scaleResourceMap(profile.Resources, densityMultiplier)
		prov.SetAllocatable(id, allocatable)
		_ = sh.SeedInventory(machine.Machine{
			ID:                      id,
			State:                   machine.StateSpeculative,
			Profile:                 profile,
			Allocatable:             allocatable,
			PricePerHour:            specPricePerHour,
			InterruptionProbability: specInterruptProb,
		})
		specSeeded++
	}
	if len(specArches) > 0 {
		// ADR-0044: machine-demand-share allocation, same as the Idle
		// tier above.
		alloc := archetype.MachineAllocation(specArches, densityMultiplier, nSpeculative, archetypeZones)
		if total := sumInts(alloc); total > nSpeculative {
			logger.Info("seed: gang floors raised speculative slots", "requested", nSpeculative, "seeding", total)
		}
		for ai := range specArches {
			a := &specArches[ai]
			for n := 0; n < alloc[ai]; n++ {
				it := a.InstanceTypes[specSeeded%len(a.InstanceTypes)]
				z, rack := seedZoneRack(a, specRackCounters[a.Name], specRacksPerZone, a.Zones)
				specRackCounters[a.Name]++
				labels := map[string]string{
					"topology.bigfleet/rack": rack,
				}
				// Mirror the Configured seed (M52.B): tag the Speculative
				// slot with its archetype so a fake-Node minted from a
				// Provisioned slot is categorisable by the same label the
				// Configured seed uses. Without it, Provision output lands
				// in a `<no value>` archetype bucket for any diagnostic.
				labels["scaletest.bigfleet/archetype"] = a.Name
				for k, v := range a.PickLabels(specRng) {
					labels[k] = v
				}
				addSpeculative(machine.Profile{
					InstanceType: it,
					Zone:         z,
					CapacityType: machine.CapacityTypeOnDemand,
					Resources:    a.PickSize(specRng),
					Labels:       labels,
				})
			}
		}
	} else {
		for i := 0; i < nSpeculative; i++ {
			t := types[i%len(types)]
			z, rack := seedZoneRack(nil, i, specRacksPerZone, zones)
			addSpeculative(machine.Profile{
				InstanceType: t,
				Zone:         z,
				CapacityType: machine.CapacityTypeOnDemand,
				Resources:    resources[t],
				Labels: map[string]string{
					"topology.bigfleet/rack": rack,
				},
			})
		}
	}

	configuredSeeded := 0
	if nConfiguredPerCluster > 0 && totalClusters > 0 && clusterStride > 0 {
		// Configured-seed: bound to "kwok-cluster-N" cluster IDs that
		// the harness's `N % shardReplicas` mapping routes to THIS
		// shard. Each owned cluster gets nConfiguredPerCluster machines.
		//
		// M31: when an archetype catalog is provided, machines are
		// distributed across archetypes by ADR-0044 machine-demand
		// share (see the Idle tier above), per cluster. Profile
		// (instance-type, zone, resources) and assigned penalties come
		// from the archetype; AssignedPriority is the top of the
		// archetype's PriorityClasses (the seed represents established
		// workloads at the top of their priority tier — burst demand
		// at lower priorities can't preempt them, only equal-tier
		// demand competes for capacity). When the catalog is empty,
		// fall back to the legacy single shape: every machine is
		// preflight.LegacyDemandInstanceType at priority 1000000,
		// matching the load-driver's legacy Pod template.
		rng := rand.New(rand.NewSource(int64(shardOrdinal) + 1))
		// ADR-0015 §4: synthetic rack pool for `Same` workloads.
		// 10 racks per zone matches a typical AZ-of-cells deployment.
		// Configured machines get a deterministic rack label drawn
		// from the pool so the load-driver's Same requirement has
		// enough variety to schedule against. Rack assignment is
		// per-archetype (seedZoneRack): sameRack archetypes in
		// contiguous blocks, the rest round-robin (ADR-0040
		// Addendum §4).
		const racksPerZone = 10
		rackCounters := map[string]int{}
		idx := 0
		var confAlloc []int
		if len(archetypes) > 0 {
			confAlloc = archetype.MachineAllocation(archetypes, densityMultiplier, nConfiguredPerCluster, archetypeZones)
			if total := sumInts(confAlloc); total > nConfiguredPerCluster {
				logger.Info("seed: gang floors raised configured machines", "requested_per_cluster", nConfiguredPerCluster, "seeding_per_cluster", total)
			}
		}
		for c := shardOrdinal; c < totalClusters; c += clusterStride {
			cluster := machine.ClusterID("kwok-cluster-" + strconv.Itoa(c))
			perCluster := 0
			addConfigured := func(profile machine.Profile, assignedPriority int32, interruptionPenalty, reclamationPenalty float64) {
				id := machine.ID(fmt.Sprintf("conf-s%d-c%d-i%d", shardOrdinal, c, perCluster))
				prov.AddConfigured(id, profile, machine.CapacityTypeBareMetal, 0, 0, cluster, assignedPriority, interruptionPenalty, reclamationPenalty)
				// ADR-0022 / M45.4: see Idle seed comment above.
				allocatable := scaleResourceMap(profile.Resources, densityMultiplier)
				prov.SetAllocatable(id, allocatable)
				_ = sh.SeedInventory(machine.Machine{
					ID:                                 id,
					State:                              machine.StateConfigured,
					Cluster:                            cluster,
					Profile:                            profile,
					Allocatable:                        allocatable,
					AssignedPriority:                   assignedPriority,
					AssignedInterruptionPenaltyDollars: interruptionPenalty,
					AssignedReclamationPenaltyDollars:  reclamationPenalty,
				})
				idx++
				perCluster++
				configuredSeeded++
			}
			if len(archetypes) > 0 {
				for ai := range archetypes {
					a := &archetypes[ai]
					for n := 0; n < confAlloc[ai]; n++ {
						it := a.InstanceTypes[idx%len(a.InstanceTypes)]
						z, rackName := seedZoneRack(a, rackCounters[a.Name], racksPerZone, a.Zones)
						rackCounters[a.Name]++
						// ADR-0015 §1: per-machine resources picked from
						// SizeBuckets when present (production-shape
						// fingerprint multiplicity); falls back to the
						// flat Resources map otherwise.
						machineResources := a.PickSize(rng)
						labels := map[string]string{}
						// Every machine carries a rack label; Same-aware
						// archetypes request it via Exists / Same. Other
						// archetypes don't see it.
						labels["topology.bigfleet/rack"] = rackName
						// M52.B (ADR-0035): tag every seeded machine with
						// its archetype so the load-driver's pre-bind phase
						// can look up the archetype from the fake-Node's
						// labels and build a matching Pod.
						labels["scaletest.bigfleet/archetype"] = a.Name
						// M35 / Item 2: per-axis label values matching the
						// archetype's LabelAxes spec. The load-driver emits
						// CRs with `In [value]` requirements drawn from the
						// same axes, so MatchProfile passes for ~1/Count of
						// seeded machines per CR — production-shaped
						// fingerprint cardinality.
						for k, v := range a.PickLabels(rng) {
							labels[k] = v
						}
						addConfigured(machine.Profile{
							InstanceType: it,
							Zone:         z,
							CapacityType: machine.CapacityTypeBareMetal,
							Resources:    machineResources,
							Labels:       labels,
						}, a.MaxPriority(), a.InterruptionPenalty, a.ReclamationPenalty)
					}
				}
			} else {
				for i := 0; i < nConfiguredPerCluster; i++ {
					it := preflight.LegacyDemandInstanceType
					addConfigured(machine.Profile{
						InstanceType: it,
						Zone:         zones[idx%len(zones)],
						CapacityType: machine.CapacityTypeBareMetal,
						Resources:    resources[it],
					}, 1000000, 8192, 65536)
				}
			}
		}
	}
	logger.Info("seed complete", "idle", idleSeeded, "speculative", specSeeded, "configured", configuredSeeded, "archetypes", len(archetypes))
}

// runShard runs the shard controller. The in-process fake provider
// lives at pkg/provider/fake and is used here for laptop / kind / dev
// installs only; production deployments dial an out-of-tree provider
// over gRPC via pkg/provider/grpcadapter (see provider-author-guide).
func runShard(args []string) error {
	fs := flag.NewFlagSet("shard", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	listen := fs.String("listen", ":7780", "address to listen on for the Shard.Session gRPC service")
	metricsAddr := fs.String("metrics-addr", ":8780", "address for the Prometheus /metrics endpoint (\"0\" disables)")
	shardID := fs.String("id", "shard-0", "this shard's stable identifier")
	dataDir := fs.String("data-dir", "./data", "directory for shard-local persistent state (epoch counter)")
	seedMachines := fs.Int("seed-machines", 0, "scaletest: pre-seed the in-process fake provider with N synthetic idle machines spread across instance types and zones; 0 disables")
	seedSpeculative := fs.Int("seed-speculative", 0, "scaletest ADR-0026: pre-seed N synthetic Speculative quota slots — the elastic-procurement tier Phase 1 falls back to (Create + bootstrap) when Idle can't satisfy a Need. Drawn from the demand archetype catalog, CapacityType OnDemand with a non-zero price. Every profile that seeds Idle should also set this; without it the harness models only the owned-capacity half of BigFleet's design. 0 disables.")
	seedConfiguredPerCluster := fs.Int("seed-configured-per-cluster", 0, "scaletest M29: pre-seed the in-process fake provider with N synthetic Configured machines per kwok cluster owned by this shard (cluster IDs of the form kwok-cluster-{c} where c % --seed-cluster-stride == this shard's ordinal). Models the production-realistic shape where most fleet inventory is running workloads. Combined with --seed-cluster-total + --seed-cluster-stride.")
	seedClusterTotal := fs.Int("seed-cluster-total", 0, "scaletest M29: total number of kwok clusters across the whole harness (i.e. kwok.clusterCount). Used by the Configured-seed loop along with --seed-cluster-stride to pick the cluster IDs this shard owns.")
	seedClusterStride := fs.Int("seed-cluster-stride", 0, "scaletest M29: total number of shard replicas in the harness (i.e. shard.replicas). The seed enumerates clusters c where c % stride == this shard's ordinal. 0 disables the Configured seed.")
	archetypesPath := fs.String("archetypes", "", "scaletest M31: path to a workload-archetype catalog YAML. When set, the seed distributes machines across archetypes by ADR-0044 machine-demand share — weight × E[replicas] / podsPerMachine, with a per-zone floor of max(groupSizeRange) for gang archetypes (instance-type, zone, resources, priority and penalties from each archetype). Gang floors may push seeded totals above the configured counts. When empty, the seed falls back to the legacy single shape (preflight.LegacyDemandInstanceType, matching the load-driver's legacy Pod template). Both this flag and the load-driver's archetypes reference must point at the same file so demand and Configured match.")
	failureRatePerSec := fs.Float64("failure-rate-per-sec", 0, "scaletest M38: per-second probability (per Configured machine) of an unsolicited provider failure (spot reclaim / hardware fault). 0 disables. Real fleets see ~0.1-1%/day; that maps to ~1.16e-8 to 1.16e-7 per second per machine. The injector runs in a background goroutine, picks a random Configured machine each tick, and transitions it to Failed via the fake provider. Exercises the shard's transitional-state-recovery + drain-grace paths under load.")
	seedDensityMultiplier := fs.Int("seed-density-multiplier", 1, "scaletest: seed each fake-inventory machine with Allocatable = N × Profile.Resources, so one machine has the real capacity of N replicas of its Profile (CPU services pack ~10/machine, GPU inference ~8/machine). N=1 keeps the legacy 1 Pod = 1 machine math. Under ADR-0027's resource-vector model this is just per-machine capacity — Phase 1's `creditExistingSupply` and `take` both diff against the machine's true Allocatable; there is no longer a per-Pod density reconstruction step that needs the multiplier to be honoured separately.")
	maxActionsPerCycle := fs.Int("max-actions-per-cycle", 0, "cap total decision actions executed per cycle so a ramp burst doesn't blow past the cycle SLO; 0 = unlimited (production default). Surplus actions roll into the next cycle.")
	executeConcurrency := fs.Int("execute-concurrency", 1, "max parallel action executors per cycle. 1 = serial (historical default). Bootstrap actions wait on per-cluster gRPC RTTs; raise for ramp-burst workloads.")
	localBootstrap := fs.Bool("local-bootstrap", false, "scaletest: render bootstrap blobs locally instead of round-tripping through the operator stream. Decouples shard cycle benchmarks from cluster-stream RTT. Production must leave this false.")
	incrementalReconcile := fs.Bool("incremental-reconcile", false, "opt into delta-only provider.List polling using the SinceRevision cursor. Off = full List every cycle (works for any provider). On = only enable for providers that honour since_revision (plan §10.6 above-conformance-threshold).")
	availableCapacityInterval := fs.Duration("available-capacity-interval", 0, "minimum interval between AvailableCapacityUpdate emits per (cluster, fingerprint). 0 = use the shard's default (5s). Below the cycle interval is wasteful (operator-side apiserver writes); much above 30s starts to feel stale to humans watching `kubectl get availablecapacity`.")
	metricsWarmupCycles := fs.Int("metrics-warmup-cycles", 0, "skip cycle-duration + per-phase histogram observations for the first N cycles. Cycle 1 of any shard does a one-time full provider.List that is not representative of steady-state cycle cost; skipping it lets p99 reflect what the SLO actually measures. Counters are not affected.")
	phaseAttributionLog := fs.Bool("phase-attribution-log", false, "ADR-0040 diagnostic: every 20th cycle, log one line with Need counts split by co-location, Phase 1 unsatisfied split by co-location, Phase 3 reclaims, and how many reclaimed machines matched (and fit) a Need Phase 1 declared unsatisfied the same cycle — the probe that found the Same-domain crediting defect. Read-only; also enabled by BIGFLEET_PHASEDUMP=1. Off by default.")
	coordinatorAddr := fs.String("coordinator-addr", "", "host:port of the coordinator's gRPC service. When set, the shard heartbeats to ReportShard so it appears in coordinator membership and can have domains assigned to it. Empty disables — single-shard / dev runs without a coordinator stay unaffected.")
	advertiseAddr := fs.String("advertise-addr", "", "host:port the coordinator should record as this shard's dial address. Defaults to --listen; in StatefulSet deploys, set to the per-pod headless-Service DNS, e.g. bigfleet-shard-0.bigfleet-shard-headless:7780.")
	heartbeatInterval := fs.Duration("coordinator-heartbeat-interval", 10*time.Second, "how often to send ReportShard to the coordinator. Below ~5s starts spamming Raft applies on registration churn; above ~30s makes coordinator-side LastHeartbeat staleness alarms fire on healthy shards.")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errFlagParse, err)
	}
	if *listen == "" {
		return errors.New("--listen is required")
	}
	if *advertiseAddr == "" {
		*advertiseAddr = *listen
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return fmt.Errorf("data-dir: %w", err)
	}
	epoch, err := fencing.LoadEpoch(filepath.Join(*dataDir, "epoch"))
	if err != nil {
		return err
	}

	// In-memory fake provider. M5 swaps this for a real gRPC client
	// adapter once an out-of-tree provider is available.
	prov := fake.New(fake.Options{InstantTransitions: true})

	cfg := shard.Config{
		ID:                        *shardID,
		Epoch:                     epoch,
		Provider:                  prov,
		Logger:                    logger,
		MaxActionsPerCycle:        *maxActionsPerCycle,
		ExecuteConcurrency:        *executeConcurrency,
		IncrementalReconcile:      *incrementalReconcile,
		AvailableCapacityInterval: *availableCapacityInterval,
		MetricsWarmupCycles:       *metricsWarmupCycles,
		PhaseAttributionLog:       *phaseAttributionLog || os.Getenv("BIGFLEET_PHASEDUMP") == "1",
	}
	if *localBootstrap {
		cfg.LocalBootstrap = func(_ context.Context, cluster machine.ClusterID, _ []needs.Requirement) ([]byte, error) {
			return []byte("# bigfleet scaletest stub bootstrap for " + string(cluster) + "\n"), nil
		}
	}
	sh, err := shard.New(cfg)
	if err != nil {
		return err
	}

	configuredEnabled := *seedConfiguredPerCluster > 0 && *seedClusterTotal > 0 && *seedClusterStride > 0
	if *seedMachines > 0 || *seedSpeculative > 0 || configuredEnabled {
		shardOrdinal := 0
		if configuredEnabled {
			ord, err := parseStatefulSetOrdinal(*shardID)
			if err != nil {
				return fmt.Errorf("--seed-configured-per-cluster requires --id ending in -N (got %q): %w", *shardID, err)
			}
			shardOrdinal = ord
		}
		var arches, demandArches []archetype.Archetype
		if *archetypesPath != "" {
			cat, err := archetype.LoadCatalog(*archetypesPath)
			if err != nil {
				return fmt.Errorf("--archetypes: %w", err)
			}
			// M34: prefer the seed-specific list when the catalog
			// provides one; otherwise the legacy single Archetypes
			// list is used for both seed and demand. ADR-0026: the
			// Speculative seed draws from the demand list — the
			// provider's elastic capacity spans what workloads ask for.
			arches = cat.ForSeed()
			demandArches = cat.ForDemand()
			logger.Info("archetype catalog loaded", "path", *archetypesPath, "seed_count", len(arches), "demand_count", len(demandArches))
		}
		seedFakeInventory(prov, sh, *seedMachines, *seedSpeculative, *seedConfiguredPerCluster, *seedClusterTotal, *seedClusterStride, shardOrdinal, *seedDensityMultiplier, arches, demandArches, logger)
	}

	srv := grpc.NewServer(grpcutil.ServerOptions()...)
	pb.RegisterShardServer(srv, sh)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	logger.Info("listening", "addr", lis.Addr().String(), "shard_id", *shardID, "epoch", epoch.Value())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cancelSig, sigs := signalContext()
	defer cancelSig()

	errCh := make(chan error, 3)
	go func() { errCh <- sh.Run(ctx) }()
	go func() { errCh <- srv.Serve(lis) }()

	// M38: background failure injector. Per-second tick; with
	// per-machine probability *failureRatePerSec, fail one random
	// Configured machine. Approximated by drawing a Bernoulli on
	// (rate × ConfiguredCount) per tick — exact at small rates,
	// fine for the 1e-8 to 1e-7 range that matches production.
	if *failureRatePerSec > 0 {
		go runFailureInjector(ctx, prov, *failureRatePerSec, logger)
	}

	// Coordinator client: registers this shard with the coordinator
	// (carrying advertise-addr) and receives instructions back. Empty
	// --coordinator-addr disables; the shard keeps running cycles
	// against its existing inventory either way (static-stability
	// hard rule). Owned by coordclient — pkg/shard does not import
	// pkg/coordinator.
	if *coordinatorAddr != "" {
		cc, err := coordclient.New(coordclient.Config{
			CoordinatorAddress: *coordinatorAddr,
			AdvertiseAddress:   *advertiseAddr,
			View:               coordclient.ViewFromShard(sh),
			CoordinatorTerm:    sh.CoordinatorTerm(),
			ReportInterval:     *heartbeatInterval,
			Logger:             logger,
		})
		if err != nil {
			return fmt.Errorf("coordclient: %w", err)
		}
		go func() { errCh <- cc.Run(ctx) }()
	}
	if *metricsAddr != "0" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		// ADR-0019 / M44.4: pprof endpoints for live cpu/heap/goroutine
		// capture during cloud runs. The bench at 50K Phase 1 calls
		// takes 20 ms total on M5 Max; the same workload measures
		// ~134 s mean per cycle in cloud. The 6000× discrepancy isn't
		// algorithmic (else the bench would reproduce it) — it's
		// cloud-specific: GC pressure, allocator contention with the
		// fold goroutine, lock contention, or CPU steal we can't see
		// from the shard's Prometheus surface. Live pprof closes that
		// gap. Off-by-default in production: leave only on the
		// scaletest harness builds (the metrics endpoint is itself
		// scaletest-shaped — no auth, no TLS).
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		metricsSrv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		logger.Info("metrics serving", "addr", *metricsAddr)
		go func() { errCh <- metricsSrv.ListenAndServe() }()
		defer func() { _ = metricsSrv.Shutdown(context.Background()) }()
	}

	select {
	case <-sigs:
		logger.Info("signal received, shutting down")
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("component exited", "err", err)
		}
	}

	cancel()
	srv.GracefulStop()
	return nil
}
