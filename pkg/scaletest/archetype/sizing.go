// Seed sizing (ADR-0044): seed machine pools are sized by machine
// demand, not workload weight. Archetype.Weight is workload-object
// frequency; machine demand per pod differs by the archetype's
// node-packing density (PodsPerMachine): a cpu-shaped archetype packs
// ~`density` Pods per machine, a whole-machine training node packs one.
// ADR-0050 makes this per-archetype (PodsPerNode) rather than the
// M66.2 "core scales / extended is always 1" special case — see
// PodsPerMachine. The share math lives here, next to the catalog types,
// so the shard's seed and the runner's effective-total computation
// cannot drift.
//
// The service-size replica distribution also lives here (moved from
// the load-driver's package main, ADR-0044 §2): demand generation and
// seed sizing must agree on E[replicas], and a table nothing can
// cross-check will drift.

package archetype

import (
	"math"
	"math/rand"
	"sort"
)

// replicaBucket is one band of the hardcoded service-size distribution.
// A workload object's replica count is a uniform draw within the
// weighted-picked bucket's [lo, hi] range.
type replicaBucket struct {
	weight int
	lo, hi int
}

// replicaDistribution is the hardcoded heavy-tailed service-size
// distribution: most services are small, a few are large. ADR-0038
// fixes this in code on purpose — it is a modelling decision, not a
// per-profile knob (YAGNI).
var replicaDistribution = []replicaBucket{
	{weight: 55, lo: 1, hi: 5},
	{weight: 30, lo: 6, hi: 25},
	{weight: 12, lo: 26, hi: 100},
	{weight: 3, lo: 101, hi: 400},
}

// StatefulReplicaCap clamps StatefulSet replica draws. StatefulSets
// create Pods ordinally/serially, so a large one bottlenecks the ramp;
// stateful workloads are kept small.
const StatefulReplicaCap = 25

// statefulArchetypes is the hardcoded set of archetype names whose
// workloads need stable identity / ordered semantics — these become
// StatefulSets; everything else becomes a Deployment. ADR-0038: the
// classification is intentionally a small in-code set, not a profile
// knob (YAGNI).
var statefulArchetypes = map[string]bool{
	"stateful-db":  true,
	"memory-cache": true,
}

// IsStateful reports whether an archetype's workload should be
// modelled as a StatefulSet rather than a Deployment. ADR-0038.
func IsStateful(archName string) bool {
	return statefulArchetypes[archName]
}

// PickReplicas draws a workload object's replica count from the
// hardcoded service-size distribution. Stateful workloads are clamped
// to StatefulReplicaCap.
func PickReplicas(rng *rand.Rand, stateful bool) int {
	full := 0
	for _, b := range replicaDistribution {
		full += b.weight
	}
	r := rng.Intn(full)
	cum := 0
	var chosen replicaBucket
	for _, b := range replicaDistribution {
		cum += b.weight
		if r < cum {
			chosen = b
			break
		}
	}
	n := chosen.lo
	if chosen.hi > chosen.lo {
		n = chosen.lo + rng.Intn(chosen.hi-chosen.lo+1)
	}
	if stateful && n > StatefulReplicaCap {
		n = StatefulReplicaCap
	}
	return n
}

// groupSizeBounds returns the [lo, hi] gang size range with
// PickGroupSize's normalisation (lo ≤ 0 → 1; hi ≤ 0 or hi < lo → lo)
// so sizing and drawing agree on what an unset range means.
func groupSizeBounds(a *Archetype) (lo, hi int) {
	lo, hi = a.GroupSizeRange[0], a.GroupSizeRange[1]
	if lo <= 0 {
		lo = 1
	}
	if hi <= 0 || hi < lo {
		hi = lo
	}
	return lo, hi
}

// isGang reports whether an archetype's workload objects form
// co-location gangs (one workload object = one gang, ADR-0038).
func isGang(a *Archetype) bool {
	return a.SameRack || a.SameZone
}

// ExpectedReplicas returns E[replicas per workload object] for the
// archetype: the mean of GroupSizeRange for gangs (one workload object
// IS one gang, ADR-0038), otherwise the expectation of the
// service-size distribution PickReplicas draws from. The stateful cap
// is folded in analytically: a bucket [lo, hi] overlapping the cap
// contributes the mean of min(x, cap) over its uniform range.
func ExpectedReplicas(a *Archetype, stateful bool) float64 {
	if isGang(a) {
		lo, hi := groupSizeBounds(a)
		return float64(lo+hi) / 2
	}
	maxN := math.MaxInt
	if stateful {
		maxN = StatefulReplicaCap
	}
	totalWeight, weighted := 0, 0.0
	for _, b := range replicaDistribution {
		totalWeight += b.weight
		weighted += float64(b.weight) * bucketMeanCapped(b.lo, b.hi, maxN)
	}
	return weighted / float64(totalWeight)
}

// bucketMeanCapped is E[min(X, maxN)] for X uniform on the integers
// [lo, hi].
func bucketMeanCapped(lo, hi, maxN int) float64 {
	switch {
	case hi <= maxN:
		return float64(lo+hi) / 2
	case lo > maxN:
		return float64(maxN)
	default:
		n := hi - lo + 1
		// Draws lo..maxN contribute themselves, maxN+1..hi contribute maxN.
		belowCap := (maxN + lo) * (maxN - lo + 1) / 2
		return (float64(belowCap) + float64(hi-maxN)*float64(maxN)) / float64(n)
	}
}

// PodsPerMachine returns how many of the archetype's Pods one seeded
// machine hosts.
//
// ADR-0050: a per-archetype PodsPerNode is the node-packing density and
// wins outright when set — it is the explicit "this many of my Pods fit
// on a node of my class," for GPU shapes as much as CPU ones (an 8-GPU
// inference node holds 8 gpu:1 Pods → PodsPerNode 8; a whole-machine
// training node holds 1 gpu:8 Pod → PodsPerNode 1).
//
// When PodsPerNode is unset, fall back to M66.2's model: `density` for
// core-only (compressible) shapes, 1 when any size bucket requests an
// extended resource (device counts don't scale with density, so a whole
// machine goes to each extended-resource Pod). This preserves today's
// behaviour for any archetype that doesn't opt into ADR-0050's
// per-archetype density.
func PodsPerMachine(a *Archetype, density int) int {
	if a.PodsPerNode > 0 {
		return a.PodsPerNode
	}
	maps := []map[string]string{a.Resources}
	if len(a.SizeBuckets) > 0 {
		// PickSize ignores the flat Resources map when buckets exist.
		maps = maps[:0]
		for i := range a.SizeBuckets {
			maps = append(maps, a.SizeBuckets[i].Resources)
		}
	}
	for _, m := range maps {
		for k := range m {
			if k != "cpu" && k != "memory" && k != "ephemeral-storage" {
				return 1
			}
		}
	}
	if density < 1 {
		return 1
	}
	return density
}

// SeedScale returns how the seed should build one machine's Allocatable
// for this archetype: the per-machine packing factor and whether
// extended (device) resources scale by it (ADR-0050).
//
//   - factor is PodsPerMachine — the number of this archetype's Pods one
//     node holds.
//   - scaleExtended is true ONLY when the archetype set PodsPerNode. That
//     opts it into ADR-0050's node-packing model, where the seed machine
//     = per-Pod resources × PodsPerNode for ALL resources including GPU
//     (gpu:1 × 8 = an 8-GPU node hosting 8 inference Pods). Archetypes
//     that DON'T set PodsPerNode keep M66.2's rule — core resources
//     scale by `density`, extended resources never scale — so the seed
//     for an un-densified GPU shape stays a whole-machine 1-Pod node.
//
// The two callers (the shard's three seed tiers and the closed-loop sim
// share this so demand-side packing and seed-side Allocatable can't
// drift; the share math lives next to the catalog types on purpose).
func SeedScale(a *Archetype, density int) (factor int, scaleExtended bool) {
	return PodsPerMachine(a, density), a.PodsPerNode > 0
}

// podShare returns the unnormalised pod-demand share of one archetype
// (ADR-0044 §1): Weight × E[replicas per workload object]. Weight ≤ 0
// counts as 1, matching NewPicker. A BurstOnly archetype (#327)
// contributes 0 — it is not part of the steady draw, so it must carry no
// steady pod- or machine-demand share (machineShares / MachinesForPods /
// catalogPodShares all build on podShare).
func podShare(a *Archetype) float64 {
	if a.BurstOnly {
		return 0
	}
	w := a.Weight
	if w <= 0 {
		w = 1
	}
	return float64(w) * ExpectedReplicas(a, IsStateful(a.Name))
}

// machineShares returns the unnormalised per-archetype machine-demand
// shares (ADR-0044 §1): machineShare(a) ∝ podShare(a) /
// podsPerMachine(a).
func machineShares(arches []Archetype, density int) []float64 {
	shares := make([]float64, len(arches))
	for i := range arches {
		shares[i] = podShare(&arches[i]) / float64(PodsPerMachine(&arches[i], density))
	}
	return shares
}

// gangFloor returns the per-zone gang floor for one archetype:
// max(GroupSizeRange) machines per zone (ADR-0044 §3 — without it the
// largest gang the catalog can draw is unsatisfiable by construction),
// or 0 for non-gang archetypes. zones < 1 counts as 1. A BurstOnly
// archetype (#327) has no steady gang floor — its gang is provisioned
// live by the burst event, not pre-seeded — so it returns 0 even though
// it is a gang. This is the floor that weight:0 alone cannot suppress.
func gangFloor(a *Archetype, zones int) int {
	if a.BurstOnly {
		return 0
	}
	if !isGang(a) {
		return 0
	}
	if zones < 1 {
		zones = 1
	}
	_, hi := groupSizeBounds(a)
	return hi * zones
}

// MachineAllocation splits totalMachines across the archetypes by
// machine-demand share (ADR-0044 §1), then raises every gang
// archetype's count to at least its per-zone floor (§3). The
// share-derived split uses a largest-remainder distribution, so it is
// deterministic and sums exactly to totalMachines — but the gang
// raises are applied ON TOP, so the returned slice may sum to more
// than totalMachines. That is intended: a fleet that runs zone-scoped
// gangs has the per-zone pool to place them, whatever the nominal
// total says. totalMachines ≤ 0 returns all zeros (a disabled tier
// gets no floor machines either).
//
// zones(a) is the archetype's zone count for the floor; callers pass
// len(a.Zones) (values < 1 count as 1).
func MachineAllocation(arches []Archetype, density, totalMachines int, zones func(a *Archetype) int) []int {
	if len(arches) == 0 {
		return nil
	}
	counts := make([]int, len(arches))
	if totalMachines <= 0 {
		return counts
	}

	shares := machineShares(arches, density)
	total := 0.0
	for _, s := range shares {
		total += s
	}
	assigned := 0
	fracs := make([]int, 0, len(arches)) // indices, sorted by remainder below
	rems := make([]float64, len(arches))
	for i, s := range shares {
		// BurstOnly archetypes (#327) carry zero steady share (podShare
		// is 0) — keep them at count 0 and out of the remainder draw so
		// no stray machine lands on them; the returned slice stays
		// index-aligned with arches (callers index alloc[i]).
		if arches[i].BurstOnly {
			continue
		}
		q := float64(totalMachines) * s / total
		counts[i] = int(q)
		rems[i] = q - float64(counts[i])
		assigned += counts[i]
		fracs = append(fracs, i)
	}
	sort.SliceStable(fracs, func(x, y int) bool {
		return rems[fracs[x]] > rems[fracs[y]]
	})
	for j := 0; len(fracs) > 0 && j < totalMachines-assigned; j++ {
		counts[fracs[j%len(fracs)]]++
	}

	for i := range arches {
		if floor := gangFloor(&arches[i], zones(&arches[i])); counts[i] < floor {
			counts[i] = floor
		}
	}
	return counts
}

// MachinesForPods returns the catalog-derived effective machine total
// needed to host totalPods (ADR-0044 §4):
//
//	Σ_a ceil(totalPods × podShare(a) / podsPerMachine(a)) + gang floors
//
// `scale.machines × density` stays the demand definition; this is the
// supply total the demand shape actually implies, which the renderer
// splits across the Configured/Idle/Speculative tiers with the
// existing fractions. Profiles with whole-machine archetypes come out
// well above the nominal — that is the realistic fleet shape. An empty
// catalog falls back to the legacy uniform packing, ceil(totalPods /
// density).
func MachinesForPods(arches []Archetype, density, totalPods int) int {
	if totalPods <= 0 {
		return 0
	}
	if len(arches) == 0 {
		d := density
		if d < 1 {
			d = 1
		}
		return (totalPods + d - 1) / d
	}
	podTotal := 0.0
	for i := range arches {
		podTotal += podShare(&arches[i])
	}
	machines := 0
	for i := range arches {
		a := &arches[i]
		pods := float64(totalPods) * podShare(a) / podTotal
		machines += int(math.Ceil(pods / float64(PodsPerMachine(a, density))))
		machines += gangFloor(a, len(a.Zones))
	}
	return machines
}
