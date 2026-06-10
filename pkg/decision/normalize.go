package decision

import (
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/intUnderflow/bigfleet/pkg/decision/occ"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// NormalizeDemand folds sub-machine Same-Needs into atomic plain
// aggregates (ADR-0041). The shard calls it once per cycle, on the
// snapshot and demand the decision phases are about to consume, so
// Phase 1/2/3 and the phase-attribution probe all see the same
// normalized slice.
//
// A Same-carrying Need is FOLDABLE iff at least one machine in the
// snapshot — the Need's cluster's Configured/Configuring, or
// shard-wide Idle/Speculative — matches its Profile and has an
// EffectiveAllocatable covering its whole AggregateResources. Such a
// gang does not need the Same machinery at all: any single machine
// with room hosts the whole gang, so co-residency is automatic.
// Foldable Needs of the same (cluster, stripped-Profile fingerprint,
// aggregate value) fold into ONE plain Need: the Same requirement is
// stripped, AggregateResources are summed, and MinUnit becomes one
// gang's aggregate — the Fleet-Scale Kubernetes §7 atomicity floor,
// so the phases' vector math only counts machines that can host a
// whole gang. That makes the machine-granular claim ledger truthful
// again: pre-fold, every sub-machine gang up-rounded to one exclusive
// machine (~2,400 gangs demanding ~2,400 machines where ~540 exist at
// uber-5k) while kube-scheduler happily packed many gangs per machine.
//
// Needs that fit no machine keep their per-gang Same Need — the
// genuinely cross-machine topology case, where machine-exclusive
// claiming is correct.
//
// The fold is conservative under fragmentation: a folded Need counts
// only machines that fit a whole gang, even though the scheduler could
// place a gang across two half-free machines on one rack. BigFleet may
// slightly over-provision in fragmented states; capacity feasibility
// is never overstated. Classification is snapshot-dependent and
// recomputed per cycle: if the fleet's largest matching machines
// disappear, the class deterministically reverts to per-gang
// Same-Needs next cycle.
//
// Output ordering is deterministic: non-Same and unfoldable Needs pass
// through in input order, folded Needs are appended in sorted
// (cluster, stripped fingerprint, aggregate key) order.
//
// Hot path: foldability is memoized per class — per (fingerprint,
// aggregate) for the shard-wide pools, per (cluster, fingerprint,
// aggregate) for the cluster's own — so the pool walks happen once per
// class per cycle, never once per gang, mirroring the SameSupplyIndex
// per-fingerprint pattern. Quantity strings are parsed once per class;
// the per-gang cost is map lookups.
func NormalizeDemand(snap *inventory.Snapshot, demand []needs.Need) []needs.Need {
	// Fast path: a cycle with no Same-carrying Needs normalizes to
	// itself, with no copying.
	hasSame := false
	for i := range demand {
		if _, ok := occ.SameRequirementKey(demand[i].Profile); ok {
			hasSame = true
			break
		}
	}
	if !hasSame {
		return demand
	}

	z := newDemandNormalizer(snap)
	out := make([]needs.Need, 0, len(demand))
	type groupKey struct {
		cluster machine.ClusterID
		fp      string // stripped-Profile fingerprint
		agg     string // canonical aggregate value key
	}
	type foldGroup struct {
		first   *needs.Need // first member in input order; all share its aggregate value
		profile needs.Profile
		count   int
		arrival int64 // earliest non-zero ArrivalUnixNanos, as in needs.Aggregate
	}
	groups := make(map[groupKey]*foldGroup)
	var order []groupKey

	for i := range demand {
		n := &demand[i]
		if _, ok := occ.SameRequirementKey(n.Profile); !ok {
			out = append(out, *n)
			continue
		}
		cls := z.aggClassFor(n.AggregateResources)
		if !z.foldable(n, cls) {
			out = append(out, *n)
			continue
		}
		stripped := z.strippedFor(n.Profile)
		k := groupKey{cluster: n.ClusterID, fp: stripped.Fingerprint(), agg: cls.key}
		g, ok := groups[k]
		if !ok {
			g = &foldGroup{first: n, profile: stripped}
			groups[k] = g
			order = append(order, k)
		}
		g.count++
		if n.ArrivalUnixNanos != 0 && (g.arrival == 0 || n.ArrivalUnixNanos < g.arrival) {
			g.arrival = n.ArrivalUnixNanos
		}
	}

	sort.Slice(order, func(a, b int) bool {
		ka, kb := order[a], order[b]
		if ka.cluster != kb.cluster {
			return ka.cluster < kb.cluster
		}
		if ka.fp != kb.fp {
			return ka.fp < kb.fp
		}
		return ka.agg < kb.agg
	})
	for _, k := range order {
		g := groups[k]
		out = append(out, needs.Need{
			ClusterID: g.first.ClusterID,
			Profile:   g.profile,
			// All members carry the same aggregate value (it is part of
			// the group key), so the group's sum is first × count and
			// MinUnit is exactly one member's aggregate (§7 floor).
			AggregateResources: needs.ScaleResources(g.first.AggregateResources, g.count),
			MinUnit:            g.first.AggregateResources,
			ArrivalUnixNanos:   g.arrival,
		})
	}
	return out
}

// resPair is one parsed dimension of an aggregate: its name and
// milli-value. The foldability walk compares machine allocatables
// against these with one ParseQuantity per positive dimension and no
// vector allocation.
type resPair struct {
	name  string
	milli int64
}

// aggClass is the per-(aggregate value) memo entry: the parsed pairs
// the pool walks compare against, and the canonical value key used for
// memoization and grouping ("16Gi" and "17179869184" collapse here).
type aggClass struct {
	pairs []resPair
	key   string
}

// demandNormalizer carries one NormalizeDemand call's memo state. The
// memos are per call on purpose — foldability is snapshot-dependent
// (ADR-0041) and must be recomputed each cycle.
type demandNormalizer struct {
	snap *inventory.Snapshot

	// aggByRaw memoizes aggregate parsing on the raw string form, so
	// the ~one-gang-per-Pod demand shape parses each workload shape's
	// quantities once per cycle instead of once per gang.
	aggByRaw map[string]*aggClass

	// strippedByFP memoizes the Same-stripped Profile per original
	// fingerprint (equal fingerprints mean equal Profiles, so the
	// rebuild — sort + fingerprint in NewProfile — runs once per class).
	strippedByFP map[string]needs.Profile

	// Foldability memos. The shard-wide half (Idle + Speculative) only
	// depends on (fingerprint, aggregate); the creditable half also
	// depends on the Need's cluster, so it is memoized per cluster —
	// two clusters of the same class may legitimately differ when
	// neither has a fitting shard-wide machine but one has a fitting
	// Configured machine of its own.
	shardFit   map[string]bool
	clusterFit map[string]bool
}

func newDemandNormalizer(snap *inventory.Snapshot) *demandNormalizer {
	return &demandNormalizer{
		snap:         snap,
		aggByRaw:     make(map[string]*aggClass),
		strippedByFP: make(map[string]needs.Profile),
		shardFit:     make(map[string]bool),
		clusterFit:   make(map[string]bool),
	}
}

// aggClassFor returns the memoized parsed form of an aggregate vector.
func (z *demandNormalizer) aggClassFor(agg []needs.ResourceQty) *aggClass {
	var raw strings.Builder
	for _, r := range agg {
		raw.WriteString(r.Name)
		raw.WriteByte('=')
		raw.WriteString(r.Quantity)
		raw.WriteByte(';')
	}
	rawKey := raw.String()
	if cls, ok := z.aggByRaw[rawKey]; ok {
		return cls
	}
	pairs := make([]resPair, 0, len(agg))
	for _, r := range agg {
		q, err := resource.ParseQuantity(r.Quantity)
		if err != nil {
			// Unparseable quantities degrade to zero, matching the
			// needs package's vector ops.
			continue
		}
		pairs = append(pairs, resPair{name: r.Name, milli: q.MilliValue()})
	}
	// Canonical by value: sort by name and key on name=milli, so two
	// string spellings of the same aggregate share a class and a group.
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].name < pairs[j].name })
	var key strings.Builder
	for _, p := range pairs {
		key.WriteString(p.name)
		key.WriteByte('=')
		key.WriteString(strconv.FormatInt(p.milli, 10))
		key.WriteByte(';')
	}
	cls := &aggClass{pairs: pairs, key: key.String()}
	z.aggByRaw[rawKey] = cls
	return cls
}

// strippedFor returns profile minus its Same requirement, rebuilt via
// needs.NewProfile so spread, priority and the penalty buckets are
// preserved and the fingerprint is canonical.
func (z *demandNormalizer) strippedFor(p needs.Profile) needs.Profile {
	fp := p.Fingerprint()
	if s, ok := z.strippedByFP[fp]; ok {
		return s
	}
	reqs := p.Requirements()
	kept := reqs[:0]
	for _, r := range reqs {
		if r.Operator == needs.OperatorSame {
			continue
		}
		kept = append(kept, r)
	}
	s := needs.NewProfile(kept, p.Spread(), p.Priority(),
		p.InterruptionPenaltyBucket(), p.ReclamationPenaltyBucket())
	z.strippedByFP[fp] = s
	return s
}

// foldable reports whether a machine exists that can host the Need's
// whole aggregate: the Need's cluster's Configured/Configuring first
// (cheap per-cluster buckets), then the shard-wide Idle/Speculative
// pools. Both halves are memoized; the walks short-circuit on the
// first fit.
func (z *demandNormalizer) foldable(n *needs.Need, cls *aggClass) bool {
	ckey := string(n.ClusterID) + "\x00" + n.Profile.Fingerprint() + "\x00" + cls.key
	fit, ok := z.clusterFit[ckey]
	if !ok {
		fit = z.clusterHasFit(n.ClusterID, n.Profile, cls)
		z.clusterFit[ckey] = fit
	}
	if fit {
		return true
	}
	skey := n.Profile.Fingerprint() + "\x00" + cls.key
	fit, ok = z.shardFit[skey]
	if !ok {
		fit = z.shardHasFit(n.Profile, cls)
		z.shardFit[skey] = fit
	}
	return fit
}

func (z *demandNormalizer) clusterHasFit(cluster machine.ClusterID, profile needs.Profile, cls *aggClass) bool {
	for _, st := range []machine.State{machine.StateConfigured, machine.StateConfiguring} {
		for _, m := range z.snap.ListByClusterState(cluster, st) {
			if MatchProfile(profile, m) && machineCoversPairs(m.EffectiveAllocatable(), cls.pairs) {
				return true
			}
		}
	}
	return false
}

// shardHasFit mirrors the SameSupplyIndex / PoolCache source
// selection: a pinned instance-type set narrows the walk to the
// per-type buckets; otherwise the full per-state list is scanned.
func (z *demandNormalizer) shardHasFit(profile needs.Profile, cls *aggClass) bool {
	types := pinnedInstanceTypes(profile)
	for _, st := range []machine.State{machine.StateIdle, machine.StateSpeculative} {
		var srcs [][]machine.Machine
		if len(types) == 0 {
			srcs = [][]machine.Machine{z.snap.ListByState(st)}
		} else {
			for _, t := range types {
				srcs = append(srcs, z.snap.ListByStateInstanceType(st, t))
			}
		}
		for _, src := range srcs {
			for i := range src {
				if MatchProfile(profile, src[i]) && machineCoversPairs(src[i].EffectiveAllocatable(), cls.pairs) {
					return true
				}
			}
		}
	}
	return false
}

// machineCoversPairs reports whether alloc covers every positive
// dimension of pairs — the existence-walk mirror of needs.Covers, with
// one ParseQuantity per dimension the aggregate actually names and an
// early exit on the first hole.
func machineCoversPairs(alloc map[string]string, pairs []resPair) bool {
	for _, p := range pairs {
		if p.milli <= 0 {
			continue
		}
		q, err := resource.ParseQuantity(alloc[p.name])
		if err != nil || q.MilliValue() < p.milli {
			return false
		}
	}
	return true
}
