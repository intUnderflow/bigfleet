// Package needs holds the shard's NeedsTable: the per-cluster aggregated
// capacity demand that the decision engine walks each cycle.
//
// Two operations matter on the hot path:
//   - Replace(clusterID, needs): full-replacement of one cluster's
//     contribution. Every roll-up that arrives is the cluster's complete
//     desired state; we never merge with prior state.
//   - Snapshot(): a priority-sorted slice the worker walks top-down.
//
// The aggregation key (Profile) is the bundle of fields that make two
// CapacityRequests collapse into one Need with count > 1. Bucketed
// penalty fields participate in the key, so workload-specific raw
// penalties don't defeat aggregation (plan §0.1 B).
package needs

import (
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// PenaltyBucket mirrors the proto PenaltyBucket enum. Aggregation collapses
// CRs whose raw penalties fall in the same bucket. The numeric ordering
// matches the proto so range checks behave identically.
type PenaltyBucket uint8

const (
	PenaltyBucketUnspecified PenaltyBucket = 0
	PenaltyBucketZero        PenaltyBucket = 1
	PenaltyBucketHalfDollar  PenaltyBucket = 2
	PenaltyBucket1           PenaltyBucket = 3
	PenaltyBucket2           PenaltyBucket = 4
	PenaltyBucket4           PenaltyBucket = 5
	PenaltyBucket8           PenaltyBucket = 6
	PenaltyBucket16          PenaltyBucket = 7
	PenaltyBucket32          PenaltyBucket = 8
	PenaltyBucket64          PenaltyBucket = 9
	PenaltyBucket128         PenaltyBucket = 10
	PenaltyBucket256         PenaltyBucket = 11
	PenaltyBucket512         PenaltyBucket = 12
	PenaltyBucket1024        PenaltyBucket = 13
	PenaltyBucket2048        PenaltyBucket = 14
	PenaltyBucket4096        PenaltyBucket = 15
	PenaltyBucket8192        PenaltyBucket = 16
	PenaltyBucket16384       PenaltyBucket = 17
	PenaltyBucket32768       PenaltyBucket = 18
	PenaltyBucket65536       PenaltyBucket = 19
	PenaltyBucket131072      PenaltyBucket = 20
	PenaltyBucket262144      PenaltyBucket = 21
	PenaltyBucket524288      PenaltyBucket = 22
	PenaltyBucket1048576     PenaltyBucket = 23
	PenaltyBucket2097152     PenaltyBucket = 24
	PenaltyBucket4194304     PenaltyBucket = 25
	PenaltyBucket8388608     PenaltyBucket = 26
	PenaltyBucketPinned      PenaltyBucket = 27
)

// Label returns a stable Prometheus-label-friendly string for the
// bucket. Used by the FinOps metrics in pkg/metrics so penalty-bucket
// breakdowns stay readable in Grafana without leaking the internal
// uint8 enum value. M25.
func (b PenaltyBucket) Label() string {
	switch b {
	case PenaltyBucketZero:
		return "0"
	case PenaltyBucketHalfDollar:
		return "0.5"
	case PenaltyBucket1:
		return "1"
	case PenaltyBucket2:
		return "2"
	case PenaltyBucket4:
		return "4"
	case PenaltyBucket8:
		return "8"
	case PenaltyBucket16:
		return "16"
	case PenaltyBucket32:
		return "32"
	case PenaltyBucket64:
		return "64"
	case PenaltyBucket128:
		return "128"
	case PenaltyBucket256:
		return "256"
	case PenaltyBucket512:
		return "512"
	case PenaltyBucket1024:
		return "1024"
	case PenaltyBucket2048:
		return "2048"
	case PenaltyBucket4096:
		return "4096"
	case PenaltyBucket8192:
		return "8192"
	case PenaltyBucket16384:
		return "16384"
	case PenaltyBucket32768:
		return "32768"
	case PenaltyBucket65536:
		return "65536"
	case PenaltyBucket131072:
		return "131072"
	case PenaltyBucket262144:
		return "262144"
	case PenaltyBucket524288:
		return "524288"
	case PenaltyBucket1048576:
		return "1048576"
	case PenaltyBucket2097152:
		return "2097152"
	case PenaltyBucket4194304:
		return "4194304"
	case PenaltyBucket8388608:
		return "8388608"
	case PenaltyBucketPinned:
		return "pinned"
	}
	return "unspecified"
}

// BucketForDollars returns the smallest bucket whose upper bound is at
// least the given raw dollar value. Bucket boundaries are powers of 2
// from $1 (PenaltyBucket1) up to $8,388,608 (PenaltyBucket8388608), plus
// PenaltyBucketZero / PenaltyBucketHalfDollar at the bottom and
// PenaltyBucketPinned for anything larger.
func BucketForDollars(dollars float64) PenaltyBucket {
	switch {
	case dollars <= 0:
		return PenaltyBucketZero
	case dollars <= 0.5:
		return PenaltyBucketHalfDollar
	}
	// Walk the power-of-two ladder. The first bucket whose upper bound
	// is >= dollars wins. PenaltyBucket1 has bound 1.0; each subsequent
	// bucket doubles. PenaltyBucket8388608 has bound 2^23 = 8,388,608.
	bound := 1.0
	for bucket := PenaltyBucket1; bucket <= PenaltyBucket8388608; bucket++ {
		if dollars <= bound {
			return bucket
		}
		bound *= 2
	}
	return PenaltyBucketPinned
}

// Operator mirrors the proto NodeSelectorRequirement.Operator enum,
// translated into a Go-native form for the hot path.
type Operator uint8

const (
	OperatorUnspecified Operator = iota
	OperatorIn
	OperatorNotIn
	OperatorExists
	OperatorDoesNotExist
	OperatorSame
)

// Requirement is a node-selector style constraint applied to candidate
// machines. Values are sorted at construction so the canonical form is
// stable.
type Requirement struct {
	Key      string
	Operator Operator
	Values   []string
}

// TopologySpread captures the autoscaler-visible part of pod topology
// spread constraints. Pass-through to the autoscaler; honoured during
// provisioning when WhenUnsatisfiable is DoNotSchedule.
type TopologySpread struct {
	TopologyKey       string
	MaxSkew           int32
	WhenUnsatisfiable WhenUnsatisfiable
}

// WhenUnsatisfiable mirrors the proto / k8s enum.
type WhenUnsatisfiable uint8

const (
	WhenUnsatisfiableUnspecified WhenUnsatisfiable = iota
	WhenUnsatisfiableDoNotSchedule
	WhenUnsatisfiableScheduleAnyway
)

// ResourceQty is one entry in a need's resource map. Stored as
// (name, quantity-string) so canonicalisation is straightforward —
// quantity-aware comparison is left to the cluster operator at
// CR-aggregation time.
type ResourceQty struct {
	Name     string
	Quantity string
}

// Profile is the aggregation key for a Need. Two CRs whose Profiles are
// equal collapse into one Need with Count = 2. Profiles are immutable
// once constructed via NewProfile; the fingerprint is computed once and
// cached so map-keyed lookups don't re-walk the slices.
type Profile struct {
	requirements              []Requirement
	resources                 []ResourceQty
	spread                    []TopologySpread
	priority                  int32
	interruptionPenaltyBucket PenaltyBucket
	reclamationPenaltyBucket  PenaltyBucket
	fingerprint               string
}

// NewProfile builds a Profile, sorting and canonicalising the inputs so
// that two semantically-equal Profiles produce the same fingerprint.
func NewProfile(
	requirements []Requirement,
	resources []ResourceQty,
	spread []TopologySpread,
	priority int32,
	interruptionPenaltyBucket, reclamationPenaltyBucket PenaltyBucket,
) Profile {
	reqs := make([]Requirement, len(requirements))
	for i, r := range requirements {
		vals := make([]string, len(r.Values))
		copy(vals, r.Values)
		sort.Strings(vals)
		reqs[i] = Requirement{Key: r.Key, Operator: r.Operator, Values: vals}
	}
	sort.Slice(reqs, func(i, j int) bool {
		if reqs[i].Key != reqs[j].Key {
			return reqs[i].Key < reqs[j].Key
		}
		return reqs[i].Operator < reqs[j].Operator
	})

	res := make([]ResourceQty, len(resources))
	copy(res, resources)
	sort.Slice(res, func(i, j int) bool { return res[i].Name < res[j].Name })

	spr := make([]TopologySpread, len(spread))
	copy(spr, spread)
	sort.Slice(spr, func(i, j int) bool { return spr[i].TopologyKey < spr[j].TopologyKey })

	p := Profile{
		requirements:              reqs,
		resources:                 res,
		spread:                    spr,
		priority:                  priority,
		interruptionPenaltyBucket: interruptionPenaltyBucket,
		reclamationPenaltyBucket:  reclamationPenaltyBucket,
	}
	p.fingerprint = p.computeFingerprint()
	return p
}

// Priority returns the priority associated with this profile.
func (p Profile) Priority() int32 { return p.priority }

// InterruptionPenaltyBucket returns the bucketed interruption penalty.
func (p Profile) InterruptionPenaltyBucket() PenaltyBucket { return p.interruptionPenaltyBucket }

// ReclamationPenaltyBucket returns the bucketed reclamation penalty.
func (p Profile) ReclamationPenaltyBucket() PenaltyBucket { return p.reclamationPenaltyBucket }

// Requirements returns a defensive copy of the requirements slice.
func (p Profile) Requirements() []Requirement {
	out := make([]Requirement, len(p.requirements))
	copy(out, p.requirements)
	return out
}

// Resources returns a defensive copy of the resources slice.
func (p Profile) Resources() []ResourceQty {
	out := make([]ResourceQty, len(p.resources))
	copy(out, p.resources)
	return out
}

// Spread returns a defensive copy of the spread slice.
func (p Profile) Spread() []TopologySpread {
	out := make([]TopologySpread, len(p.spread))
	copy(out, p.spread)
	return out
}

// Fingerprint returns a stable canonical key for this Profile. Equal
// fingerprints mean equal Profiles; equal Profiles mean equal fingerprints.
func (p Profile) Fingerprint() string { return p.fingerprint }

func (p Profile) computeFingerprint() string {
	var b strings.Builder
	b.WriteString("p=")
	b.WriteString(strconv.FormatInt(int64(p.priority), 10))
	b.WriteString("|ipb=")
	b.WriteString(strconv.FormatUint(uint64(p.interruptionPenaltyBucket), 10))
	b.WriteString("|rpb=")
	b.WriteString(strconv.FormatUint(uint64(p.reclamationPenaltyBucket), 10))
	b.WriteString("|reqs=")
	for _, r := range p.requirements {
		b.WriteString(r.Key)
		b.WriteByte(':')
		b.WriteString(strconv.FormatUint(uint64(r.Operator), 10))
		b.WriteByte(':')
		b.WriteString(strings.Join(r.Values, ","))
		b.WriteByte(';')
	}
	b.WriteString("|res=")
	for _, r := range p.resources {
		b.WriteString(r.Name)
		b.WriteByte('=')
		b.WriteString(r.Quantity)
		b.WriteByte(';')
	}
	b.WriteString("|spr=")
	for _, s := range p.spread {
		b.WriteString(s.TopologyKey)
		b.WriteByte(':')
		b.WriteString(strconv.FormatInt(int64(s.MaxSkew), 10))
		b.WriteByte(':')
		b.WriteString(strconv.FormatUint(uint64(s.WhenUnsatisfiable), 10))
		b.WriteByte(';')
	}
	return b.String()
}

// Need is one row of the NeedsTable: a count of identically-shaped
// machines that one cluster currently wants.
//
// Group is an opaque per-Need co-location bucket. Two Needs with the
// same (Cluster, Profile.Fingerprint) but different Group are kept
// separate by Aggregate, so each group preserves its own Same-operator
// co-location requirement. Empty Group means "no co-location group" and
// aggregates with other empty-Group Needs sharing the fingerprint.
//
// Group is in-memory state; it is not part of the wire format. The
// operator populates it from CR ownerReferences during rollup so that
// CRs from different workloads (StatefulSets, Jobs, etc.) become
// distinct wire-level CapacityNeeds even when their Profiles are
// identical, and the Phase 1 allocator can co-locate each group
// independently.
type Need struct {
	ClusterID        machine.ClusterID
	Profile          Profile
	Count            int
	ArrivalUnixNanos int64
	Group            string
}

// Aggregate groups a slice of Needs by (cluster, profile fingerprint,
// group), summing counts. Useful in tests and when the operator wants
// to merge raw per-CR observations into the wire-level Need
// representation. CRs from the same workload (same Group) collapse
// into one Need; CRs from different workloads stay separate even if
// they share a Profile fingerprint.
func Aggregate(in []Need) []Need {
	type key struct {
		cluster machine.ClusterID
		fp      string
		group   string
	}
	idx := make(map[key]int, len(in))
	out := make([]Need, 0, len(in))
	for _, n := range in {
		k := key{n.ClusterID, n.Profile.Fingerprint(), n.Group}
		if at, ok := idx[k]; ok {
			out[at].Count += n.Count
			// Keep earliest arrival time so age calculations are accurate.
			if n.ArrivalUnixNanos != 0 && (out[at].ArrivalUnixNanos == 0 || n.ArrivalUnixNanos < out[at].ArrivalUnixNanos) {
				out[at].ArrivalUnixNanos = n.ArrivalUnixNanos
			}
			continue
		}
		idx[k] = len(out)
		out = append(out, n)
	}
	return out
}

// Table is the per-shard aggregated demand store. Replace per cluster is
// O(n) in the cluster's contribution; Snapshot is O(N log N) over the
// whole shard at most once per dirty cycle.
type Table struct {
	mu        sync.RWMutex
	byCluster map[machine.ClusterID][]Need
	cached    []Need
	dirty     bool
}

// NewTable returns an empty Table.
func NewTable() *Table {
	return &Table{
		byCluster: make(map[machine.ClusterID][]Need),
	}
}

// Replace replaces all of cluster's needs with the given slice. Empty
// slice (or nil) clears the cluster's contribution. Always atomic from
// the snapshot's point of view.
func (t *Table) Replace(cluster machine.ClusterID, n []Need) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(n) == 0 {
		delete(t.byCluster, cluster)
	} else {
		copyN := make([]Need, len(n))
		copy(copyN, n)
		t.byCluster[cluster] = copyN
	}
	t.dirty = true
}

// Snapshot returns the full needs list, sorted by priority desc and then
// by arrival time asc (older first). The returned slice is owned by the
// caller and may be mutated freely.
func (t *Table) Snapshot() []Need {
	t.mu.RLock()
	if !t.dirty && t.cached != nil {
		out := make([]Need, len(t.cached))
		copy(out, t.cached)
		t.mu.RUnlock()
		return out
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	total := 0
	for _, ns := range t.byCluster {
		total += len(ns)
	}
	all := make([]Need, 0, total)
	for _, ns := range t.byCluster {
		all = append(all, ns...)
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Profile.priority != all[j].Profile.priority {
			return all[i].Profile.priority > all[j].Profile.priority
		}
		if all[i].ArrivalUnixNanos != all[j].ArrivalUnixNanos {
			return all[i].ArrivalUnixNanos < all[j].ArrivalUnixNanos
		}
		return all[i].ClusterID < all[j].ClusterID
	})
	t.cached = all
	t.dirty = false
	out := make([]Need, len(all))
	copy(out, all)
	return out
}

// Stats returns coarse counts for monitoring. Cheap to call; does not
// rebuild the snapshot.
func (t *Table) Stats() Stats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	s := Stats{Clusters: len(t.byCluster)}
	for _, ns := range t.byCluster {
		s.Needs += len(ns)
		for _, n := range ns {
			s.PendingMachines += n.Count
		}
	}
	return s
}

// Stats summarises the table.
type Stats struct {
	Clusters        int
	Needs           int
	PendingMachines int
}
