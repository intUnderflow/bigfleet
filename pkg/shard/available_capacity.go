package shard

import (
	"sync"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/conv"
	"github.com/intUnderflow/bigfleet/pkg/inventory"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// availableCapacityCache decides whether an AC emit should fire for a
// given (cluster, fingerprint) on this cycle. Two layers:
//
//  1. Coalesce: if the (count, confidence, cost) tuple matches the
//     last emit's tuple, skip. Saves the operator-side apiserver
//     from writing CRs that just rewrite the existing state.
//
//  2. Rate-limit: even when the tuple changed, throttle emits to at
//     most once per minInterval per (cluster, fingerprint). The
//     shard cycle runs at ~10 Hz under load; AC is paper §6.2
//     "eventually consistent" — operators see state changes within
//     the rate-limit window, not every cycle. Without this, a 50-
//     cluster harness emits 500 AC writes/sec into the operator-side
//     kine sqlite, dwarfing the load that AC itself describes.
//
// Default interval is 5 seconds — far slower than a cycle, far
// faster than a human looking at `kubectl get availablecapacity`.
//
// The cache survives session reconnects: a reconnect only resets
// state for the affected cluster (via forget); other clusters keep
// their dedup state, so reconnects don't trigger a fleet-wide AC
// re-emit storm.
type availableCapacityCache struct {
	mu          sync.Mutex
	entries     map[acCacheKey]acCacheEntry
	minInterval time.Duration
	now         func() time.Time // injectable for tests
}

type acCacheKey struct {
	cluster machine.ClusterID
	fp      string
}

type acCacheValue struct {
	count      int32
	confidence pb.AvailableCapacityUpdate_Confidence
	cost       float64
}

type acCacheEntry struct {
	value    acCacheValue
	lastEmit time.Time
}

const defaultAvailableCapacityInterval = 5 * time.Second

func newAvailableCapacityCache(interval time.Duration) *availableCapacityCache {
	if interval <= 0 {
		interval = defaultAvailableCapacityInterval
	}
	return &availableCapacityCache{
		entries:     make(map[acCacheKey]acCacheEntry),
		minInterval: interval,
		now:         time.Now,
	}
}

// shouldEmit reports whether to emit a frame for (cluster, fingerprint)
// with the given tuple now. Returns false when the tuple is unchanged
// from the last emit, or when the rate-limit window hasn't elapsed
// since the last emit. On a positive return the cache is updated.
func (c *availableCapacityCache) shouldEmit(cluster machine.ClusterID, fp string, v acCacheValue) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := acCacheKey{cluster, fp}
	now := c.now()
	prev, exists := c.entries[k]
	if exists {
		if prev.value == v {
			return false // coalesce
		}
		if now.Sub(prev.lastEmit) < c.minInterval {
			return false // rate-limit
		}
	}
	c.entries[k] = acCacheEntry{value: v, lastEmit: now}
	return true
}

// forget drops cached entries for a cluster (e.g., on session loss).
// Best-effort: the next emit will re-establish the cache.
func (c *availableCapacityCache) forget(cluster machine.ClusterID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.entries {
		if k.cluster == cluster {
			delete(c.entries, k)
		}
	}
}

// emitAvailableCapacity pushes one AvailableCapacityUpdate per
// (cluster, distinct profile fingerprint) down the matching operator
// session, summarising the shard's current ability to satisfy that
// profile. Eventually-consistent hint per the paper §6.2; the operator
// upserts an AvailableCapacity CR per profile fingerprint on the
// cluster's API server.
//
// Called once per cycle from runCycleCapturing after Phase 1/2/3.
// Cheap: O(clusters × distinct_fingerprints × pinned_instance_types)
// snapshot-bucket length reads, no per-machine walks. The snapshot's
// per-(state, instance-type) buckets are pre-built at fold time so
// each Count call is O(1).
//
// Confidence ladder:
//   - HIGH   = at least one Idle machine matches (no waiting on Create)
//   - MEDIUM = no Idle but at least one Speculative slot matches
//   - NONE   = neither
//
// Per-fingerprint emission ensures the operator's CR set tracks the
// cluster's *demanded* shapes — we don't enumerate every potential
// profile the inventory could match, only the ones this cluster
// actually asks for. That keeps the CR cardinality on the cluster API
// server bounded by the cluster's distinct workload shapes, not by
// inventory size.
func (s *Shard) emitAvailableCapacity(snap *inventory.Snapshot, demand []needs.Need) {
	if snap == nil || len(demand) == 0 {
		return
	}
	// Group demand by cluster, dedup by fingerprint within each.
	byCluster := make(map[machine.ClusterID]map[string]needs.Profile)
	for _, n := range demand {
		fps := byCluster[n.ClusterID]
		if fps == nil {
			fps = make(map[string]needs.Profile)
			byCluster[n.ClusterID] = fps
		}
		fps[n.Profile.Fingerprint()] = n.Profile
	}
	for cluster, fps := range byCluster {
		sess := s.lookupSession(cluster)
		if sess == nil {
			continue
		}
		for fp, profile := range fps {
			upd := buildAvailableCapacityUpdate(snap, profile, fp)
			v := acCacheValue{
				count:      upd.GetAvailableCount(),
				confidence: upd.GetConfidence(),
				cost:       upd.GetCostPerHour(),
			}
			if !s.acCache.shouldEmit(cluster, fp, v) {
				continue // coalesce + rate-limit
			}
			if err := sess.SendAvailableCapacityUpdate(upd); err != nil {
				s.log.Debug("emitAvailableCapacity send failed", "cluster", cluster, "fp", fp, "err", err)
			}
		}
	}
}

// buildAvailableCapacityUpdate computes the per-cluster, per-profile
// hint. cheapestPrice walks the matching idle bucket once if any; we
// don't try to be exact across multi-type profiles (the cheapest
// instance type wins).
func buildAvailableCapacityUpdate(snap *inventory.Snapshot, profile needs.Profile, fp string) *pb.AvailableCapacityUpdate {
	types := pinnedInstanceTypesForAC(profile)

	idle, spec := 0, 0
	cheapest := 0.0
	if len(types) == 0 {
		idle = snap.CountByState(machine.StateIdle)
		spec = snap.CountByState(machine.StateSpeculative)
	} else {
		for _, t := range types {
			idle += snap.CountByStateInstanceType(machine.StateIdle, t)
			spec += snap.CountByStateInstanceType(machine.StateSpeculative, t)
			// Pre-sorted by (price, id); first entry is cheapest.
			if bucket := snap.ListByStateInstanceType(machine.StateIdle, t); len(bucket) > 0 {
				if cheapest == 0 || bucket[0].PricePerHour < cheapest {
					cheapest = bucket[0].PricePerHour
				}
			}
		}
	}

	confidence := pb.AvailableCapacityUpdate_CONFIDENCE_NONE
	if idle > 0 {
		confidence = pb.AvailableCapacityUpdate_CONFIDENCE_HIGH
	} else if spec > 0 {
		confidence = pb.AvailableCapacityUpdate_CONFIDENCE_MEDIUM
	}

	upd := &pb.AvailableCapacityUpdate{
		SupersedesKey:  "available:" + fp,
		Requirements:   conv.RequirementsToProto(profile.Requirements()),
		AvailableCount: int32(idle + spec), //nolint:gosec // bounded by inventory size
		Confidence:     confidence,
		CostPerHour:    cheapest,
	}
	if res := profile.Resources(); len(res) > 0 {
		out := make(map[string]string, len(res))
		for _, r := range res {
			out[r.Name] = r.Quantity
		}
		upd.Resources = &pb.Resources{Resources: out}
	}
	return upd
}

// pinnedInstanceTypesForAC mirrors decision.pinnedInstanceTypes but
// lives here to avoid importing the decision package from shard's
// emit-time path. Returns nil for profiles that don't pin to a finite
// instance-type set (the AC emitter then falls back to all-state
// counts).
func pinnedInstanceTypesForAC(p needs.Profile) []string {
	for _, r := range p.Requirements() {
		if r.Key != "node.kubernetes.io/instance-type" {
			continue
		}
		if r.Operator == needs.OperatorIn && len(r.Values) > 0 {
			return r.Values
		}
		return nil
	}
	return nil
}
