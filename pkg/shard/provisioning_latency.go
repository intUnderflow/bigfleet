package shard

import (
	"time"

	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	"github.com/intUnderflow/bigfleet/pkg/needs"
)

// observeRolledUpDemand records the first-seen time for any
// (cluster, profile fingerprint) in the rollup that wasn't tracked
// before, and prunes entries whose fingerprint is no longer present.
// Called once per rollup ingest (paper §10.7).
func (s *Shard) observeRolledUpDemand(cluster machine.ClusterID, ns []needs.Need) {
	now := time.Now()
	seen := make(map[string]struct{}, len(ns))
	for _, n := range ns {
		seen[n.Profile.Fingerprint()] = struct{}{}
	}

	s.demandMu.Lock()
	defer s.demandMu.Unlock()

	tracked, ok := s.demandObservedAt[cluster]
	if !ok {
		tracked = make(map[string]time.Time, len(ns))
		s.demandObservedAt[cluster] = tracked
	}
	for fp := range seen {
		if _, exists := tracked[fp]; !exists {
			tracked[fp] = now
		}
	}
	for fp := range tracked {
		if _, still := seen[fp]; !still {
			delete(tracked, fp)
		}
	}
}

// observeProvisioningLatency emits one histogram sample for a Configure
// completion: time since this (cluster, fingerprint) was first observed
// in a rollup. Returns silently if no first-observed time exists (the
// fingerprint may have been pruned, or this transition isn't tied to
// rolled-up demand).
func (s *Shard) observeProvisioningLatency(cluster machine.ClusterID, fingerprint string) {
	s.demandMu.Lock()
	t, ok := s.demandObservedAt[cluster][fingerprint]
	s.demandMu.Unlock()
	if !ok {
		return
	}
	metrics.ShardProvisioningLatency.Observe(time.Since(t).Seconds())
}
