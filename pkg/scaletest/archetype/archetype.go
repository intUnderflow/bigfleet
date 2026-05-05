// Package archetype defines the shared "workload archetype" catalog
// used by the scaletest harness. Both the load-driver (which creates
// CapacityRequests against the per-cluster apiserver) and the shard
// binary (which seeds Configured machines into the in-process fake
// provider) read the same catalog so demand and pre-bound inventory
// describe the same fleet shape.
//
// An archetype is one of the recurring workload patterns observed in
// real fleets — GPU training, GPU inference, CPU batch, CPU service,
// memory-bound DBs, critical realtime. Each carries a frequency
// weight, a set of acceptable instance types and zones, the resources
// it asks for, the priority levels it can run at, and its
// interruption + reclamation penalties.
//
// See test/scaletest/profiles/archetypes/realistic.yaml for the
// production-realistic catalog. M31.
package archetype

import (
	"fmt"
	"math/rand"
	"os"

	"gopkg.in/yaml.v3"
)

// Catalog is the top-level YAML shape mounted as a ConfigMap into
// both the load-driver and shard pods.
type Catalog struct {
	Archetypes []Archetype `yaml:"archetypes"`
}

// Archetype describes one workload pattern. Both producer (CR)
// and consumer (Configured machine) sides read this same struct.
type Archetype struct {
	Name                string            `yaml:"name"`
	Weight              int               `yaml:"weight"`
	InstanceTypes       []string          `yaml:"instanceTypes"`
	Zones               []string          `yaml:"zones"`
	Resources           map[string]string `yaml:"resources"`
	PriorityClasses     []int32           `yaml:"priorityClasses"`
	InterruptionPenalty float64           `yaml:"interruptionPenalty"`
	ReclamationPenalty  float64           `yaml:"reclamationPenalty"`

	// SizeBuckets (ADR-0015 §1) — when non-empty, per-CR resources
	// are picked weighted-random from this list and the top-level
	// Resources field is ignored. Catalog-wide this multiplies the
	// distinct-fingerprint count to production-shape (~hundreds per
	// cluster instead of one per archetype).
	SizeBuckets []SizeBucket `yaml:"sizeBuckets"`

	// MeanLifetimeSeconds (ADR-0015 §2) — exponential-mean CR
	// lifetime. 0 = effectively immortal (long-running services);
	// the load-driver doesn't age these. Short-lived archetypes
	// (batch jobs, CI) carry positive values.
	MeanLifetimeSeconds int `yaml:"meanLifetimeSeconds"`

	// SameRack (ADR-0015 §4) — when true, the load-driver emits
	// CRs with a `Same` requirement on `topology.bigfleet/rack`,
	// modelling tightly-coupled workloads (multi-GPU training,
	// distributed-DB replicas). Group sizes drawn from
	// GroupSizeRange (defaults to [1, 1] if unset, i.e. single-
	// machine — Same is then a no-op).
	SameRack       bool   `yaml:"sameRack"`
	GroupSizeRange [2]int `yaml:"groupSizeRange"`
}

// SizeBucket is one entry in an archetype's size distribution.
// Picked weighted-random per CR; supplies the Resources map that
// would otherwise come from Archetype.Resources.
type SizeBucket struct {
	Weight    int               `yaml:"weight"`
	Resources map[string]string `yaml:"resources"`
}

// LoadCatalog reads + parses the archetype catalog at path. Returns
// the parsed Catalog or an error.
func LoadCatalog(path string) (Catalog, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Catalog{}, fmt.Errorf("read %s: %w", path, err)
	}
	var c Catalog
	if err := yaml.Unmarshal(b, &c); err != nil {
		return Catalog{}, fmt.Errorf("parse %s: %w", path, err)
	}
	for i, a := range c.Archetypes {
		if a.Name == "" {
			return Catalog{}, fmt.Errorf("archetype[%d]: name required", i)
		}
		if len(a.InstanceTypes) == 0 {
			return Catalog{}, fmt.Errorf("archetype[%d] %q: at least one instanceType required", i, a.Name)
		}
		if a.Weight < 0 {
			return Catalog{}, fmt.Errorf("archetype[%d] %q: weight must be ≥ 0", i, a.Name)
		}
	}
	return c, nil
}

// MaxPriority returns the highest priority in the archetype's
// PriorityClasses. Used by the seed to assign the established
// workload's "running at the top of its tier" priority. Returns
// 1000 when PriorityClasses is empty (matches the load-driver
// fallback).
func (a Archetype) MaxPriority() int32 {
	if len(a.PriorityClasses) == 0 {
		return 1000
	}
	max := a.PriorityClasses[0]
	for _, p := range a.PriorityClasses[1:] {
		if p > max {
			max = p
		}
	}
	return max
}

// Picker chooses an archetype with frequency proportional to weight.
type Picker struct {
	cum  []int
	by   []*Archetype
	full int
}

// NewPicker builds a weighted-random picker over the catalog. Returns
// nil when the catalog is empty (callers fall back to legacy single-
// shape behaviour).
func NewPicker(arches []Archetype) *Picker {
	if len(arches) == 0 {
		return nil
	}
	p := &Picker{}
	for i := range arches {
		w := arches[i].Weight
		if w <= 0 {
			w = 1
		}
		p.full += w
		p.cum = append(p.cum, p.full)
		p.by = append(p.by, &arches[i])
	}
	return p
}

// Pick returns the chosen archetype, or nil when the picker is nil.
func (p *Picker) Pick(rng *rand.Rand) *Archetype {
	if p == nil {
		return nil
	}
	r := rng.Intn(p.full)
	for i, c := range p.cum {
		if r < c {
			return p.by[i]
		}
	}
	return p.by[len(p.by)-1]
}

// PickSize returns one of the archetype's size buckets, weighted by
// each bucket's Weight field. Returns the archetype's flat Resources
// map if SizeBuckets is empty (legacy single-shape).
func (a *Archetype) PickSize(rng *rand.Rand) map[string]string {
	if len(a.SizeBuckets) == 0 {
		return a.Resources
	}
	full := 0
	for _, b := range a.SizeBuckets {
		w := b.Weight
		if w <= 0 {
			w = 1
		}
		full += w
	}
	r := rng.Intn(full)
	cum := 0
	for _, b := range a.SizeBuckets {
		w := b.Weight
		if w <= 0 {
			w = 1
		}
		cum += w
		if r < cum {
			return b.Resources
		}
	}
	return a.SizeBuckets[len(a.SizeBuckets)-1].Resources
}

// PickGroupSize returns the number of machines in a single Same-rack
// group for this archetype. Returns 1 (no grouping) when GroupSizeRange
// is unset.
func (a *Archetype) PickGroupSize(rng *rand.Rand) int {
	lo, hi := a.GroupSizeRange[0], a.GroupSizeRange[1]
	if lo <= 0 {
		lo = 1
	}
	if hi <= 0 || hi < lo {
		hi = lo
	}
	if lo == hi {
		return lo
	}
	return lo + rng.Intn(hi-lo+1)
}
