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
//
// M34 (Item 1): SeedArchetypes and DemandArchetypes optionally split
// the seed-side distribution (Configured machines pre-populated into
// the fake provider) from the demand-side distribution (CRs the
// load-driver creates). Real fleets show this drift: the seed reflects
// what's been running for a while; demand reflects what's being
// submitted right now. They share archetype shapes but the *frequency*
// distribution differs — seed is biased toward long-lived archetypes,
// demand may be biased toward short-lived burst-y ones.
//
// When both SeedArchetypes and DemandArchetypes are empty, both sides
// fall back to Archetypes (legacy single-catalog behaviour, unchanged).
type Catalog struct {
	Archetypes       []Archetype `yaml:"archetypes"`
	SeedArchetypes   []Archetype `yaml:"seedArchetypes"`
	DemandArchetypes []Archetype `yaml:"demandArchetypes"`
}

// ForSeed returns the archetype list the Configured-seed should
// distribute over. SeedArchetypes wins if non-empty; otherwise falls
// back to Archetypes.
func (c Catalog) ForSeed() []Archetype {
	if len(c.SeedArchetypes) > 0 {
		return c.SeedArchetypes
	}
	return c.Archetypes
}

// ForDemand returns the archetype list the load-driver should pick
// from. DemandArchetypes wins if non-empty; otherwise falls back to
// Archetypes.
func (c Catalog) ForDemand() []Archetype {
	if len(c.DemandArchetypes) > 0 {
		return c.DemandArchetypes
	}
	return c.Archetypes
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

	// LabelAxes (M35 / Item 2) — per-archetype label dimensions
	// that multiply per-CR fingerprint cardinality. Each axis
	// declares a Key (the label name on the Configured machine /
	// `In` requirement on the CR) and a Count (the value space:
	// values are "{key}-0" through "{key}-{count-1}"). Per CR a
	// uniform random value is drawn per axis; per seeded Configured
	// machine the same draw populates Profile.Labels. Models real
	// production CR shapes that pin to labels like
	// `app=foo, team=search, version=v1.4` — with 50 teams × 200
	// apps × 5 versions = 50_000 distinct fingerprints per cluster.
	LabelAxes []LabelAxis `yaml:"labelAxes"`
}

// LabelAxis is one production-style label dimension.
type LabelAxis struct {
	Key   string `yaml:"key"`
	Count int    `yaml:"count"`
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
	if err := validateArchetypes("archetypes", c.Archetypes); err != nil {
		return Catalog{}, err
	}
	if err := validateArchetypes("seedArchetypes", c.SeedArchetypes); err != nil {
		return Catalog{}, err
	}
	if err := validateArchetypes("demandArchetypes", c.DemandArchetypes); err != nil {
		return Catalog{}, err
	}
	return c, nil
}

func validateArchetypes(field string, arches []Archetype) error {
	for i, a := range arches {
		if a.Name == "" {
			return fmt.Errorf("%s[%d]: name required", field, i)
		}
		if len(a.InstanceTypes) == 0 {
			return fmt.Errorf("%s[%d] %q: at least one instanceType required", field, i, a.Name)
		}
		if a.Weight < 0 {
			return fmt.Errorf("%s[%d] %q: weight must be ≥ 0", field, i, a.Name)
		}
	}
	return nil
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

// PickLabels returns a per-axis value draw, suitable both for setting
// labels on a seeded Configured machine and for emitting `In [value]`
// requirements on a CR. Returns nil when LabelAxes is empty.
// M35 / Item 2.
func (a *Archetype) PickLabels(rng *rand.Rand) map[string]string {
	if len(a.LabelAxes) == 0 {
		return nil
	}
	out := make(map[string]string, len(a.LabelAxes))
	for _, ax := range a.LabelAxes {
		count := ax.Count
		if count <= 0 {
			count = 1
		}
		out[ax.Key] = fmt.Sprintf("%s-%d", ax.Key, rng.Intn(count))
	}
	return out
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
