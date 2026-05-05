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
