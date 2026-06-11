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
	"strings"

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
	SameRack bool `yaml:"sameRack"`
	// SameZone (M66.2, complexity audit Action 2) — zone-scope
	// co-location: the load-driver emits podAffinity on
	// topology.kubernetes.io/zone instead of the rack key. This is how
	// real fleets place gangs larger than a rack (the catalog's
	// previous rack-coherent 64-256-node gangs exceeded any physical
	// rack and fabricated the demand behind the ADR-0042 layer).
	// Mutually exclusive with SameRack.
	SameZone       bool   `yaml:"sameZone"`
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
	//
	// Under ADR-0027 this is the *only* mechanism that creates
	// per-CR fingerprint diversity within an archetype: the Profile
	// fingerprint no longer includes resource shape, so varying
	// sizeBuckets alone produces a single Profile that aggregates
	// across resource sizes. LabelAxes is how the harness models
	// realistic CR fan-out for the operator's aggregate-resource
	// rollup path.
	LabelAxes []LabelAxis `yaml:"labelAxes"`

	// AllowPartial (scale-test review) — when true, marks an archetype
	// as "co-located but not gang." Such archetypes carry a `Same()`
	// requirement (rack affinity for replication latency) but the
	// workload tolerates partial fills: 3 of 5 requested machines
	// start, the operator re-requests the missing 2 next cycle. This
	// is the dominant production rack-affinity pattern (databases,
	// in-memory caches with peer chatter).
	//
	// Distinct from gang scheduling (gpu-training-*), where partial
	// fills are failure (MPI all-reduce needs every worker to start
	// simultaneously). The current proto carries no atomic-gang flag;
	// AllowPartial is documentation/forward-compat for a future ADR
	// that adds explicit gang semantics. At present every Need is
	// partial-fill-tolerant by default — AllowPartial just records
	// authorial intent so the future opt-in field can be derived.
	AllowPartial bool `yaml:"allowPartial"`

	// SpreadConstraintProb (scale-test review) — probability that a
	// per-CR draw carries the SpreadConstraint below. Models the
	// industry pattern that not every Pod in a Deployment carries
	// topologySpreadConstraints; the default-spread-from-template
	// pattern means ~45% of tiny services do, ~75% of medium ones,
	// 100% of HA-critical workloads. 0.0 = never; 1.0 = always.
	SpreadConstraintProb float64 `yaml:"spreadConstraintProb"`

	// SpreadConstraint (scale-test review) — the topology spread
	// constraint emitted on Pods drawn from this archetype, with
	// probability SpreadConstraintProb. The load-driver sets the
	// constraint's LabelSelector to the Pod's own labels so each
	// Pod participates in the spread group for its archetype.
	SpreadConstraint *SpreadConstraint `yaml:"spreadConstraint"`
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

// SpreadConstraint mirrors the autoscaler-relevant subset of
// Kubernetes' TopologySpreadConstraint. The load-driver emits this
// on a per-Pod basis when its archetype's SpreadConstraintProb roll
// hits; UPC's pod→CR translation carries it through to
// CapacityRequest.Spec.TopologySpread, and operator rollup folds it
// into the Need's Profile.Spread.
type SpreadConstraint struct {
	// TopologyKey is the Node label the spread is computed over.
	// Industry pattern (scale-test review): ~80% spread on
	// `topology.kubernetes.io/zone`, ~15% on `kubernetes.io/hostname`
	// (forces 1 Pod per Node — strict anti-affinity for replication),
	// ~5% custom keys.
	TopologyKey string `yaml:"topologyKey"`

	// MaxSkew is the maximum permitted difference between the number
	// of matching Pods in any two topology domains. 1 = strict (each
	// domain within 1 of the others); higher = more permissive.
	MaxSkew int32 `yaml:"maxSkew"`

	// WhenUnsatisfiable is one of "DoNotSchedule" (strict; Phase 1
	// must respect or shortfall) or "ScheduleAnyway" (best-effort).
	// Production mix (scale-test review): ~35% DoNotSchedule, ~65%
	// ScheduleAnyway.
	WhenUnsatisfiable string `yaml:"whenUnsatisfiable"`
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
		if a.SpreadConstraintProb < 0 || a.SpreadConstraintProb > 1 {
			return fmt.Errorf("%s[%d] %q: spreadConstraintProb must be in [0, 1]", field, i, a.Name)
		}
		if a.SpreadConstraint != nil {
			if a.SpreadConstraint.TopologyKey == "" {
				return fmt.Errorf("%s[%d] %q: spreadConstraint.topologyKey required when spreadConstraint is set", field, i, a.Name)
			}
			if a.SpreadConstraint.MaxSkew < 1 {
				return fmt.Errorf("%s[%d] %q: spreadConstraint.maxSkew must be ≥ 1", field, i, a.Name)
			}
			switch a.SpreadConstraint.WhenUnsatisfiable {
			case "DoNotSchedule", "ScheduleAnyway":
			default:
				return fmt.Errorf("%s[%d] %q: spreadConstraint.whenUnsatisfiable must be DoNotSchedule or ScheduleAnyway", field, i, a.Name)
			}
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
		// The value prefix is the key's last path segment, not the
		// whole key: a key like "scaletest.bigfleet/app" would
		// otherwise yield "scaletest.bigfleet/app-3", and a '/' is
		// not a valid character in a Kubernetes label *value* — the
		// apiserver rejects the Node/CR outright.
		prefix := ax.Key
		if i := strings.LastIndex(prefix, "/"); i >= 0 {
			prefix = prefix[i+1:]
		}
		out[ax.Key] = fmt.Sprintf("%s-%d", prefix, rng.Intn(count))
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

// PickSpread returns the archetype's SpreadConstraint with probability
// SpreadConstraintProb, or nil otherwise. Models the industry pattern
// that not every Pod in a Deployment carries spread (default-spread-
// from-template templates produce ~45% adoption on tiny services and
// ~75% on medium services, per scale-test review).
func (a *Archetype) PickSpread(rng *rand.Rand) *SpreadConstraint {
	if a.SpreadConstraint == nil || a.SpreadConstraintProb <= 0 {
		return nil
	}
	if a.SpreadConstraintProb >= 1.0 {
		return a.SpreadConstraint
	}
	if rng.Float64() < a.SpreadConstraintProb {
		return a.SpreadConstraint
	}
	return nil
}
