package needs

import (
	"math"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Parsed-form resource-vector helpers for hot paths.
//
// The canonical []ResourceQty form stores quantities as strings, so
// every arithmetic op via combineResources allocates two maps + a set
// + an output slice and parses every input string. That is fine for
// rollup / config-time paths but ruinous in Phase 1's takeCoLocated
// score loop, which iterates every unclaimed machine in every bucket
// on every call. At cloud scale (bigfleet-uber #17 measured ~31K
// takeCoLocated calls per shard cycle at uber-50k) the per-call
// allocation overhead is what blows Phase 1 past its envelope.
//
// These helpers convert quantities to int64 milli-units at a chosen
// boundary (typically the bucket build in takeCoLocated). All hot-path
// arithmetic then runs on plain int64s with zero allocations and no
// internal big-int growth (resource.Quantity.Add allocates as its
// inf.Dec representation grows, so a "parsed Quantity" form is no
// cheaper than the string form for accumulators).
//
// Int64 milli-units fits every practical demand vector: a 1M-Pod
// cluster needing 4 cores per Pod is 4×10⁹ milli-cpu, well under
// int64.MaxValue (≈9.2×10¹⁸). Saturated addition clamps at MaxInt64
// so we never wrap silently if a workload ever does grow that large.

// ParsedQty is a parsed resource quantity in milli-units.
type ParsedQty struct {
	Name  string
	Milli int64
}

// ParseQtyMilli parses a quantity string into milli-units.
// Unparseable values degrade to zero.
func ParseQtyMilli(s string) int64 {
	q, _ := resource.ParseQuantity(s)
	return q.MilliValue()
}

// ParseQtysMap parses a []ResourceQty into a name→milli-units map.
func ParseQtysMap(in []ResourceQty) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	m := make(map[string]int64, len(in))
	for _, r := range in {
		m[r.Name] = ParseQtyMilli(r.Quantity)
	}
	return m
}

// ParseAllocatableMap parses a machine.EffectiveAllocatable-shaped
// (name → quantity-string) map into a []ParsedQty.
func ParseAllocatableMap(m map[string]string) []ParsedQty {
	if len(m) == 0 {
		return nil
	}
	out := make([]ParsedQty, 0, len(m))
	for k, v := range m {
		out = append(out, ParsedQty{Name: k, Milli: ParseQtyMilli(v)})
	}
	return out
}

// AddParsedInto adds src's quantities into dst in place. Saturates
// at MaxInt64 on overflow. Zero allocations.
func AddParsedInto(dst map[string]int64, src []ParsedQty) {
	for _, r := range src {
		a := dst[r.Name]
		b := r.Milli
		if b > 0 && a > math.MaxInt64-b {
			dst[r.Name] = math.MaxInt64
			continue
		}
		dst[r.Name] = a + b
	}
}

// SubParsedInto subtracts src from dst in place, clamping negative
// results to zero (matches SubResources's saturating semantics).
func SubParsedInto(dst map[string]int64, src []ParsedQty) {
	for _, r := range src {
		v := dst[r.Name] - r.Milli
		if v < 0 {
			v = 0
		}
		dst[r.Name] = v
	}
}

// CoversParsed reports whether have (parsed alloc slice) covers want
// (parsed requirement map) on every positive dimension of want.
// have is typically tiny (1–3 entries) so the linear scan beats a map.
func CoversParsed(have []ParsedQty, want map[string]int64) bool {
	for name, w := range want {
		if w <= 0 {
			continue
		}
		var h int64
		found := false
		for _, r := range have {
			if r.Name == name {
				h = r.Milli
				found = true
				break
			}
		}
		if !found || h < w {
			return false
		}
	}
	return true
}

// CoversMaps reports whether have covers want (both parsed maps).
func CoversMaps(have, want map[string]int64) bool {
	for name, w := range want {
		if w <= 0 {
			continue
		}
		if have[name] < w {
			return false
		}
	}
	return true
}

// IsZeroMap reports whether m has no positive dimensions.
func IsZeroMap(m map[string]int64) bool {
	for _, v := range m {
		if v > 0 {
			return false
		}
	}
	return true
}

// ClearMap empties m for reuse as a scratch accumulator. The compiler
// lowers this to runtime.mapclear.
func ClearMap(m map[string]int64) {
	for k := range m {
		delete(m, k)
	}
}
