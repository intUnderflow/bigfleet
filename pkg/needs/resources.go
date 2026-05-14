package needs

import (
	"sort"

	"k8s.io/apimachinery/pkg/api/resource"
)

// Resource-vector arithmetic over []ResourceQty.
//
// ADR-0027: demand is a constrained aggregate resource request, so the
// hot path needs to add, max, and compare resource vectors. These are
// the shared primitives — the operator's roll-up aggregation (summing
// per-CR requests into a CapacityNeed's aggregate_resources, folding
// per-CR units into min_unit) and Phase 1's demand-vs-supply diff both
// build on them.
//
// A dimension absent from a vector is treated as zero. Results are
// canonical (sorted by Name) so they round-trip through Need
// construction stably. Quantity strings that don't parse as Kubernetes
// resource.Quantity degrade to zero — the operator canonicalises
// quantities at CR-aggregation time, so malformed values are not
// expected on the hot path.

// AddResources returns the per-dimension vector sum of two resource
// slices.
func AddResources(a, b []ResourceQty) []ResourceQty {
	return combineResources(a, b, func(x, y resource.Quantity) resource.Quantity {
		x.Add(y)
		return x
	})
}

// MaxResources returns the per-dimension maximum of two resource slices.
// Used to fold each CR's atomic schedulable unit into a CapacityNeed's
// min_unit.
func MaxResources(a, b []ResourceQty) []ResourceQty {
	return combineResources(a, b, func(x, y resource.Quantity) resource.Quantity {
		if y.Cmp(x) > 0 {
			return y
		}
		return x
	})
}

func combineResources(a, b []ResourceQty, fn func(x, y resource.Quantity) resource.Quantity) []ResourceQty {
	am := toQtyMap(a)
	bm := toQtyMap(b)
	names := make(map[string]struct{}, len(am)+len(bm))
	for n := range am {
		names[n] = struct{}{}
	}
	for n := range bm {
		names[n] = struct{}{}
	}
	out := make([]ResourceQty, 0, len(names))
	for n := range names {
		// Missing keys read back as the zero Quantity, which is a valid 0.
		r := fn(am[n], bm[n])
		out = append(out, ResourceQty{Name: n, Quantity: r.String()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func toQtyMap(in []ResourceQty) map[string]resource.Quantity {
	m := make(map[string]resource.Quantity, len(in))
	for _, r := range in {
		q, _ := resource.ParseQuantity(r.Quantity)
		m[r.Name] = q
	}
	return m
}

// SubResources returns the per-dimension saturating difference a - b
// (negative dimensions clamp to zero). Phase 1 turns a demand vector and
// a supply vector into a deficit vector with it.
func SubResources(a, b []ResourceQty) []ResourceQty {
	return combineResources(a, b, func(x, y resource.Quantity) resource.Quantity {
		x.Sub(y)
		if x.Sign() < 0 {
			return resource.Quantity{}
		}
		return x
	})
}

// Covers reports whether have satisfies want on every dimension want
// names. Dimensions present in have but absent from want are irrelevant;
// a dimension in want but absent from have is treated as have = 0.
func Covers(have, want []ResourceQty) bool {
	hm := toQtyMap(have)
	for _, w := range want {
		wq, _ := resource.ParseQuantity(w.Quantity)
		hq := hm[w.Name]
		if hq.Cmp(wq) < 0 {
			return false
		}
	}
	return true
}

// IsZero reports whether every dimension of v is non-positive — i.e. the
// vector represents no remaining demand.
func IsZero(v []ResourceQty) bool {
	for _, r := range v {
		q, _ := resource.ParseQuantity(r.Quantity)
		if q.Sign() > 0 {
			return false
		}
	}
	return true
}

// ScaleResources returns v with every dimension multiplied by n. Turns a
// per-replica resource shape and a replica count into an aggregate
// demand vector. n <= 0 (or an empty v) yields nil.
func ScaleResources(v []ResourceQty, n int) []ResourceQty {
	if n <= 0 || len(v) == 0 {
		return nil
	}
	out := make([]ResourceQty, len(v))
	for i, r := range v {
		q, _ := resource.ParseQuantity(r.Quantity)
		scaled := q.DeepCopy()
		for j := 1; j < n; j++ {
			scaled.Add(q)
		}
		out[i] = ResourceQty{Name: r.Name, Quantity: scaled.String()}
	}
	return out
}

// ResourceQtysFromMap converts a name→quantity-string map (the shape
// machine.EffectiveAllocatable / proto resource maps use) into the
// canonical sorted []ResourceQty form the vector ops operate on.
func ResourceQtysFromMap(m map[string]string) []ResourceQty {
	if len(m) == 0 {
		return nil
	}
	out := make([]ResourceQty, 0, len(m))
	for k, v := range m {
		out = append(out, ResourceQty{Name: k, Quantity: v})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
