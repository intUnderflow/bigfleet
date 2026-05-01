// Package scenario holds the registered simulator scenarios. Each
// is a Go-defined sim.Scenario factory, accessible by name via Lookup
// and All. cmd/fauxctl uses these to drive the simulator.
package scenario

import (
	"fmt"
	"sort"

	"github.com/intUnderflow/bigfleet/sim"
)

// Factory builds a fresh Scenario per call. Returning a fresh value
// keeps successive simulator runs independent.
type Factory func() sim.Scenario

var registry = map[string]Factory{}

// Register binds a factory under a name. Panics on duplicate.
func Register(name string, f Factory) {
	if _, ok := registry[name]; ok {
		panic("scenario already registered: " + name)
	}
	registry[name] = f
}

// Lookup returns the factory for name, or false.
func Lookup(name string) (Factory, bool) {
	f, ok := registry[name]
	return f, ok
}

// Names returns the registered scenario names, sorted.
func Names() []string {
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// All returns every registered scenario, freshly constructed.
func All() []sim.Scenario {
	out := make([]sim.Scenario, 0, len(registry))
	for _, n := range Names() {
		out = append(out, registry[n]())
	}
	return out
}

// Must wraps Lookup with a panic-on-missing for cmd-line usage.
func Must(name string) sim.Scenario {
	f, ok := Lookup(name)
	if !ok {
		panic(fmt.Sprintf("unknown scenario %q (have: %v)", name, Names()))
	}
	return f()
}
