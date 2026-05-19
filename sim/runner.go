// Package sim is the deterministic simulator for the BigFleet decision
// engine. It runs the same pkg/decision, pkg/shard, and pkg/needs code
// the production binary uses, against an in-memory provider/fake. No
// real time.Ticker, no goroutines, no gRPC — every cycle is invoked
// synchronously via shard.Step so traces are reproducible.
//
// Scenarios live in sim/scenario as Go-defined structs. Each scenario
// declares an initial provider inventory, a timeline of events
// (rollups arriving for clusters), and assertions about end state.
//
// Use cmd/fauxctl to run, record, and verify scenarios. The CI nightly
// soak job uses the long-running scenarios under sim/scenario.
package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/intUnderflow/bigfleet/pkg/decision"
	"github.com/intUnderflow/bigfleet/pkg/fencing"
	"github.com/intUnderflow/bigfleet/pkg/machine"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	"github.com/intUnderflow/bigfleet/pkg/provider/fake"
	"github.com/intUnderflow/bigfleet/pkg/shard"
)

// Scenario is the declarative shape of a simulator run.
type Scenario struct {
	// Name is a stable identifier used by fauxctl.
	Name string

	// Description shows up in fauxctl list and in test output.
	Description string

	// InitialIdle seeds the fake provider with idle machines before
	// the first cycle runs. Models pre-existing bare-metal /
	// reserved capacity.
	InitialIdle []SeedMachine

	// InitialSpeculative seeds quota slots — speculative machines
	// the shard can elect to provision via Phase 1's fallback path.
	InitialSpeculative []SeedMachine

	// Events fire in order. Each event applies a NeedsTable.Replace
	// for one cluster and runs N cycles of the shard.
	Events []Event

	// Assertions evaluate against the final state after all events
	// have run.
	Assertions []Assertion

	// BeforeRun, if non-nil, runs once after the fake provider has
	// been seeded but before any cycle fires. Fault-injection
	// scenarios use this hook to queue Provider.FailNext entries.
	BeforeRun func(prov *fake.Provider)
}

// SeedMachine describes one machine to seed into the fake provider
// before the scenario starts.
type SeedMachine struct {
	ID            machine.ID
	InstanceType  string
	Zone          string
	CapacityType  machine.CapacityType
	PricePerHour  float64
	InterruptionP float64
	Resources     map[string]string
}

// Event is one step in a scenario's timeline.
type Event struct {
	// Cluster the rollup applies to. NeedsTable.Replace is full-
	// replacement, so the event's Needs entirely defines the
	// cluster's demand from this step onward.
	Cluster machine.ClusterID

	// Needs applied via Replace. Empty list = withdrawal.
	Needs []needs.Need

	// CyclesAfter is how many shard.Step calls run after the
	// rollup is applied. 1 is typical for a clean, single-cycle
	// outcome; some scenarios (preemption) need 2+ cycles to fully
	// resolve.
	CyclesAfter int
}

// Assertion is a predicate evaluated against the shard's final state.
// Each assertion has a name (for trace output) and a check function
// that returns nil on pass or an error describing the failure.
type Assertion struct {
	Name  string
	Check func(s *shard.Shard) error
}

// TraceEvent is one entry in a captured trace. Marshalled as JSON,
// one event per line, so goldens are human-diff-able.
type TraceEvent struct {
	Step    int            `json:"step"`
	Kind    string         `json:"kind"`
	Cluster string         `json:"cluster,omitempty"`
	Actions []ActionTrace  `json:"actions,omitempty"`
	State   *StateSnapshot `json:"state,omitempty"`
	Notes   string         `json:"notes,omitempty"`
}

// ActionTrace is one row in a trace's actions field.
type ActionTrace struct {
	Kind      string `json:"kind"`
	MachineID string `json:"machine_id"`
	Cluster   string `json:"cluster,omitempty"`
}

// StateSnapshot summarises the inventory at a step boundary.
type StateSnapshot struct {
	TotalMachines int            `json:"total"`
	ByState       map[string]int `json:"by_state"`
	Configured    map[string]int `json:"configured_per_cluster,omitempty"`
}

// Trace is the ordered event log captured during a Run.
type Trace []TraceEvent

// WriteTo serialises the trace to w as JSON lines.
func (tr Trace) WriteTo(w io.Writer) (int64, error) {
	var n int64
	enc := json.NewEncoder(w)
	for _, e := range tr {
		if err := enc.Encode(e); err != nil {
			return n, err
		}
		n += int64(len(e.Kind))
	}
	return n, nil
}

// Result is what Run returns: the captured trace plus the assertion
// outcomes.
type Result struct {
	Trace      Trace
	Assertions []AssertionResult
}

// AssertionResult is one assertion's outcome.
type AssertionResult struct {
	Name string
	Pass bool
	Err  error
}

// AllPassed reports whether every assertion in r passed.
func (r *Result) AllPassed() bool {
	for _, a := range r.Assertions {
		if !a.Pass {
			return false
		}
	}
	return true
}

// Run executes a Scenario and captures a Trace.
func Run(ctx context.Context, sc Scenario) (*Result, error) {
	prov := fake.New(fake.Options{InstantTransitions: true, Seed: 0xC0FFEE})
	for _, m := range sc.InitialIdle {
		prov.AddIdle(m.ID, machine.Profile{
			InstanceType: m.InstanceType, Zone: m.Zone, CapacityType: m.CapacityType,
			Resources: m.Resources,
		}, m.CapacityType, m.PricePerHour, m.InterruptionP)
	}
	for _, m := range sc.InitialSpeculative {
		prov.AddSpeculative(m.ID, machine.Profile{
			InstanceType: m.InstanceType, Zone: m.Zone, CapacityType: m.CapacityType,
			Resources: m.Resources,
		}, m.CapacityType, m.PricePerHour, m.InterruptionP)
	}

	if sc.BeforeRun != nil {
		sc.BeforeRun(prov)
	}

	tmpDir, err := os.MkdirTemp("", "bigfleet-sim-"+sc.Name+"-")
	if err != nil {
		return nil, fmt.Errorf("tmp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	epoch, err := fencing.LoadEpoch(filepath.Join(tmpDir, "epoch"))
	if err != nil {
		return nil, fmt.Errorf("load epoch: %w", err)
	}

	// Pre-size: each event contributes 1 + CyclesAfter trace entries
	// plus scenario_start + scenario_end.
	trace := make(Trace, 0, 2+2*len(sc.Events))
	step := 0

	sh, err := shard.New(shard.Config{
		ID:               "sim-" + sc.Name,
		Epoch:            epoch,
		Provider:         prov,
		CycleInterval:    1 * time.Second, // unused; cycles are driven via Step
		BootstrapTimeout: 1 * time.Second,
		LocalBootstrap: func(ctx context.Context, cluster machine.ClusterID, _ []needs.Requirement) ([]byte, error) {
			return []byte("# sim bootstrap for " + string(cluster) + "\n"), nil
		},
		OnActions: func(actions []decision.Action) {
			// Captured later from Step's return value; no-op here so
			// the simulator can attribute actions to the step they
			// fired in.
			_ = actions
		},
	})
	if err != nil {
		return nil, fmt.Errorf("shard new: %w", err)
	}

	trace = append(trace, TraceEvent{
		Step:  step,
		Kind:  "scenario_start",
		Notes: sc.Name,
		State: snapshotState(sh),
	})

	for _, ev := range sc.Events {
		step++
		// Apply the rollup.
		sh.ApplyRollup(ev.Cluster, ev.Needs)
		trace = append(trace, TraceEvent{
			Step:    step,
			Kind:    "rollup",
			Cluster: string(ev.Cluster),
			Notes:   fmt.Sprintf("needs=%d", len(ev.Needs)),
		})
		// Run the requested number of cycles for this event.
		cycles := ev.CyclesAfter
		if cycles == 0 {
			cycles = 1
		}
		for i := 0; i < cycles; i++ {
			step++
			actions := sh.Step(ctx)
			trace = append(trace, TraceEvent{
				Step:    step,
				Kind:    "cycle",
				Actions: traceActions(actions),
				State:   snapshotState(sh),
			})
		}
	}

	// Evaluate assertions against the final state.
	res := &Result{Trace: trace}
	for _, a := range sc.Assertions {
		err := a.Check(sh)
		res.Assertions = append(res.Assertions, AssertionResult{
			Name: a.Name, Pass: err == nil, Err: err,
		})
	}
	step++
	trace = append(trace, TraceEvent{
		Step:  step,
		Kind:  "scenario_end",
		State: snapshotState(sh),
	})
	res.Trace = trace
	return res, nil
}

func traceActions(actions []decision.Action) []ActionTrace {
	if len(actions) == 0 {
		return nil
	}
	out := make([]ActionTrace, 0, len(actions))
	for _, a := range actions {
		out = append(out, ActionTrace{
			Kind: a.Kind.String(), MachineID: string(a.MachineID),
			Cluster: string(a.Cluster),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].MachineID < out[j].MachineID
	})
	return out
}

func snapshotState(s *shard.Shard) *StateSnapshot {
	snap := s.Inventory().Snapshot()
	out := &StateSnapshot{
		TotalMachines: snap.Len(),
		ByState:       make(map[string]int),
		Configured:    make(map[string]int),
	}
	for _, m := range snap.All() {
		out.ByState[m.State.String()]++
		if m.State == machine.StateConfigured && m.Cluster != "" {
			out.Configured[string(m.Cluster)]++
		}
	}
	return out
}
