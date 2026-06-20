// Command scaletest-runner orchestrates one BigFleet scale-test run.
//
//	scaletest-runner \
//	    --kubeconfig=$HOME/.kube/config \
//	    --profile=test/scaletest/profiles/dev-5k.yaml \
//	    --duration=10m \
//	    --output=./results/$(date +%Y%m%d)-dev-5k/
//
// What it does, in order:
//
//  1. Read the profile YAML.
//  2. Detect the target (kind / homelab-ish / EKS / GKE) from the
//     current kubeconfig context name and warn about cost.
//  3. helm install the scaletest chart with the profile values.
//  4. Wait for steady state (every kwok pod reports its CR target met).
//  5. Sleep --duration.
//  6. Snapshot Prometheus TSDB to a tarball, scp it out via kubectl cp.
//  7. Emit a summary JSON: profile, target, scale, key metrics p50/p99,
//     pass/fail, estimated and actual cost.
//  8. helm uninstall (deferred; runs even on Ctrl-C / panic).
//
// Cost is computed from the profile's costEstimate block × actual run
// duration. AWS Cost Explorer reconciliation is a separate `reconcile`
// subcommand that runs 24h after the run.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
)

type costEstimate struct {
	VCPU              int     `yaml:"vCPU"               json:"vCPU"`
	MemoryGB          int     `yaml:"memoryGB"           json:"memoryGB"`
	AWSSpotUSDPerHour float64 `yaml:"awsSpotUsdPerHour"  json:"awsSpotUsdPerHour"`
	Notes             string  `yaml:"notes"              json:"notes"`
}

type profileFile struct {
	KWOK struct {
		ClusterCount int `yaml:"clusterCount"`
	} `yaml:"kwok"`
	Shard struct {
		Replicas     int `yaml:"replicas"`
		SeedMachines int `yaml:"seedMachines"`
	} `yaml:"shard"`
	LoadProfile struct {
		Target          int `yaml:"target"`
		DurationSeconds int `yaml:"durationSeconds"`
		// SettleSeconds is the runtime view of profileV2.LoadProfile.
		// SettleSeconds, copied across in the merge path. See the doc
		// comment on the profileV2 field for semantics.
		SettleSeconds int `yaml:"settleSeconds"`
	} `yaml:"loadProfile"`
	// RampBudget overrides the ramp-to-steady-state deadline. Format
	// is any time.ParseDuration string ("30m", "1h45m"). Empty / unset
	// → use the default formula (M22):
	//   max(15min, totalCRs / 750 CR/sec, durationSeconds × 0.5).
	// 750 CR/sec is sized 1.5× over the 1M de-risk's measured ~1110
	// CR/sec aggregate, with a 15 min floor for small profiles where
	// the formula would otherwise undershoot cold-start time.
	RampBudget string `yaml:"rampBudget"`
	// RunnerActions are fired by the runner during the soak window.
	// AtSeconds is offset from steady-state-reached. Action is one of
	//   kill-coordinator-leader: delete the coordinator's leader pod;
	//                            asserts coordinator_raft_term advances
	//                            by at least 1 within 60 s.
	//   kill-shard-<podname>:    delete the named shard StatefulSet pod;
	//                            asserts the pod is rescheduled and
	//                            resumes publishing cycle metrics
	//                            within 60 s.
	// Unrecognised actions are recorded as failures with no fire-side
	// effects.
	RunnerActions []runnerAction `yaml:"runnerActions"`
	CostEstimate  costEstimate   `yaml:"costEstimate"`

	// SLO holds per-profile gate overrides. ADR-0014's defaults
	// (cycle p99 ≤ 5s, internal binding p99 ≤ 5s) target the
	// in-process fake-provider tier. Profiles running at production-
	// defensible settings (M39 conservative QPS, M40 30-60s rollup)
	// raise these. ADR-0018: internalBindingLatencyP99Seconds is
	// BigFleet-internal-only — the harness's fake provider returns
	// instantly, so user-facing latency is not measured here.
	SLO sloOverrides `yaml:"slo"`
}

type sloOverrides struct {
	// InternalBindingLatencyP99Seconds is RETIRED to informational by
	// ADR-0054: under the default uncapped real kube-scheduler it is an
	// end-to-end pod-bind p99 dominated by scheduler retry/backoff WAIT +
	// the reprovision back-edge — neither BigFleet's deliverable — so it
	// no longer gates pass() or soakFailFastCheck. Still parsed for
	// backward-compat (profiles that set it don't error) and still scraped
	// into summary.json as regime-context.
	InternalBindingLatencyP99Seconds float64 `yaml:"internalBindingLatencyP99Seconds"`
	ShardCycleDurationP99Seconds     float64 `yaml:"shardCycleDurationP99Seconds"`
	OperatorRollupP99Seconds         float64 `yaml:"operatorRollupP99Seconds"`
	OperatorAckP99Seconds            float64 `yaml:"operatorAckP99Seconds"`
	// MaxReclaimActionsDuringSoak bounds the Reclaim-action count over
	// the measured soak window (V2 runs only). Default 0 preserves the
	// original "zero reclaims" assertion. A positive value accepts the
	// engine's structurally non-zero endogenous async-actuation reclaim
	// floor (bigfleet-uber #65-69), which is proven coverage-harmless;
	// a regression (sustained high churn, far above the bound) still
	// trips. Author-owned posture number, like ReclaimGrace.
	MaxReclaimActionsDuringSoak int `yaml:"maxReclaimActionsDuringSoak"`
	// ShardConfigurePhaseP99Seconds gates the per-machine wall-clock
	// Idle→Configuring→Configured inside executeBootstrap — the
	// capacity-materialization LATENCY BigFleet owns end to end (ADR-0054
	// Half 1, the held bar that inherits ADR-0020's 15s method). Gated
	// only when > 0. PROVISIONAL posture number, AUTHOR-OWNED (like
	// MaxReclaimActionsDuringSoak): ratified after the dev-50 + uber-5k
	// re-measure reports the de-tailed actuals (#78 measured 0.56s).
	ShardConfigurePhaseP99Seconds float64 `yaml:"shardConfigurePhaseP99Seconds"`
	// BootstrapSuccessRatio is the MIN acceptable
	// success/(success+failure) ratio for Bootstrap actions over the soak
	// — materialization THROUGHPUT, the counterpart to configure-phase's
	// latency (ADR-0054 Half 1). It closes the throughput-collapse hole
	// the latency + shortfall gates miss under ADR-0052 in-flight
	// crediting. Gated only when > 0; 0 = unset/skip. NOTE: this is a
	// minimum — pass() fails when the measured ratio is BELOW it, the
	// opposite direction from the latency gates. PROVISIONAL posture
	// number, AUTHOR-OWNED (e.g. 0.99).
	BootstrapSuccessRatio float64 `yaml:"bootstrapSuccessRatio"`
	// OperatorNodeStateUpdateP99Seconds gates the operator publishing
	// UpcomingNode=Ready after the shard signals Configured — the one
	// BigFleet-owned hop with zero prior runner coverage (ADR-0054 Half
	// 1; the Drop S >=102s tail lived here). Gated only when > 0.
	// PROVISIONAL posture number, AUTHOR-OWNED (cloud 1.5s; dev profiles
	// loosen for the kine write tail). M79.8 (#79 ratification): the ~1s
	// p99 is apiserver-WRITE bound (handler = trivial compute + 2-3
	// apiserver round-trips; same class as operatorAck), not operator
	// logic — bar sized to the apiserver-write regime, finalized by the
	// per-op duration histogram on the next clean run.
	OperatorNodeStateUpdateP99Seconds float64 `yaml:"operatorNodeStateUpdateP99Seconds"`
	// EndToEndPodBindP50Seconds is a LOOSE liveness floor on the
	// end-to-end pod-bind p50 (ADR-0054 Half 2): p50 sits below the
	// scheduler-retry tail, so a p50 blowup means the COMMON bind path
	// broke. Explicitly coarse liveness, NOT the release gate (which
	// moved onto the BigFleet-property bars above). Gated only when > 0.
	// PROVISIONAL posture number, AUTHOR-OWNED (cloud 10s; dev profiles
	// loosen for the kine write tail).
	EndToEndPodBindP50Seconds float64 `yaml:"endToEndPodBindP50Seconds"`

	// --- Inverted SLO gates (engine-correctness profiles) ---
	//
	// The default verdict treats a standing shortfall as a hard FAILURE
	// (shardShortfalls != 0 fails pass(); a standing shortfall + frozen
	// acquisitions trips the steady-state plateau detector). That is
	// correct for a capacity-met run, but it CANNOT express two
	// engine-correctness tests where a standing shortfall (or a Preempt)
	// is the EXPECTED pass condition:
	//
	//   1. Genuine-scarcity priority-throttle: supply < demand by design.
	//      The sole-throttle hard rule (CLAUDE.md) says BigFleet satisfies
	//      demand strictly priority-descending and leaves the surplus
	//      LOW-priority demand as a standing shortfall — NEVER starving
	//      high-priority. Here a non-zero, CONVERGED shortfall is a PASS.
	//   2. Phase-2 preemption: a zero-headroom LOW-priority fill plus a
	//      HIGH-priority burst that can only bind by preempting incumbents.
	//      Here a non-zero Preempt-action count is a PASS.
	//
	// All fields default zero-valued-off, so every existing profile's
	// pass() (and steady-state wait) behaviour is byte-identical.

	// ExpectStandingShortfall inverts the shortfall verdict for this run.
	// When true: the default `shardShortfalls != 0 → fail` gate is
	// REPLACED by "shortfall must be > 0 AND converged" (the standing
	// shortfall is the expected steady state, not a failure), and the
	// steady-state wait treats a stable shortfall with flat acquisitions
	// as steady rather than tripping the demand-side plateau fail-fast.
	// Every OTHER gate (cycle / configure / bootstrap / node-state / …)
	// still applies. Default false = the unconditional ADR-0045/0054
	// shortfall-zero gate, unchanged.
	//
	// LIMITATION (engine-metric gap, reported, NOT worked around): the
	// sole-throttle hard rule's "confined to the lowest priority tier,
	// zero high-priority shortfall" half is NOT assertable here. The
	// only shortfall metric is the aggregate `bigfleet_shard_shortfalls`
	// gauge (and an age-bucketed twin); neither carries a priority /
	// priority-class label. Asserting confinement would require an engine
	// metric/label change, which is author-gated. This gate therefore
	// asserts only the runner-feasible half: shortfall > 0 and converged.
	ExpectStandingShortfall bool `yaml:"expectStandingShortfall"`
	// ShortfallStabilityMax bounds |Δ shortfalls| over the soak window —
	// the convergence half of the inverted shortfall gate. Only consulted
	// when ExpectStandingShortfall is true. A converged scarcity run holds
	// its shortfall roughly flat (the engine has settled into the
	// priority-throttled steady state); a GROWING shortfall means demand
	// is outrunning the engine, which is a real failure even under
	// scarcity. Default 0 → strict (no growth tolerated). The delta is
	// measured by the shardShortfallsDelta query (signed; an absolute
	// value is compared so a shrinking shortfall also passes).
	ShortfallStabilityMax float64 `yaml:"shortfallStabilityMax"`
	// ExpectPreemptions asserts the engine actually preempted: the
	// cumulative Preempt-action count (bigfleet_shard_actions_total
	// {kind="Preempt"}) must be > 0. The Preempt counter EXISTS as an
	// engine metric (pkg/metrics/metrics.go ShardActionsTotal, kind
	// label includes "Preempt"; emitted by Phase 2,
	// pkg/decision/phase2_inversions.go), so this gate is fully
	// runner-feasible. Default false = no preemption assertion.
	ExpectPreemptions bool `yaml:"expectPreemptions"`
}

type runnerAction struct {
	AtSeconds int    `yaml:"atSeconds"`
	Action    string `yaml:"action"`
}

// profileBurst mirrors the load-driver's burstSpec (ADR-0015 §3,
// test/scaletest/cmd/load-driver/main.go) — the runner does not interpret
// these; it copies the fields into the rendered loadProfile so the chart
// passes them through to the load-driver's profile.yaml verbatim. #327.
//
//   - AtSeconds:       offset from the load-driver's own start (NOT the
//     runner's soak start).
//   - Archetype:       the archetype name to inject; resolved against the
//     full catalog including burstOnly archetypes.
//   - ExtraTarget:     extra Pods worth of demand; for a gang archetype
//     ExtraTarget:1 yields one full gang (the load-driver does not
//     truncate a forced gang to remaining headroom).
//   - DurationSeconds: how long before the burst objects are drained
//     (0 = never drain — the gang lives for the rest of the run).
//   - Selectivity:     fraction of clusters that participate (a Bernoulli
//     trial per driver). 1.0 = every cluster; for a single-gang
//     foundation-training event a small selectivity models one cluster's
//     job arriving.
type profileBurst struct {
	AtSeconds       int     `yaml:"atSeconds"`
	Archetype       string  `yaml:"archetype"`
	ExtraTarget     int     `yaml:"extraTarget"`
	DurationSeconds int     `yaml:"durationSeconds"`
	Selectivity     float64 `yaml:"selectivity"`
}

// profileScaleDown is a scheduled sustained demand drop (reclaim-cycle
// drill): at AtSeconds the load-driver sheds steady workloads until the
// active Pod count falls to Target×TargetMultiplier, driving Phase 3
// reclaim. Threaded into the load-driver's loadProfile.scaleDowns.
type profileScaleDown struct {
	AtSeconds        int     `yaml:"atSeconds"`
	TargetMultiplier float64 `yaml:"targetMultiplier"`
}

// substrateFile is the runtime-side half of ADR-0034: it describes the
// hosts the scale test will run on, the per-cluster apiserver operating
// point, and the kwok-pod resource budget. Orthogonal to profileFile,
// which describes the test itself (scale, catalog, density, ramp). The
// runner merges the two into Helm values; the profile YAML stays
// substrate-agnostic.
//
// Validation lives on `validate()`; nonsense values (zero capacity,
// unknown storage backend, etc.) are caught before any helm install
// touches the cluster.
type substrateFile struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name        string `yaml:"name"`
		Description string `yaml:"description"`
	} `yaml:"metadata"`
	Host struct {
		VCPU      int `yaml:"vCPU"`
		MemoryGiB int `yaml:"memoryGiB"`
	} `yaml:"host"`
	Cluster struct {
		// PodsPerCluster is the substrate's "comfortable Pod ceiling"
		// per kwok apiserver. Past this point bind throughput tails off
		// (kine WAL, etcd watch, kube-scheduler list-watch).
		PodsPerCluster           int    `yaml:"podsPerCluster"`
		ClustersPerHost          int    `yaml:"clustersPerHost"`
		Storage                  string `yaml:"storage"` // "etcd" or "kine"
		BindThroughputPodsPerSec int    `yaml:"bindThroughputPodsPerSec"`
	} `yaml:"cluster"`
	KwokPod struct {
		// Requests and Limits are **per-container** budgets. The chart
		// applies them identically to both containers in the kwok pod
		// (apiserver + workload). Per-Pod totals are 2× these values.
		// The substrate's host.vCPU/memoryGiB should be calibrated
		// against `2 × kwokPod.requests × cluster.clustersPerHost` plus
		// system-under-test overhead.
		Requests              substrateResourceMap `yaml:"requests"`
		Limits                substrateResourceMap `yaml:"limits"`
		SharedVolumeSizeLimit string               `yaml:"sharedVolumeSizeLimit"`
	} `yaml:"kwokPod"`
	APIServer struct {
		ExtraFlags []string `yaml:"extraFlags"`
	} `yaml:"apiserver"`
	CostEstimate struct {
		PerHostUSDPerHour float64 `yaml:"perHostUsdPerHour"`
		Notes             string  `yaml:"notes"`
	} `yaml:"costEstimate"`
	Provisioning string `yaml:"provisioning"`
}

type substrateResourceMap struct {
	CPU    string `yaml:"cpu"`
	Memory string `yaml:"memory"`
}

// profileV2 is the substrate-agnostic test definition introduced by
// ADR-0034. It is the second half of the BYO-substrate split:
// profileV2 + substrateFile compose into a runnable scale test. Pure
// test description — no clusterCount, no kwokPod resources, no
// per-host cost. Those live on substrateFile.
//
// The legacy `profileFile` struct stays in place until Stage 7 of the
// ADR-0034 migration deletes the legacy YAMLs.
type profileV2 struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name        string `yaml:"name"`
		Description string `yaml:"description"`
	} `yaml:"metadata"`
	Scale struct {
		Machines int `yaml:"machines"`
		Density  int `yaml:"density"` // Pods per machine
	} `yaml:"scale"`
	Catalog struct {
		Archetypes string `yaml:"archetypes"` // "realistic" or "uniform"
	} `yaml:"catalog"`
	Seed struct {
		ConfiguredFraction    float64 `yaml:"configuredFraction"`
		SpeculativeMultiplier int     `yaml:"speculativeMultiplier"`
		IdleHeadroomFraction  float64 `yaml:"idleHeadroomFraction"`
		// PreBind (M52.B, ADR-0035): when true, the load-driver
		// fast-binds the initial Pod fill to fake-Nodes via the Bind
		// API (after the Pods have gone through the realistic
		// Unschedulable → UPC → CapacityRequest path), bypassing the
		// slow kube-scheduler bulk-bind ramp. Default false keeps the
		// scheduler-bound ramp.
		PreBind bool `yaml:"preBind"`
	} `yaml:"seed"`
	LoadProfile struct {
		RampSeconds    int     `yaml:"rampSeconds"`
		SoakSeconds    int     `yaml:"soakSeconds"`
		ChurnPerMinute float64 `yaml:"churnPerMinute"`
		// SettleSeconds delays the start of the reclaim-action
		// measurement window by this many seconds after soakStart, so
		// the window covers the SETTLED portion of the soak rather than
		// the post-fill settling transient. The fleet keeps actuating
		// for 1-2 min after "steady declared" (ADR-0021 async execute);
		// that decaying tail dominates a full-soak integral and inflates
		// the reclaim count even though steady-state churn is much lower
		// (bigfleet-uber #65-69). Only the reclaim baseline snapshot
		// moves; bind-latency / cycle / rollup SLOs are unaffected.
		// Default 0 = snapshot at soakStart (current behaviour, every
		// other profile unchanged). Guard: a value ≥ soakSeconds falls
		// back to the soakStart snapshot so a misconfig can't leave the
		// window empty.
		SettleSeconds int `yaml:"settleSeconds"`
		// Bursts (#327, ADR-0015 §3) are passed through verbatim to the
		// load-driver's profile (the chart toYaml's the whole loadProfile
		// map into profile.yaml, which the load-driver parses with the
		// same field). A burst event injects ExtraTarget extra Pods worth
		// of a NAMED archetype at AtSeconds-from-driver-start, then drains
		// after DurationSeconds. The realism profile (5k.yaml) uses this
		// to inject one gpu-training-large gang mid-run — foundation-model
		// training modelled as an occasional burst rather than a
		// steady-catalog archetype (it is burstOnly in realistic.yaml, so
		// the burst is the ONLY source of that demand). Without this field
		// a bursts: block in a V2 profile would be silently dropped — the
		// V2 struct, not the chart, is the gate.
		Bursts []profileBurst `yaml:"bursts"`
		// ScaleDowns (reclaim-cycle drill): scheduled sustained demand
		// drops threaded into the load-driver. Like bursts, the V2 struct
		// is the gate — a scaleDowns block is dropped unless declared here.
		ScaleDowns []profileScaleDown `yaml:"scaleDowns"`
	} `yaml:"loadProfile"`
	// RampBudget overrides the rampSeconds-derived deadline. Same
	// semantics as profileFile.RampBudget (M22). Empty → use
	// rampSeconds; non-empty → time.ParseDuration string wins.
	RampBudget    string         `yaml:"rampBudget"`
	RunnerActions []runnerAction `yaml:"runnerActions"`
	SLO           sloOverrides   `yaml:"slo"`

	// Topology overrides the size-derived shard count and the default
	// single-replica coordinator. Both default (when zero) to the derived
	// shard count and a 1-replica coordinator — the size-ladder profiles
	// leave it unset and render exactly as before. The failover profiles
	// set it to stand up a multi-shard / multi-replica-coordinator deploy
	// for the kill/partition scenarios that a size-derived single shard
	// cannot express (perShardMachineCeiling=500k ⇒ realistic profiles are
	// single-shard by scale).
	Topology struct {
		ShardReplicas       int `yaml:"shardReplicas"`
		CoordinatorReplicas int `yaml:"coordinatorReplicas"`
	} `yaml:"topology"`

	// Shard carries optional shard-engine overrides a profile can set
	// directly (vs the seed tiers derived from Seed/Scale). Today: the M38
	// failure-injector rate, so a chaos profile can drive sustained
	// spot/hardware-fault churn above the chart's production-realistic
	// default (shard.failureRatePerSec, ~1.16e-7/s per Configured machine).
	// 0 = unset → the chart default applies, so every existing profile
	// renders unchanged.
	Shard struct {
		FailureRatePerSec float64 `yaml:"failureRatePerSec"`
	} `yaml:"shard"`
}

// validate returns nil if the profile is well-formed.
func (p profileV2) validate() error {
	name := p.Metadata.Name
	if name == "" {
		name = "<unnamed>"
	}
	if p.Scale.Machines <= 0 {
		return fmt.Errorf("profile %q: scale.machines must be > 0 (got %d)", name, p.Scale.Machines)
	}
	if p.Scale.Density <= 0 {
		return fmt.Errorf("profile %q: scale.density must be > 0 (got %d)", name, p.Scale.Density)
	}
	if p.LoadProfile.RampSeconds <= 0 {
		return fmt.Errorf("profile %q: loadProfile.rampSeconds must be > 0 (got %d)", name, p.LoadProfile.RampSeconds)
	}
	if p.LoadProfile.SoakSeconds < 0 {
		return fmt.Errorf("profile %q: loadProfile.soakSeconds must be ≥ 0 (got %d)", name, p.LoadProfile.SoakSeconds)
	}
	if p.LoadProfile.ChurnPerMinute < 0 {
		return fmt.Errorf("profile %q: loadProfile.churnPerMinute must be ≥ 0 (got %g)", name, p.LoadProfile.ChurnPerMinute)
	}
	if p.Seed.ConfiguredFraction < 0 || p.Seed.ConfiguredFraction > 1 {
		return fmt.Errorf("profile %q: seed.configuredFraction must be in [0, 1] (got %g)", name, p.Seed.ConfiguredFraction)
	}
	if p.Seed.IdleHeadroomFraction < 0 {
		return fmt.Errorf("profile %q: seed.idleHeadroomFraction must be ≥ 0 (got %g)", name, p.Seed.IdleHeadroomFraction)
	}
	return nil
}

// mergedConfig is the derived view produced by merge(profile,
// substrate). It carries everything Stage 3 needs to render Helm
// values: cluster geometry, host count, cost estimate, and the
// ramp-feasibility verdict.
type mergedConfig struct {
	ProfileName    string
	SubstrateName  string
	TotalPods      int
	PodsPerCluster int
	ClusterCount   int
	HostsNeeded    int

	// EstimatedUSD = HostsNeeded × perHostUsdPerHour × duration-hours.
	// Duration includes a 10 min teardown allowance on top of ramp +
	// soak. Zero for free substrates.
	EstimatedUSD  float64
	DurationHours float64

	// RampFeasible reports whether the substrate's declared bind
	// throughput can sustain the profile's ramp demand. False ≠
	// fatal; ramp-tail-off is sometimes the regime we want to
	// exercise. The note explains the comparison either way.
	RampFeasible     bool
	RampFeasibleNote string
}

// merge composes profile + substrate into a mergedConfig. Both inputs
// must be pre-validated (readProfileV2 / readSubstrate handle that);
// merge itself is pure arithmetic + feasibility comparison.
//
// Geometry: clusterCount = ceil(totalPods / podsPerCluster).
// hostsNeeded = ceil(clusterCount / clustersPerHost) + 1, the +1
// covers the system-under-test pods (shard, coordinator, prometheus).
//
// Ramp feasibility: profile asks for totalPods/rampSeconds Pods bound
// per second; substrate can supply clusterCount ×
// bindThroughputPodsPerSec. Demand ≤ supply → feasible.
func merge(p profileV2, s substrateFile) (mergedConfig, error) {
	totalPods := p.Scale.Machines * p.Scale.Density
	if totalPods <= 0 {
		return mergedConfig{}, fmt.Errorf("merge: scale.machines × scale.density = %d (must be > 0)", totalPods)
	}

	podsPerCluster := s.Cluster.PodsPerCluster
	clusterCount := ceilDiv(totalPods, podsPerCluster)
	hostsNeeded := ceilDiv(clusterCount, s.Cluster.ClustersPerHost) + 1

	rampSeconds := p.LoadProfile.RampSeconds
	demand := float64(totalPods) / float64(rampSeconds)
	supply := float64(clusterCount) * float64(s.Cluster.BindThroughputPodsPerSec)
	rampFeasible := demand <= supply
	var rampNote string
	switch {
	case rampFeasible:
		rampNote = fmt.Sprintf("ramp demand %.1f Pods/s ≤ substrate supply %.1f Pods/s (%d clusters × %d Pods/s/cluster)",
			demand, supply, clusterCount, s.Cluster.BindThroughputPodsPerSec)
	default:
		rampNote = fmt.Sprintf("ramp demand %.1f Pods/s > substrate supply %.1f Pods/s — ramp will tail off (run anyway to exercise that regime)",
			demand, supply)
	}

	durationSeconds := p.LoadProfile.RampSeconds + p.LoadProfile.SoakSeconds + 600 // +10min teardown
	durationHours := float64(durationSeconds) / 3600.0
	estimatedUSD := float64(hostsNeeded) * s.CostEstimate.PerHostUSDPerHour * durationHours

	return mergedConfig{
		ProfileName:      p.Metadata.Name,
		SubstrateName:    s.Metadata.Name,
		TotalPods:        totalPods,
		PodsPerCluster:   podsPerCluster,
		ClusterCount:     clusterCount,
		HostsNeeded:      hostsNeeded,
		EstimatedUSD:     estimatedUSD,
		DurationHours:    durationHours,
		RampFeasible:     rampFeasible,
		RampFeasibleNote: rampNote,
	}, nil
}

// ceilDiv returns ceil(a / b) for positive integers. Panics if b <= 0;
// callers must pre-validate (substrateFile.validate() rules out the
// zero case for podsPerCluster + clustersPerHost).
func ceilDiv(a, b int) int {
	if b <= 0 {
		panic(fmt.Sprintf("ceilDiv: divisor %d must be positive", b))
	}
	return (a + b - 1) / b
}

// perShardMachineCeiling is the documented per-shard inventory cap
// (bigfleet.md §16, M11.x). One shard handles ≤ 500K machines under
// management; renderHelmValues uses this to derive the shard
// replica count from profile.scale.machines.
const perShardMachineCeiling = 500_000

func shardReplicas(machines int) int {
	r := ceilDiv(machines, perShardMachineCeiling)
	if r < 1 {
		return 1
	}
	return r
}

// loadCatalogArchetypes resolves the workload-archetype catalog the
// profile names (profile.catalog.archetypes) to the standalone YAML at
// <profileDir>/archetypes/<name>.yaml and returns its archetype list
// twice: verbatim ([]any, injected into loadProfile.archetypes so the
// standalone catalog file is the single source of truth — the chart no
// longer carries its own drift-prone copy) and typed (the seed-side
// list, for the ADR-0044 effective-machine computation — the same
// archetype.LoadCatalog parse the shard binary runs on the injected
// copy). Empty name → "realistic".
func loadCatalogArchetypes(profilePath, catalogName string) ([]any, []archetype.Archetype, error) {
	if catalogName == "" {
		catalogName = "realistic"
	}
	path := filepath.Join(filepath.Dir(profilePath), "archetypes", catalogName+".yaml")
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read archetype catalog: %w", err)
	}
	var doc struct {
		Archetypes []any `yaml:"archetypes"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, nil, fmt.Errorf("parse archetype catalog %s: %w", path, err)
	}
	if len(doc.Archetypes) == 0 {
		return nil, nil, fmt.Errorf("archetype catalog %s has no archetypes", path)
	}
	cat, err := archetype.LoadCatalog(path)
	if err != nil {
		return nil, nil, fmt.Errorf("parse archetype catalog (typed): %w", err)
	}
	return doc.Archetypes, cat.ForSeed(), nil
}

// renderHelmValues turns a (profile, substrate, mergedConfig) triple
// into the values map the scaletest chart consumes. Geometry comes
// from mergedConfig; per-Pod resources come from the substrate;
// scale + load knobs come from the profile.
//
// Substrate.kwokPod.requests/limits are per-container budgets
// applied identically to the kwok pod's apiserver and workload
// containers. The host budget should be calibrated against 2 ×
// requests × clustersPerHost.
//
// Operational tuning knobs (executeConcurrency, podShim
// concurrency, etc.) use values that match today's `uber-*`
// shape. Production substrates beyond the documented ceilings may
// need to override via `--set` at install time; that's deferred
// follow-up.
func renderHelmValues(p profileV2, s substrateFile, m mergedConfig, archetypes []any, typedArchetypes []archetype.Archetype) map[string]any {
	resourceMap := func(r substrateResourceMap) map[string]string {
		return map[string]string{"cpu": r.CPU, "memory": r.Memory}
	}
	prometheusReqs := map[string]string{"cpu": "1", "memory": "4Gi"}
	prometheusLims := map[string]string{"cpu": "4", "memory": "12Gi"}
	if m.ClusterCount >= 100 {
		prometheusReqs = map[string]string{"cpu": "4", "memory": "16Gi"}
		prometheusLims = map[string]string{"cpu": "8", "memory": "32Gi"}
	}

	replicas := shardReplicas(p.Scale.Machines)
	if p.Topology.ShardReplicas > 0 {
		// Failover profiles override the size-derived (single) shard count
		// to stand up a multi-shard deploy for kill/partition scenarios.
		replicas = p.Topology.ShardReplicas
	}
	coordinatorReplicas := 1
	if p.Topology.CoordinatorReplicas > 0 {
		coordinatorReplicas = p.Topology.CoordinatorReplicas
	}
	// ADR-0035 seed math. Three tiers cooperate to put the cluster in
	// steady state at install:
	//   - Configured: machines bound to clusters, sized to cover the
	//     full Pod demand at install (seed.configuredFraction × machines,
	//     split per-cluster).
	//   - Idle: per-shard buffer for churn-driven replacement
	//     (seed.idleHeadroomFraction × machines).
	//   - Speculative: elastic procurement quota (seed.speculativeMultiplier
	//     × machines, ADR-0026 — the harness must model the whole
	//     capacity model).
	//
	// ADR-0044 §4: the machine count the fractions multiply is the
	// catalog-derived effective total, not scale.machines. The nominal
	// keeps defining demand (totalPods = machines × density); the
	// effective total is what that demand implies as supply once
	// whole-machine (extended-resource) archetypes pack 1 Pod per
	// machine instead of `density`, plus per-zone gang floors.
	// Catalogs with whole-machine archetypes seed well above nominal —
	// that is the realistic fleet shape.
	//
	// Sizing per-cluster vs per-shard mirrors the shard binary's CLI:
	//   --seed-configured-per-cluster is per-cluster (the harness's
	//   N % stride == ordinal mapping fans the cluster IDs to shards).
	//   --seed-machines + --seed-speculative are per-shard totals.
	machinesEffective := archetype.MachinesForPods(typedArchetypes, p.Scale.Density, m.TotalPods)
	configuredPerCluster := 0
	if p.Seed.ConfiguredFraction > 0 {
		configuredPerCluster = int(float64(machinesEffective) * p.Seed.ConfiguredFraction / float64(m.ClusterCount))
	}
	idlePerShard := int(float64(machinesEffective) * p.Seed.IdleHeadroomFraction / float64(replicas))
	speculativePerShard := 0
	if p.Seed.SpeculativeMultiplier > 0 {
		speculativePerShard = machinesEffective * p.Seed.SpeculativeMultiplier / replicas
	}

	values := map[string]any{
		"namespace": "bigfleet-scaletest",
		"kwok": map[string]any{
			"storage":      s.Cluster.Storage,
			"clusterCount": m.ClusterCount,
			"apiserverResources": map[string]any{
				"requests": resourceMap(s.KwokPod.Requests),
				"limits":   resourceMap(s.KwokPod.Limits),
			},
			"workloadResources": map[string]any{
				"requests": resourceMap(s.KwokPod.Requests),
				"limits":   resourceMap(s.KwokPod.Limits),
			},
			"sharedVolumeSizeLimit": s.KwokPod.SharedVolumeSizeLimit,
		},
		"podShim": map[string]any{
			"binderConcurrency":       256,
			"upcomingNodeConcurrency": 32,
		},
		"shard": map[string]any{
			"replicas":                 replicas,
			"seedMachines":             idlePerShard,
			"seedSpeculative":          speculativePerShard,
			"seedConfiguredPerCluster": configuredPerCluster,
			"seedDensityMultiplier":    p.Scale.Density,
			"maxActionsPerCycle":       4096,
			"executeConcurrency":       256,
			"incrementalReconcile":     m.ClusterCount >= 100,
			"metricsWarmupCycles":      5,
			// The probe that makes a failed run self-diagnosing: Need
			// counts split by co-location + Phase 1 unsatisfied +
			// Phase 3 reclaim overlap, every 20th cycle. Read-only,
			// one log line — always on for V2 runs.
			"phaseAttributionLog": true,
		},
		"coordinator": map[string]any{
			"enabled":  true,
			"replicas": coordinatorReplicas,
		},
		"harness": map[string]any{
			"scheduler": "kube-scheduler",
		},
		"loadProfile": map[string]any{
			"target":          s.Cluster.PodsPerCluster,
			"churnPerMinute":  p.LoadProfile.ChurnPerMinute,
			"durationSeconds": p.LoadProfile.SoakSeconds,
			"preBind":         p.Seed.PreBind,
			"archetypes":      archetypes,
		},
		// loadProfile.bursts is added below (only when the profile sets
		// them) so the default chart values are unchanged for profiles
		// without bursts.
		"operator": map[string]any{
			"qps":            200,
			"burst":          400,
			"ackConcurrency": 64,
			"rollupInterval": "0s",
		},
		"prometheus": map[string]any{
			"resources": map[string]any{
				"requests": prometheusReqs,
				"limits":   prometheusLims,
			},
		},
		"costEstimate": map[string]any{
			"vCPU":              s.Host.VCPU * m.HostsNeeded,
			"memoryGB":          s.Host.MemoryGiB * m.HostsNeeded,
			"awsSpotUsdPerHour": s.CostEstimate.PerHostUSDPerHour * float64(m.HostsNeeded),
			"notes": fmt.Sprintf("BYO: profile %q × substrate %q = %d hosts of %dvCPU/%dGiB; seed machines %d nominal → %d effective (ADR-0044 catalog machine-demand shares + gang floors)",
				p.Metadata.Name, s.Metadata.Name, m.HostsNeeded, s.Host.VCPU, s.Host.MemoryGiB, p.Scale.Machines, machinesEffective),
		},
	}

	if p.RampBudget != "" {
		values["rampBudget"] = p.RampBudget
	}
	// #327: thread burst events into the load-driver's profile. The chart
	// toYaml's the whole loadProfile map into profile.yaml, so setting
	// bursts here is all that's needed — the load-driver already parses
	// the bursts field (burstSpec). Only set it when present so profiles
	// without bursts render exactly as before.
	if len(p.LoadProfile.Bursts) > 0 {
		values["loadProfile"].(map[string]any)["bursts"] = p.LoadProfile.Bursts
	}
	// scaleDowns ride the same loadProfile toYaml path as bursts; the
	// load-driver parses loadProfile.scaleDowns (scaleDownSpec). Only set
	// when present so profiles without it render exactly as before.
	if len(p.LoadProfile.ScaleDowns) > 0 {
		values["loadProfile"].(map[string]any)["scaleDowns"] = p.LoadProfile.ScaleDowns
	}
	// shard.failureRatePerSec override: only set when a chaos profile asks
	// for an elevated rate, so profiles without it inherit the chart's
	// production-realistic default unchanged.
	if p.Shard.FailureRatePerSec > 0 {
		values["shard"].(map[string]any)["failureRatePerSec"] = p.Shard.FailureRatePerSec
	}
	if len(p.RunnerActions) > 0 {
		values["runnerActions"] = p.RunnerActions
	}
	if p.SLO != (sloOverrides{}) {
		values["slo"] = p.SLO
	}
	return values
}

// writeRenderedValues marshals the values map to YAML and writes it
// to a temp file in the output directory. Returns the file path the
// caller passes to helm install.
func writeRenderedValues(values map[string]any, outputDir string) (string, error) {
	b, err := yaml.Marshal(values)
	if err != nil {
		return "", fmt.Errorf("marshal values: %w", err)
	}
	path := filepath.Join(outputDir, "rendered-values.yaml")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", fmt.Errorf("write rendered values: %w", err)
	}
	return path, nil
}

// validate returns nil if the substrate is well-formed. Field errors
// are wrapped with the substrate's metadata.name (or "<unnamed>" if
// not set) so the runner's --substrate validation failure points at
// the file the user passed.
func (s substrateFile) validate() error {
	name := s.Metadata.Name
	if name == "" {
		name = "<unnamed>"
	}
	if s.Host.VCPU <= 0 {
		return fmt.Errorf("substrate %q: host.vCPU must be > 0 (got %d)", name, s.Host.VCPU)
	}
	if s.Host.MemoryGiB <= 0 {
		return fmt.Errorf("substrate %q: host.memoryGiB must be > 0 (got %d)", name, s.Host.MemoryGiB)
	}
	if s.Cluster.PodsPerCluster <= 0 {
		return fmt.Errorf("substrate %q: cluster.podsPerCluster must be > 0 (got %d)", name, s.Cluster.PodsPerCluster)
	}
	if s.Cluster.ClustersPerHost <= 0 {
		return fmt.Errorf("substrate %q: cluster.clustersPerHost must be > 0 (got %d)", name, s.Cluster.ClustersPerHost)
	}
	switch s.Cluster.Storage {
	case "etcd", "kine":
		// ok
	default:
		return fmt.Errorf("substrate %q: cluster.storage must be \"etcd\" or \"kine\" (got %q)", name, s.Cluster.Storage)
	}
	if s.Cluster.BindThroughputPodsPerSec <= 0 {
		return fmt.Errorf("substrate %q: cluster.bindThroughputPodsPerSec must be > 0 (got %d)", name, s.Cluster.BindThroughputPodsPerSec)
	}
	if s.KwokPod.Requests.CPU == "" {
		return fmt.Errorf("substrate %q: kwokPod.requests.cpu is required", name)
	}
	if s.KwokPod.Requests.Memory == "" {
		return fmt.Errorf("substrate %q: kwokPod.requests.memory is required", name)
	}
	if s.KwokPod.Limits.CPU == "" {
		return fmt.Errorf("substrate %q: kwokPod.limits.cpu is required", name)
	}
	if s.KwokPod.Limits.Memory == "" {
		return fmt.Errorf("substrate %q: kwokPod.limits.memory is required", name)
	}
	if s.CostEstimate.PerHostUSDPerHour < 0 {
		return fmt.Errorf("substrate %q: costEstimate.perHostUsdPerHour must be ≥ 0 (got %g)", name, s.CostEstimate.PerHostUSDPerHour)
	}
	return nil
}

type runResult struct {
	RunID   string `json:"runId"`
	Profile string `json:"profile"`
	Target  struct {
		Context string `json:"context"`
		Kind    string `json:"kind"`
	} `json:"target"`
	Cost struct {
		EstimatedUSD float64 `json:"estimatedUsd"`
		Hours        float64 `json:"hours"`
	} `json:"cost"`
	Scale struct {
		KWOKClusters  int `json:"kwokClusters"`
		MachinesPerCR int `json:"machinesPerCr"`
		TotalCRs      int `json:"totalCrs"`
		// Multi-shard / inventory totals (M12 onwards). Older runs
		// have these as 0 and must be read as "shardReplicas defaults
		// to 1 and seedMachines defaults to 0" when rendering.
		ShardReplicas        int `json:"shardReplicas"`
		SeedMachinesPerShard int `json:"seedMachinesPerShard"`
		AggregateInventory   int `json:"aggregateInventory"`
	} `json:"scale"`
	Metrics map[string]float64 `json:"metrics"`
	// UnmeasuredSLOs lists GATED metrics that read the -1 sentinel
	// (scrape failed or the metric source doesn't exist in this run
	// mode — e.g. the steady-bind histogram is pod-shim-emitted, so
	// kube-scheduler-mode profiles have no bind-latency source until
	// the M52 follow-on lands). M66.3: a pass with entries here is a
	// pass-with-named-gaps, not a clean pass — every gate used to skip
	// sentinels SILENTLY, which made the headline binding-latency SLO
	// vacuous on every active profile without anyone noticing.
	UnmeasuredSLOs []string `json:"unmeasuredSLOs,omitempty"`
	// RunnerActions records what the runner fired during the soak,
	// including whether each action's expected outcome was observed.
	// Empty for runs without runnerActions: in the profile.
	RunnerActions []runnerActionResult `json:"runnerActions,omitempty"`
	Passed        bool                 `json:"passed"`
	Failure       string               `json:"failure,omitempty"`
	// Failures lists assertion violations from runnerActions. A run with
	// non-empty Failures fails overall — Passed is false and Failure
	// summarises the first violation. Used to regression-check the
	// static-stability invariant on every release run.
	Failures []string `json:"failures,omitempty"`
}

type runnerActionResult struct {
	Action      string `json:"action"`
	AtSeconds   int    `json:"atSeconds"`
	FiredAt     string `json:"firedAt,omitempty"` // RFC3339, "" if not fired
	FireError   string `json:"fireError,omitempty"`
	Assertion   string `json:"assertion,omitempty"`
	Asserted    bool   `json:"asserted"`
	AssertError string `json:"assertError,omitempty"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "scaletest-runner:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("scaletest-runner", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	kubeconfig := fs.String("kubeconfig", os.Getenv("KUBECONFIG"), "kubeconfig path")
	profilePath := fs.String("profile", "", "profile YAML (test/scaletest/profiles/*.yaml)")
	substratePath := fs.String("substrate", "", "substrate YAML (ADR-0034 BYO). When set, profile is read as substrate-agnostic profileV2 + substrate is merged at install time. Omit to use the legacy profile-direct-to-helm path.")
	chartPath := fs.String("chart", "test/scaletest/chart", "path to the harness chart")
	duration := fs.Duration("duration", 0, "how long to soak after steady state (defaults to profile.loadProfile.durationSeconds)")
	maxDuration := fs.Duration("max-duration", 2*time.Hour, "hard cap; teardown if not done")
	skipPreflight := fs.Bool("skip-preflight", false, "skip the matching-capacity preflight (M60 rung 0.5) — for deliberate over-subscription experiments")
	output := fs.String("output", "", "output directory for summary + snapshot")
	yes := fs.Bool("yes", false, "skip cost confirmation prompt")
	keep := fs.Bool("keep", false, "skip teardown (debugging only)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *profilePath == "" {
		return errors.New("--profile required")
	}
	if *output == "" {
		return errors.New("--output required")
	}

	// ADR-0034: BYO substrate path. When --substrate is set, read the
	// profile as substrate-agnostic profileV2, merge with the
	// substrate, render to helm values, and install with those.
	// Without --substrate, fall through to the legacy
	// profile-as-values path.
	var (
		prof         profileFile
		mergedValues string // rendered values file path; "" if legacy path
		mergedCfg    mergedConfig
		mergedActive bool
	)
	if *substratePath != "" {
		pv2, err := readProfileV2(*profilePath)
		if err != nil {
			return err
		}
		sub, err := readSubstrate(*substratePath)
		if err != nil {
			return err
		}
		cfg, err := merge(pv2, sub)
		if err != nil {
			return err
		}
		archetypes, typedArchetypes, err := loadCatalogArchetypes(*profilePath, pv2.Catalog.Archetypes)
		if err != nil {
			return err
		}
		mergedCfg = cfg
		mergedActive = true
		// Convert profileV2 fields the rest of the runner still reads
		// off `prof` (ramp-budget resolver, runnerActions firing, SLO
		// overrides). The legacy profileFile is the runner's internal
		// "view" of the run; mergedValues is what helm sees.
		prof.KWOK.ClusterCount = cfg.ClusterCount
		prof.LoadProfile.Target = cfg.PodsPerCluster
		prof.LoadProfile.DurationSeconds = pv2.LoadProfile.SoakSeconds
		prof.LoadProfile.SettleSeconds = pv2.LoadProfile.SettleSeconds
		prof.RampBudget = pv2.RampBudget
		prof.RunnerActions = pv2.RunnerActions
		prof.SLO = pv2.SLO
		prof.CostEstimate.AWSSpotUSDPerHour = sub.CostEstimate.PerHostUSDPerHour * float64(cfg.HostsNeeded)
		prof.CostEstimate.VCPU = sub.Host.VCPU * cfg.HostsNeeded
		prof.CostEstimate.MemoryGB = sub.Host.MemoryGiB * cfg.HostsNeeded

		if err := os.MkdirAll(*output, 0o755); err != nil {
			return fmt.Errorf("output dir: %w", err)
		}
		mergedValues, err = writeRenderedValues(renderHelmValues(pv2, sub, cfg, archetypes, typedArchetypes), *output)
		if err != nil {
			return err
		}
	} else {
		p, err := readProfile(*profilePath)
		if err != nil {
			return err
		}
		prof = p
		// M60 rung 0.5: matching-capacity preflight for no-catalog
		// (legacy single-shape) profiles. A profile whose seeded
		// matching capacity sits below the bind gate cannot pass — the
		// fill plateaus and the ramp budget burns (the dev-50 stall:
		// 4,800 slots vs a 4,950 gate, 10 minutes per attempt).
		if !*skipPreflight {
			if err := legacyPreflight(*profilePath); err != nil {
				return fmt.Errorf("%w (--skip-preflight to run anyway, e.g. for a deliberate over-subscription experiment)", err)
			}
		}
	}

	name := strings.TrimSuffix(filepath.Base(*profilePath), ".yaml")
	runID := fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102-150405"), name)

	if *duration == 0 {
		*duration = time.Duration(prof.LoadProfile.DurationSeconds) * time.Second
		if *duration == 0 {
			*duration = 10 * time.Minute
		}
	}
	if *duration > *maxDuration {
		return fmt.Errorf("duration %s > max-duration %s", *duration, *maxDuration)
	}

	ctx, ctxCancel := signalCtx()
	defer ctxCancel()

	// Detect target.
	contextName, err := currentContext(*kubeconfig)
	if err != nil {
		return fmt.Errorf("detect kube context: %w", err)
	}
	tgtKind := classifyTarget(contextName)
	estCost := prof.CostEstimate.AWSSpotUSDPerHour * duration.Hours()
	if mergedActive {
		fmt.Fprintf(os.Stderr,
			"profile %s + substrate %s on context %s (kind=%s)\n"+
				"  scale: %d clusters × %d Pods = %d total\n"+
				"  hosts needed: %d (%dvCPU/%dGiB each)\n"+
				"  duration: %s\n"+
				"  estimated cost: $%.2f\n"+
				"  ramp feasibility: %s\n",
			mergedCfg.ProfileName, mergedCfg.SubstrateName, contextName, tgtKind,
			mergedCfg.ClusterCount, mergedCfg.PodsPerCluster, mergedCfg.TotalPods,
			mergedCfg.HostsNeeded, prof.CostEstimate.VCPU/mergedCfg.HostsNeeded, prof.CostEstimate.MemoryGB/mergedCfg.HostsNeeded,
			duration, estCost, mergedCfg.RampFeasibleNote,
		)
	} else {
		fmt.Fprintf(os.Stderr,
			"profile %s on context %s (kind=%s)\n"+
				"  scale: %d clusters × %d CRs = %d total\n"+
				"  duration: %s\n"+
				"  estimated cost (cloud baseline): $%.2f\n",
			name, contextName, tgtKind,
			prof.KWOK.ClusterCount, prof.LoadProfile.Target,
			prof.KWOK.ClusterCount*prof.LoadProfile.Target,
			duration, estCost,
		)
	}
	if !*yes && tgtKind == "cloud" && estCost >= 5.00 {
		if err := confirm("proceed with this paid run? [y/N]: "); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(*output, 0o755); err != nil {
		return fmt.Errorf("output dir: %w", err)
	}

	// Install and arrange teardown.
	releaseName := "scaletest"
	namespace := "bigfleet-scaletest"
	valuesForHelm := *profilePath
	if mergedActive {
		valuesForHelm = mergedValues
	}
	if err := helmInstall(ctx, *kubeconfig, *chartPath, valuesForHelm, releaseName, namespace, runID); err != nil {
		return fmt.Errorf("helm install: %w", err)
	}
	defer func() {
		if *keep {
			fmt.Fprintln(os.Stderr, "--keep set; leaving chart installed")
			return
		}
		fmt.Fprintln(os.Stderr, "tearing down")
		teardownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := helmUninstall(teardownCtx, *kubeconfig, releaseName, namespace); err != nil {
			fmt.Fprintln(os.Stderr, "teardown:", err)
		}
	}()

	// Wait for steady state. Per ADR-0035, this is a *sanity check*
	// — the actual pass/fail signal is the steady-state SLO histograms
	// over the soak window (see pass(), which gates on per-CR binding
	// latency, cycle p99, rollup p99, ack p99). The "must reach the
	// per-cluster target" rule still applies: SLO measurement against
	// an under-loaded shard isn't meaningful. The budget elapsing
	// produces a "didn't reach steady state" error, not an SLO failure;
	// the post-mortem distinction matters for diagnosing harness vs
	// system-under-test regressions.
	//
	// With seed.preBind=true (the ADR-0035 default for new profiles),
	// the load-driver fast-binds the initial fill to fake-Nodes as
	// they appear, so steady state is reached without the kube-
	// scheduler bulk-bind ramp. Legacy profiles with preBind=false
	// still pay for a scheduler-bound ramp; the budget formula (M22)
	// accounts for those.
	totalCRs := prof.KWOK.ClusterCount * prof.LoadProfile.Target
	rampBudget, rampSource := resolveRampBudget(prof, totalCRs)
	fmt.Fprintf(os.Stderr, "ramp budget: %s (%s) [sanity check; SLO gating runs over the soak window per ADR-0035]\n", rampBudget, rampSource)
	pfArgs := strings.Join(kArgs(*kubeconfig, "-n", namespace, "port-forward", "svc/grafana", "3000:3000"), " ")
	fmt.Fprintf(os.Stderr, "live dashboard: kubectl %s  →  http://localhost:3000/d/bigfleet-scaletest\n", pfArgs)
	if mergedActive {
		// M77a / ADR-0045: V2 profiles gate on BigFleet's actual
		// contract — demand covered by bound capacity — not pod-bind
		// percentage. See waitForSteadyStateV2.
		if err := waitForSteadyStateV2(ctx, *kubeconfig, namespace, prof.KWOK.ClusterCount, prof.LoadProfile.Target, rampBudget, prof.SLO.ExpectStandingShortfall); err != nil {
			return fmt.Errorf("steady state: %w", err)
		}
	} else {
		if err := waitForSteadyState(ctx, *kubeconfig, namespace, prof.KWOK.ClusterCount, prof.LoadProfile.Target, rampBudget); err != nil {
			return fmt.Errorf("steady state: %w", err)
		}
	}
	fmt.Fprintln(os.Stderr, "steady state reached; soaking", duration)

	// M77a / ADR-0045 steady-window health: the Reclaim action counter is
	// measured across the soak; the post-soak delta is a gated failing
	// condition on V2 runs. The engine has a structurally non-zero
	// endogenous async-actuation reclaim floor (ADR-0021 async execute,
	// proven coverage-harmless in bigfleet-uber #65-69), so the gate is
	// bounded-reclaim (CHANGE 2 below), not zero. The fleet also keeps
	// settling 1-2 min after "steady declared", and that decaying tail
	// inflates a full-soak integral; loadProfile.settleSeconds delays the
	// baseline snapshot so the measured window is the SETTLED portion of
	// the soak (de-tail). Default 0 = snapshot here at soakStart (every
	// other profile unchanged). When set, the snapshot is taken later via
	// the one-shot settle timer in the soak select-loop below.
	//
	// settleActive is gated on a sane configuration: settle > 0 and
	// strictly less than the soak duration. A settle ≥ soak would leave
	// the window empty/unmeasured, so we clamp to the soakStart snapshot
	// and warn (a misconfig must not silently disable the gate).
	settleDelay := time.Duration(prof.LoadProfile.SettleSeconds) * time.Second
	settleActive := mergedActive && settleDelay > 0 && settleDelay < *duration
	if mergedActive && settleDelay > 0 && !settleActive {
		fmt.Fprintf(os.Stderr,
			"WARNING: loadProfile.settleSeconds %s ≥ soak duration %s — "+
				"reclaim window would be empty; falling back to soakStart snapshot\n",
			settleDelay, *duration)
	}
	reclaimsAtSteady := -1
	if mergedActive && !settleActive {
		reclaimsAtSteady = readReclaimActions(ctx, *kubeconfig, namespace)
	}

	soakStart := time.Now()
	// Soak.
	soakCtx, cancelSoak := context.WithTimeout(ctx, *duration)
	// runnerActions: fire-and-record each action at its scheduled
	// offset from soakStart. The fire side runs concurrently with the
	// soak; the assertion side runs after teardown using prom queries
	// from the snapshot.
	actionResults := scheduleRunnerActions(soakCtx, *kubeconfig, namespace, soakStart, prof.RunnerActions)
	// Drop V: soak-progress heartbeat. The Drop T run hung 10 min past
	// the 30 min soak deadline with the main goroutine pinned in select
	// — most likely macOS app-nap suspending the process after its
	// stdio went quiet at the +5 min fail-fast print. A per-minute
	// heartbeat both keeps stdio warm (anti-nap) and gives the watcher
	// a "still alive, N min into soak" signal so a future hang becomes
	// obvious instead of looking like a slow snapshot.
	heartbeatTicker := time.NewTicker(60 * time.Second)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		for {
			select {
			case <-soakCtx.Done():
				return
			case t := <-heartbeatTicker.C:
				fmt.Fprintf(os.Stderr, "soak heartbeat: %s into %s\n",
					t.Sub(soakStart).Round(time.Second), *duration)
			}
		}
	}()
	defer func() {
		heartbeatTicker.Stop()
		<-heartbeatDone
	}()
	// Drop M / Drop Z: fail-fast at 10 min into soak. Originally +5 min
	// (Drop M) but two consecutive runs (Drop X churn-fix and Drop Y
	// rate(2m) window) aborted at this mark with bind p99 ~15-17 s
	// despite the chain settling to 8-10 s p99 by +14 min. The chain's
	// catch-up window — Pods CREATED during the ramp's tail or first
	// minute of soak that bind 5-10 s later, plus the operator/shard
	// inventory rebalancing from the cold start — is longer than the
	// original +5 min budget assumed. +10 min lands cleanly past the
	// catch-up, and combined with Drop Y's rate(2m) window samples
	// the +8..+10 min slice which is pure post-catch-up steady state.
	// Trade-off: a 30 min soak that's truly failing burns 5 extra min
	// here. Worth it to avoid false-positive aborts that look like
	// runner artefacts to the watcher.
	failFastDelay := 10 * time.Minute
	failFastTimer := time.NewTimer(failFastDelay)
	failFastFired := false
	// De-tail (CHANGE 1): when settleActive, snapshot the reclaim
	// baseline at soakStart+settleDelay instead of pre-soak, so the
	// measured window is the SETTLED tail of the soak. One-shot: the
	// channel is nil when inactive, which makes its select case block
	// forever (never selected) — the standard nil-channel idiom. The
	// snapshot runs on the main soak goroutine (no concurrent writer to
	// reclaimsAtSteady), so a plain assignment is race-free.
	var settleTimerC <-chan time.Time
	if settleActive {
		settleTimer := time.NewTimer(settleDelay)
		defer settleTimer.Stop()
		settleTimerC = settleTimer.C
	}
loop:
	for {
		select {
		case <-soakCtx.Done():
			if !failFastFired && !failFastTimer.Stop() {
				<-failFastTimer.C
			}
			break loop
		case <-settleTimerC:
			// Settle mark reached: take the baseline now. The post-soak
			// delta then covers only the settled window. A failed read
			// leaves the -1 sentinel, surfaced via unmeasuredGated.
			settleTimerC = nil // one-shot
			reclaimsAtSteady = readReclaimActions(ctx, *kubeconfig, namespace)
			fmt.Fprintf(os.Stderr,
				"reclaim baseline snapshot taken at settle mark (+%s into soak); "+
					"measuring the settled window\n", settleDelay)
		case <-failFastTimer.C:
			failFastFired = true
			ok, reason := soakFailFastCheck(ctx, *kubeconfig, namespace, prof.SLO)
			if !ok {
				fmt.Fprintln(os.Stderr, "soak 10min fail-fast: aborting —", reason)
				cancelSoak()
				break loop
			}
			fmt.Fprintln(os.Stderr, "soak 10min fail-fast: passing —", reason)
		}
	}
	cancelSoak()
	if err := ctx.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	// Snapshot Prometheus TSDB. Hard deadline so a dead cluster
	// (apiserver gone, kubectl exec hangs) can't pin the runner
	// indefinitely — M44.3's cloud run hung 7h50m on this exact path
	// when the in-cluster Prometheus pod went away mid-soak.
	snapCtx, cancelSnap := context.WithTimeout(context.Background(), 5*time.Minute)
	snapPath := filepath.Join(*output, "prometheus-snapshot.tar.gz")
	if err := snapshotPrometheus(snapCtx, *kubeconfig, namespace, snapPath); err != nil {
		fmt.Fprintln(os.Stderr, "prometheus snapshot:", err)
	}
	cancelSnap()

	// Pull metrics summary. Same deadline rationale.
	metricsCtx, cancelMetrics := context.WithTimeout(context.Background(), 2*time.Minute)
	metrics := readKeyMetrics(metricsCtx, *kubeconfig, namespace, *duration)
	if mergedActive {
		// Reclaim count over the steady window (M77a / ADR-0045).
		// Raw counter delta, not a rate window: rate()/increase()
		// extrapolation can both invent and hide single-digit
		// increments, so we read the absolute counter at the baseline
		// (soakStart, or the settle mark when settleSeconds is set —
		// CHANGE 1) and again here at end, and subtract. -1 =
		// unmeasured (a read failed, or the soak ended before the
		// settle mark fired); surfaced via unmeasuredGated, not
		// silently passed. The bound is applied in the assertion below
		// (CHANGE 2).
		reclaimsAtEnd := readReclaimActions(metricsCtx, *kubeconfig, namespace)
		if reclaimsAtSteady >= 0 && reclaimsAtEnd >= 0 {
			metrics["reclaimActionsDuringSoak"] = float64(reclaimsAtEnd - reclaimsAtSteady)
		} else {
			metrics["reclaimActionsDuringSoak"] = -1
		}
	}
	cancelMetrics()

	res := runResult{
		RunID:   runID,
		Profile: name,
		Metrics: metrics,
	}
	res.Target.Context = contextName
	res.Target.Kind = tgtKind
	res.Cost.EstimatedUSD = estCost
	res.Cost.Hours = duration.Hours()
	res.Scale.KWOKClusters = prof.KWOK.ClusterCount
	res.Scale.MachinesPerCR = prof.LoadProfile.Target
	res.Scale.TotalCRs = prof.KWOK.ClusterCount * prof.LoadProfile.Target
	// shard.replicas defaults to 1 when omitted; older profiles
	// rendered correctly under that assumption pre-M12.
	res.Scale.ShardReplicas = prof.Shard.Replicas
	if res.Scale.ShardReplicas == 0 {
		res.Scale.ShardReplicas = 1
	}
	res.Scale.SeedMachinesPerShard = prof.Shard.SeedMachines
	res.Scale.AggregateInventory = res.Scale.ShardReplicas * res.Scale.SeedMachinesPerShard
	res.RunnerActions = actionResults
	// Assert per-action expected outcomes against the prom snapshot.
	// Passing assertions are silent; any violation appends to
	// res.Failures and fails the overall run.
	for i := range res.RunnerActions {
		assertRunnerActionOutcome(context.Background(), *kubeconfig, namespace, &res.RunnerActions[i], soakStart)
		if !res.RunnerActions[i].Asserted {
			res.Failures = append(res.Failures, fmt.Sprintf("runnerAction %s @t=%ds: %s",
				res.RunnerActions[i].Action,
				res.RunnerActions[i].AtSeconds,
				res.RunnerActions[i].AssertError,
			))
		}
	}
	// M77a / ADR-0045: bounded reclaim churn over the steady window is a
	// gated condition on V2 runs. The contract is BOUNDED-reclaim, not
	// zero:
	//   - Zero is unachievable on the async engine. ADR-0021's async
	//     execute means the fleet self-perturbs at a structurally
	//     non-zero rate; bigfleet-uber #65-69 diagnosed this floor as a
	//     coverage-harmless endogenous self-perturbation (#67), not the
	//     bootstrap≈reclaim oscillation defect M67 removed, and measured
	//     it robust at ~340 over a 180s soak un-de-tailed (#69).
	//   - That ~340 is inflated by the post-fill settling transient: the
	//     reclaim RATE decays through the soak (1.91/s soak-average vs
	//     0.52-0.86/s at soak-END), because the gate opens its window at
	//     "steady declared" while the fleet keeps settling 1-2 min. The
	//     settle window (CHANGE 1, loadProfile.settleSeconds) removes
	//     that settling-transient inflation by snapshotting the baseline
	//     after the fleet has settled.
	//   - The bound (slo.maxReclaimActionsDuringSoak) accepts the
	//     residual steady floor. It is still a real gate: a regression —
	//     the bootstrap≈reclaim oscillation resurfacing as sustained high
	//     churn, far above the bound — still trips it. Default 0 keeps
	//     the original zero-reclaim assertion for every other profile.
	if v, ok := metrics["reclaimActionsDuringSoak"]; ok && v > float64(prof.SLO.MaxReclaimActionsDuringSoak) {
		res.Failures = append(res.Failures, fmt.Sprintf(
			"reclaimActionsDuringSoak %.0f > %d — Phase 3 reclaim churn over the steady window exceeded the bound (ADR-0045: reclaim follows demand shrinkage only; the bound accepts the proven async-actuation floor, bigfleet-uber #65-69)",
			v, prof.SLO.MaxReclaimActionsDuringSoak))
	}
	res.Passed, res.Failure = pass(metrics, res.Scale.TotalCRs, res.Scale.ShardReplicas, prof.SLO)
	res.UnmeasuredSLOs = unmeasuredGated(metrics, prof.SLO)
	if len(res.UnmeasuredSLOs) > 0 {
		fmt.Fprintf(os.Stderr, "\nWARNING: %d gated SLO(s) UNMEASURED this run (sentinel -1) — the pass verdict does not cover them: %s\n\n",
			len(res.UnmeasuredSLOs), strings.Join(res.UnmeasuredSLOs, ", "))
	}
	if len(res.Failures) > 0 && res.Passed {
		// SLO numbers passed but a runnerAction assertion didn't fire
		// — the static-stability invariant requires both.
		res.Passed = false
		res.Failure = res.Failures[0]
	}

	summary := filepath.Join(*output, "summary.json")
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		// Don't lose the run by silently writing 0 bytes — surface the
		// marshal failure (NaN floats from prom queries are the usual
		// culprit; promQuery now filters them but new metrics could
		// reintroduce the issue).
		return fmt.Errorf("marshal summary: %w", err)
	}
	if err := os.WriteFile(summary, b, 0o644); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}
	fmt.Fprintln(os.Stderr, "wrote", summary)
	if !res.Passed {
		return errors.New(res.Failure)
	}
	return nil
}

func signalCtx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		fmt.Fprintln(os.Stderr, "signal received, cancelling")
		cancel()
	}()
	return ctx, cancel
}

func readProfile(path string) (profileFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return profileFile{}, err
	}
	var p profileFile
	if err := yaml.Unmarshal(b, &p); err != nil {
		return profileFile{}, err
	}
	return p, nil
}

// readProfileV2 parses a substrate-agnostic profile YAML (ADR-0034)
// and returns it validated.
func readProfileV2(path string) (profileV2, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return profileV2{}, err
	}
	var p profileV2
	if err := yaml.Unmarshal(b, &p); err != nil {
		return profileV2{}, fmt.Errorf("profile %s: %w", path, err)
	}
	if err := p.validate(); err != nil {
		return profileV2{}, err
	}
	return p, nil
}

// readSubstrate parses a BYO-substrate YAML file (ADR-0034) and
// returns it validated. nil-error is the only success path; callers
// pass the returned value straight to the merge logic.
func readSubstrate(path string) (substrateFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return substrateFile{}, err
	}
	var s substrateFile
	if err := yaml.Unmarshal(b, &s); err != nil {
		return substrateFile{}, fmt.Errorf("substrate %s: %w", path, err)
	}
	if err := s.validate(); err != nil {
		return substrateFile{}, err
	}
	return s, nil
}

// classifyTarget is best-effort: looks at the kubeconfig context name
// for cloud-y patterns. The runner doesn't trust this for cost
// charging — it's only used to decide whether to prompt.
func classifyTarget(context string) string {
	switch {
	case strings.Contains(context, "kind"):
		return "kind"
	case strings.Contains(context, "eks"), strings.Contains(context, "aws"):
		return "cloud"
	case strings.Contains(context, "gke"), strings.Contains(context, "gcp"):
		return "cloud"
	case strings.Contains(context, "aks"), strings.Contains(context, "azure"):
		return "cloud"
	case strings.Contains(context, "scw"), strings.Contains(context, "scaleway"), strings.Contains(context, "kapsule"):
		return "cloud"
	default:
		return "unknown"
	}
}

func currentContext(kubeconfig string) (string, error) {
	args := []string{"config", "current-context"}
	if kubeconfig != "" {
		args = append([]string{"--kubeconfig", kubeconfig}, args...)
	}
	out, err := exec.Command("kubectl", args...).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func helmInstall(ctx context.Context, kubeconfig, chart, valuesFile, release, ns, runID string) error {
	args := []string{
		"upgrade", "--install", release, chart,
		"--namespace", ns, "--create-namespace",
		"--values", valuesFile,
		"--set", "runId=" + runID,
		// 20 min ceiling: 500 KWOK pods × 2 containers each is a real
		// cold-install workload (image pull + kine init + apiserver
		// bring-up per pod). Helm's --wait blocks on every pod
		// reaching Ready; the dominant time at 5M scale is
		// per-cluster apiserver readiness, not image pull. 10 min was
		// fine through 50 KWOK pods; 20 gives headroom for 500 with
		// budget left over to abort honestly if something deadlocks.
		"--wait", "--timeout", "20m",
	}
	if kubeconfig != "" {
		args = append([]string{"--kubeconfig", kubeconfig}, args...)
	}
	cmd := exec.CommandContext(ctx, "helm", args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func helmUninstall(ctx context.Context, kubeconfig, release, ns string) error {
	args := []string{"uninstall", release, "--namespace", ns}
	if kubeconfig != "" {
		args = append([]string{"--kubeconfig", kubeconfig}, args...)
	}
	cmd := exec.CommandContext(ctx, "helm", args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}
	// Best-effort namespace cleanup.
	_ = exec.CommandContext(ctx, "kubectl", "delete", "ns", ns, "--wait=false").Run()
	return nil
}

// resolveRampBudget picks the ramp-to-steady-state deadline.
//
// The default formula is the max of three terms (M22):
//   - 15 min floor (cold-start kine writes, image-pull, kubelet
//     bring-up across hundreds of pods are slow on first install)
//   - totalCRs / 750 CR/sec — empirical floor from the 1M de-risk,
//     which sustained ~1110 CR/sec aggregate; sizing at 750 gives
//     ~1.5× headroom over observed throughput
//   - durationSeconds × 0.5 — keeps profiles with very long soaks
//     (e.g. failover-soak's 60min) from undershooting the ramp
//
// Profile-level `rampBudget: …` overrides everything when set. The
// returned string explains which clause won so failed-at-deadline
// runs can be diagnosed from runner.log without going back to the
// profile YAML.
func resolveRampBudget(prof profileFile, totalCRs int) (time.Duration, string) {
	if prof.RampBudget != "" {
		if d, err := time.ParseDuration(prof.RampBudget); err == nil && d > 0 {
			return d, "from profile.rampBudget"
		}
	}
	const minRamp = 15 * time.Minute
	const sustainedFloorCRsPerSec = 750.0
	budget, source := minRamp, "15-min floor"
	if totalCRs > 0 {
		t := time.Duration(float64(totalCRs)/sustainedFloorCRsPerSec*float64(time.Second)) + 1*time.Second
		if t > budget {
			budget = t
			source = fmt.Sprintf("totalCRs %d / 750 CR/sec", totalCRs)
		}
	}
	if t := time.Duration(prof.LoadProfile.DurationSeconds) * time.Second / 2; t > budget {
		budget = t
		source = fmt.Sprintf("durationSeconds %d × 0.5", prof.LoadProfile.DurationSeconds)
	}
	return budget, source
}

func waitForSteadyState(ctx context.Context, kubeconfig, ns string, clusterCount int, perClusterTarget int, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	// Steady state requires (a) every kwok pod's containers all Ready,
	// (b) load-driver has ramped to ≥ 99.9 % of target, AND (c) the
	// chain has bound ≥ 99 % of target.
	//
	// ADR-0021: with the persistent execute pool, the chain sustains
	// ~50-85 binds/sec on scaleway-50k — ramps drain in the budget.
	// Re-tightened the gate so soak begins only after the ramp
	// backlog is fully drained. Otherwise post-fill steady-tagged
	// Pods queue behind the unfinished ramp and their binding
	// latency includes that wait time, even though the load-driver
	// has long since hit target. The 1 % slop absorbs transient
	// create/delete races during churn (the load-driver may briefly
	// drop below target as deletions run ahead of creations).
	target := int(0.999 * float64(clusterCount*perClusterTarget))
	chainAliveThreshold := int(0.99 * float64(clusterCount*perClusterTarget))
	// Plateau fail-fast: when binds AND bootstraps are both frozen for
	// plateauTicks consecutive polls while binds sits below the gate,
	// the fill is structurally stuck (the 2026-06-11 dev-50 incident:
	// binds pinned at exactly 4,800 = matching-seed capacity for 45
	// ticks until the 10m budget — 60m on uber-* profiles — expired).
	// Both counters frozen distinguishes a dead chain from a slow tail:
	// a live-but-slow fill moves at least one of them. ramp-active pods
	// must all be Ready so a crash-looping kwok pod doesn't masquerade
	// as a plateau.
	const plateauTicks = 12 // × 10s ticker = 2 minutes of zero movement
	frozen := 0
	lastBinds, lastBootstraps := -1, -1
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("did not reach steady state within %s", budget)
		}
		ready, err := countReadyKWOKPods(ctx, kubeconfig, ns)
		active := -1
		binds := -1
		bootstraps := -1
		if err == nil && ready >= clusterCount {
			active = readActiveCRs(ctx, kubeconfig, ns)
			// ADR-0022 / M45.4: pod bind success is the canonical
			// end-of-chain signal under Pod-mode (1 bind per Pod,
			// regardless of seedDensityMultiplier). The previous gate
			// was Bootstrap success, which under density>1 caps at
			// totalPods/density and never reaches the threshold —
			// dev-5k at density=10 with 5000 Pods only ever runs ~500
			// Bootstraps. Keep reading Bootstraps for the log line so
			// the chain's machine-emit rate stays visible.
			binds = readPodBindsSucceeded(ctx, kubeconfig, ns)
			bootstraps = readBootstrapsExecuted(ctx, kubeconfig, ns)
			if active >= target && binds >= chainAliveThreshold {
				fmt.Fprintf(os.Stderr, "  waiting: pods %d/%d ready, active %d/%d, binds %d/%d, bootstraps %d (≥ %d, gate cleared)\n",
					ready, clusterCount, active, target, binds, target, bootstraps, chainAliveThreshold)
				return nil
			}
			if active >= target && binds >= 0 && binds == lastBinds && bootstraps == lastBootstraps {
				frozen++
				if frozen >= plateauTicks {
					return fmt.Errorf(
						"bind plateau: binds frozen at %d for %s (gate %d, gap %d) with bootstraps frozen at %d — the fill is structurally stuck, not slow; check seeded matching capacity vs demand shape (profile preflight)",
						binds, time.Duration(plateauTicks)*10*time.Second, chainAliveThreshold, chainAliveThreshold-binds, bootstraps)
				}
			} else {
				frozen = 0
			}
			lastBinds, lastBootstraps = binds, bootstraps
		}
		fmt.Fprintf(os.Stderr, "  waiting: pods %d/%d ready, active %d/%d, binds %d/%d, bootstraps %d (need binds ≥ %d)\n",
			ready, clusterCount, active, target, binds, target, bootstraps, chainAliveThreshold)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// waitForSteadyStateV2 is the steady-state gate for V2 (catalog +
// substrate) profiles, redefined around the ADR-0045 contract (M77a;
// spec recorded in plan §12 with the M67 engine work): BigFleet
// promises demand covered by bound capacity — it does NOT promise pod
// placement, so a bind-percentage gate asserts a promise the system
// under test never made. Steady state is, all together:
//
//	(a) every kwok pod Ready — the harness is alive;
//	(b) demand fully ramped: active CRs ≥ 99.9 % of target AND the
//	    shard's NeedsTable reports demand rows (roll-ups flowing) —
//	    without this, "no shortfall" is vacuous;
//	(c) demand covered: bigfleet_shard_shortfalls == 0. The gauge is
//	    full-replacement per cycle from Phase 2's unresolved set, so
//	    zero means every Need's deficit was claimable from bound or
//	    newly-claimed capacity — genuine undersupply (Same gangs short
//	    of per-zone machines, parked classes) holds it > 0;
//	(d) acquisitions quiescent: the Bootstrap+Provision emit counter is
//	    flat for quiesceTicks consecutive polls. Claims count from the
//	    moment they're made (ADR-0045 §2), so (c) alone can read zero
//	    while executes are still draining — flat acquisition emission
//	    is what says fulfillment finished rather than being claimed-
//	    ahead, and it also catches an emit-fail-re-emit loop that (c)
//	    is blind to.
//
// Pod-bind progress stays on every waiting line — it's the cluster-side
// telemetry that makes a stuck run diagnosable — but does not gate:
// satisfied-but-stuck is the cluster's problem (ADR-0045 §4).
//
// Fail-fast: with demand at target, a standing shortfall AND frozen
// acquisitions for plateauTicks means the engine has demand it can
// neither cover nor make progress against — structurally stuck (seed /
// catalog shape mismatch, or an engine defect), so fail in 2 minutes
// instead of burning the ramp budget. This re-keys the legacy frozen-
// binds plateau detector onto demand-side liveness.
// expectShortfall inverts the demand-coverage half of the gate for
// genuine-scarcity engine-correctness runs (sloOverrides.ExpectStandingShortfall):
// supply is < demand by design, so steady state is "demand ramped, a
// standing shortfall present, acquisitions flat" rather than
// "shortfalls == 0". The plateau fail-fast — which normally reads a
// standing shortfall + frozen acquisitions as a structurally-stuck
// engine — is the EXPECTED converged state here, so it is suppressed.
func waitForSteadyStateV2(ctx context.Context, kubeconfig, ns string, clusterCount int, perClusterTarget int, budget time.Duration, expectShortfall bool) error {
	deadline := time.Now().Add(budget)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	target := int(0.999 * float64(clusterCount*perClusterTarget))
	const (
		quiesceTicks = 3  // × 10s ticker = 30s of covered demand + flat acquisitions
		plateauTicks = 12 // × 10s ticker = 2 minutes of standing shortfall + no movement
	)
	stable, frozen := 0, 0
	lastAcq := -1
	active, shortfalls, acq, reclaims, binds := -1, -1, -1, -1, -1
	for {
		if time.Now().After(deadline) {
			// Name the failure shape in the error: covered demand
			// (shortfalls 0) with acquisitions AND reclaims still moving
			// is the bootstrap≈reclaim oscillation signature — the class
			// ADR-0045 removes by construction, observed live on the
			// first M77a kind run (a3-highgpu-8g pool, machines ping-
			// ponging between clusters at static demand).
			return fmt.Errorf("did not reach steady state within %s (last: active %d/%d, shortfalls %d, acquisitions %d, reclaims %d, binds %d — acquisitions+reclaims still moving at covered demand is the ADR-0045 oscillation signature; standing shortfall is genuine undersupply)",
				budget, active, target, shortfalls, acq, reclaims, binds)
		}
		ready, err := countReadyKWOKPods(ctx, kubeconfig, ns)
		demandRows := -1
		active, shortfalls, acq, reclaims, binds = -1, -1, -1, -1, -1
		if err == nil && ready >= clusterCount {
			active = readActiveCRs(ctx, kubeconfig, ns)
			shortfalls = readShardShortfalls(ctx, kubeconfig, ns)
			demandRows = readShardDemandRows(ctx, kubeconfig, ns)
			acq = readAcquisitionActions(ctx, kubeconfig, ns)
			reclaims = readReclaimActions(ctx, kubeconfig, ns)
			binds = readPodBindsSucceeded(ctx, kubeconfig, ns) // reported, never gated
			demandReady := active >= target && demandRows > 0
			acqFlat := acq >= 0 && acq == lastAcq
			// Coverage condition: normally "no shortfall"; inverted for a
			// scarcity run to "standing shortfall present" (supply < demand
			// by design — the engine has priority-throttled, so the residual
			// LOW-priority shortfall is the steady state, not a defect).
			coverageSteady := shortfalls == 0
			if expectShortfall {
				coverageSteady = shortfalls > 0
			}
			if demandReady && coverageSteady && acqFlat {
				stable++
				if stable >= quiesceTicks {
					fmt.Fprintf(os.Stderr, "  steady (ADR-0045): pods %d/%d ready, active %d/%d, shortfalls %d%s, acquisitions flat at %d for %s, binds %d (reported, not gated)\n",
						ready, clusterCount, active, target, shortfalls,
						map[bool]string{true: " (standing — expected under scarcity)", false: " (== 0, covered)"}[expectShortfall],
						acq, time.Duration(quiesceTicks)*10*time.Second, binds)
					return nil
				}
			} else {
				stable = 0
			}
			// Plateau fail-fast: a standing shortfall + frozen acquisitions
			// is "stuck" for a capacity-met run, but it is the EXPECTED
			// converged steady state for a scarcity run (handled by
			// coverageSteady above), so suppress it when expectShortfall.
			if !expectShortfall && demandReady && shortfalls > 0 && acqFlat {
				frozen++
				if frozen >= plateauTicks {
					return fmt.Errorf(
						"demand-side plateau: %d standing shortfall(s) with acquisitions frozen at %d for %s at full demand (active %d/%d, binds %d) — the engine has demand it can neither cover nor make progress against; check seed/catalog shape vs demand, then suspect the engine",
						shortfalls, acq, time.Duration(plateauTicks)*10*time.Second, active, target, binds)
				}
			} else {
				frozen = 0
			}
			lastAcq = acq
		}
		fmt.Fprintf(os.Stderr, "  waiting: pods %d/%d ready, active %d/%d, shortfalls %d, demand rows %d, acquisitions %d, binds %d (gate: shortfalls == 0 + acquisitions flat ×%d)\n",
			ready, clusterCount, active, target, shortfalls, demandRows, acq, binds, quiesceTicks)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// countReadyKWOKPods returns the number of kwok-cluster pods whose
// pod-level Ready condition is true (i.e., ALL containers in the pod
// are Ready, not just the first). Pre-fix this only inspected
// containerStatuses[0] — broken in the harness-split shape where the
// pod has [apiserver, workload] containers.
func countReadyKWOKPods(ctx context.Context, kubeconfig, ns string) (int, error) {
	args := []string{
		"-n", ns,
		"get", "pods", "-l", "app.kubernetes.io/component=kwok-cluster",
		"-o", `jsonpath={range .items[*]}{.status.conditions[?(@.type=='Ready')].status}{"\n"}{end}`,
	}
	if kubeconfig != "" {
		args = append([]string{"--kubeconfig", kubeconfig}, args...)
	}
	out, err := exec.CommandContext(ctx, "kubectl", args...).Output()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) == "True" {
			count++
		}
	}
	return count, nil
}

// readActiveCRs returns the cluster-wide sum(scaletest_loadgen_cr_active)
// from Prometheus, or -1 if unavailable. Best-effort: a transient
// Prometheus query failure during ramp returns -1 and the caller
// retries on the next tick.
func readActiveCRs(ctx context.Context, kubeconfig, ns string) int {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	v, err := promQuery(queryCtx, kubeconfig, ns, "sum(scaletest_loadgen_cr_active)")
	if err != nil {
		return -1
	}
	return int(v)
}

// readBootstrapsExecuted returns the cumulative count of successfully-
// completed Bootstrap actions from the shard. Each = one machine
// successfully transitioned Idle → Configured for some demand.
//
// M44.4 Drop B: the original gate read scaletest_loadgen_cr_active —
// the count of CRs the load-driver has *created*, not the count of
// machines actually bound. A 50 K-CR run could "clear the gate" with
// only ~2 K bindings while 48 K CRs sat unbound; soak then ran
// against a chain catastrophically falling behind, and the binding
// histogram only reported p99 over the small fraction that did bind.
// Gating on Bootstrap success ensures the chain has caught up before
// soak begins, regardless of mode.
func readBootstrapsExecuted(ctx context.Context, kubeconfig, ns string) int {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	v, err := promQuery(queryCtx, kubeconfig, ns, `sum(bigfleet_shard_action_execute_outcomes_total{kind="Bootstrap",outcome="success"})`)
	if err != nil {
		return -1
	}
	return int(v)
}

// readPodBindsSucceeded returns the cumulative count of Pods the
// scaletest pod-shim has successfully bound to a fake-Node (either via
// our own /binding subresource call or via a concurrent reconcile that
// got there first). Equal to the Pod-mode chain's "Pods placed" count.
//
// ADR-0022 / M45.4: this replaces Bootstrap success as the steady-state
// gate, because Bootstrap counts machines and a density>1 seed serves
// `density` Pods per machine — so the Bootstrap count saturates at
// totalPods/density and never reaches the totalPods-shaped threshold.
// Pod bind success is one-per-Pod regardless of density.
func readPodBindsSucceeded(ctx context.Context, kubeconfig, ns string) int {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Two paths, two metric sources (ADR-0023):
	//   - pod-shim harness path: per-bind counter, one tick per /binding
	//     success. Long-standing semantics.
	//   - kube-scheduler harness path: node-creator emits a gauge of
	//     currently-bound Pods (Pods with spec.nodeName!="") by
	//     periodic List. For ramp-gate purposes (first-time-≥-target),
	//     gauge ≈ counter because no churn has started yet.
	//
	// `or on() vector(0)` lets the query fold either source's absence
	// to 0 instead of returning no-data. The sum of "scheduler-path
	// gauge OR pod-shim counter" is whichever path is active.
	q := `sum(bigfleet_scaletest_node_creator_bound_pods) or on() sum(bigfleet_scaletest_pod_shim_pod_bind_attempts_total{outcome=~"success|bound_by_other"})`
	v, err := promQuery(queryCtx, kubeconfig, ns, q)
	if err != nil {
		return -1
	}
	return int(v)
}

// readShardShortfalls returns the fleet-wide sum of the shard's
// unresolved-shortfall gauge, or -1 if unavailable. The plain query
// (no vector(0) fold) is deliberate: bigfleet_shard_shortfalls is
// registered at shard start via promauto, so an empty result means the
// shard hasn't been scraped yet — that must read "not steady", not
// "zero shortfalls".
func readShardShortfalls(ctx context.Context, kubeconfig, ns string) int {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	v, err := promQuery(queryCtx, kubeconfig, ns, `sum(bigfleet_shard_shortfalls)`)
	if err != nil {
		return -1
	}
	return int(v)
}

// readShardDemandRows returns the total NeedsTable row count across
// penalty buckets, or -1 if unavailable. > 0 means roll-ups have
// reached the shard — the precondition that makes "zero shortfalls"
// meaningful rather than vacuous.
func readShardDemandRows(ctx context.Context, kubeconfig, ns string) int {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	v, err := promQuery(queryCtx, kubeconfig, ns, `sum(bigfleet_shard_demand_machines)`)
	if err != nil {
		return -1
	}
	return int(v)
}

// readAcquisitionActions returns the cumulative count of acquisition
// actions the decision engine emitted (Bootstrap + Provision), or -1
// if Prometheus is unreachable. `or on() vector(0)` folds an absent
// series to 0 — a fully pre-seeded run may legitimately never emit
// either kind, and CounterVec children don't exist until first
// increment.
func readAcquisitionActions(ctx context.Context, kubeconfig, ns string) int {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	q := `sum(bigfleet_shard_actions_total{kind=~"Bootstrap|Provision"}) or on() vector(0)`
	v, err := promQuery(queryCtx, kubeconfig, ns, q)
	if err != nil {
		return -1
	}
	return int(v)
}

// readReclaimActions returns the cumulative count of Reclaim actions
// the decision engine emitted, or -1 if Prometheus is unreachable.
// Same absent-series fold as readAcquisitionActions: a healthy steady
// run never increments the counter, so its series may not exist.
func readReclaimActions(ctx context.Context, kubeconfig, ns string) int {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	q := `sum(bigfleet_shard_actions_total{kind="Reclaim"}) or on() vector(0)`
	v, err := promQuery(queryCtx, kubeconfig, ns, q)
	if err != nil {
		return -1
	}
	return int(v)
}

func snapshotPrometheus(ctx context.Context, kubeconfig, ns, dest string) error {
	// Trigger a Prometheus admin-API snapshot, then kubectl cp the dir out.
	pod := "prometheus-0"
	body, err := exec.CommandContext(ctx, "kubectl", kArgs(kubeconfig, "-n", ns, "exec", "-c", "tools", pod, "--",
		"curl", "-fsS", "-X", "POST", "http://localhost:9090/api/v1/admin/tsdb/snapshot")...).Output()
	if err != nil {
		return fmt.Errorf("trigger snapshot: %w", err)
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || resp.Status != "success" {
		return fmt.Errorf("snapshot api: %s", string(body))
	}
	src := fmt.Sprintf("%s/%s:/prometheus/snapshots/%s", ns, pod, resp.Data.Name)
	tmp := dest + ".dir"
	if err := exec.CommandContext(ctx, "kubectl",
		kArgs(kubeconfig, "cp", src, tmp)...).Run(); err != nil {
		return fmt.Errorf("kubectl cp: %w", err)
	}
	defer os.RemoveAll(tmp)
	return exec.CommandContext(ctx, "tar", "-czf", dest, "-C", filepath.Dir(tmp), filepath.Base(tmp)).Run()
}

// sloWindow renders the Prometheus range window the steady-state SLO
// queries use: the soak duration, capped at the canonical 5m (a longer
// soak shouldn't widen the window — 5 minutes of steady state IS the
// SLO's definition) and floored at 1m (rate() needs ≥2 scrapes).
// Shorter gates (the dev-50 integration rung's 3m soak) shrink the window to match so
// the queries never reach back into the fill's tail — the [5m]
// literals were written when every soak was ≥5m, and a 3m soak with
// unchanged windows would silently average ~2m of ramp drain into the
// steady-state percentiles.
func sloWindow(soak time.Duration) string {
	w := soak
	if w > 5*time.Minute {
		w = 5 * time.Minute
	}
	if w < time.Minute {
		w = time.Minute
	}
	if w%time.Minute == 0 {
		return fmt.Sprintf("%dm", int(w.Minutes()))
	}
	return fmt.Sprintf("%ds", int(w.Seconds()))
}

// readKeyMetrics queries Prometheus for the runner's SLO metrics. Per-
// query errors map to a -1 sentinel in the result so the summary makes
// the gap visible without aborting the whole run. soak sizes the
// steady-state rate windows (see sloWindow); the [15m] raft-term query
// is deliberately untouched — it watches leader stability across the
// whole run, not the steady-state window.
func readKeyMetrics(ctx context.Context, kubeconfig, ns string, soak time.Duration) map[string]float64 {
	queries := map[string]string{
		// Cycle p99 is reported as the worst-shard number — the SLO
		// applies per shard, not aggregated. With shard.replicas: 1
		// max(by pod) reduces to the single shard. With shard.replicas:
		// N a single overshooting shard will show through here instead
		// of being diluted by its faster siblings. Histograms are
		// already bucketed per pod by the scrape, so quantile→max is
		// statistically meaningful for SLO gating.
		"shardCycleDurationP99Seconds":       `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_cycle_duration_seconds_bucket[5m]))))`,
		"shardProvisioningLatencyP99Seconds": `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_provisioning_latency_seconds_bucket[5m]))))`,
		"shardProvisioningLatencyP50Seconds": `max(histogram_quantile(0.50, sum by (le, pod) (rate(bigfleet_shard_provisioning_latency_seconds_bucket[5m]))))`,
		// ADR-0018: internal binding latency. The harness's fake
		// provider returns instantly, so this measures BigFleet's
		// internal contribution only — *not* user-facing latency.
		// Production user-facing latency = this + provider time
		// (5-180s real-cloud bring-up); validate that elsewhere
		// (provider conformance suite + production canaries).
		// ADR-0017: only the per-Pod histogram gates a release. M44
		// flipped Pod-mode to the default, so every profile populates
		// this histogram; the gate is active everywhere. The -1
		// sentinel skip in pass() is retained for failed scrapes.
		//
		// M44.4: query the steady-state histogram only. The all-Pods
		// histogram includes the initial fill, which is a synthetic
		// thundering-herd workload not representative of production
		// (production has existing inventory + steady-state churn +
		// occasional small bursts, not 50 K cold-start Pods). The
		// load-driver tags Pods scaletest.bigfleet/state="steady"
		// after the cluster has reached its target Pod count;
		// pod-shim observes those into the steady histogram.
		//
		// Drop L: rate window instead of cumulative read. The
		// cumulative histogram includes every steady-tagged Pod
		// since the shard started — including post-target Pods
		// that were created during the ramp's tail and bound late
		// because the chain hadn't reached steady throughput yet.
		// Those observations dominate the cumulative p99 forever:
		// even at the end of a clean soak, the run's verdict is
		// pegged at 102.4 s (+Inf bucket boundary) because those
		// stale observations remain.
		//
		// Using rate(...[5m]) captures only the last 5 min of binds.
		// Soak duration is 30 min, so the window sits comfortably
		// inside steady state. Pre-soak observations from the
		// ramp's drain are no longer counted — which matches what
		// the SLO is actually trying to measure: "if the chain is
		// healthy, what's the user-facing latency of an arriving
		// Pod?"
		"internalBindingLatencyP99Seconds": `histogram_quantile(0.99, sum by (le) (rate(bigfleet_scaletest_pod_bind_latency_steady_seconds_bucket[5m])))`,
		// ADR-0054 Half 2: the loose end-to-end pod-bind p50 liveness
		// floor. p50 sits below the uncapped-scheduler retry tail, so a
		// p50 blowup means the COMMON bind path broke — a real liveness
		// signal — while the p99 above is informational regime-context.
		"endToEndPodBindP50Seconds": `histogram_quantile(0.50, sum by (le) (rate(bigfleet_scaletest_pod_bind_latency_steady_seconds_bucket[5m])))`,
		// M79.4: non-saturating cross-check for the p99 above. The p99 is a
		// histogram_quantile, which CANNOT exceed the top finite bucket — so
		// a p99 sitting on the top le reads as a clean number even when the
		// true tail is far higher (this silently produced bigfleet-uber #77's
		// bogus "76-102s" on the old 102.4s ceiling). This gauge is the raw
		// running max; if it greatly exceeds the p99, the histogram is
		// saturating and the p99 must not be trusted as the real tail.
		"internalBindingLatencyMaxSeconds": `max(bigfleet_scaletest_pod_bind_latency_steady_max_seconds)`,
		// Drop Q: pod-shim chain breakdown. Together with shardProvisioning*
		// (Phase 1 emit → Bootstrap complete) these localise where the
		// internalBindingLatencyP99Seconds tail lives. UpcomingNode-to-Node
		// is the fake-Node controller's reconcile queueing; Node-to-Bound
		// is the podBinder + Watches handler. If both are low and
		// shardProvisioning* is high, the chain bottleneck is upstream of
		// pod-shim entirely.
		"upcomingToNodeP99Seconds": `histogram_quantile(0.99, sum by (le) (rate(bigfleet_scaletest_pod_shim_upcoming_to_node_latency_seconds_bucket[5m])))`,
		"nodeToBoundP99Seconds":    `histogram_quantile(0.99, sum by (le) (rate(bigfleet_scaletest_pod_shim_node_to_bound_latency_seconds_bucket[5m])))`,
		"upcomingToNodeP50Seconds": `histogram_quantile(0.50, sum by (le) (rate(bigfleet_scaletest_pod_shim_upcoming_to_node_latency_seconds_bucket[5m])))`,
		"nodeToBoundP50Seconds":    `histogram_quantile(0.50, sum by (le) (rate(bigfleet_scaletest_pod_shim_node_to_bound_latency_seconds_bucket[5m])))`,
		// Drop R: shard-side configure-phase. configurePhase is the gap
		// pod-shim observes as UpcomingNode age between phase=Configuring
		// and phase=Ready. requestBootstrap is the synchronous stream RPC
		// inside that gap; subtracting it leaves Provider.Configure +
		// applyTransition (local work).
		"shardConfigurePhaseP99Seconds":   `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_configure_phase_seconds_bucket[5m]))))`,
		"shardConfigurePhaseP50Seconds":   `max(histogram_quantile(0.50, sum by (le, pod) (rate(bigfleet_shard_configure_phase_seconds_bucket[5m]))))`,
		"shardRequestBootstrapP99Seconds": `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_request_bootstrap_seconds_bucket[5m]))))`,
		"shardRequestBootstrapP50Seconds": `max(histogram_quantile(0.50, sum by (le, pod) (rate(bigfleet_shard_request_bootstrap_seconds_bucket[5m]))))`,
		// Drop W: symmetric Reclaim-side timing.
		"shardDrainPhaseP99Seconds": `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_drain_phase_seconds_bucket[5m]))))`,
		"shardDrainPhaseP50Seconds": `max(histogram_quantile(0.50, sum by (le, pod) (rate(bigfleet_shard_drain_phase_seconds_bucket[5m]))))`,
		"operatorRollupP99Seconds":  `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_rollup_duration_seconds_bucket[5m])))`,
		// Per-phase rollup decomposition (informational; bigfleet-uber #80
		// prove-first). operatorRollupP99 is gated; these localise where the
		// ~1s tail at uber-5k actually goes. list = apiserver LIST round-trip
		// (cache deep-copy + per-object proto decode) — an apiserver READ, the
		// read analog of the node-state write hop; build = buildRollup()
		// aggregate+marshal, the only engine-compute phase; enqueue = stream
		// slot replacement, near-zero unless the send is stuck. If list ≈ the
		// aggregate and build+enqueue ≈ 0, rollup is apiserver-read-bound (bar
		// ratifies like node-state); if build dominates, it's real engine cost.
		"operatorRollupListP99Seconds":    `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_rollup_phase_duration_seconds_bucket{phase="list"}[5m])))`,
		"operatorRollupBuildP99Seconds":   `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_rollup_phase_duration_seconds_bucket{phase="build"}[5m])))`,
		"operatorRollupEnqueueP99Seconds": `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_rollup_phase_duration_seconds_bucket{phase="enqueue"}[5m])))`,
		"operatorAckP99Seconds":           `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_acknowledge_duration_seconds_bucket[5m])))`,
		// ADR-0054 Half 1: the operator publishing UpcomingNode=Ready
		// after the shard signals Configured — the last BigFleet-owned hop
		// (pkg/operator/upcoming.go:54). Previously instrumented but never
		// queried; gated by sloOverrides.OperatorNodeStateUpdateP99Seconds.
		"operatorNodeStateUpdateP99Seconds": `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_node_state_update_duration_seconds_bucket[5m])))`,
		"coordinatorApplyOpsPerSec":         `sum(rate(bigfleet_coordinator_apply_total[5m]))`,
		"shardShortfalls":                   `sum(bigfleet_shard_shortfalls)`,
		// shardShortfallsDelta is the convergence half of the inverted
		// shortfall gate (sloOverrides.ExpectStandingShortfall). It is the
		// |change| in the aggregate shortfall gauge over the soak window —
		// a converged scarcity steady state holds it ≈ 0; a growing
		// shortfall (demand outrunning the engine) shows here. abs() keeps
		// it ≥ 0 so the -1 failed-scrape sentinel stays unambiguous. The
		// [5m] window is rewritten to the soak window below, same as the
		// rate gates. Only consulted when ExpectStandingShortfall is set;
		// inert otherwise.
		"shardShortfallsDelta": `abs(delta(sum(bigfleet_shard_shortfalls)[5m:15s]))`,
		// preemptActions is the cumulative Phase-2 Preempt-action count
		// (engine-correctness preemption gate, sloOverrides.ExpectPreemptions).
		// The Preempt counter exists (ShardActionsTotal{kind="Preempt"});
		// `or on() vector(0)` folds the absent series (a run that never
		// preempted has no child) to 0, so 0 genuinely means "never
		// preempted". Only consulted when ExpectPreemptions is set.
		"preemptActions": `sum(bigfleet_shard_actions_total{kind="Preempt"}) or on() vector(0)`,
		// loadgenCRsActive uses min_over_time across the last 5 min of
		// soak so the post-soak gate catches "ramped to target then
		// drifted below" runs without false-positiving on the very last
		// scrape, which lands during teardown when kwok pods are being
		// killed (the in-process sum trivially craters when half the
		// pods stop reporting). One past run reported 30,399 active
		// because of exactly this teardown artifact; in-soak the
		// number was 49,999-50,000 throughout.
		"loadgenCRsActive":        `min_over_time(sum(scaletest_loadgen_cr_active)[5m:15s])`,
		"loadgenCRsCreatedPerSec": `sum(rate(scaletest_loadgen_cr_created_total[5m]))`,
		// Per-phase p99s. Required to distinguish "the whole cycle is
		// slow" from "one phase has a long tail" (M11.21 added the
		// histogram; the runner's summary now surfaces it).
		"shardPhaseReconcileP99Seconds": `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_cycle_phase_duration_seconds_bucket{phase="reconcile"}[5m]))))`,
		"shardPhase1P99Seconds":         `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_cycle_phase_duration_seconds_bucket{phase="phase1"}[5m]))))`,
		"shardPhase2P99Seconds":         `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_cycle_phase_duration_seconds_bucket{phase="phase2"}[5m]))))`,
		"shardPhase3P99Seconds":         `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_cycle_phase_duration_seconds_bucket{phase="phase3"}[5m]))))`,
		"shardPhaseExecuteP99Seconds":   `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_cycle_phase_duration_seconds_bucket{phase="execute"}[5m]))))`,
		// Multi-shard health: how many distinct shard pods reported a
		// cycle in the last 5 min. The runner gates this against
		// shard.replicas so a crash-looping shard can't hide behind
		// max-by-pod aggregation (max would just exclude it).
		"shardsReportingCycle": `count(count by (pod) (bigfleet_shard_cycle_duration_seconds_count{component="shard"}))`,
		// Coordinator health (gated). apply_total error rate must be ~0;
		// a non-zero rate means Raft Apply is failing or the FSM is
		// rejecting commands. Observed during M12 self-registration as
		// "fsm_error" when AddShard hits ErrShardExists, but the
		// grpc_server.go handler swallows those — non-zero error here
		// is a real bug.
		"coordinatorApplyErrorRate": `sum(rate(bigfleet_coordinator_apply_total{outcome=~"error|fsm_error"}[5m])) / clamp_min(sum(rate(bigfleet_coordinator_apply_total[5m])), 1)`,
		// ADR-0054 Half 1: Bootstrap materialization throughput =
		// success / (success + all-failures) over the soak window. The
		// metric is bigfleet_shard_action_execute_outcomes_total with
		// labels {kind, outcome}; success is outcome="success" and every
		// other outcome (no_session, transition_error, blob_error,
		// configure_error, ctx_canceled, fenced) is a non-success for the
		// Bootstrap kind (pkg/metrics/metrics.go:164, pkg/shard/execute.go:55).
		// Gated as a MIN by sloOverrides.BootstrapSuccessRatio.
		// M79.7 (bigfleet-uber #79 dev-50 step-0 catch): the original form
		// `success_rate / clamp_min(total_rate, 1)` was WRONG. clamp_min(.,1)
		// was meant to avoid 0/0, but it floors the DENOMINATOR at 1.0, so
		// whenever the steady bootstrap rate is < 1/s (the normal case for a
		// full-preBind fleet whose only bootstraps are sparse churn
		// replacements, ~0.2/s) the ratio collapses to success_rate/1.0 ≈ the
		// raw rate (~0.2) DESPITE 100% success — a false MIN-gate failure
		// (step-0 measured 569/569 = 100% success yet the gate read ~0.2).
		// Correct form: a true ratio (rate/rate divides out the per-second
		// scale, so 0.2/0.2 = 1.0 at any throughput), zero-guarded so an
		// empty window (no bootstraps at all → nothing to assess) reads 1.0
		// and passes rather than 0/0. `(denom > 0)` drops the zero-total case
		// so the division is empty, and `or vector(1)` substitutes 1.0.
		"bootstrapSuccessRatio": `(sum(rate(bigfleet_shard_action_execute_outcomes_total{kind="Bootstrap",outcome="success"}[5m])) / (sum(rate(bigfleet_shard_action_execute_outcomes_total{kind="Bootstrap"}[5m])) > 0)) or vector(1)`,
		// Operator outbox drops (gated). The session-outbox bounded queue
		// drops messages on overflow; under heavy bootstrap load this
		// can lose BootstrapBlobResponse / ReclaimAck. Should be 0/sec
		// throughout the soak.
		"operatorOutboxDropsPerSec": `sum(rate(bigfleet_operator_outbox_dropped_total[5m]))`,
		// Coordinator pending-instructions ceiling (informational): a
		// rising max means the coordinator is dispatching faster than
		// the shards can ack. Instruction queues are bounded by the
		// pending map; stable means the loop is closing.
		"coordinatorPendingMax": `max(bigfleet_coordinator_pending_instructions)`,
		// Coordinator term-change count over the last 15 min
		// (informational). 0 means the leader was stable; > 0 means
		// re-election under load.
		"coordinatorTermChanges15m": `max(changes(bigfleet_coordinator_raft_term[15m]))`,
		// Per-shard inventory balance (informational): min/max ratio
		// across shard pods. Each shard should hold roughly the same
		// number of seeded machines; significant skew suggests a shard
		// failed seed-time partially.
		"shardInventoryMinMaxRatio": `min(sum by (pod) (bigfleet_shard_inventory_machines)) / clamp_min(max(sum by (pod) (bigfleet_shard_inventory_machines)), 1)`,
	}
	win := sloWindow(soak)
	out := make(map[string]float64, len(queries))
	for k, q := range queries {
		q = strings.ReplaceAll(q, "[5m:15s]", "["+win+":15s]")
		q = strings.ReplaceAll(q, "[5m]", "["+win+"]")
		v, err := promQuery(ctx, kubeconfig, ns, q)
		if err != nil {
			out[k] = -1
			continue
		}
		out[k] = v
	}
	return out
}

// promQuery hits Prometheus through `kubectl exec wget` so we don't
// need a port-forward — works on any cluster.
func promQuery(ctx context.Context, kubeconfig, ns, query string) (float64, error) {
	body, err := exec.CommandContext(ctx, "kubectl",
		kArgs(kubeconfig, "-n", ns, "exec", "-c", "tools", "prometheus-0", "--",
			"curl", "-fsS",
			fmt.Sprintf("http://localhost:9090/api/v1/query?query=%s", urlEncode(query)),
		)...).Output()
	if err != nil {
		return 0, err
	}
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, err
	}
	if resp.Status != "success" || len(resp.Data.Result) == 0 {
		return 0, fmt.Errorf("query empty: %s", query)
	}
	s, ok := resp.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("query value not string")
	}
	var v float64
	_, err = fmt.Sscanf(s, "%f", &v)
	if err != nil {
		return 0, err
	}
	// Prometheus returns "NaN" or "+Inf" / "-Inf" when a query has
	// undefined output (e.g., histogram_quantile against an empty
	// bucket window). These can't be JSON-marshalled — silently they
	// cause the entire summary.json write to produce 0 bytes. Map to
	// an error so readKeyMetrics records the existing -1 sentinel.
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("query returned non-finite (%s): %s", s, query)
	}
	return v, nil
}

// soakFailFastCheck reads the two release-gating SLOs 10 min into the
// soak and returns ok=false if either is already off-budget. Uses a
// rate(...[2m]) window — narrower than the [5m] used in the final
// summary — so the sample comes from the +8..+10 min slice, fully
// past the chain's cold-start catch-up. Drop M originally fired this
// at +5 min, but Drop X / Drop Y runs showed the +5 min p99 still
// reflecting catch-up latency from Pods CREATED late-ramp / early-soak
// (15-17 s p99 at +5, 8-10 s by +14). Cost of waiting 5 more min:
// a truly-failing 30 min soak burns the extra time. Benefit: false-
// positive aborts on transitionally-slow soaks go away. If both prom
// queries fail (Prometheus pod gone, exec hung, etc.) the soak is
// allowed to continue so a transient infra blip doesn't abort the run.
func soakFailFastCheck(ctx context.Context, kubeconfig, ns string, slo sloOverrides) (bool, string) {
	qctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// ADR-0054: in-soak and post-soak gates stay in lockstep — the
	// release gate is now the per-machine configure-phase p99 (the
	// capacity-materialization latency BigFleet owns), NOT the end-to-end
	// pod-bind p99 (which is uncapped-scheduler / reprovision-bound and
	// only informational). 15s is the held configure-phase bar; profiles
	// override via slo.shardConfigurePhaseP99Seconds.
	target := 15.0
	if slo.ShardConfigurePhaseP99Seconds > 0 {
		target = slo.ShardConfigurePhaseP99Seconds
	}
	cycleTarget := 5.0
	if slo.ShardCycleDurationP99Seconds > 0 {
		cycleTarget = slo.ShardCycleDurationP99Seconds
	}
	configure, errB := promQuery(qctx, kubeconfig, ns, `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_configure_phase_seconds_bucket[2m]))))`)
	cycle, errC := promQuery(qctx, kubeconfig, ns, `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_cycle_duration_seconds_bucket[2m]))))`)
	if errB != nil && errC != nil {
		return true, fmt.Sprintf("queries unavailable (configure=%v cycle=%v) — continuing", errB, errC)
	}
	if errB == nil && configure > target {
		return false, fmt.Sprintf("shardConfigurePhaseP99Seconds %.3fs > %.1fs SLO", configure, target)
	}
	if errC == nil && cycle > cycleTarget {
		return false, fmt.Sprintf("shardCycleDurationP99Seconds %.3fs > %.1fs envelope", cycle, cycleTarget)
	}
	return true, fmt.Sprintf("configure=%.3fs cycle=%.3fs", configure, cycle)
}

// urlEncode percent-encodes a PromQL query for use as the value of
// `?query=`. The previous hand-rolled implementation only escaped a
// handful of characters and silently corrupted queries containing
// `{label="value"}` — the `=` inside the matcher was interpreted as
// a new query-string parameter boundary, causing the entire phase
// histogram set (e.g. shardPhase{1,2,3,Execute,Reconcile}P99Seconds)
// to return malformed → empty → -1 in summary.json. Any reserved
// character outside the unreserved set gets percent-encoded by
// net/url.QueryEscape, which is exactly the right contract here.
func urlEncode(s string) string { return url.QueryEscape(s) }

func kArgs(kubeconfig string, rest ...string) []string {
	if kubeconfig != "" {
		return append([]string{"--kubeconfig", kubeconfig}, rest...)
	}
	return rest
}

// pass enforces the runner's SLO budget. ADR-0054 reframed the steady
// release gate off the end-to-end pod-bind p99 (which BigFleet does not
// control under an uncapped real kube-scheduler) and onto BigFleet's
// capacity-delivery deliverable plus its ADR-0045 coverage contract:
//
//   - shardConfigurePhaseP99 ≤ 15 s (held bar; ADR-0054, inherits
//     ADR-0020's method). Per-machine Idle→Configured wall-clock — the
//     capacity-materialization latency BigFleet owns end to end. Gated
//     only when the profile sets slo.shardConfigurePhaseP99Seconds.
//   - bootstrapSuccessRatio ≥ 0.99 (MIN gate; ADR-0054). Materialization
//     throughput — success/(success+failure) Bootstraps. Closes the
//     throughput-collapse hole latency + shortfall gates miss under
//     ADR-0052 in-flight crediting. Gated only when set.
//   - operatorNodeStateUpdateP99 ≤ 1 s (ADR-0054). The operator
//     UpcomingNode=Ready publish hop — previously uncovered. Gated only
//     when set.
//   - shardShortfalls == 0 (ADR-0045/0054). Demand covered by bound
//     capacity; promoted from precondition to verdict.
//   - endToEndPodBindP50 ≤ 10 s (LOOSE liveness; ADR-0054 Half 2). NOT
//     the release gate; the end-to-end p99 + raw-max stay informational
//     (uncapped-scheduler / reprovision-bound). Gated only when set.
//
// internalBindingLatencyP99 (the end-to-end pod-bind p99) is RETIRED to
// informational by ADR-0054 — still scraped into summary.json, no longer
// gates. ADR-0014/0018's original 15s rollup-sized arithmetic assumed
// the fake-provider in-process floor; under the real scheduler it is
// structurally unreachable. Its 15s held bar moved to configure-phase.
//
//   - shardCycleDurationP99 ≤ rollupInterval / 2 (default 10 s → 5 s).
//     Throughput envelope — if the shard can't finish a snapshot
//     before the next rollup arrives, backlog accumulates and binding
//     latency drifts. ADR-0014 §2: not a release gate by itself, but
//     a real fail signal — the next cycle compounds the lag, so
//     enforce it as a floor.
//
//   - operatorRollupP99 ≤ 1 s.          Best observed: 122 ms (scaleway-50k).
//     One rollup pipeline turn (list CRs, aggregate, enqueue) must finish
//     well within the 10 s rollup interval at any reasonable cluster size.
//
//   - operatorAckP99 ≤ 12 s.            Best observed: 9.97 s (scaleway-50k).
//     This batch is bounded by the operator's per-CR status-write QPS
//     against the apiserver. A 1 K-CR ramp at QPS=50/Burst=100 needs ~10 s
//     of wall-clock just for the writes; 12 s allows ~20 % run-to-run
//     variance. Tighten when the operator gains batched status writes
//     or its QPS budget is raised on profile.
//
// Cycle wall-clock and per-phase histograms remain in summary.json as
// informational metrics; they're alerted on by the operator's
// monitoring stack but no longer block a release.
// pass evaluates the run's steady-state SLO histograms against the
// profile's thresholds. Per ADR-0035: SLOs are measured over the soak
// window with continuous churn, not from ramp behaviour. Per ADR-0054
// the headline signals are the BigFleet capacity-delivery hops
// (configure-phase p99, Bootstrap success ratio, node-state-update p99,
// shortfalls==0) plus the throughput envelope (shard cycle p99) and the
// operator pipeline (rollup p99, ack p99) — what BigFleet actually
// delivers in steady state, not the uncapped-scheduler-bound end-to-end
// pod-bind p99.
//
// Reaching steady state is a prerequisite for pass() to be called at
// all (the runner gates on it before soak starts). Ramp throughput is
// captured in the summary but does not gate pass/fail.
// unmeasuredGated returns the gated SLO metrics whose value is the -1
// sentinel. pass() skips sentinels by design (a failed scrape must not
// flip a verdict), but skipping SILENTLY made vacuous passes invisible
// (M66.3 / complexity audit §2: the headline binding-latency SLO read
// -1 on every kube-scheduler-mode profile because its histogram is
// pod-shim-emitted, and nothing said so).
func unmeasuredGated(m map[string]float64, slo sloOverrides) []string {
	// Always-gated keys (no per-profile opt-out): these gate on every run.
	gated := []string{
		"shardCycleDurationP99Seconds",
		"operatorRollupP99Seconds",
		"operatorAckP99Seconds",
		// V2 runs only (the key is absent on legacy runs): the ADR-0045
		// zero-reclaim-churn condition. -1 = one of the two counter
		// reads failed, so the condition was asserted on nothing.
		"reclaimActionsDuringSoak",
		// ADR-0054: shortfalls==0 is promoted from precondition to a hard
		// verdict, so an unmeasured shortfalls read now hides a vacuous
		// pass — track it. The series exists from shard start (no
		// vector(0) fold in readShardShortfalls), so -1 means a failed
		// scrape, not "zero".
		"shardShortfalls",
	}
	// ADR-0054: the new BigFleet-property bars gate only when their
	// posture number is set (> 0). Adding them unconditionally would flag
	// a non-gating key as vacuous forever (the exact reason
	// internalBindingLatencyP99Seconds is no longer here — it gates on
	// nothing now). Mirror pass()'s set-then-gate condition.
	if slo.ShardConfigurePhaseP99Seconds > 0 {
		gated = append(gated, "shardConfigurePhaseP99Seconds")
	}
	if slo.BootstrapSuccessRatio > 0 {
		gated = append(gated, "bootstrapSuccessRatio")
	}
	if slo.OperatorNodeStateUpdateP99Seconds > 0 {
		gated = append(gated, "operatorNodeStateUpdateP99Seconds")
	}
	if slo.EndToEndPodBindP50Seconds > 0 {
		gated = append(gated, "endToEndPodBindP50Seconds")
	}
	// Inverted-gate keys gate only when the inverted posture is declared.
	// ExpectStandingShortfall makes the shortfall-convergence delta
	// load-bearing; ExpectPreemptions makes the Preempt counter
	// load-bearing. (shardShortfalls itself is always-gated above.)
	if slo.ExpectStandingShortfall {
		gated = append(gated, "shardShortfallsDelta")
	}
	if slo.ExpectPreemptions {
		gated = append(gated, "preemptActions")
	}
	var out []string
	for _, k := range gated {
		if v, ok := m[k]; ok && v < 0 {
			out = append(out, k)
		}
	}
	return out
}

func pass(m map[string]float64, totalCRs, shardReplicas int, slo sloOverrides) (bool, string) {
	cycleEnvelopeTarget := 5.0
	if slo.ShardCycleDurationP99Seconds > 0 {
		cycleEnvelopeTarget = slo.ShardCycleDurationP99Seconds
	}
	rollupTarget := 1.0
	if slo.OperatorRollupP99Seconds > 0 {
		rollupTarget = slo.OperatorRollupP99Seconds
	}
	ackTarget := 12.0
	if slo.OperatorAckP99Seconds > 0 {
		ackTarget = slo.OperatorAckP99Seconds
	}
	// Sustained-load floor: the run is invalid if loadgenCRsActive
	// drifted away from the target during the soak. We already gate
	// at the steady-state ramp, but a ramp that just-barely-passed
	// and then collapsed under churn would still produce SLO numbers
	// against an under-loaded shard. Allow 0.1 % drift; below that
	// the run isn't measuring what the SLO is about.
	if totalCRs > 0 {
		if v, ok := m["loadgenCRsActive"]; ok {
			minActive := 0.999 * float64(totalCRs)
			if v < minActive {
				return false, fmt.Sprintf("loadgenCRsActive %.0f < %.0f (99.9%% of target %d) — run did not sustain target load", v, minActive, totalCRs)
			}
		}
	}
	// Every configured shard must have published cycle metrics. Without
	// this gate, a crash-looping shard is invisible to the per-pod
	// max(by pod) aggregation used for cycle p99 (max just excludes
	// the missing pod).
	if shardReplicas > 0 {
		if v, ok := m["shardsReportingCycle"]; ok && v >= 0 && int(v) < shardReplicas {
			return false, fmt.Sprintf("shardsReportingCycle %d < shard.replicas %d — at least one shard isn't reporting metrics", int(v), shardReplicas)
		}
	}
	// ADR-0054 Half 1 — BigFleet capacity-delivery release gates. The
	// end-to-end pod-bind p99 (internalBindingLatencyP99Seconds) is
	// RETIRED to informational: under the default uncapped real
	// kube-scheduler it is dominated by scheduler retry WAIT + the
	// reprovision back-edge, neither BigFleet's deliverable. The verdict
	// now reads the hops BigFleet owns. Each new gate is set-then-gate:
	// the threshold gates only when the profile's posture number is > 0;
	// the -1 sentinel (failed scrape) is skipped so a scrape miss never
	// flips a verdict (it surfaces via unmeasuredGated instead).
	//
	// Capacity-materialization LATENCY: per-machine Idle→Configured.
	if slo.ShardConfigurePhaseP99Seconds > 0 {
		if v, ok := m["shardConfigurePhaseP99Seconds"]; ok && v >= 0 && v > slo.ShardConfigurePhaseP99Seconds {
			return false, fmt.Sprintf("shardConfigurePhaseP99Seconds %.3fs > %.1fs SLO (ADR-0054 — per-machine capacity-materialization latency BigFleet owns)", v, slo.ShardConfigurePhaseP99Seconds)
		}
	}
	// Capacity-materialization THROUGHPUT: Bootstrap success ratio. MIN
	// gate — fails when the measured ratio is BELOW the target (opposite
	// direction from the latency gates). Closes the throughput-collapse
	// hole that latency + shortfall gates miss under ADR-0052 crediting.
	if slo.BootstrapSuccessRatio > 0 {
		if v, ok := m["bootstrapSuccessRatio"]; ok && v >= 0 && v < slo.BootstrapSuccessRatio {
			return false, fmt.Sprintf("bootstrapSuccessRatio %.4f < %.4f min SLO (ADR-0054 — Bootstrap materialization throughput collapse: machines failing/retrying Configure)", v, slo.BootstrapSuccessRatio)
		}
	}
	// Last BigFleet-owned hop: operator publishing UpcomingNode=Ready.
	if slo.OperatorNodeStateUpdateP99Seconds > 0 {
		if v, ok := m["operatorNodeStateUpdateP99Seconds"]; ok && v >= 0 && v > slo.OperatorNodeStateUpdateP99Seconds {
			return false, fmt.Sprintf("operatorNodeStateUpdateP99Seconds %.3fs > %.1fs SLO (ADR-0054 — operator UpcomingNode-publish hop)", v, slo.OperatorNodeStateUpdateP99Seconds)
		}
	}
	// ADR-0045 coverage contract, promoted from steady-state precondition
	// to post-soak verdict (ADR-0054): demand covered by bound capacity.
	// The cheapest anti-reframe-to-pass guard. -1 = failed scrape (the
	// series exists from shard start), skipped.
	//
	// INVERTED for engine-correctness scarcity runs: when the profile
	// declares ExpectStandingShortfall, supply is < demand by design and a
	// standing shortfall is the EXPECTED steady state (sole-throttle hard
	// rule: satisfy priority-descending, leave the LOW-priority surplus as
	// a shortfall). The gate then asserts the opposite — shortfall MUST be
	// > 0 and converged (|Δ| ≤ ShortfallStabilityMax) — rather than == 0.
	// NOTE the confinement half ("zero high-priority shortfall") is not
	// assertable: no priority-labelled shortfall metric exists (see the
	// ExpectStandingShortfall doc). -1 is still skipped (failed scrape).
	if slo.ExpectStandingShortfall {
		if v, ok := m["shardShortfalls"]; ok && v >= 0 {
			if v == 0 {
				return false, "shardShortfalls == 0 but expectStandingShortfall is set (scarcity run: supply < demand by design — a covered run means the seed wasn't actually scarce, the test is vacuous)"
			}
			// shardShortfallsDelta is abs(delta(...)) in PromQL, so a real
			// value is always ≥ 0; -1 is the failed-scrape sentinel (skip),
			// matching every other gate's convention.
			if d, ok := m["shardShortfallsDelta"]; ok && d >= 0 && d > slo.ShortfallStabilityMax {
				return false, fmt.Sprintf("shardShortfallsDelta %.0f (|Δ| over soak) > %.0f tolerance — standing shortfall is not converged (demand outrunning the engine under scarcity, not a priority-throttled steady state)", d, slo.ShortfallStabilityMax)
			}
		}
	} else if v, ok := m["shardShortfalls"]; ok && v >= 0 && v != 0 {
		return false, fmt.Sprintf("shardShortfalls %.0f != 0 (ADR-0045/0054 — demand not covered by bound capacity)", v)
	}
	// Phase-2 preemption assertion (engine-correctness preemption run):
	// the engine must have actually preempted an incumbent. The Preempt
	// counter exists (bigfleet_shard_actions_total{kind="Preempt"}); the
	// absent-series fold means a never-incremented counter reads 0, so 0
	// here genuinely means "never preempted", a failure for this run.
	if slo.ExpectPreemptions {
		if v, ok := m["preemptActions"]; ok && v >= 0 && v == 0 {
			return false, "preemptActions == 0 but expectPreemptions is set — the engine never emitted a Phase-2 Preempt action (a HIGH-priority burst against a zero-headroom LOW-priority fill must bind by preempting incumbents)"
		}
	}
	// ADR-0054 Half 2 — LOOSE end-to-end pod-bind p50 liveness floor. NOT
	// the release gate (which moved onto the BigFleet-property bars
	// above); p50 sits below the uncapped-scheduler retry tail, so a p50
	// blowup means the COMMON bind path broke. Gated only when set.
	if slo.EndToEndPodBindP50Seconds > 0 {
		if v, ok := m["endToEndPodBindP50Seconds"]; ok && v >= 0 && v > slo.EndToEndPodBindP50Seconds {
			return false, fmt.Sprintf("endToEndPodBindP50Seconds %.3fs > %.1fs loose liveness floor (ADR-0054 Half 2 — common bind path broke; p99 is informational)", v, slo.EndToEndPodBindP50Seconds)
		}
	}
	// ADR-0014 throughput envelope: cycle p99 ≤ rollupInterval / 2 so
	// the shard always finishes one snapshot before the next rollup
	// arrives. Default rollupInterval is 10s → envelope = 5s. Profile
	// can raise via slo.shardCycleDurationP99Seconds (e.g. 30s rollup
	// → 15s envelope).
	if v, ok := m["shardCycleDurationP99Seconds"]; ok && v > cycleEnvelopeTarget {
		return false, fmt.Sprintf("shardCycleDurationP99Seconds %.3fs > %.1fs throughput envelope (rollupInterval/2; backlog will accumulate)", v, cycleEnvelopeTarget)
	}
	if v, ok := m["operatorRollupP99Seconds"]; ok && v > rollupTarget {
		return false, fmt.Sprintf("operatorRollupP99Seconds %.3fs > %.1fs SLO", v, rollupTarget)
	}
	if v, ok := m["operatorAckP99Seconds"]; ok && v > ackTarget {
		return false, fmt.Sprintf("operatorAckP99Seconds %.3fs > %.1fs SLO", v, ackTarget)
	}
	// Coordinator-side gates (M12 onwards: shards self-register, so
	// coordinator metrics are real signal). FSM Apply errors mean
	// Raft is rejecting commands or returning errors. Outbox drops
	// mean the operator session lost frames silently.
	if v, ok := m["coordinatorApplyErrorRate"]; ok && v > 0.001 {
		return false, fmt.Sprintf("coordinatorApplyErrorRate %.4f > 0.001 — coordinator FSM is rejecting commands", v)
	}
	if v, ok := m["operatorOutboxDropsPerSec"]; ok && v > 0 {
		return false, fmt.Sprintf("operatorOutboxDropsPerSec %.3f > 0 — operator session-outbox dropped messages", v)
	}
	return true, ""
}

func confirm(prompt string) error {
	fmt.Fprint(os.Stderr, prompt)
	var ans string
	if _, err := fmt.Scanln(&ans); err != nil {
		return err
	}
	if !strings.HasPrefix(strings.ToLower(ans), "y") {
		return errors.New("aborted by user")
	}
	return nil
}

// _ = http.MethodGet keeps the import in case we later prefer
// in-process HTTP over kubectl-exec-wget.
var _ = http.MethodGet
