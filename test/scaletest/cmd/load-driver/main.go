// Command load-driver emits realistic CapacityRequest churn against a
// KWOK-backed apiserver inside a scaletest pod.
//
// One load-driver per simulated cluster. It maintains a target
// population of CapacityRequests (count from the profile), then churns
// at a configured rate: every churn tick a fraction of CRs are deleted
// and recreated with new names. Deletion → recreation models pod
// completion + resubmission, which is the dominant CR lifecycle in
// production.
//
// Profiles are YAML, mounted from a ConfigMap by the harness chart:
//
//	target: 1000              # steady-state CR count
//	churnPerMinute: 0.05      # 5% of CRs replaced per minute
//	burstAtStart: 0           # extra CRs created at t=0 then drained
//	priorityClasses: [100, 1000, 1000000]   # round-robin per CR
//	durationSeconds: 1800     # how long to keep churning; 0 = forever
//
// Metrics:
//
//	scaletest_loadgen_cr_created_total
//	scaletest_loadgen_cr_deleted_total
//	scaletest_loadgen_cr_active
//	scaletest_loadgen_create_errors_total{kind}
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
)

type profile struct {
	Target          int     `yaml:"target"`
	ChurnPerMinute  float64 `yaml:"churnPerMinute"`
	BurstAtStart    int     `yaml:"burstAtStart"`
	PriorityClasses []int32 `yaml:"priorityClasses"`
	DurationSeconds int     `yaml:"durationSeconds"`

	// Archetypes: a list of workload templates, weighted-picked on every
	// createOne. When non-empty, the GPU-only single-shape fallback
	// below is bypassed and CRs are emitted from the chosen archetype.
	// Both the load-driver and the shard's Configured seed read this
	// list (M31). When empty, behaviour is identical to pre-M31:
	// instance-type=a3-highgpu-8g, nvidia.com/gpu=8, priority from
	// PriorityClasses (or 1000), penalties 8192/65536. See
	// test/scaletest/profiles/archetypes/realistic.yaml for the
	// production-realistic catalog.
	Archetypes []archetype.Archetype `yaml:"archetypes"`

	// DemandArchetypes (M34 / Item 1): when non-empty, the load-
	// driver picks from this list instead of Archetypes. Lets a
	// profile express "what's already running" (Archetypes /
	// seedArchetypes) separately from "what's being submitted right
	// now" (demandArchetypes). Real fleets show drift between the
	// two; the previous single-catalog harness made Phase 3 matching
	// artificially easy.
	DemandArchetypes []archetype.Archetype `yaml:"demandArchetypes"`

	// SeedArchetypes is parsed but unused by the load-driver — the
	// shard binary reads it from the same ConfigMap. Declared here
	// so unmarshal doesn't reject the field.
	SeedArchetypes []archetype.Archetype `yaml:"seedArchetypes"`

	// Bursts (ADR-0015 §3): each driver independently rolls a
	// Bernoulli trial against Selectivity at startup. Pods that win
	// the trial schedule the burst at AtSeconds-from-start (relative
	// to driver start, NOT runner soak start — the runner schedules
	// its actions independently). At burst time the driver ramps
	// Target by ExtraTarget for DurationSeconds, then drains back.
	Bursts []burstSpec `yaml:"bursts"`

	// ClusterSizeDistribution (ADR-0015 §5): override the per-pod
	// effective Target via a heavy-tailed distribution. Each pod
	// computes its multiplier deterministically from POD_NAME's
	// ordinal so the same pod always gets the same multiplier.
	// When empty, every pod uses Target unmodified.
	ClusterSizeDistribution []sizeBucket `yaml:"clusterSizeDistribution"`

	// JitteredChurn (M41 / Item 8): when true, the per-tick churn
	// count is drawn from Poisson(perTick) instead of the
	// deterministic perTick value. Real apiserver write patterns
	// have second-by-second variance; without jitter every tick is
	// identical and the p99.9 tail behaviour is invisible. Default
	// false for backward compatibility with existing profiles.
	JitteredChurn bool `yaml:"jitteredChurn"`

	// MicroBurstRatePerMinute (M41 / Item 8): expected number of
	// minute-scale micro-bursts per minute per pod. On each ticking
	// second a Bernoulli(rate/60) trial fires; on success the tick's
	// churn is multiplied by MicroBurstFactor for that one tick.
	// Models the "p99.9 tail" — large but rare write spikes that
	// real apiservers see during deploy windows or queue drains.
	// 0 disables.
	MicroBurstRatePerMinute float64 `yaml:"microBurstRatePerMinute"`
	MicroBurstFactor        float64 `yaml:"microBurstFactor"`

	// Mode (M43a / Item 10, M44 default flip): "" or "pods" emit
	// Pods with archetype-shaped nodeAffinity + resources, leaving
	// the rest of the production loop (mark-Unschedulable,
	// bigfleet-unschedulable-pod-controller, Node creation, Pod
	// binding) to the bigfleet-scaletest-pod-shim (M43b/c). This is
	// the realistic shape — what users actually feel — so it's the
	// default. "cr" is the opt-in legacy shape that bypasses the
	// Pod layer entirely (cheap, but doesn't exercise the user-
	// facing binding-latency path; bindingLatencyP99 gate skipped
	// via -1 sentinel per ADR-0017).
	Mode string `yaml:"mode"`

	// ReconcilePerTickCap (M45.5): per-tick limit on creates/deletes
	// applied by `reconcileTarget`. The 20/tick default was sized
	// for ~1000-Pod-per-cluster targets where a 50-Pod drift catches
	// up in 2-3 sec. At 100K-Pod-per-cluster targets (density=100 →
	// 50K machines fleet-wide), 33 Pods/sec is the minimum ramp-rate
	// per cluster to fill in under an hour — the cap has to scale or
	// `loadgenCRsActive` plateaus arbitrarily far below target.
	// 0 falls back to the historical default of 20. Profiles bumping
	// per-cluster Pod targets should bump this proportionally.
	ReconcilePerTickCap int `yaml:"reconcilePerTickCap"`

	// PreBind (M52.B, ADR-0035): when true, the load-driver binds the
	// initial Pod fill to fake-Nodes via the Bind API after rampTo,
	// bypassing kube-scheduler's slow filter/score cycle. The Pods
	// still go through the realistic Unschedulable → UPC →
	// CapacityRequest path first (rampTo creates them without
	// Spec.NodeName); only the binding is fast-pathed. Soak churn
	// Pods are never pre-bound — they go through kube-scheduler, the
	// path the steady-state SLO measures.
	//
	// Default false = scheduler-bound ramp. ADR-0035's steady-state
	// methodology sets this true so the install reaches steady state
	// without paying the kube-scheduler bulk-bind ramp.
	PreBind bool `yaml:"preBind"`
}

func (p *profile) reconcilePerTickCap() int {
	if p.ReconcilePerTickCap > 0 {
		return p.ReconcilePerTickCap
	}
	return 20
}

type burstSpec struct {
	AtSeconds       int     `yaml:"atSeconds"`
	Archetype       string  `yaml:"archetype"`
	ExtraTarget     int     `yaml:"extraTarget"`
	DurationSeconds int     `yaml:"durationSeconds"`
	Selectivity     float64 `yaml:"selectivity"`
}

type sizeBucket struct {
	Fraction         float64 `yaml:"fraction"`
	TargetMultiplier float64 `yaml:"targetMultiplier"`
}

// demandArchetypes returns the archetype list the load-driver should
// pick from. DemandArchetypes wins if non-empty; otherwise falls back
// to the shared Archetypes list. M34 / Item 1.
func (p profile) demandArchetypes() []archetype.Archetype {
	if len(p.DemandArchetypes) > 0 {
		return p.DemandArchetypes
	}
	return p.Archetypes
}

var (
	created = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scaletest_loadgen_cr_created_total",
	})
	deleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scaletest_loadgen_cr_deleted_total",
	})
	active = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scaletest_loadgen_cr_active",
	})
	target = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scaletest_loadgen_cr_target",
		Help: "Effective per-cluster CR/Pod target after M36 cluster-size-skew adjustments. Summing across clusters gives the runner's totalCRs gate denominator. Used by the dashboard's sustained-load SLO panel (load-active / target).",
	})
	errs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scaletest_loadgen_errors_total",
	}, []string{"kind"})

	// anchorsBound counts sameRack co-location-group anchor pods the
	// load-driver force-binds to break the podAffinity bootstrap
	// deadlock (ADR-0025). One per group; real kube-scheduler places
	// the rest of each group. BigFleet is unaffected — this only moves
	// the user-facing bind metric for co-located workloads.
	anchorsBound = promauto.NewCounter(prometheus.CounterOpts{
		Name: "scaletest_loadgen_anchors_bound_total",
		Help: "Count of sameRack co-location-group anchor pods force-bound by the load-driver (ADR-0025 gang-scheduler stand-in).",
	})

	// steadyStateMetric is 1 once this load-driver has filled to its
	// target Pod count (cluster has finished its initial fill); new
	// Pods after that are churn replacements carrying the
	// scaletest.bigfleet/state="steady" label. Aggregate across all
	// clusters to drive the dashboard's "test phase" indicator: sum =
	// kwok.clusterCount means the whole fleet is in steady state and
	// the SLO histogram is recording.
	steadyStateMetric = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "scaletest_loadgen_steady_state",
		Help: "1 if this cluster's load-driver has reached its target Pod count (so subsequent Pod creations are churn replacements tagged scaletest.bigfleet/state=\"steady\"); 0 during initial fill. Aggregate sum across clusters drives the dashboard's test-phase indicator.",
	})

	// Drop U: per-phase wall-clock start time. Combined with time() in
	// PromQL this yields "time in current phase" without the dashboard
	// having to guess from the abstract steady-state indicator. ramp is
	// stamped at load-driver startup; steady is stamped at the moment
	// the cluster first reaches its target Pod count. Aggregated across
	// clusters: min(... phase="ramp") = first cluster to start;
	// min(... phase="steady") = first cluster to reach target; max =
	// last. A run-wide "soak began at" is min(... phase="steady") since
	// the runner waits for every cluster to be steady before declaring
	// steady state.
	phaseStartedAt = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "scaletest_loadgen_phase_started_at_seconds",
		Help: "Unix-time when this load-driver entered each phase. phase=ramp is set at process start; phase=steady is set when the cluster first reaches its target Pod count. Unset/zero means the phase has not been entered.",
	}, []string{"phase"})
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "load-driver:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("load-driver", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	clusterID := fs.String("cluster-id", "", "stable cluster ID; used as a CR name prefix")
	kubeconfig := fs.String("kubeconfig", "", "path to kubeconfig")
	profilePath := fs.String("profile", "", "path to load profile YAML")
	metricsAddr := fs.String("metrics-addr", ":8771", "/metrics endpoint")
	demandReadyFile := fs.String("demand-ready-file", "", "path to touch once the initial Pod fill completes; the harness blocks the BigFleet operator's start on this file (ADR-0036)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *clusterID == "" {
		return errors.New("--cluster-id required")
	}
	if *profilePath == "" {
		return errors.New("--profile required")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	prof, err := loadProfile(*profilePath)
	if err != nil {
		return fmt.Errorf("profile: %w", err)
	}
	// ADR-0015 §5: cluster-size skew. Scale the per-pod effective
	// Target deterministically by POD_NAME ordinal so the test sees
	// a heavy-tailed distribution of cluster sizes (a few big
	// clusters dominate fleet load; long tail of small ones provides
	// fingerprint diversity).
	if mult := podSizeMultiplier(*clusterID, prof.ClusterSizeDistribution); mult != 1.0 {
		// M36 (Item 3): kwok pod resource budgets are uniform across
		// the StatefulSet; multipliers above 2× start risking OOM /
		// QPS exhaustion on the workload-side container. Clamp the
		// effective multiplier and log the divergence so summary.json
		// reflects what actually ran, not what was requested.
		const safeMaxMultiplier = 2.0
		clamped := mult
		if clamped > safeMaxMultiplier {
			logger.Warn("cluster-size multiplier clamped — per-pod resource budgets are uniform; raise kwok.workloadResources or split into per-size StatefulSets to lift the cap",
				"requested_multiplier", mult,
				"clamped_to", safeMaxMultiplier,
			)
			clamped = safeMaxMultiplier
		}
		newTarget := int(float64(prof.Target) * clamped)
		logger.Info("cluster-size skew applied", "base_target", prof.Target, "multiplier", clamped, "effective_target", newTarget)
		prof.Target = newTarget
	}
	target.Set(float64(prof.Target))
	logger.Info("profile loaded",
		"target", prof.Target,
		"churn_per_minute", prof.ChurnPerMinute,
		"burst", prof.BurstAtStart,
		"duration_s", prof.DurationSeconds,
	)

	kc, cs, err := newKubeClient(*kubeconfig)
	if err != nil {
		return fmt.Errorf("kube client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		logger.Info("signal received, shutting down")
		cancel()
	}()

	// /metrics on a separate goroutine.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()
	defer func() { _ = srv.Shutdown(context.Background()) }()

	// Drop U: stamp ramp-phase start so the dashboard can compute
	// time-in-phase without guessing from steady_state alone.
	phaseStartedAt.WithLabelValues("ramp").Set(float64(time.Now().Unix()))

	d := &driver{
		clusterID:       *clusterID,
		log:             logger,
		k:               kc,
		cs:              cs,
		prof:            prof,
		rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
		known:           make(map[string]crMeta, prof.Target),
		picker:          archetype.NewPicker(prof.demandArchetypes()),
		archByName:      indexArchetypes(prof.demandArchetypes()),
		demandReadyFile: *demandReadyFile,
	}
	if d.picker != nil {
		logger.Info("archetypes loaded", "count", len(prof.Archetypes))
	}
	return d.run(ctx)
}

type driver struct {
	clusterID string
	log       *slog.Logger
	k         client.Client
	// cs is the typed clientset, used only for the Binding subresource
	// in the ADR-0025 anchor path — controller-runtime's
	// SubResource("binding") has known cache-layer issues.
	cs         *kubernetes.Clientset
	prof       profile
	rng        *rand.Rand
	picker     *archetype.Picker
	archByName map[string]*archetype.Archetype

	mu     sync.Mutex
	known  map[string]crMeta
	seq    uint64
	groups map[string]*driverGroup // archetype name → current group

	// steadyState flips true the first time the driver's active count
	// reaches the profile target. Pods created after that point carry
	// the bigfleet.lucy.sh/steady-state=true label so pod-shim's binder
	// can record their latency in the *steady-state* histogram. The
	// initial fill produces a thundering-herd binding pattern that's
	// not representative of production; isolating the post-fill churn
	// in its own metric keeps the SLO honest.
	steadyState atomic.Bool

	// demandReadyFile, when non-empty, is touched once the initial
	// Pod fill (rampTo) completes — i.e. once the cluster's demand is
	// established as CapacityRequests via UPC. The harness entrypoint
	// blocks the BigFleet operator's startup on this file so the
	// operator's first rollup reflects real demand (ADR-0036): a
	// production operator joins a cluster that already has workloads,
	// and the harness must mirror that ordering.
	demandReadyFile string
}

// driverGroup is a per-archetype Same-rack group. Stays open until
// `remaining` CRs have been allocated, then a fresh group is minted.
type driverGroup struct {
	uid       string
	remaining int
}

// crMeta records per-CR bookkeeping needed for ADR-0015 §2 lifetime
// aging. The archetype name keys back into archByName when picking
// which CRs to age out.
type crMeta struct {
	archetype string
}

// poissonInt samples an integer from Poisson(mean) via the Knuth
// algorithm. O(mean), fine for the small means used in M41's churn
// jitter (typically <50). Returns 0 for non-positive means.
func poissonInt(rng *rand.Rand, mean float64) int {
	if mean <= 0 {
		return 0
	}
	L := math.Exp(-mean)
	k := 0
	p := 1.0
	for p > L {
		k++
		p *= rng.Float64()
	}
	return k - 1
}

// podSizeMultiplier returns the per-pod target scaling factor from the
// distribution. Pods are bucketed by ordinal modulo a denominator of
// 100 so each fraction-band lands deterministically on a contiguous
// ordinal range — pod-0 always gets the same bucket, pod-1 always
// gets the same bucket, regardless of which other pods exist.
//
// ADR-0015 §5. Returns 1.0 when distribution is empty.
func podSizeMultiplier(podName string, dist []sizeBucket) float64 {
	if len(dist) == 0 {
		return 1.0
	}
	ord := ordinalFromPodName(podName)
	bucket := ord % 100
	cum := 0.0
	for _, b := range dist {
		cum += b.Fraction * 100
		if float64(bucket) < cum {
			return b.TargetMultiplier
		}
	}
	// Fractions don't sum to 1.0 — apply the last bucket so we don't
	// accidentally return 0 (which would make the pod create no CRs).
	return dist[len(dist)-1].TargetMultiplier
}

// ordinalFromPodName extracts the trailing integer from a pod name.
// "kwok-cluster-7" → 7. Returns 0 when no trailing integer is found.
func ordinalFromPodName(podName string) int {
	for i := len(podName) - 1; i >= 0; i-- {
		if podName[i] == '-' {
			suffix := podName[i+1:]
			n := 0
			for _, c := range suffix {
				if c < '0' || c > '9' {
					return 0
				}
				n = n*10 + int(c-'0')
			}
			return n
		}
	}
	return 0
}

// shouldBurst decides whether this pod participates in a given burst
// trigger. Deterministic from (podName, atSeconds) so the same pod
// always reaches the same decision across restarts within a single
// run. ADR-0015 §3.
func shouldBurst(podName string, b burstSpec) bool {
	if b.Selectivity >= 1.0 {
		return true
	}
	if b.Selectivity <= 0.0 {
		return false
	}
	// Cheap deterministic hash: FNV-32 of (podName||atSeconds).
	const offset32 = 2166136261
	const prime32 = 16777619
	h := uint32(offset32)
	for _, c := range podName {
		h = (h ^ uint32(c)) * prime32
	}
	for v := uint32(b.AtSeconds); v > 0; v >>= 8 {
		h = (h ^ (v & 0xff)) * prime32
	}
	return float64(h)/float64(^uint32(0)) < b.Selectivity
}

func indexArchetypes(arches []archetype.Archetype) map[string]*archetype.Archetype {
	if len(arches) == 0 {
		return nil
	}
	out := make(map[string]*archetype.Archetype, len(arches))
	for i := range arches {
		out[arches[i].Name] = &arches[i]
	}
	return out
}

func (d *driver) run(ctx context.Context) error {
	// ADR-0025: stand in for a gang scheduler — force-bind one anchor
	// per sameRack co-location group so the group's self-referential
	// podAffinity can bootstrap. Runs for the whole driver lifetime,
	// across ramp and steady-state churn.
	go d.anchorSameRackGroups(ctx)

	// Phase 1: ramp to target — create the Pods. They are created
	// WITHOUT Spec.NodeName, so they go Unschedulable → UPC →
	// CapacityRequest. This establishes the cluster's demand before
	// the BigFleet operator starts: the harness entrypoint blocks the
	// operator on the demand-ready file written below, so the
	// operator's first rollup reflects real demand and ADR-0036's
	// Phase 3 gate releases on a genuine (non-empty) rollup.
	d.log.Info("ramping to target", "count", d.prof.Target)
	if err := d.rampTo(ctx, d.prof.Target); err != nil {
		return fmt.Errorf("ramp: %w", err)
	}

	// Signal demand saturation. Every target Pod now exists; UPC has
	// had the rampTo duration to CR-ify them. The entrypoint unblocks
	// the BigFleet operator once it sees this file.
	if d.demandReadyFile != "" {
		if err := os.WriteFile(d.demandReadyFile, []byte("ready\n"), 0o644); err != nil {
			d.log.Warn("demand-ready-file write failed", "path", d.demandReadyFile, "err", err)
		} else {
			d.log.Info("demand saturated; wrote demand-ready file", "path", d.demandReadyFile)
		}
	}

	// M52.B (ADR-0035): pre-bind the initial Pods to fake-Nodes,
	// bypassing the slow kube-scheduler ramp. Patient retry — fake-
	// Nodes only materialise after the operator starts (which the
	// entrypoint did once it saw the demand-ready file) and the
	// shard → UpcomingNode → node-creator pipeline runs. Soak churn
	// Pods are NOT pre-bound; they go through kube-scheduler so the
	// steady-state SLO measures the production path. PreBind=false
	// skips this — the cluster then reaches steady state via the
	// scheduler-bound ramp.
	if d.prof.PreBind {
		d.preBindInitialPods(ctx)
	}

	// Phase 2: optional initial burst (above target, then drain back).
	if d.prof.BurstAtStart > 0 {
		d.log.Info("burst", "extra", d.prof.BurstAtStart)
		if err := d.rampTo(ctx, d.prof.Target+d.prof.BurstAtStart); err != nil {
			return fmt.Errorf("burst: %w", err)
		}
	}

	// Phase 3: steady-state churn until ctx is cancelled. The runner
	// owns lifecycle (helm uninstall on soak end); the load-driver
	// MUST NOT self-terminate, because its exit cascades through the
	// container supervise loop into pod death, which crashes the
	// metric scrape during the very window the runner is reading
	// post-soak metrics. profile.durationSeconds is now consumed only
	// by the runner for the soak budget.

	// Churn tick fires once per second. Replace a fraction of CRs sized
	// to hit churnPerMinute averaged over each minute. Drop X: keep
	// perTick as a float so churnPerMinute below 0.06 still represents
	// faithfully. The previous int(target*churn/60) with a `<1 → 1`
	// floor pinned per-cluster churn to 1 Pod/sec for any churn ≤ 1/min,
	// so e.g. churnPerMinute=0.02 produced the same 50/sec fleet churn
	// as 0.05 — a constant 50 Pods/sec across 50 clusters. The
	// accumulator below emits 1 churn when the running fractional
	// remainder crosses 1, so the tick rate matches the configured
	// per-minute target down to arbitrarily small rates.
	perTickFloat := float64(d.prof.Target) * d.prof.ChurnPerMinute / 60.0
	churnAccum := 0.0
	d.log.Info("steady state", "churn_per_tick", perTickFloat)

	// ADR-0015 §3: schedule burst events that this pod participates in.
	// shouldBurst is deterministic so a pod restart re-arms the same
	// bursts it had originally been chosen for.
	type pendingBurst struct {
		fireAt  time.Time
		drainAt time.Time
		spec    burstSpec
		extraOn bool
	}
	bursts := make([]*pendingBurst, 0, len(d.prof.Bursts))
	startedAt := time.Now()
	for _, b := range d.prof.Bursts {
		if !shouldBurst(d.clusterID, b) {
			continue
		}
		pb := &pendingBurst{
			fireAt:  startedAt.Add(time.Duration(b.AtSeconds) * time.Second),
			drainAt: startedAt.Add(time.Duration(b.AtSeconds+b.DurationSeconds) * time.Second),
			spec:    b,
		}
		bursts = append(bursts, pb)
		d.log.Info("burst scheduled", "archetype", b.Archetype, "at_s", b.AtSeconds, "extra", b.ExtraTarget, "duration_s", b.DurationSeconds)
	}

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-tick.C:
			// Drop X: accumulator carries the fractional remainder so
			// e.g. perTickFloat=0.33 emits one churn every ~3 ticks
			// instead of being floored to zero (or, previously, ceiled
			// to one).
			churnAccum += perTickFloat
			thisTick := int(churnAccum)
			churnAccum -= float64(thisTick)
			// M41: Poisson jitter on per-tick churn count, plus
			// optional minute-scale micro-bursts. Without jitter,
			// every tick produces identical write rate and the p99.9
			// tail is invisible.
			if d.prof.JitteredChurn && perTickFloat > 0 {
				thisTick = poissonInt(d.rng, perTickFloat)
			}
			if d.prof.MicroBurstRatePerMinute > 0 && d.prof.MicroBurstFactor > 1 {
				if d.rng.Float64() < d.prof.MicroBurstRatePerMinute/60.0 {
					thisTick = int(float64(thisTick) * d.prof.MicroBurstFactor)
				}
			}
			if thisTick > 0 {
				d.churn(ctx, thisTick)
			}
			// ADR-0015 §2: per-archetype lifetime aging.
			d.ageByLifetime(ctx)
			// ADR-0015 §3: ramp / drain bursts. extraOn flips the
			// pod's effective target up while the burst is active.
			for _, pb := range bursts {
				if !pb.extraOn && !now.Before(pb.fireAt) && now.Before(pb.drainAt) {
					pb.extraOn = true
					d.log.Info("burst firing", "archetype", pb.spec.Archetype, "extra", pb.spec.ExtraTarget)
				}
				if pb.extraOn && !now.Before(pb.drainAt) {
					pb.extraOn = false
					d.log.Info("burst drained", "archetype", pb.spec.Archetype)
				}
			}
			extra := 0
			for _, pb := range bursts {
				if pb.extraOn {
					extra += pb.spec.ExtraTarget
				}
			}
			// Reconcile actual count back toward target+burst-extra.
			d.reconcileTarget(ctx, d.prof.Target+extra)
		}
	}
}

// ageByLifetime replaces a fraction of CRs proportional to each
// archetype's mean lifetime. Run once per tick (1s).
func (d *driver) ageByLifetime(ctx context.Context) {
	if d.archByName == nil {
		return
	}
	counts := d.activeCountByArchetype()
	for name, n := range counts {
		a := d.archByName[name]
		if a == nil || a.MeanLifetimeSeconds <= 0 {
			continue
		}
		// Expected replacements per second = n / mean_lifetime. Sample
		// a small Poisson around it via fractional accumulation: take
		// the integer part deterministically and the fractional part
		// via Bernoulli on the rng. Simple and unbiased at low rates.
		rate := float64(n) / float64(a.MeanLifetimeSeconds)
		nReplace := int(rate)
		if d.rng.Float64() < rate-float64(nReplace) {
			nReplace++
		}
		for i := 0; i < nReplace; i++ {
			old, ok := d.popRandomFromArchetype(name)
			if !ok {
				break
			}
			if err := d.deleteOne(ctx, old); err != nil {
				errs.WithLabelValues("delete").Inc()
			}
			if err := d.createOne(ctx); err != nil {
				errs.WithLabelValues("create").Inc()
			}
		}
	}
}

func (d *driver) rampTo(ctx context.Context, want int) error {
	for d.activeCount() < want {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := d.createOne(ctx); err != nil {
			errs.WithLabelValues("create").Inc()
			d.log.Warn("create failed", "err", err)
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !d.steadyState.Swap(true) {
		steadyStateMetric.Set(1)
		phaseStartedAt.WithLabelValues("steady").Set(float64(time.Now().Unix()))
	}
	return nil
}

func (d *driver) churn(ctx context.Context, n int) {
	for i := 0; i < n; i++ {
		name, ok := d.popRandom()
		if !ok {
			return
		}
		if err := d.deleteOne(ctx, name); err != nil {
			errs.WithLabelValues("delete").Inc()
		}
		if err := d.createOne(ctx); err != nil {
			errs.WithLabelValues("create").Inc()
		}
	}
}

// reconcileTarget drives the active count toward the given target,
// up to a per-tick budget of 20 ops in either direction. Used both
// by the legacy reconcile loop and by ADR-0015 §3's burst-aware tick
// (which passes Target + extra burst capacity).
func (d *driver) reconcileTarget(ctx context.Context, target int) {
	got := d.activeCount()
	cap := d.prof.reconcilePerTickCap()
	switch {
	case got < target:
		for i := 0; i < target-got && i < cap; i++ {
			_ = d.createOne(ctx)
		}
	case got > target:
		extra := got - target
		for i := 0; i < extra && i < cap; i++ {
			if name, ok := d.popRandom(); ok {
				_ = d.deleteOne(ctx, name)
			}
		}
	}
}

func (d *driver) createOne(ctx context.Context) error {
	d.mu.Lock()
	d.seq++
	d.mu.Unlock()

	if d.prof.Mode == "pods" {
		return d.createOnePod(ctx)
	}

	name := fmt.Sprintf("%s-cr-%06d", d.clusterID, d.seq)
	cr, archName := d.buildCRWithArchetype(name)
	if err := d.k.Create(ctx, cr); err != nil {
		return err
	}
	d.mu.Lock()
	d.known[name] = crMeta{archetype: archName}
	d.mu.Unlock()
	created.Inc()
	active.Set(float64(d.activeCount()))
	return nil
}

// createOnePod is the M43a path: emit a Pod (not a CR) with archetype-
// shaped affinity + resources + annotations. Pods stay Pending until
// M43b wires the bigfleet-scaletest-pod-shim that marks them
// Unschedulable and lets bigfleet-unschedulable-pod-controller emit
// CRs from there.
func (d *driver) createOnePod(ctx context.Context) error {
	name := fmt.Sprintf("%s-pod-%06d", d.clusterID, d.seq)
	pod, archName := d.buildPodWithArchetype(name)
	if err := d.k.Create(ctx, pod); err != nil {
		return err
	}
	d.mu.Lock()
	d.known[name] = crMeta{archetype: archName}
	now := len(d.known)
	d.mu.Unlock()
	created.Inc()
	active.Set(float64(now))
	// Flip steady-state once the cluster has reached its target Pod
	// count. Subsequent Pods (churn replacements) carry the
	// scaletest.bigfleet/state="steady" label so pod-shim can record
	// their bind latency in the steady-state histogram. Idempotent —
	// the atomic stays true once flipped, even if churn briefly drops
	// the count below target.
	if d.prof.Target > 0 && now >= d.prof.Target && !d.steadyState.Swap(true) {
		steadyStateMetric.Set(1)
		phaseStartedAt.WithLabelValues("steady").Set(float64(time.Now().Unix()))
	}
	return nil
}

// labelArchetype is set on every seeded Configured machine (and so on
// every kwok fake-Node derived from one). The load-driver's pre-bind
// phase uses it to recognise BigFleet-managed fake-Nodes.
const labelArchetype = "scaletest.bigfleet/archetype"

// preBindInitialPods binds the initial Pod fill to fake-Nodes via the
// Bind API, bypassing kube-scheduler's slow filter/score cycle. M52.B
// (ADR-0035). The Pods were already created by rampTo *without*
// Spec.NodeName — so they went Unschedulable → UPC → CapacityRequest,
// keeping the demand path realistic — and this step just binds them
// fast once supply (fake-Nodes) materialises.
//
// Patient retry: fake-Nodes appear only after the operator starts and
// the shard → UpcomingNode → node-creator pipeline runs, so the loop
// keeps sweeping until every initial Pod is bound or the deadline
// fires. Soak churn Pods created after this returns are NOT pre-bound
// — they go through kube-scheduler, which is the path the steady-state
// SLO must measure.
// preBindConcurrency caps in-flight Bind calls per sweep. A single
// goroutine was the M52.B initial-fill bottleneck; the apiserver
// handles this fan-out comfortably.
const preBindConcurrency = 64

func (d *driver) preBindInitialPods(ctx context.Context) {
	d.log.Info("pre-bind: binding initial Pods to fake-Nodes as they appear")
	// Safety valve only — the loop exits as soon as every initial Pod is
	// bound. 15 min was too short: a 500K-Pod fleet fills in ~25-30 min
	// at the observed pre-bind rate (bigfleet-uber #43), so a short
	// deadline quit mid-fill and left the tail to the slow scheduler.
	deadline := time.Now().Add(45 * time.Minute)
	for {
		if ctx.Err() != nil {
			return
		}
		// Unbound Pods only — server-side field selector, so the list
		// shrinks as binding progresses.
		unbound, err := d.cs.CoreV1().Pods("default").List(ctx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=",
		})
		if err != nil {
			d.log.Warn("pre-bind: list unbound pods", "err", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if len(unbound.Items) == 0 {
			d.log.Info("pre-bind: all initial Pods bound")
			return
		}
		var nodes corev1.NodeList
		if err := d.k.List(ctx, &nodes); err != nil {
			d.log.Warn("pre-bind: list nodes", "err", err)
			time.Sleep(2 * time.Second)
			continue
		}
		var allPods corev1.PodList
		if err := d.k.List(ctx, &allPods, client.InNamespace("default")); err != nil {
			d.log.Warn("pre-bind: list pods", "err", err)
			time.Sleep(2 * time.Second)
			continue
		}
		// Fake-Nodes are seeded density-100, so a Node hosts many Pods.
		// Track per-Node remaining capacity = Allocatable − Σ(requests of
		// Pods already bound there) and bin-pack against it; index Nodes
		// by archetype so a Pod only considers Nodes it actually matches.
		remaining := make(map[string]corev1.ResourceList, len(nodes.Items))
		byArchetype := make(map[string][]*corev1.Node)
		for j := range nodes.Items {
			n := &nodes.Items[j]
			arch, ok := n.Labels[labelArchetype]
			if !ok {
				continue
			}
			rl := corev1.ResourceList{}
			for k, v := range n.Status.Allocatable {
				rl[k] = v.DeepCopy()
			}
			remaining[n.Name] = rl
			byArchetype[arch] = append(byArchetype[arch], n)
		}
		for i := range allPods.Items {
			p := &allPods.Items[i]
			if p.Spec.NodeName == "" {
				continue
			}
			if rem, ok := remaining[p.Spec.NodeName]; ok {
				subtractRequests(rem, podRequests(p))
			}
		}
		// Plan assignments against the local remaining-capacity map, then
		// execute the Bind calls in parallel.
		type assignment struct {
			pod  *corev1.Pod
			node string
		}
		plan := make([]assignment, 0, len(unbound.Items))
		for i := range unbound.Items {
			pod := &unbound.Items[i]
			req := podRequests(pod)
			for _, n := range byArchetype[pod.Labels[labelArchetype]] {
				rem := remaining[n.Name]
				if !fitsRequests(rem, req) {
					continue
				}
				subtractRequests(rem, req)
				plan = append(plan, assignment{pod: pod, node: n.Name})
				break
			}
		}
		var bound atomic.Int64
		var wg sync.WaitGroup
		sem := make(chan struct{}, preBindConcurrency)
		for _, a := range plan {
			wg.Add(1)
			sem <- struct{}{}
			go func(a assignment) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := d.bindPod(ctx, a.pod, a.node); err != nil {
					d.log.Warn("pre-bind: bind failed", "pod", a.pod.Name, "node", a.node, "err", err)
					return
				}
				bound.Add(1)
			}(a)
		}
		wg.Wait()
		if b := bound.Load(); b > 0 {
			d.log.Info("pre-bind progress", "bound_this_pass", b, "still_unbound", int64(len(unbound.Items))-b)
		}
		if time.Now().After(deadline) {
			d.log.Warn("pre-bind: deadline reached with Pods still unbound")
			return
		}
		time.Sleep(2 * time.Second)
	}
}

// podRequests sums container resource requests across a Pod.
func podRequests(pod *corev1.Pod) corev1.ResourceList {
	out := corev1.ResourceList{}
	for _, c := range pod.Spec.Containers {
		for k, v := range c.Resources.Requests {
			cur := out[k]
			cur.Add(v)
			out[k] = cur
		}
	}
	return out
}

// fitsRequests reports whether `remaining` covers `req` on every resource.
func fitsRequests(remaining, req corev1.ResourceList) bool {
	for k, q := range req {
		have, ok := remaining[k]
		if !ok || have.Cmp(q) < 0 {
			return false
		}
	}
	return true
}

// subtractRequests decrements `remaining` by `req` in place.
func subtractRequests(remaining, req corev1.ResourceList) {
	for k, q := range req {
		if have, ok := remaining[k]; ok {
			have.Sub(q)
			remaining[k] = have
		}
	}
}

const (
	// labelCoLocationGroup tags every Pod / CR of one sameRack group
	// with a shared, group-unique value. The Pod's podAffinity selects
	// it; the operator aggregates CRs carrying an equal CoLocation term
	// into one Need. ADR-0024.
	labelCoLocationGroup = "scaletest.bigfleet/co-location-group"
	// topologyKeyRack is the node-label key sameRack archetypes
	// co-locate on — the TopologyKey of their podAffinity term, which
	// the operator turns into a Same(rack) requirement.
	topologyKeyRack = "topology.bigfleet/rack"
)

// buildPodWithArchetype mirrors buildCRWithArchetype but emits a Pod.
// Returns the Pod plus the archetype name (or "" for the legacy
// single-shape fallback).
func (d *driver) buildPodWithArchetype(name string) (*corev1.Pod, string) {
	if a := d.picker.Pick(d.rng); a != nil {
		return d.buildArchetypePod(name, a), a.Name
	}
	return d.buildLegacyPod(name), ""
}

// buildArchetypePod constructs a Pod with the archetype's affinity
// terms, resources, priority, penalty annotations, and (for sameRack
// archetypes) a real podAffinity term — the co-location signal the
// UPC translates into CR.Spec.CoLocation and the operator turns into
// a Same requirement at roll-up (ADR-0024).
func (d *driver) buildArchetypePod(name string, a *archetype.Archetype) *corev1.Pod {
	terms := []corev1.NodeSelectorRequirement{}
	if len(a.InstanceTypes) > 0 {
		terms = append(terms, corev1.NodeSelectorRequirement{
			Key:      "node.kubernetes.io/instance-type",
			Operator: corev1.NodeSelectorOpIn,
			Values:   append([]string(nil), a.InstanceTypes...),
		})
	}
	if len(a.Zones) > 0 {
		terms = append(terms, corev1.NodeSelectorRequirement{
			Key:      "topology.kubernetes.io/zone",
			Operator: corev1.NodeSelectorOpIn,
			Values:   append([]string(nil), a.Zones...),
		})
	}
	for k, v := range a.PickLabels(d.rng) {
		terms = append(terms, corev1.NodeSelectorRequirement{
			Key:      k,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{v},
		})
	}
	resources := corev1.ResourceList{}
	for k, v := range a.PickSize(d.rng) {
		resources[corev1.ResourceName(k)] = resource.MustParse(v)
	}
	pri := int32(1000)
	if len(a.PriorityClasses) > 0 {
		pri = a.PriorityClasses[d.rng.Intn(len(a.PriorityClasses))]
	}
	labels := map[string]string{"scaletest.bigfleet/archetype": a.Name}
	if d.steadyState.Load() {
		// M44.4: marker for pod-shim to record steady-state binding
		// latency in a separate histogram. Pods created during the
		// initial fill (cluster ramping from 0 → target) are excluded
		// — that thundering-herd binding pattern isn't representative
		// of production and shouldn't dominate the SLO. The "state"
		// suffix leaves room for additional values later (e.g. "burst").
		// Uses the harness-specific scaletest.bigfleet/* prefix so it
		// can't be mistaken for a production-bigfleet label.
		labels["scaletest.bigfleet/state"] = "steady"
	}
	meta := metav1.ObjectMeta{
		Name:      name,
		Namespace: "default",
		Labels:    labels,
		// Penalty annotations the unschedulable-pod-controller reads
		// per pkg/controller/cr (M16 — bigfleet.lucy.sh/{interruption,
		// reclamation}-penalty). Using the archetype values directly
		// keeps the harness's CR shape identical between Mode=cr and
		// Mode=pods.
		Annotations: map[string]string{
			"bigfleet.lucy.sh/interruption-penalty": formatDollars(a.InterruptionPenalty),
			"bigfleet.lucy.sh/reclamation-penalty":  formatDollars(a.ReclamationPenalty),
		},
	}
	affinity := &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: terms,
				}},
			},
		},
	}
	if a.SameRack {
		// ADR-0024: co-location is a real podAffinity term. The Pod
		// carries a group-unique label and requires co-scheduling with
		// peers carrying it on topology.bigfleet/rack. The UPC projects
		// this into CR.Spec.CoLocation; the operator aggregates equal
		// terms into one Need with a Same(rack) requirement. A fresh
		// group label every PickGroupSize pods keeps each training-job-
		// shaped group distinct.
		gid := d.allocateOrGetGroupUID(a)
		labels[labelCoLocationGroup] = gid
		affinity.PodAffinity = &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{labelCoLocationGroup: gid},
				},
				TopologyKey: topologyKeyRack,
			}},
		}
	}
	// the scale-test review: topology spread, probabilistically per the
	// archetype's SpreadConstraintProb. The selector matches the
	// archetype label so each Pod participates in the spread group for
	// its archetype. UPC's pod→CR translator carries this through to
	// CapacityRequest.Spec.TopologySpread; operator rollup folds it
	// into the Need's Profile.Spread.
	var topologySpread []corev1.TopologySpreadConstraint
	if sc := a.PickSpread(d.rng); sc != nil {
		topologySpread = []corev1.TopologySpreadConstraint{{
			TopologyKey:       sc.TopologyKey,
			MaxSkew:           sc.MaxSkew,
			WhenUnsatisfiable: corev1.UnsatisfiableConstraintAction(sc.WhenUnsatisfiable),
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"scaletest.bigfleet/archetype": a.Name},
			},
		}}
	}
	pod := &corev1.Pod{
		ObjectMeta: meta,
		Spec: corev1.PodSpec{
			Priority: &pri,
			Containers: []corev1.Container{{
				Name:  "workload",
				Image: "registry.k8s.io/pause:3.10",
				Resources: corev1.ResourceRequirements{
					// Extended resources (nvidia.com/gpu and any
					// custom resource we model) are non-overcommitable
					// in Kubernetes and require Limits to equal
					// Requests. Mirror it for every resource so the
					// apiserver doesn't reject the Pod.
					Requests: resources,
					Limits:   resources,
				},
			}},
			Affinity:                  affinity,
			TopologySpreadConstraints: topologySpread,
		},
	}
	return pod
}

func (d *driver) buildLegacyPod(name string) *corev1.Pod {
	pri := int32(1000)
	if len(d.prof.PriorityClasses) > 0 {
		pri = d.prof.PriorityClasses[d.rng.Intn(len(d.prof.PriorityClasses))]
	}
	resources := corev1.ResourceList{
		"nvidia.com/gpu": resource.MustParse("8"),
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Annotations: map[string]string{
				"bigfleet.lucy.sh/interruption-penalty": "8192",
				"bigfleet.lucy.sh/reclamation-penalty":  "65536",
			},
		},
		Spec: corev1.PodSpec{
			Priority: &pri,
			Containers: []corev1.Container{{
				Name:  "workload",
				Image: "registry.k8s.io/pause:3.10",
				Resources: corev1.ResourceRequirements{
					Requests: resources,
					Limits:   resources,
				},
			}},
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: []corev1.NodeSelectorRequirement{{
								Key:      "node.kubernetes.io/instance-type",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"a3-highgpu-8g"},
							}},
						}},
					},
				},
			},
		},
	}
}

// buildCRWithArchetype is buildCR plus the archetype name (or "" for
// the legacy single-shape path) so per-CR aging knows which archetype
// to attribute the CR to.
func (d *driver) buildCRWithArchetype(name string) (*bfv1alpha1.CapacityRequest, string) {
	if a := d.picker.Pick(d.rng); a != nil {
		return d.buildArchetypeCR(name, a), a.Name
	}
	return d.buildCR(name), ""
}

// buildCR constructs the CapacityRequest spec for a single CR. When
// archetypes are configured the picker chooses one weighted-random;
// otherwise the legacy single-shape (a3-highgpu-8g GPU) is emitted
// for compatibility with pre-M31 profiles.
func (d *driver) buildCR(name string) *bfv1alpha1.CapacityRequest {
	if a := d.picker.Pick(d.rng); a != nil {
		return d.buildArchetypeCR(name, a)
	}
	pri := int32(1000)
	if len(d.prof.PriorityClasses) > 0 {
		pri = d.prof.PriorityClasses[d.rng.Intn(len(d.prof.PriorityClasses))]
	}
	intr := resource.MustParse("8192")
	recl := resource.MustParse("65536")
	return &bfv1alpha1.CapacityRequest{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: bfv1alpha1.CapacityRequestSpec{
			Requirements: []corev1.NodeSelectorRequirement{{
				Key:      "node.kubernetes.io/instance-type",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"a3-highgpu-8g"},
			}},
			Resources: corev1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("8"),
			},
			Priority:            pri,
			InterruptionPenalty: &intr,
			ReclamationPenalty:  &recl,
		},
	}
}

func (d *driver) buildArchetypeCR(name string, a *archetype.Archetype) *bfv1alpha1.CapacityRequest {
	reqs := []corev1.NodeSelectorRequirement{}
	if len(a.InstanceTypes) > 0 {
		reqs = append(reqs, corev1.NodeSelectorRequirement{
			Key:      "node.kubernetes.io/instance-type",
			Operator: corev1.NodeSelectorOpIn,
			Values:   append([]string(nil), a.InstanceTypes...),
		})
	}
	if len(a.Zones) > 0 {
		reqs = append(reqs, corev1.NodeSelectorRequirement{
			Key:      "topology.kubernetes.io/zone",
			Operator: corev1.NodeSelectorOpIn,
			Values:   append([]string(nil), a.Zones...),
		})
	}
	// ADR-0015 §1: per-CR resources picked weighted-random from
	// SizeBuckets when present; falls back to the flat Resources
	// map otherwise.
	resources := corev1.ResourceList{}
	for k, v := range a.PickSize(d.rng) {
		resources[corev1.ResourceName(k)] = resource.MustParse(v)
	}
	// M35 / Item 2: per-axis label requirements. PickLabels draws a
	// random value per axis (e.g. "team-7", "app-42") and we emit
	// `In [value]` for each. Multiplies per-CR fingerprint
	// cardinality into the production range.
	for k, v := range a.PickLabels(d.rng) {
		reqs = append(reqs, corev1.NodeSelectorRequirement{
			Key:      k,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{v},
		})
	}
	pri := int32(1000)
	if len(a.PriorityClasses) > 0 {
		pri = a.PriorityClasses[d.rng.Intn(len(a.PriorityClasses))]
	}
	intr := resource.MustParse(formatDollars(a.InterruptionPenalty))
	recl := resource.MustParse(formatDollars(a.ReclamationPenalty))
	meta := metav1.ObjectMeta{
		Name:      name,
		Namespace: "default",
		Labels:    map[string]string{"scaletest.bigfleet/archetype": a.Name},
	}
	var coLocation *bfv1alpha1.CoLocationTerm
	if a.SameRack {
		// ADR-0024: Mode=cr sets the structured CoLocation field
		// directly — the same shape the UPC produces from a Pod's
		// podAffinity in Mode=pods. A fresh group label every
		// PickGroupSize CRs keeps each training-job-shaped group
		// distinct; the operator aggregates equal CoLocation terms
		// into one Need with a Same(rack) requirement.
		gid := d.allocateOrGetGroupUID(a)
		meta.Labels[labelCoLocationGroup] = gid
		coLocation = &bfv1alpha1.CoLocationTerm{
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{labelCoLocationGroup: gid},
			},
			TopologyKey: topologyKeyRack,
		}
	}
	// the scale-test review: topology spread, probabilistically. Mirrors
	// the Pod-mode wiring above — sets the CR's TopologySpread field
	// directly (same shape UPC produces from Pod.Spec.TopologySpreadConstraints).
	var spread []bfv1alpha1.TopologySpreadConstraint
	if sc := a.PickSpread(d.rng); sc != nil {
		spread = []bfv1alpha1.TopologySpreadConstraint{{
			TopologyKey:       sc.TopologyKey,
			MaxSkew:           sc.MaxSkew,
			WhenUnsatisfiable: corev1.UnsatisfiableConstraintAction(sc.WhenUnsatisfiable),
		}}
	}
	return &bfv1alpha1.CapacityRequest{
		ObjectMeta: meta,
		Spec: bfv1alpha1.CapacityRequestSpec{
			Requirements:        reqs,
			Resources:           resources,
			Priority:            pri,
			CoLocation:          coLocation,
			TopologySpread:      spread,
			InterruptionPenalty: &intr,
			ReclamationPenalty:  &recl,
		},
	}
}

// allocateOrGetGroupUID returns a stable per-group UID for the
// archetype. The driver tracks how many CRs of this archetype have
// been emitted into the current group; once the group is full
// (per archetype.PickGroupSize) a fresh UID is generated. M37 / ADR-
// 0015 §4: this UID is the operator's co-location signal — every
// CR sharing it gets a Same constraint after rollup.
func (d *driver) allocateOrGetGroupUID(a *archetype.Archetype) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.groups == nil {
		d.groups = map[string]*driverGroup{}
	}
	g, ok := d.groups[a.Name]
	if !ok || g.remaining <= 0 {
		g = &driverGroup{
			uid:       fmt.Sprintf("%s-%s-grp-%d", d.clusterID, a.Name, d.seq),
			remaining: a.PickGroupSize(d.rng),
		}
		d.groups[a.Name] = g
	}
	g.remaining--
	return g.uid
}

// formatDollars renders a float-dollar penalty as a fixed-point
// resource.Quantity-parseable string. resource.MustParse rejects "0"
// in some contexts but accepts integer strings like "8192", so we
// emit integers when the value is integral.
func formatDollars(v float64) string {
	if v == float64(int64(v)) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%f", v)
}

func (d *driver) deleteOne(ctx context.Context, name string) error {
	var obj client.Object
	if d.prof.Mode == "pods" {
		obj = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	} else {
		obj = &bfv1alpha1.CapacityRequest{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	}
	if err := d.k.Delete(ctx, obj); err != nil && !errIsNotFound(err) {
		return err
	}
	d.mu.Lock()
	delete(d.known, name)
	d.mu.Unlock()
	deleted.Inc()
	active.Set(float64(d.activeCount()))
	return nil
}

func (d *driver) popRandom() (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.known) == 0 {
		return "", false
	}
	idx := d.rng.Intn(len(d.known))
	i := 0
	for name := range d.known {
		if i == idx {
			return name, true
		}
		i++
	}
	return "", false
}

// popRandomFromArchetype picks a random known CR whose archetype
// matches archName. Returns ("", false) if none exists. Used by the
// lifetime-aging path so deletes are attributed to the correct
// archetype's churn rate.
func (d *driver) popRandomFromArchetype(archName string) (string, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	matches := make([]string, 0, len(d.known))
	for name, meta := range d.known {
		if meta.archetype == archName {
			matches = append(matches, name)
		}
	}
	if len(matches) == 0 {
		return "", false
	}
	return matches[d.rng.Intn(len(matches))], true
}

// activeCountByArchetype returns a map of archetype-name to count of
// active CRs. Used by the lifetime-aging tick.
func (d *driver) activeCountByArchetype() map[string]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := map[string]int{}
	for _, meta := range d.known {
		out[meta.archetype]++
	}
	return out
}

func (d *driver) activeCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.known)
}

func loadProfile(path string) (profile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return profile{}, err
	}
	var p profile
	if err := yaml.Unmarshal(b, &p); err != nil {
		return profile{}, err
	}
	if p.Target <= 0 {
		return profile{}, errors.New("profile.target must be > 0")
	}
	// M44: Pod-mode is the default. Empty Mode normalises to "pods"
	// so every profile exercises the user-facing Pod → bound chain
	// unless it explicitly opts into the legacy "cr" shape.
	if p.Mode == "" {
		p.Mode = "pods"
	}
	return p, nil
}

func newKubeClient(explicit string) (client.Client, *kubernetes.Clientset, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if explicit != "" {
		rules.ExplicitPath = explicit
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, nil, err
	}
	cfg.QPS = 200
	cfg.Burst = 400

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))
	k, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return nil, nil, err
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}
	return k, cs, nil
}

// anchorSameRackGroups is the harness's gang-scheduler stand-in
// (ADR-0025). A sameRack pod carries a self-referential required
// podAffinity, which real kube-scheduler cannot bootstrap from an
// empty cluster — the first pod of a group has no running peer to
// co-locate with (a documented Kubernetes limitation; production
// fleets delegate gang placement to Volcano / Kueue / coscheduling).
// This loop force-binds one pod of each anchorless group onto a
// fresh, rack-labelled, resource-fitting fake-Node; kwok marks it
// Running, and real kube-scheduler then places the rest of the group
// onto the same rack via podAffinity.
//
// BigFleet is untouched — it provisions capacity for the aggregated
// Same Need regardless; this only moves the user-facing bind metric.
func (d *driver) anchorSameRackGroups(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.reconcileAnchors(ctx); err != nil {
				d.log.Warn("anchor reconcile failed", "err", err)
			}
		}
	}
}

// reconcileAnchors force-binds one anchor per anchorless sameRack
// group. Idempotent and stateless: groups that already have a bound
// pod are skipped, and nodes already hosting a co-location-group pod
// are not reused. Scoped to co-location-labelled pods via a label
// selector — at the largest profiles this list is worth revisiting
// with an informer (ADR-0025 consequences).
func (d *driver) reconcileAnchors(ctx context.Context) error {
	var pods corev1.PodList
	if err := d.k.List(ctx, &pods,
		client.InNamespace("default"),
		client.HasLabels{labelCoLocationGroup},
	); err != nil {
		return fmt.Errorf("list co-location pods: %w", err)
	}
	type groupState struct {
		anchored bool
		pending  []*corev1.Pod
	}
	groups := map[string]*groupState{}
	claimed := map[string]bool{} // nodes already hosting a co-location group
	for i := range pods.Items {
		p := &pods.Items[i]
		gid := p.Labels[labelCoLocationGroup]
		gs := groups[gid]
		if gs == nil {
			gs = &groupState{}
			groups[gid] = gs
		}
		if p.Spec.NodeName != "" {
			gs.anchored = true
			claimed[p.Spec.NodeName] = true
			continue
		}
		gs.pending = append(gs.pending, p)
	}
	var needy []*groupState
	for _, gs := range groups {
		if !gs.anchored && len(gs.pending) > 0 {
			needy = append(needy, gs)
		}
	}
	if len(needy) == 0 {
		return nil
	}
	var nodes corev1.NodeList
	if err := d.k.List(ctx, &nodes); err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	for _, gs := range needy {
		anchor := gs.pending[0]
		for i := range nodes.Items {
			n := &nodes.Items[i]
			// The node must carry the rack topology key (else the
			// rest of the group's podAffinity has no domain to attach
			// to), be unclaimed, and fit the anchor's resources.
			if claimed[n.Name] || n.Labels[topologyKeyRack] == "" || !nodeFitsPod(n, anchor) {
				continue
			}
			if err := d.bindPod(ctx, anchor, n.Name); err != nil {
				d.log.Warn("anchor bind failed", "pod", anchor.Name, "node", n.Name, "err", err)
				break // retry this group next tick
			}
			anchorsBound.Inc()
			claimed[n.Name] = true
			break
		}
	}
	return nil
}

// bindPod force-binds pod onto nodeName via the Binding subresource,
// bypassing the scheduler (and its podAffinity predicate). Uses the
// typed clientset — controller-runtime's SubResource("binding") has
// known cache-layer issues (see test/scaletest/cmd/pod-shim).
func (d *driver) bindPod(ctx context.Context, pod *corev1.Pod, nodeName string) error {
	return d.cs.CoreV1().Pods(pod.Namespace).Bind(ctx, &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name},
		Target:     corev1.ObjectReference{Kind: "Node", Name: nodeName},
	}, metav1.CreateOptions{})
}

// nodeFitsPod is a coarse resource-fit check: every resource the pod
// requests must be ≤ the node's Allocatable. Sufficient for the
// harness — fake-Nodes are provisioned per-archetype, so a node that
// fits the anchor fits the identically-shaped rest of the group.
func nodeFitsPod(node *corev1.Node, pod *corev1.Pod) bool {
	req := corev1.ResourceList{}
	for _, c := range pod.Spec.Containers {
		for k, v := range c.Resources.Requests {
			cur := req[k]
			cur.Add(v)
			req[k] = cur
		}
	}
	for k, q := range req {
		alloc, ok := node.Status.Allocatable[k]
		if !ok || alloc.Cmp(q) < 0 {
			return false
		}
	}
	return true
}

// errIsNotFound checks for apiserver "not found" without pulling in the
// full apimachinery error helpers.
func errIsNotFound(err error) bool {
	type statusErr interface{ Status() metav1.Status }
	if s, ok := err.(statusErr); ok {
		return s.Status().Reason == metav1.StatusReasonNotFound
	}
	return false
}
