// Command load-driver emits realistic, controller-managed workload churn
// against a KWOK-backed apiserver inside a scaletest pod.
//
// One load-driver per simulated cluster. It creates real
// controller-managed workload objects (Deployment / StatefulSet) until
// the per-cluster Pod target is met, then churns at a configured rate:
// every churn tick a fraction of Pods are deleted — and their owning
// controller recreates them. Deletion → recreation models pod completion
// + resubmission, which is the dominant Pod lifecycle in production.
//
// ADR-0038: workloads are controller-managed objects, not bare Pods. A
// bare Pod, once evicted by a Phase 3 drain, is gone forever — that made
// every reclaim permanently destroy demand and produced an unbounded
// supply-thrash cascade. Deployment/StatefulSet controllers recreate
// evicted Pods, so demand is conserved and Phase 3 self-arrests at the
// true surplus.
//
// Profiles are YAML, mounted from a ConfigMap by the harness chart:
//
//	target: 1000              # steady-state Pod count
//	churnPerMinute: 0.05      # 5% of Pods replaced per minute
//	burstAtStart: 0           # extra Pods created at t=0 then drained
//	priorityClasses: [100, 1000, 1000000]   # round-robin per workload
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
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/yaml.v3"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
)

type profile struct {
	Target          int     `yaml:"target"`
	ChurnPerMinute  float64 `yaml:"churnPerMinute"`
	BurstAtStart    int     `yaml:"burstAtStart"`
	PriorityClasses []int32 `yaml:"priorityClasses"`
	DurationSeconds int     `yaml:"durationSeconds"`

	// Archetypes: a list of workload templates, weighted-picked on every
	// workload-object creation. When non-empty, the GPU-only single-shape
	// fallback below is bypassed and workloads are emitted from the
	// chosen archetype. Both the load-driver and the shard's Configured
	// seed read this list (M31). When empty, behaviour is identical to
	// pre-M31: instance-type=a3-highgpu-8g, nvidia.com/gpu=8, priority
	// from PriorityClasses (or 1000), penalties 8192/65536. See
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

	// ReconcilePerTickCap is parsed-but-unused (ADR-0038): the
	// per-tick reconcile loop it bounded was a hand-rolled
	// pseudo-ReplicaSet, now replaced by real Deployment/StatefulSet
	// controllers. The field is kept so mounted ConfigMaps that still
	// set it don't fail unmarshal; remove it once no profile carries
	// it.
	ReconcilePerTickCap int `yaml:"reconcilePerTickCap"`

	// PreBind (M52.B, ADR-0035): when true, the load-driver binds the
	// initial Pod fill to fake-Nodes via the Bind API after rampTo,
	// bypassing kube-scheduler's slow filter/score cycle. The Pods
	// still go through the realistic Unschedulable → UPC →
	// CapacityRequest path first (the workload controllers create them
	// without Spec.NodeName); only the binding is fast-pathed. Soak
	// churn Pods are never pre-bound — they go through kube-scheduler,
	// the path the steady-state SLO measures.
	//
	// Default false = scheduler-bound ramp. ADR-0035's steady-state
	// methodology sets this true so the install reaches steady state
	// without paying the kube-scheduler bulk-bind ramp.
	PreBind bool `yaml:"preBind"`
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

// statefulArchetypes is the hardcoded set of archetype names whose
// workloads need stable identity / ordered semantics — these become
// StatefulSets; everything else becomes a Deployment. ADR-0038: the
// classification is intentionally a small in-code set, not a profile
// knob (YAGNI).
var statefulArchetypes = map[string]bool{
	"stateful-db":  true,
	"memory-cache": true,
}

// isStateful reports whether an archetype's workload should be modelled
// as a StatefulSet rather than a Deployment. ADR-0038.
func isStateful(archName string) bool {
	return statefulArchetypes[archName]
}

// replicaBucket is one band of the hardcoded service-size distribution.
// A workload object's replica count is a uniform draw within the
// weighted-picked bucket's [lo, hi] range.
type replicaBucket struct {
	weight int
	lo, hi int
}

// replicaDistribution is the hardcoded heavy-tailed service-size
// distribution: most services are small, a few are large. ADR-0038
// fixes this in code on purpose — it is a modelling decision, not a
// per-profile knob (YAGNI).
var replicaDistribution = []replicaBucket{
	{weight: 55, lo: 1, hi: 5},
	{weight: 30, lo: 6, hi: 25},
	{weight: 12, lo: 26, hi: 100},
	{weight: 3, lo: 101, hi: 400},
}

// statefulReplicaCap clamps StatefulSet replica draws. StatefulSets
// create Pods ordinally/serially, so a large one bottlenecks the ramp;
// stateful workloads are kept small.
const statefulReplicaCap = 25

// drawReplicas returns the replica count for one workload object.
// sameRack archetypes draw from the archetype's GroupSizeRange
// (ADR-0040 §3: one workload object is one co-location gang, and the
// heavy-tailed service-size distribution produced gangs of up to ~400
// whole machines — unsatisfiable in any topology the harness runs);
// everything else draws from the service-size distribution via
// pickReplicas. remaining > 0 caps the draw so the ramp lands on
// target — a truncated final sameRack group is acceptable, every Need
// is partial-fill-tolerant in v1 (ADR-0040 §2). Always ≥ 1.
func drawReplicas(rng *rand.Rand, a *archetype.Archetype, stateful bool, remaining int) int {
	var n int
	if a != nil && a.SameRack {
		n = a.PickGroupSize(rng)
	} else {
		n = pickReplicas(rng, stateful)
	}
	if remaining > 0 && n > remaining {
		n = remaining
	}
	if n < 1 {
		n = 1
	}
	return n
}

// pickReplicas draws a workload object's replica count from the
// hardcoded service-size distribution. Stateful workloads are clamped to
// statefulReplicaCap.
func pickReplicas(rng *rand.Rand, stateful bool) int {
	full := 0
	for _, b := range replicaDistribution {
		full += b.weight
	}
	r := rng.Intn(full)
	cum := 0
	var chosen replicaBucket
	for _, b := range replicaDistribution {
		cum += b.weight
		if r < cum {
			chosen = b
			break
		}
	}
	n := chosen.lo
	if chosen.hi > chosen.lo {
		n = chosen.lo + rng.Intn(chosen.hi-chosen.lo+1)
	}
	if stateful && n > statefulReplicaCap {
		n = statefulReplicaCap
	}
	return n
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
	clusterID := fs.String("cluster-id", "", "stable cluster ID; used as a workload-object name prefix")
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
		workloads:       make(map[string]workloadMeta),
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

	mu  sync.Mutex
	seq uint64
	// workloads tracks every controller-managed workload object this
	// driver has created. Keyed by object name. activeCount() returns
	// Σreplicas across this map — the Pod target the gauges report.
	workloads map[string]workloadMeta
	// steadyMarked flips true the first time the cluster reaches its
	// target Pod count. Pod templates built after that point carry the
	// scaletest.bigfleet/state="steady" label so the binder records
	// their latency in the steady-state histogram. The initial fill
	// produces a thundering-herd binding pattern that's not
	// representative of production; isolating post-fill churn in its
	// own metric keeps the SLO honest.
	steadyMarked bool

	// demandReadyFile, when non-empty, is touched once the initial
	// Pod fill (rampTo) completes — i.e. once the cluster's demand is
	// established as CapacityRequests via UPC. The harness entrypoint
	// blocks the BigFleet operator's startup on this file so the
	// operator's first rollup reflects real demand (ADR-0036): a
	// production operator joins a cluster that already has workloads,
	// and the harness must mirror that ordering.
	demandReadyFile string
}

// workloadKind distinguishes the two controller-managed object kinds the
// load-driver creates.
type workloadKind int

const (
	kindDeployment workloadKind = iota
	kindStatefulSet
)

// workloadMeta records per-workload-object bookkeeping. ADR-0038
// replaces the per-Pod `known`/`crMeta` map with one entry per workload
// object; the Pod population is owned by the object's controller.
type workloadMeta struct {
	kind      workloadKind
	archetype string
	replicas  int
	// burst marks an object created by a burst event, so drain can
	// delete exactly the burst objects again.
	burst bool
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

	// Phase 1: ramp to target — create the workload objects. Their
	// controllers create Pods WITHOUT Spec.NodeName, so each Pod goes
	// Unschedulable → UPC → CapacityRequest. This establishes the
	// cluster's demand before the BigFleet operator starts: the harness
	// entrypoint blocks the operator on the demand-ready file written
	// below, so the operator's first rollup reflects real demand and
	// ADR-0036's Phase 3 gate releases on a genuine (non-empty) rollup.
	d.log.Info("ramping to target", "count", d.prof.Target)
	if err := d.rampTo(ctx, d.prof.Target, false); err != nil {
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
		if err := d.rampTo(ctx, d.prof.Target+d.prof.BurstAtStart, true); err != nil {
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

	// Churn tick fires once per second. Delete a fraction of Pods sized
	// to hit churnPerMinute averaged over each minute; their owning
	// controllers recreate them. Drop X: keep perTick as a float so
	// churnPerMinute below 0.06 still represents faithfully. The
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
			// ADR-0015 §3: ramp / drain bursts. extraOn flips while
			// the burst is active; the burst's extra workload objects
			// are created on the rising edge and deleted on the
			// falling edge.
			for _, pb := range bursts {
				if !pb.extraOn && !now.Before(pb.fireAt) && now.Before(pb.drainAt) {
					pb.extraOn = true
					d.log.Info("burst firing", "archetype", pb.spec.Archetype, "extra", pb.spec.ExtraTarget)
					if err := d.rampExtra(ctx, pb.spec.ExtraTarget); err != nil {
						d.log.Warn("burst ramp failed", "err", err)
					}
				}
				if pb.extraOn && !now.Before(pb.drainAt) {
					pb.extraOn = false
					d.log.Info("burst drained", "archetype", pb.spec.Archetype)
					d.drainBurst(ctx)
				}
			}
		}
	}
}

// ageByLifetime deletes a Poisson-rate fraction of Pods for each
// finite-lifetime archetype; the Pods' owning controllers recreate them
// — modelling a fresh batch run. Run once per tick (1s). ADR-0015 §2.
func (d *driver) ageByLifetime(ctx context.Context) {
	if d.archByName == nil {
		return
	}
	counts := d.replicasByArchetype()
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
		if nReplace > 0 {
			// Delete-only: the ReplicaSet / StatefulSet controller
			// recreates each deleted Pod, which is the fresh batch run.
			d.deletePods(ctx, nReplace, name)
		}
	}
}

// rampTo creates workload objects until the sum of their replica counts
// reaches `want`. ADR-0038: demand is established by controller-managed
// objects, not bare Pods — once an object exists its controller owns the
// Pod population. burst marks the created objects so a later drain can
// remove exactly them.
func (d *driver) rampTo(ctx context.Context, want int, burst bool) error {
	for d.activeCount() < want {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		remaining := want - d.activeCount()
		if err := d.createWorkload(ctx, burst, remaining); err != nil {
			errs.WithLabelValues("create").Inc()
			d.log.Warn("create failed", "err", err)
			time.Sleep(50 * time.Millisecond)
		}
	}
	if burst {
		return nil
	}
	d.markSteadyState()
	return nil
}

// rampExtra creates burst workload objects whose replica counts sum to
// at least `extra`. They are tagged burst=true so drainBurst removes
// exactly them.
func (d *driver) rampExtra(ctx context.Context, extra int) error {
	added := 0
	for added < extra {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		before := d.activeCount()
		if err := d.createWorkload(ctx, true, extra-added); err != nil {
			errs.WithLabelValues("create").Inc()
			d.log.Warn("burst create failed", "err", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		added += d.activeCount() - before
	}
	return nil
}

// drainBurst deletes every workload object created by a burst. The
// objects' controllers tear down their Pods, returning demand to the
// pre-burst target.
func (d *driver) drainBurst(ctx context.Context) {
	d.mu.Lock()
	var names []string
	for name, m := range d.workloads {
		if m.burst {
			names = append(names, name)
		}
	}
	d.mu.Unlock()
	for _, name := range names {
		if err := d.deleteWorkload(ctx, name); err != nil {
			errs.WithLabelValues("delete").Inc()
		}
	}
}

// markSteadyState flips the steady-state metric the first time the
// cluster reaches its target Pod count.
func (d *driver) markSteadyState() {
	d.mu.Lock()
	already := d.steadyMarked
	d.steadyMarked = true
	d.mu.Unlock()
	if already {
		return
	}
	steadyStateMetric.Set(1)
	phaseStartedAt.WithLabelValues("steady").Set(float64(time.Now().Unix()))
}

// churn deletes n random Pods (label-selected by archetype); each Pod's
// owning Deployment/StatefulSet controller recreates it. ADR-0038: churn
// is "delete a Pod, its controller recreates it" — not delete+recreate
// of a bare object.
func (d *driver) churn(ctx context.Context, n int) {
	d.deletePods(ctx, n, "")
}

// createWorkload builds and creates one controller-managed workload
// object (Deployment or StatefulSet, by archetype classification),
// records it in the workloads map, and updates gauges. `remaining`, when
// positive, caps the replica count so the ramp doesn't badly overshoot
// the target on its final object; 0 means no cap.
func (d *driver) createWorkload(ctx context.Context, burst bool, remaining int) error {
	d.mu.Lock()
	d.seq++
	seq := d.seq
	d.mu.Unlock()

	a := d.picker.Pick(d.rng)
	archName := ""
	if a != nil {
		archName = a.Name
	}
	stateful := isStateful(archName)
	replicas := drawReplicas(d.rng, a, stateful, remaining)

	name := fmt.Sprintf("%s-%s-%d", d.clusterID, archetypeNameOrLegacy(archName), seq)
	tmpl := d.buildPodTemplate(a)

	var obj client.Object
	kind := kindDeployment
	if stateful {
		kind = kindStatefulSet
		obj = buildStatefulSet(name, int32(replicas), tmpl)
	} else {
		obj = buildDeployment(name, int32(replicas), tmpl)
	}
	if err := d.k.Create(ctx, obj); err != nil {
		return err
	}
	d.mu.Lock()
	d.workloads[name] = workloadMeta{kind: kind, archetype: archName, replicas: replicas, burst: burst}
	d.mu.Unlock()
	created.Inc()
	active.Set(float64(d.activeCount()))
	return nil
}

// archetypeNameOrLegacy returns the archetype name for object naming,
// substituting "legacy" when no archetype catalog is configured.
func archetypeNameOrLegacy(archName string) string {
	if archName == "" {
		return "legacy"
	}
	return archName
}

const (
	// labelWorkload is the per-object unique label that ties a
	// Deployment/StatefulSet to exactly its own Pods. It is the
	// controller's Selector and is also on the Pod template.
	labelWorkload = "scaletest.bigfleet/workload"
	// labelArchetype is set on every seeded Configured machine (and so
	// on every kwok fake-Node derived from one), and on every Pod
	// template the load-driver builds. churn/aging label-select Pods by
	// it; the pre-bind phase uses it to recognise BigFleet-managed
	// fake-Nodes.
	labelArchetype = "scaletest.bigfleet/archetype"
)

// buildDeployment wraps a pod template in a Deployment. ADR-0038:
// stateless archetypes are controller-managed via Deployment so an
// evicted Pod is recreated by its ReplicaSet.
func buildDeployment(name string, replicas int32, tmpl corev1.PodTemplateSpec) *appsv1.Deployment {
	tmpl.Labels[labelWorkload] = name
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{labelWorkload: name},
			},
			Template: tmpl,
		},
	}
}

// buildStatefulSet wraps a pod template in a StatefulSet. ADR-0038:
// stateful archetypes use StatefulSet for stable-identity / ordered
// semantics only — no volumeClaimTemplates, the harness does not model
// storage.
func buildStatefulSet(name string, replicas int32, tmpl corev1.PodTemplateSpec) *appsv1.StatefulSet {
	tmpl.Labels[labelWorkload] = name
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: name,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{labelWorkload: name},
			},
			Template: tmpl,
		},
	}
}

const (
	// labelCoLocationGroup tags every Pod of one sameRack group with a
	// shared, group-unique value. The Pod's podAffinity selects it; the
	// operator aggregates CRs carrying an equal CoLocation term into one
	// Need. ADR-0024.
	labelCoLocationGroup = "scaletest.bigfleet/co-location-group"
	// topologyKeyRack is the node-label key sameRack archetypes
	// co-locate on — the TopologyKey of their podAffinity term, which
	// the operator turns into a Same(rack) requirement.
	topologyKeyRack = "topology.bigfleet/rack"
)

// buildPodTemplate constructs the pod template for one workload object.
// ADR-0038: a workload object's replicas all share ONE shape — one
// PickSize, one PickLabels, one priority, one spread draw, one
// co-location group label. Both buildDeployment and buildStatefulSet
// embed this. The template carries every field the unschedulable-pod
// controller reads (labels, penalty annotations, Affinity,
// TopologySpreadConstraints) so the Unschedulable → UPC →
// CapacityRequest chain is unchanged.
func (d *driver) buildPodTemplate(a *archetype.Archetype) corev1.PodTemplateSpec {
	if a == nil {
		return d.buildLegacyPodTemplate()
	}
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
	labels := map[string]string{labelArchetype: a.Name}
	if d.steadyState() {
		// M44.4: marker for the binder to record steady-state binding
		// latency in a separate histogram. Pods created during the
		// initial fill (cluster ramping from 0 → target) are excluded
		// — that thundering-herd binding pattern isn't representative
		// of production and shouldn't dominate the SLO.
		labels["scaletest.bigfleet/state"] = "steady"
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
		// ADR-0024: co-location is a real podAffinity term. Every Pod
		// of this workload object carries one group-unique label and
		// requires co-scheduling with peers carrying it on
		// topology.bigfleet/rack. The UPC projects this into
		// CR.Spec.CoLocation; the operator aggregates equal terms into
		// one Need with a Same(rack) requirement. ADR-0038: one
		// workload object IS one co-location group — every replica
		// shares the group label — so the group size is the object's
		// replica count, drawn from the archetype's GroupSizeRange in
		// createWorkload (ADR-0040 §3).
		gid := fmt.Sprintf("%s-%s-grp-%d", d.clusterID, a.Name, d.nextSeq())
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
	// scale-test review: topology spread, probabilistically per the
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
				MatchLabels: map[string]string{labelArchetype: a.Name},
			},
		}}
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
			// Penalty annotations the unschedulable-pod-controller reads
			// per pkg/controller/cr (M16 — bigfleet.lucy.sh/{interruption,
			// reclamation}-penalty).
			Annotations: map[string]string{
				"bigfleet.lucy.sh/interruption-penalty": formatDollars(a.InterruptionPenalty),
				"bigfleet.lucy.sh/reclamation-penalty":  formatDollars(a.ReclamationPenalty),
			},
		},
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
}

// buildLegacyPodTemplate is the pre-M31 single-shape fallback used when
// no archetype catalog is configured: a3-highgpu-8g GPU Pods.
func (d *driver) buildLegacyPodTemplate() corev1.PodTemplateSpec {
	pri := int32(1000)
	if len(d.prof.PriorityClasses) > 0 {
		pri = d.prof.PriorityClasses[d.rng.Intn(len(d.prof.PriorityClasses))]
	}
	resources := corev1.ResourceList{
		"nvidia.com/gpu": resource.MustParse("8"),
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{},
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

// nextSeq returns a fresh monotonic sequence number under the lock.
func (d *driver) nextSeq() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seq++
	return d.seq
}

// preBindConcurrency caps in-flight Bind calls per sweep. A single
// goroutine was the M52.B initial-fill bottleneck; the apiserver
// handles this fan-out comfortably.
const preBindConcurrency = 64

// preBindInitialPods binds the initial Pod fill to fake-Nodes via the
// Bind API, bypassing kube-scheduler's slow filter/score cycle. M52.B
// (ADR-0035). The Pods were created by the workload controllers
// *without* Spec.NodeName — so they went Unschedulable → UPC →
// CapacityRequest, keeping the demand path realistic — and this step
// just binds them fast once supply (fake-Nodes) materialises.
//
// ADR-0038 guard: workload controllers create Pods asynchronously,
// slightly after the workload objects exist, so the loop does not
// declare done on "0 unbound" until the live Pod count has reached the
// target — otherwise it could exit before the controllers have created
// every Pod.
func (d *driver) preBindInitialPods(ctx context.Context) {
	d.log.Info("pre-bind: binding initial Pods to fake-Nodes as they appear")
	// Safety valve only — the loop exits as soon as every initial Pod is
	// bound. 45 min covers a 500K-Pod fleet's fill at the observed
	// pre-bind rate (bigfleet-uber #43).
	deadline := time.Now().Add(45 * time.Minute)
	want := d.activeCount()
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
		var allPods corev1.PodList
		if err := d.k.List(ctx, &allPods, client.InNamespace("default")); err != nil {
			d.log.Warn("pre-bind: list pods", "err", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if len(unbound.Items) == 0 {
			// ADR-0038: don't declare done until the workload
			// controllers have actually created every Pod. A
			// momentary "0 unbound" early in the ramp just means the
			// controllers haven't created the Pods yet.
			if len(allPods.Items) >= want {
				d.log.Info("pre-bind: all initial Pods bound", "pods", len(allPods.Items))
				return
			}
			d.log.Info("pre-bind: waiting for controllers to create Pods", "live", len(allPods.Items), "want", want)
			time.Sleep(2 * time.Second)
			continue
		}
		var nodes corev1.NodeList
		if err := d.k.List(ctx, &nodes); err != nil {
			d.log.Warn("pre-bind: list nodes", "err", err)
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
		// ADR-0040: a group with a member already bound (the ADR-0025
		// anchor, or a previous sweep) is pinned to that member's rack —
		// planning the remainder anywhere else would scatter the group.
		nodeRack := make(map[string]string, len(nodes.Items))
		for j := range nodes.Items {
			if rack, ok := nodes.Items[j].Labels[topologyKeyRack]; ok {
				nodeRack[nodes.Items[j].Name] = rack
			}
		}
		pinnedRack := map[string]string{}
		for i := range allPods.Items {
			p := &allPods.Items[i]
			if p.Spec.NodeName == "" {
				continue
			}
			gid := p.Labels[labelCoLocationGroup]
			if gid == "" {
				continue
			}
			if rack, ok := nodeRack[p.Spec.NodeName]; ok {
				pinnedRack[gid] = rack
			}
		}
		// Plan assignments against the local remaining-capacity map, then
		// execute the Bind calls in parallel. Co-located groups are
		// planned whole-group onto a single rack (ADR-0040); see
		// planPreBind.
		unboundPtrs := make([]*corev1.Pod, 0, len(unbound.Items))
		for i := range unbound.Items {
			unboundPtrs = append(unboundPtrs, &unbound.Items[i])
		}
		plan := planPreBind(unboundPtrs, byArchetype, remaining, pinnedRack)
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

// assignment is one planned Pod→Node binding within a pre-bind sweep.
type assignment struct {
	pod  *corev1.Pod
	node string
}

// planPreBind is preBindInitialPods' planning step, factored pure for
// unit tests. Pods carrying the co-location-group label are planned as
// whole groups, rack-coherently (ADR-0040): the fast-path previously
// bound groups scattered across racks — a placement a real scheduler
// can never produce, since required podAffinity holds the group
// pending instead. Groups are planned before non-group Pods so the
// constrained placements aren't starved by greedy singles; non-group
// Pods keep the original first-fit walk. Mutates remaining in place.
func planPreBind(unbound []*corev1.Pod, byArchetype map[string][]*corev1.Node, remaining map[string]corev1.ResourceList, pinnedRack map[string]string) []assignment {
	plan := make([]assignment, 0, len(unbound))

	groups := map[string][]*corev1.Pod{}
	var groupIDs []string
	var singles []*corev1.Pod
	for _, pod := range unbound {
		gid := pod.Labels[labelCoLocationGroup]
		if gid == "" {
			singles = append(singles, pod)
			continue
		}
		if _, seen := groups[gid]; !seen {
			groupIDs = append(groupIDs, gid)
		}
		groups[gid] = append(groups[gid], pod)
	}
	sort.Strings(groupIDs)

	for _, gid := range groupIDs {
		plan = append(plan, planGroupOntoRack(groups[gid], byArchetype, remaining, pinnedRack[gid])...)
	}

	for _, pod := range singles {
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
	return plan
}

// planGroupOntoRack places one whole co-location group onto a single
// rack, or nowhere. Candidate racks are the topology.bigfleet/rack
// values among the group's archetype-matching nodes (just pinnedRack
// when a member is already bound there); each is tried in name order
// — deterministic across sweeps — by bin-packing the group against a
// scratch copy of the rack's remaining capacity. The first rack that
// fits every Pod wins and the scratch is committed. If none fits, the
// group stays pending for a later sweep / kube-scheduler / the
// ADR-0025 anchor — never scattered.
func planGroupOntoRack(group []*corev1.Pod, byArchetype map[string][]*corev1.Node, remaining map[string]corev1.ResourceList, pinnedRack string) []assignment {
	// One workload object is one co-location group, so every Pod shares
	// the first Pod's archetype (ADR-0038).
	arch := group[0].Labels[labelArchetype]
	racks := map[string][]*corev1.Node{}
	var rackNames []string
	for _, n := range byArchetype[arch] {
		rack, ok := n.Labels[topologyKeyRack]
		if !ok || (pinnedRack != "" && rack != pinnedRack) {
			continue
		}
		if _, seen := racks[rack]; !seen {
			rackNames = append(rackNames, rack)
		}
		racks[rack] = append(racks[rack], n)
	}
	sort.Strings(rackNames)

	for _, rack := range rackNames {
		nodes := racks[rack]
		scratch := make(map[string]corev1.ResourceList, len(nodes))
		for _, n := range nodes {
			scratch[n.Name] = copyResourceList(remaining[n.Name])
		}
		assignments := make([]assignment, 0, len(group))
		fits := true
		for _, pod := range group {
			req := podRequests(pod)
			placed := false
			for _, n := range nodes {
				if !fitsRequests(scratch[n.Name], req) {
					continue
				}
				subtractRequests(scratch[n.Name], req)
				assignments = append(assignments, assignment{pod: pod, node: n.Name})
				placed = true
				break
			}
			if !placed {
				fits = false
				break
			}
		}
		if !fits {
			continue
		}
		for name, rl := range scratch {
			remaining[name] = rl
		}
		return assignments
	}
	return nil
}

func copyResourceList(in corev1.ResourceList) corev1.ResourceList {
	out := make(corev1.ResourceList, len(in))
	for k, v := range in {
		out[k] = v.DeepCopy()
	}
	return out
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

// deleteWorkload deletes a tracked workload object (Deployment or
// StatefulSet). The controller tears down its Pods. Used by burst drain.
func (d *driver) deleteWorkload(ctx context.Context, name string) error {
	d.mu.Lock()
	m, ok := d.workloads[name]
	d.mu.Unlock()
	if !ok {
		return nil
	}
	var obj client.Object
	switch m.kind {
	case kindStatefulSet:
		obj = &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	default:
		obj = &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	}
	// Foreground deletion so the controller's Pods are GC'd with it.
	policy := metav1.DeletePropagationForeground
	if err := d.k.Delete(ctx, obj, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !errIsNotFound(err) {
		return err
	}
	d.mu.Lock()
	delete(d.workloads, name)
	d.mu.Unlock()
	deleted.Inc()
	active.Set(float64(d.activeCount()))
	return nil
}

// deletePods deletes up to n random Pods, restricted to archetype
// archName when non-empty. The Pods' owning Deployment/StatefulSet
// controllers recreate them — ADR-0038's churn / aging primitive.
// Pod selection is by label, not by an in-memory map: the load-driver
// tracks workload objects, not individual Pods.
func (d *driver) deletePods(ctx context.Context, n int, archName string) {
	if n <= 0 {
		return
	}
	opts := []client.ListOption{client.InNamespace("default")}
	if archName != "" {
		opts = append(opts, client.MatchingLabels{labelArchetype: archName})
	} else {
		// Only delete Pods the load-driver's workloads own — the
		// archetype label is on every workload Pod template.
		opts = append(opts, client.HasLabels{labelArchetype})
	}
	var pods corev1.PodList
	if err := d.k.List(ctx, &pods, opts...); err != nil {
		errs.WithLabelValues("delete").Inc()
		d.log.Warn("churn: list pods", "err", err)
		return
	}
	if len(pods.Items) == 0 {
		return
	}
	// Random sample without replacement via a partial Fisher-Yates
	// shuffle over the index space.
	idx := make([]int, len(pods.Items))
	for i := range idx {
		idx[i] = i
	}
	if n > len(idx) {
		n = len(idx)
	}
	for i := 0; i < n; i++ {
		j := i + d.rng.Intn(len(idx)-i)
		idx[i], idx[j] = idx[j], idx[i]
		p := &pods.Items[idx[i]]
		if err := d.k.Delete(ctx, p); err != nil && !errIsNotFound(err) {
			errs.WithLabelValues("delete").Inc()
			continue
		}
		deleted.Inc()
	}
}

// activeCount returns Σreplicas across every tracked workload object —
// the cluster's Pod target. ADR-0038: the gauge that meant "active Pod
// target" still means that, now computed as the sum of workload-object
// replica counts rather than a per-Pod map size.
func (d *driver) activeCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	sum := 0
	for _, m := range d.workloads {
		sum += m.replicas
	}
	return sum
}

// replicasByArchetype returns Σreplicas grouped by archetype name. Used
// by the lifetime-aging tick to size per-archetype Pod deletions.
func (d *driver) replicasByArchetype() map[string]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := map[string]int{}
	for _, m := range d.workloads {
		out[m.archetype] += m.replicas
	}
	return out
}

// steadyState reports whether the cluster has reached its target Pod
// count. Pods built after this point carry the
// scaletest.bigfleet/state="steady" label.
func (d *driver) steadyState() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.steadyMarked
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
	// ADR-0038: workloads are Deployments / StatefulSets; register the
	// apps/v1 types so the controller-runtime client can create them.
	// clientgoscheme already carries apps/v1, but register it
	// explicitly so the dependency is visible.
	utilruntime.Must(appsv1.AddToScheme(scheme))
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
