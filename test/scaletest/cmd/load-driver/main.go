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
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
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

	kc, err := newKubeClient(*kubeconfig)
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

	d := &driver{
		clusterID:  *clusterID,
		log:        logger,
		k:          kc,
		prof:       prof,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		known:      make(map[string]crMeta, prof.Target),
		picker:     archetype.NewPicker(prof.demandArchetypes()),
		archByName: indexArchetypes(prof.demandArchetypes()),
	}
	if d.picker != nil {
		logger.Info("archetypes loaded", "count", len(prof.Archetypes))
	}
	return d.run(ctx)
}

type driver struct {
	clusterID  string
	log        *slog.Logger
	k          client.Client
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
	// Phase 1: ramp to target.
	d.log.Info("ramping to target", "count", d.prof.Target)
	if err := d.rampTo(ctx, d.prof.Target); err != nil {
		return fmt.Errorf("ramp: %w", err)
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
	// to hit churnPerMinute averaged over each minute.
	perTick := int(float64(d.prof.Target) * d.prof.ChurnPerMinute / 60.0)
	if perTick < 1 && d.prof.ChurnPerMinute > 0 {
		perTick = 1
	}
	d.log.Info("steady state", "churn_per_tick", perTick)

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
			thisTick := perTick
			// M41: Poisson jitter on per-tick churn count, plus
			// optional minute-scale micro-bursts. Without jitter,
			// every tick produces identical write rate and the p99.9
			// tail is invisible.
			if d.prof.JitteredChurn && perTick > 0 {
				thisTick = poissonInt(d.rng, float64(perTick))
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
	switch {
	case got < target:
		for i := 0; i < target-got && i < 20; i++ {
			_ = d.createOne(ctx)
		}
	case got > target:
		extra := got - target
		for i := 0; i < extra && i < 20; i++ {
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
	}
	return nil
}

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
// archetypes) an OwnerReference to drive the operator's owner→Same
// translation downstream.
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
	if a.SameRack {
		gid := d.allocateOrGetGroupUID(a)
		meta.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "scaletest.bigfleet/v1alpha1",
			Kind:       "ScaletestWorkload",
			Name:       fmt.Sprintf("%s-group-%s", a.Name, gid),
			UID:        types.UID(gid),
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
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{
							MatchExpressions: terms,
						}},
					},
				},
			},
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
	// ADR-0015 §4 / M37: tightly-coupled workloads use the operator's
	// owner-grouping path to trigger Same translation during rollup.
	// The CRD doesn't carry the Same requirement directly; it carries
	// an OwnerReference, and the operator (configured with
	// CoLocationKey = "topology.bigfleet/rack") emits Same(rack) on
	// the rolled-up Need. See pkg/operator/rollup.go withSameRequirement.
	// (M33's earlier Exists-on-rack approach was wrong — Exists
	// does NOT trigger Same translation; only OwnerRef-grouped CRs do.)
	// The OwnerReferences are attached at the metadata level below;
	// no requirement is added to the spec.

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
	if a.SameRack {
		// ADR-0015 §4 / M37: synthetic OwnerReference shared across
		// the group triggers the operator's owner→Same translation.
		// We allocate a new group UID every PickGroupSize CRs of this
		// archetype on this driver so individual training-job-shaped
		// groups stay distinct (each gets its own Same constraint).
		gid := d.allocateOrGetGroupUID(a)
		meta.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "scaletest.bigfleet/v1alpha1",
			Kind:       "ScaletestWorkload",
			Name:       fmt.Sprintf("%s-group-%s", a.Name, gid),
			UID:        types.UID(gid),
		}}
	}
	return &bfv1alpha1.CapacityRequest{
		ObjectMeta: meta,
		Spec: bfv1alpha1.CapacityRequestSpec{
			Requirements:        reqs,
			Resources:           resources,
			Priority:            pri,
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

func newKubeClient(explicit string) (client.Client, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if explicit != "" {
		rules.ExplicitPath = explicit
	}
	cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	cfg, err := cc.ClientConfig()
	if err != nil {
		return nil, err
	}
	cfg.QPS = 200
	cfg.Burst = 400

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))
	return client.New(cfg, client.Options{Scheme: scheme})
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
