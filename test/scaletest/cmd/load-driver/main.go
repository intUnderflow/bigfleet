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
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
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
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
)

type profile struct {
	Target          int     `yaml:"target"`
	ChurnPerMinute  float64 `yaml:"churnPerMinute"`
	BurstAtStart    int     `yaml:"burstAtStart"`
	PriorityClasses []int32 `yaml:"priorityClasses"`
	DurationSeconds int     `yaml:"durationSeconds"`
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
	errs = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "scaletest_loadgen_errors_total",
	}, []string{"kind"})
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
		clusterID: *clusterID,
		log:       logger,
		k:         kc,
		prof:      prof,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		known:     make(map[string]struct{}, prof.Target),
	}
	return d.run(ctx)
}

type driver struct {
	clusterID string
	log       *slog.Logger
	k         client.Client
	prof      profile
	rng       *rand.Rand

	mu    sync.Mutex
	known map[string]struct{}
	seq   uint64
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

	// Phase 3: steady-state churn until duration elapses (or forever).
	deadline := time.Time{}
	if d.prof.DurationSeconds > 0 {
		deadline = time.Now().Add(time.Duration(d.prof.DurationSeconds) * time.Second)
	}

	// Churn tick fires once per second. Replace a fraction of CRs sized
	// to hit churnPerMinute averaged over each minute.
	perTick := int(float64(d.prof.Target) * d.prof.ChurnPerMinute / 60.0)
	if perTick < 1 && d.prof.ChurnPerMinute > 0 {
		perTick = 1
	}
	d.log.Info("steady state", "churn_per_tick", perTick)

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			if !deadline.IsZero() && time.Now().After(deadline) {
				d.log.Info("duration reached, exiting")
				return nil
			}
			if perTick > 0 {
				d.churn(ctx, perTick)
			}
			// Reconcile actual count back toward target on every tick to
			// repair any drift from failed Create / Delete calls.
			d.reconcile(ctx)
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

func (d *driver) reconcile(ctx context.Context) {
	got := d.activeCount()
	switch {
	case got < d.prof.Target:
		for i := 0; i < d.prof.Target-got && i < 20; i++ {
			_ = d.createOne(ctx)
		}
	case got > d.prof.Target+d.prof.BurstAtStart:
		extra := got - d.prof.Target
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
	name := fmt.Sprintf("%s-cr-%06d", d.clusterID, d.seq)
	d.mu.Unlock()

	pri := int32(1000)
	if len(d.prof.PriorityClasses) > 0 {
		pri = d.prof.PriorityClasses[d.rng.Intn(len(d.prof.PriorityClasses))]
	}
	intr := resource.MustParse("8192")
	recl := resource.MustParse("65536")
	cr := &bfv1alpha1.CapacityRequest{
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
	if err := d.k.Create(ctx, cr); err != nil {
		return err
	}
	d.mu.Lock()
	d.known[name] = struct{}{}
	d.mu.Unlock()
	created.Inc()
	active.Set(float64(d.activeCount()))
	return nil
}

func (d *driver) deleteOne(ctx context.Context, name string) error {
	cr := &bfv1alpha1.CapacityRequest{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	if err := d.k.Delete(ctx, cr); err != nil && !errIsNotFound(err) {
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
