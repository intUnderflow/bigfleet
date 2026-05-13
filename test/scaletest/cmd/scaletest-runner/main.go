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
	InternalBindingLatencyP99Seconds float64 `yaml:"internalBindingLatencyP99Seconds"`
	ShardCycleDurationP99Seconds     float64 `yaml:"shardCycleDurationP99Seconds"`
	OperatorRollupP99Seconds         float64 `yaml:"operatorRollupP99Seconds"`
	OperatorAckP99Seconds            float64 `yaml:"operatorAckP99Seconds"`
}

type runnerAction struct {
	AtSeconds int    `yaml:"atSeconds"`
	Action    string `yaml:"action"`
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
	chartPath := fs.String("chart", "test/scaletest/chart", "path to the harness chart")
	duration := fs.Duration("duration", 0, "how long to soak after steady state (defaults to profile.loadProfile.durationSeconds)")
	maxDuration := fs.Duration("max-duration", 2*time.Hour, "hard cap; teardown if not done")
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

	prof, err := readProfile(*profilePath)
	if err != nil {
		return err
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
	if err := helmInstall(ctx, *kubeconfig, *chartPath, *profilePath, releaseName, namespace, runID); err != nil {
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

	// Wait for steady state: every kwok pod's load-driver must reach
	// the per-cluster target. Runs that ramp short of target are
	// invalid (they measure the SLO against an under-loaded shard);
	// the runner requires the full target and fails the run if the
	// budget elapses without reaching it.
	totalCRs := prof.KWOK.ClusterCount * prof.LoadProfile.Target
	rampBudget, rampSource := resolveRampBudget(prof, totalCRs)
	fmt.Fprintf(os.Stderr, "ramp budget: %s (%s)\n", rampBudget, rampSource)
	pfArgs := strings.Join(kArgs(*kubeconfig, "-n", namespace, "port-forward", "svc/grafana", "3000:3000"), " ")
	fmt.Fprintf(os.Stderr, "live dashboard: kubectl %s  →  http://localhost:3000/d/bigfleet-scaletest\n", pfArgs)
	if err := waitForSteadyState(ctx, *kubeconfig, namespace, prof.KWOK.ClusterCount, prof.LoadProfile.Target, rampBudget); err != nil {
		return fmt.Errorf("steady state: %w", err)
	}
	fmt.Fprintln(os.Stderr, "steady state reached; soaking", duration)

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
loop:
	for {
		select {
		case <-soakCtx.Done():
			if !failFastFired && !failFastTimer.Stop() {
				<-failFastTimer.C
			}
			break loop
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
	metrics := readKeyMetrics(metricsCtx, *kubeconfig, namespace)
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
	res.Passed, res.Failure = pass(metrics, res.Scale.TotalCRs, res.Scale.ShardReplicas, prof.SLO)
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

// readKeyMetrics queries Prometheus for the runner's SLO metrics. Per-
// query errors map to a -1 sentinel in the result so the summary makes
// the gap visible without aborting the whole run.
func readKeyMetrics(ctx context.Context, kubeconfig, ns string) map[string]float64 {
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
		// sentinel skip in pass() is retained for opt-in CR-mode
		// runs (mode: cr) and for failed scrapes.
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
		"operatorAckP99Seconds":     `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_acknowledge_duration_seconds_bucket[5m])))`,
		"coordinatorApplyOpsPerSec": `sum(rate(bigfleet_coordinator_apply_total[5m]))`,
		"shardShortfalls":           `sum(bigfleet_shard_shortfalls)`,
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
	out := make(map[string]float64, len(queries))
	for k, q := range queries {
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
	target := 15.0
	if slo.InternalBindingLatencyP99Seconds > 0 {
		target = slo.InternalBindingLatencyP99Seconds
	}
	cycleTarget := 5.0
	if slo.ShardCycleDurationP99Seconds > 0 {
		cycleTarget = slo.ShardCycleDurationP99Seconds
	}
	bind, errB := promQuery(qctx, kubeconfig, ns, `histogram_quantile(0.99, sum by (le) (rate(bigfleet_scaletest_pod_bind_latency_steady_seconds_bucket[2m])))`)
	cycle, errC := promQuery(qctx, kubeconfig, ns, `max(histogram_quantile(0.99, sum by (le, pod) (rate(bigfleet_shard_cycle_duration_seconds_bucket[2m]))))`)
	if errB != nil && errC != nil {
		return true, fmt.Sprintf("queries unavailable (bind=%v cycle=%v) — continuing", errB, errC)
	}
	if errB == nil && bind > target {
		return false, fmt.Sprintf("internalBindingLatencyP99Seconds %.3fs > %.1fs SLO", bind, target)
	}
	if errC == nil && cycle > cycleTarget {
		return false, fmt.Sprintf("shardCycleDurationP99Seconds %.3fs > %.1fs envelope", cycle, cycleTarget)
	}
	return true, fmt.Sprintf("bind=%.3fs cycle=%.3fs", bind, cycle)
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

// pass enforces the runner's SLO budget per ADR-0014 + ADR-0018:
// internal binding latency (BigFleet's contribution, harness fake
// provider returns instantly) is a regression detector; cycle
// wall-clock is a tracked throughput envelope. User-facing
// binding latency = this + provider_capacity_create_latency, and
// the second term lives outside this harness (provider conformance
// suite + production canaries — see ADR-0018).
//
//   - internalBindingLatencyP99 ≤ 15 s.  Regression detector. ADR-0018, ADR-0020.
//     Measured via the per-Pod histogram (ADR-0017) emitted by the
//     bigfleet-scaletest-pod-shim. The 15 s budget = ~10 s rollupInterval
//     ceiling (a Pod arriving just after a rollup tick waits one full
//     interval before the shard sees its CR) + ~5 s chain-execution
//     headroom (shard cycle + binder burst-drain). Profiles that
//     lower rollupInterval may lower this SLO accordingly:
//     `internalBindingLatencyP99 ≤ rollupInterval + 5 s` is the
//     recommended ceiling. ADR-0020.
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
func pass(m map[string]float64, totalCRs, shardReplicas int, slo sloOverrides) (bool, string) {
	internalBindingLatencyTarget := 15.0 // ADR-0020: ~10 s rollupInterval ceiling + ~5 s chain headroom
	if slo.InternalBindingLatencyP99Seconds > 0 {
		internalBindingLatencyTarget = slo.InternalBindingLatencyP99Seconds
	}
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
	// ADR-0014 release gate: binding latency. -1 means the metric was
	// unavailable (Prometheus query failed); skip rather than gate on
	// a sentinel.
	if v, ok := m["internalBindingLatencyP99Seconds"]; ok && v >= 0 && v > internalBindingLatencyTarget {
		return false, fmt.Sprintf("internalBindingLatencyP99Seconds %.3fs > %.1fs SLO (ADR-0018 — internal-only; real-provider time is not measured here, see also ADR-0014)", v, internalBindingLatencyTarget)
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
