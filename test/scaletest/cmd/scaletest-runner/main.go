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
	CostEstimate costEstimate `yaml:"costEstimate"`
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
	Passed  bool               `json:"passed"`
	Failure string             `json:"failure,omitempty"`
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
	// the runner now requires the full target and fails the run if
	// the budget elapses without reaching it. Budget: max(15min,
	// durationSeconds×0.5) to give cold-start kine writes room.
	rampBudget := 15 * time.Minute
	if t := time.Duration(prof.LoadProfile.DurationSeconds) * time.Second / 2; t > rampBudget {
		rampBudget = t
	}
	if err := waitForSteadyState(ctx, *kubeconfig, namespace, prof.KWOK.ClusterCount, prof.LoadProfile.Target, rampBudget); err != nil {
		return fmt.Errorf("steady state: %w", err)
	}
	fmt.Fprintln(os.Stderr, "steady state reached; soaking", duration)

	// Soak.
	soakCtx, cancelSoak := context.WithTimeout(ctx, *duration)
	<-soakCtx.Done()
	cancelSoak()
	if err := ctx.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	// Snapshot Prometheus TSDB.
	snapPath := filepath.Join(*output, "prometheus-snapshot.tar.gz")
	if err := snapshotPrometheus(context.Background(), *kubeconfig, namespace, snapPath); err != nil {
		fmt.Fprintln(os.Stderr, "prometheus snapshot:", err)
	}

	// Pull metrics summary.
	metrics := readKeyMetrics(context.Background(), *kubeconfig, namespace)

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
	res.Passed, res.Failure = pass(metrics, res.Scale.TotalCRs, res.Scale.ShardReplicas)

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
		"--wait", "--timeout", "10m",
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

func waitForSteadyState(ctx context.Context, kubeconfig, ns string, clusterCount int, perClusterTarget int, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	// Steady state requires (a) every kwok pod's containers all Ready
	// and (b) load-driver has ramped to ≥ 99.9 % of target. Tests
	// that soak against an under-loaded shard measure the wrong
	// thing; runs that don't reach target fail at the gate. The
	// 0.1 % slop absorbs transient create/delete races during the
	// load-driver's churn phase (a single CR being recreated as
	// the gate measures, etc.) — a hard 100 % is too tight in
	// practice.
	target := int(0.999 * float64(clusterCount*perClusterTarget))
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("did not reach steady state within %s", budget)
		}
		ready, err := countReadyKWOKPods(ctx, kubeconfig, ns)
		active := -1
		if err == nil && ready >= clusterCount {
			active = readActiveCRs(ctx, kubeconfig, ns)
			if active >= target {
				return nil
			}
		}
		fmt.Fprintf(os.Stderr, "  waiting: pods %d/%d ready, active %d/%d\n", ready, clusterCount, active, target)
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
		"operatorRollupP99Seconds":           `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_rollup_duration_seconds_bucket[5m])))`,
		"operatorAckP99Seconds":              `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_acknowledge_duration_seconds_bucket[5m])))`,
		"coordinatorApplyOpsPerSec":          `sum(rate(bigfleet_coordinator_apply_total[5m]))`,
		"shardShortfalls":                    `sum(bigfleet_shard_shortfalls)`,
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

// pass enforces the runner's SLO budget. Each threshold is the best
// observed value (across passing baseline runs) plus a small variance
// margin — they're regression detectors, not aspirational targets.
//
//   - shardCycleDurationP99 ≤ 100 ms.   Best observed: 1.8 ms (scaleway-50k).
//     Headroom is large because the decision engine is intrinsically fast;
//     a regression that pushed past 100 ms would be a real architectural
//     problem, not a tuning issue.
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
func pass(m map[string]float64, totalCRs, shardReplicas int) (bool, string) {
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
	if v, ok := m["shardCycleDurationP99Seconds"]; ok && v > 0.1 {
		return false, fmt.Sprintf("shardCycleDurationP99Seconds %.3fs > 100ms SLO", v)
	}
	if v, ok := m["operatorRollupP99Seconds"]; ok && v > 1.0 {
		return false, fmt.Sprintf("operatorRollupP99Seconds %.3fs > 1s SLO", v)
	}
	if v, ok := m["operatorAckP99Seconds"]; ok && v > 12.0 {
		return false, fmt.Sprintf("operatorAckP99Seconds %.3fs > 12s SLO", v)
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
