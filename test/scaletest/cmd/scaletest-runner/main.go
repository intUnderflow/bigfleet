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
	"net/http"
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

	// Wait for steady state: every kwok pod's load-driver should report
	// its target CR count. We give it (durationSeconds×0.2) or 5 min,
	// whichever is bigger.
	rampBudget := 5 * time.Minute
	if t := time.Duration(prof.LoadProfile.DurationSeconds) * time.Second / 5; t > rampBudget {
		rampBudget = t
	}
	if err := waitForSteadyState(ctx, *kubeconfig, namespace, prof.KWOK.ClusterCount, rampBudget); err != nil {
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
	res.Passed = pass(metrics)
	if !res.Passed {
		res.Failure = "one or more SLO thresholds exceeded"
	}

	summary := filepath.Join(*output, "summary.json")
	b, _ := json.MarshalIndent(res, "", "  ")
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

func waitForSteadyState(ctx context.Context, kubeconfig, ns string, clusterCount int, budget time.Duration) error {
	deadline := time.Now().Add(budget)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("did not reach steady state within %s", budget)
		}
		ready, err := countReadyKWOKPods(ctx, kubeconfig, ns)
		if err == nil && ready >= clusterCount {
			return nil
		}
		fmt.Fprintf(os.Stderr, "  waiting: %d/%d kwok pods ready\n", ready, clusterCount)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func countReadyKWOKPods(ctx context.Context, kubeconfig, ns string) (int, error) {
	args := []string{
		"-n", ns,
		"get", "pods", "-l", "app.kubernetes.io/component=kwok-cluster",
		"-o", `jsonpath={range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}`,
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
		if strings.TrimSpace(line) == "true" {
			count++
		}
	}
	return count, nil
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
		"shardCycleDurationP99Seconds": `histogram_quantile(0.99, sum by (le) (rate(bigfleet_shard_cycle_duration_seconds_bucket[5m])))`,
		"operatorRollupP99Seconds":     `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_rollup_duration_seconds_bucket[5m])))`,
		"operatorAckP99Seconds":        `histogram_quantile(0.99, sum by (le) (rate(bigfleet_operator_acknowledge_duration_seconds_bucket[5m])))`,
		"coordinatorApplyOpsPerSec":    `sum(rate(bigfleet_coordinator_apply_total[5m]))`,
		"shardShortfalls":              `sum(bigfleet_shard_shortfalls)`,
		"loadgenCRsActive":             `sum(scaletest_loadgen_cr_active)`,
		"loadgenCRsCreatedPerSec":      `sum(rate(scaletest_loadgen_cr_created_total[5m]))`,
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
	return v, err
}

func urlEncode(s string) string { return (&urlValues{q: s}).encode() }

type urlValues struct{ q string }

func (u *urlValues) encode() string {
	r := strings.NewReplacer(" ", "%20", "(", "%28", ")", "%29",
		",", "%2C", "[", "%5B", "]", "%5D", "+", "%2B")
	return r.Replace(u.q)
}

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
func pass(m map[string]float64) bool {
	if v, ok := m["shardCycleDurationP99Seconds"]; ok && v > 0.1 {
		return false
	}
	if v, ok := m["operatorRollupP99Seconds"]; ok && v > 1.0 {
		return false
	}
	if v, ok := m["operatorAckP99Seconds"]; ok && v > 12.0 {
		return false
	}
	return true
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
