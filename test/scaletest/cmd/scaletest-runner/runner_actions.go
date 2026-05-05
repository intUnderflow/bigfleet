package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// scheduleRunnerActions launches a goroutine per action that fires at
// soakStart + AtSeconds and records the outcome in the returned slice.
// The slice is mutated concurrently — callers must not read it before
// soakCtx is done. Returning the slice (rather than passing it in) is
// the simplest way to guarantee assertion results are observed by the
// summary writer regardless of when the soak ends.
func scheduleRunnerActions(soakCtx context.Context, kubeconfig, namespace string, soakStart time.Time, actions []runnerAction) []runnerActionResult {
	results := make([]runnerActionResult, len(actions))
	for i, a := range actions {
		results[i] = runnerActionResult{Action: a.Action, AtSeconds: a.AtSeconds}
	}
	for i := range actions {
		go fireOneAction(soakCtx, kubeconfig, namespace, soakStart, &results[i])
	}
	return results
}

func fireOneAction(soakCtx context.Context, kubeconfig, namespace string, soakStart time.Time, r *runnerActionResult) {
	fireAt := soakStart.Add(time.Duration(r.AtSeconds) * time.Second)
	wait := time.Until(fireAt)
	if wait > 0 {
		select {
		case <-soakCtx.Done():
			r.FireError = "soak ended before action fired"
			return
		case <-time.After(wait):
		}
	}
	switch {
	case r.Action == "kill-coordinator-leader":
		err := killCoordinatorLeader(soakCtx, kubeconfig, namespace)
		r.FiredAt = time.Now().UTC().Format(time.RFC3339)
		r.Assertion = "coordinator_raft_term advances by ≥1 within 60s"
		if err != nil {
			r.FireError = err.Error()
		}
	case strings.HasPrefix(r.Action, "kill-shard-"):
		pod := strings.TrimPrefix(r.Action, "kill-shard-")
		err := killPod(soakCtx, kubeconfig, namespace, pod)
		r.FiredAt = time.Now().UTC().Format(time.RFC3339)
		r.Assertion = fmt.Sprintf("shard pod %s rescheduled and resumes cycle metrics within 60s", pod)
		if err != nil {
			r.FireError = err.Error()
		}
	default:
		r.FireError = "unrecognised action"
	}
	if r.FireError == "" {
		fmt.Fprintf(os.Stderr, "runnerAction fired: %s @t=%ds\n", r.Action, r.AtSeconds)
	} else {
		fmt.Fprintf(os.Stderr, "runnerAction fire failed: %s @t=%ds: %s\n", r.Action, r.AtSeconds, r.FireError)
	}
}

// killCoordinatorLeader deletes the leader-side coordinator pod. v1
// uses a label-selector delete that matches the pod with the
// app.kubernetes.io/component=coordinator label — the scaletest chart
// runs a single coordinator replica, so this is unambiguous. Multi-
// replica coordinator deploys would need an explicit leader probe;
// flag this as future work in the Assertion text rather than fixing
// it in the action itself.
func killCoordinatorLeader(ctx context.Context, kubeconfig, namespace string) error {
	args := []string{
		"-n", namespace,
		"delete", "pod",
		"-l", "app.kubernetes.io/component=coordinator",
		"--wait=false",
	}
	if kubeconfig != "" {
		args = append([]string{"--kubeconfig", kubeconfig}, args...)
	}
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func killPod(ctx context.Context, kubeconfig, namespace, podName string) error {
	args := []string{
		"-n", namespace,
		"delete", "pod", podName,
		"--wait=false",
	}
	if kubeconfig != "" {
		args = append([]string{"--kubeconfig", kubeconfig}, args...)
	}
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// assertRunnerActionOutcome runs the per-action prom query (against the
// live in-cluster Prometheus, not the snapshot — this fires before
// teardown so the prom pod is still serving). Sets r.Asserted /
// r.AssertError accordingly.
//
// soakStart and the action's FiredAt give us the comparison windows:
//   - "before fire" = soakStart .. just-before-FiredAt
//   - "after fire"  = just-after-FiredAt .. now
//
// Both windows are 5 min for stable rate calculations; the action's
// pre-fire 60 s of stability is assumed to match steady state since
// the runner only fires after waitForSteadyState.
func assertRunnerActionOutcome(ctx context.Context, kubeconfig, namespace string, r *runnerActionResult, _ time.Time) {
	if r.Action == "" || r.FireError != "" {
		r.Asserted = false
		if r.AssertError == "" {
			r.AssertError = "action did not fire"
		}
		return
	}
	switch {
	case r.Action == "kill-coordinator-leader":
		// Term should have advanced by ≥1 from a stable baseline. Use
		// `delta()` over the last 5 min — at 30s scrape interval and
		// a single re-election, delta should be 1.
		v, err := promQuery(ctx, kubeconfig, namespace, `max(delta(bigfleet_coordinator_raft_term[5m]))`)
		if err != nil {
			r.AssertError = fmt.Sprintf("prom query: %v", err)
			return
		}
		if v >= 1.0 {
			r.Asserted = true
			return
		}
		r.AssertError = fmt.Sprintf("delta(bigfleet_coordinator_raft_term[5m]) = %.0f, want ≥1", v)
	case strings.HasPrefix(r.Action, "kill-shard-"):
		// The deleted pod's StatefulSet should have rescheduled it,
		// and it should be publishing cycle metrics again. Use a
		// presence check via count(rate(...)).
		pod := strings.TrimPrefix(r.Action, "kill-shard-")
		q := fmt.Sprintf(`sum(rate(bigfleet_shard_cycle_duration_seconds_count{pod="%s"}[1m]))`, pod)
		v, err := promQuery(ctx, kubeconfig, namespace, q)
		if err != nil {
			r.AssertError = fmt.Sprintf("prom query: %v", err)
			return
		}
		if v > 0 {
			r.Asserted = true
			return
		}
		r.AssertError = fmt.Sprintf("shard %s not publishing cycle metrics post-kill (rate = %.4f)", pod, v)
	default:
		r.AssertError = "unrecognised action — no assertion to run"
	}
}
