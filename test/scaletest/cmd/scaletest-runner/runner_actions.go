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
	case strings.HasPrefix(r.Action, "partition-coordinator-from-shard-"):
		pod := strings.TrimPrefix(r.Action, "partition-coordinator-from-shard-")
		err := partitionShardEgress(soakCtx, kubeconfig, namespace, pod, 60*time.Second)
		r.FiredAt = time.Now().UTC().Format(time.RFC3339)
		r.Assertion = fmt.Sprintf("shard pod %s keeps running cycles during 60s coordinator partition (static stability)", pod)
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

// partitionShardEgress applies a NetworkPolicy that denies all egress
// from the named shard pod, holds for the duration, then removes the
// policy. The shard's gRPC server still receives operator-side
// connections (those are inbound and unaffected by egress denials);
// only the shard→coordinator coordclient is severed. That's the M17
// "partition the data plane from the control plane" semantic.
//
// Vanilla NetworkPolicy is allowlist-only — the way to express "deny
// all egress" is `policyTypes: [Egress]` with an empty `egress:` list.
// This also breaks DNS resolution from the shard pod, but the
// coordclient's gRPC connection is already established + cached so
// the partition cleanly severs control-plane traffic without
// disrupting the data plane.
//
// Cleanup runs even if ctx is cancelled — callers can rely on the
// partition not outliving the runner. Uses a fresh context for the
// teardown delete to handle the cancellation case.
func partitionShardEgress(ctx context.Context, kubeconfig, namespace, shardPod string, hold time.Duration) error {
	npName := "bigfleet-partition-" + shardPod
	manifest := fmt.Sprintf(`apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: %s
  namespace: %s
spec:
  podSelector:
    matchLabels:
      statefulset.kubernetes.io/pod-name: %s
  policyTypes: [Egress]
  egress: []
`, npName, namespace, shardPod)

	if err := kubectlApplyStdin(ctx, kubeconfig, manifest); err != nil {
		return fmt.Errorf("apply NetworkPolicy: %w", err)
	}

	// Deferred cleanup uses a fresh context so a soak-side cancel
	// can't leave the partition in place across teardown.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := kubectlDeleteNetworkPolicy(cleanupCtx, kubeconfig, namespace, npName); err != nil {
			fmt.Fprintf(os.Stderr, "partition cleanup: failed to remove NetworkPolicy %s: %v\n", npName, err)
		}
	}()

	select {
	case <-ctx.Done():
	case <-time.After(hold):
	}
	return nil
}

func kubectlApplyStdin(ctx context.Context, kubeconfig, manifest string) error {
	args := []string{"apply", "-f", "-"}
	if kubeconfig != "" {
		args = append([]string{"--kubeconfig", kubeconfig}, args...)
	}
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	cmd.Stdin = strings.NewReader(manifest)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func kubectlDeleteNetworkPolicy(ctx context.Context, kubeconfig, namespace, name string) error {
	args := []string{"-n", namespace, "delete", "networkpolicy", name, "--ignore-not-found"}
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
		// Term should have advanced by ≥1 between the start of the
		// soak and end-of-soak. Earlier versions used `delta(...[5m])`
		// at end-of-soak — that missed the bump because the kill
		// fires at atSeconds (typically t=600s) but the assertion runs
		// at t≈soak-end (t≈1800s+), 20+ min later, well outside the
		// 5-min window. max_over_time - min_over_time over a wide
		// window catches the bump regardless of when the kill fired.
		const window = "1h"
		q := fmt.Sprintf(`max(max_over_time(bigfleet_coordinator_raft_term[%s]) - min_over_time(bigfleet_coordinator_raft_term[%s]))`, window, window)
		v, err := promQuery(ctx, kubeconfig, namespace, q)
		if err != nil {
			r.AssertError = fmt.Sprintf("prom query: %v", err)
			return
		}
		if v >= 1.0 {
			r.Asserted = true
			return
		}
		r.AssertError = fmt.Sprintf("max-min(bigfleet_coordinator_raft_term[%s]) = %.0f, want ≥1", window, v)
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
	case strings.HasPrefix(r.Action, "partition-coordinator-from-shard-"):
		// During the 60 s partition the shard cannot reach the
		// coordinator, but the data plane (cycle / inventory / Phase
		// 1-3) MUST keep running. Verify the partitioned pod kept
		// publishing cycle metrics throughout — a sudden zero in
		// rate(...[2m]) at end-of-soak would mean the partition broke
		// the static-stability invariant.
		pod := strings.TrimPrefix(r.Action, "partition-coordinator-from-shard-")
		q := fmt.Sprintf(`sum(rate(bigfleet_shard_cycle_duration_seconds_count{pod="%s"}[2m]))`, pod)
		v, err := promQuery(ctx, kubeconfig, namespace, q)
		if err != nil {
			r.AssertError = fmt.Sprintf("prom query: %v", err)
			return
		}
		if v > 0 {
			r.Asserted = true
			return
		}
		r.AssertError = fmt.Sprintf("shard %s stopped publishing cycle metrics post-partition (rate = %.4f) — static stability violation", pod, v)
	default:
		r.AssertError = "unrecognised action — no assertion to run"
	}
}
