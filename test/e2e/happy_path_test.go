//go:build e2e

package e2e_test

import (
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// Happy-path single-cluster e2e: 4 unschedulable pods → 4 CRs → 4
// Configured machines on the fake provider → 4 UpcomingNode CRs.
//
// Demonstrates the full pipeline (CR controller → operator → shard →
// fake provider → status feedback) end to end through a real kind
// cluster's apiserver.
func TestE2E_HappyPath_PodsToConfigured(t *testing.T) {
	env := startE2EEnv(t)
	env.addIdleMachines("gpu", 4, "a3-highgpu-8g", 8)

	for i := 0; i < 4; i++ {
		env.createUnschedulablePod("trainer-"+strconv.Itoa(i), 8, 1_000_000)
	}

	// Step 1: 4 CRs exist (the CR controller has reconciled the pods).
	env.waitFor(120*time.Second, func() bool {
		var list bfv1alpha1.CapacityRequestList
		if err := env.kc.List(env.ctx, &list); err != nil {
			return false
		}
		t.Logf("CRs in cluster: %d", len(list.Items))
		return len(list.Items) >= 4
	}, "4 CapacityRequests created")

	// Step 2: 4 machines reach Configured on the shard (the operator
	// rollup landed and the shard executed Phase 1).
	env.waitFor(120*time.Second, func() bool {
		c := env.shard.Inventory().Snapshot().CountByState(machine.StateConfigured)
		states := map[machine.State]int{}
		for _, m := range env.shard.Inventory().Snapshot().All() {
			states[m.State]++
		}
		t.Logf("inventory: configured=%d states=%v", c, states)
		return c >= 4
	}, "4 machines Configured on shard")

	// Step 3: each Configured machine is bound to cluster-e2e (the
	// operator's cluster id).
	configured := 0
	for _, m := range env.shard.Inventory().Snapshot().All() {
		if m.State == machine.StateConfigured && m.Cluster == "cluster-e2e" {
			configured++
		}
	}
	if configured < 4 {
		t.Errorf("Configured for cluster-e2e = %d, want >=4", configured)
	}

	// Step 4: CRs have transitioned to Acknowledged.
	var crs bfv1alpha1.CapacityRequestList
	if err := env.kc.List(env.ctx, &crs); err != nil {
		t.Fatalf("list CRs: %v", err)
	}
	ackd := 0
	for _, cr := range crs.Items {
		if cr.Status.Phase == bfv1alpha1.CapacityRequestAcknowledged {
			ackd++
		}
	}
	if ackd < 4 {
		t.Errorf("Acknowledged CRs = %d, want >=4", ackd)
	}

	// Step 5: pods are still Pending (the fake provider doesn't
	// actually join nodes to the kind cluster). The control plane
	// view is what matters here.
	var pods corev1.PodList
	if err := env.kc.List(env.ctx, &pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	pending := 0
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodPending {
			pending++
		}
	}
	if pending < 4 {
		t.Errorf("Pending pods = %d, want >=4 (no real kubelet to schedule on)", pending)
	}
}
