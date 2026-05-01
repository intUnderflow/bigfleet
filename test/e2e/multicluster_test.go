//go:build e2e

package e2e_test

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/machine"
)

// TestE2E_MultiCluster_CrossClusterPreemption: two real kind clusters
// share a single shard. cluster-batch's pods at low priority hold all
// 4 GPU machines; cluster-train arrives with high-priority pods and
// the same instance-type need. The shard's Phase 2 emits Preempt
// actions for cluster-batch's machines; once they drain back to Idle,
// the next cycle's Phase 1 reassigns them to cluster-train.
//
// End state: cluster-train has all 4 Configured machines; cluster-batch
// has 0. cluster-batch's pods stay Pending — the kubelet doesn't have
// real GPU nodes to run them on, but BigFleet's control-plane
// behaviour is what we're testing.
func TestE2E_MultiCluster_CrossClusterPreemption(t *testing.T) {
	env := startMultiClusterEnv(t, []string{"cluster-batch", "cluster-train"}, 4)

	// Use PriorityClasses (not the integer Priority field — kind's
	// admission rejects that). high gap > 900K → 10s drain grace.
	env.addPriorityClassIn("cluster-batch", "bigfleet-low", 100_000)
	env.addPriorityClassIn("cluster-train", "bigfleet-high", 1_000_000)

	// cluster-batch claims first.
	for i := 0; i < 4; i++ {
		env.createPodWithPriorityClassIn("cluster-batch", podName("batch", i), "bigfleet-low", 8)
	}
	env.waitFor(60*time.Second, func() bool {
		count := 0
		for _, m := range env.shard.Inventory().Snapshot().All() {
			if m.State == machine.StateConfigured && m.Cluster == "cluster-batch" {
				count++
			}
		}
		return count == 4
	}, "cluster-batch claims 4 machines")

	// cluster-train arrives wanting the same.
	for i := 0; i < 4; i++ {
		env.createPodWithPriorityClassIn("cluster-train", podName("train", i), "bigfleet-high", 8)
	}

	// Eventually all 4 machines should be configured for cluster-train.
	env.waitFor(60*time.Second, func() bool {
		count := 0
		for _, m := range env.shard.Inventory().Snapshot().All() {
			if m.State == machine.StateConfigured && m.Cluster == "cluster-train" {
				count++
			}
		}
		return count == 4
	}, "cluster-train ends with 4 Configured")

	// And cluster-batch has none.
	count := 0
	for _, m := range env.shard.Inventory().Snapshot().All() {
		if m.State == machine.StateConfigured && m.Cluster == "cluster-batch" {
			count++
		}
	}
	if count != 0 {
		t.Errorf("cluster-batch still has %d Configured machines; expected 0", count)
	}

	// cluster-batch's CRs and pods still exist (BigFleet doesn't
	// touch the user's pods directly; preemption goes through the
	// operator's reclaim instruction → cordon + graceful shutdown).
	var batchCRs bfv1alpha1.CapacityRequestList
	if err := env.rig("cluster-batch").kubeClient.List(env.ctx, &batchCRs); err != nil {
		t.Fatalf("list cluster-batch CRs: %v", err)
	}
	if len(batchCRs.Items) == 0 {
		t.Errorf("expected cluster-batch CRs to still exist after preemption")
	}
}

// TestE2E_MultiCluster_StaticStability: bring three clusters to
// steady state, stop the shard's grpc server, verify all three
// clusters' pods + CRs survive.
func TestE2E_MultiCluster_StaticStability(t *testing.T) {
	env := startMultiClusterEnv(t, []string{"cluster-a", "cluster-b", "cluster-c"}, 6)
	for _, id := range []string{"cluster-a", "cluster-b", "cluster-c"} {
		for i := 0; i < 2; i++ {
			env.createUnschedulablePodIn(id, podName(id, i), 8)
		}
	}

	// Wait until at least one CR is Acknowledged in each cluster
	// (the pipeline ran end-to-end for everyone).
	for _, id := range []string{"cluster-a", "cluster-b", "cluster-c"} {
		id := id
		env.waitFor(60*time.Second, func() bool {
			var list bfv1alpha1.CapacityRequestList
			if err := env.rig(id).kubeClient.List(env.ctx, &list); err != nil {
				return false
			}
			for _, cr := range list.Items {
				if cr.Status.Phase == bfv1alpha1.CapacityRequestAcknowledged {
					return true
				}
			}
			return false
		}, "at least one CR Acknowledged in "+id)
	}

	// Snapshot before-state per cluster.
	beforePods := make(map[string]int)
	beforeCRs := make(map[string]int)
	for _, r := range env.clusters {
		var pods corev1.PodList
		_ = r.kubeClient.List(env.ctx, &pods)
		beforePods[r.id] = len(pods.Items)
		var crs bfv1alpha1.CapacityRequestList
		_ = r.kubeClient.List(env.ctx, &crs)
		beforeCRs[r.id] = len(crs.Items)
	}

	// Kill the shard. Operators will start logging reconnect
	// failures; that's fine. The clusters' data planes (pods, CRs,
	// scheduler) keep running.
	env.shardSrv.Stop()
	time.Sleep(5 * time.Second)

	for _, r := range env.clusters {
		var pods corev1.PodList
		if err := r.kubeClient.List(env.ctx, &pods); err != nil {
			t.Errorf("%s list pods after shard outage: %v", r.id, err)
			continue
		}
		if len(pods.Items) < beforePods[r.id] {
			t.Errorf("%s lost pods during shard outage: before=%d after=%d",
				r.id, beforePods[r.id], len(pods.Items))
		}
		var crs bfv1alpha1.CapacityRequestList
		if err := r.kubeClient.List(env.ctx, &crs); err != nil {
			t.Errorf("%s list CRs after shard outage: %v", r.id, err)
			continue
		}
		if len(crs.Items) < beforeCRs[r.id] {
			t.Errorf("%s lost CRs during shard outage: before=%d after=%d",
				r.id, beforeCRs[r.id], len(crs.Items))
		}
		for _, p := range pods.Items {
			if p.Status.Phase == corev1.PodFailed {
				t.Errorf("%s pod %s entered Failed during shard outage", r.id, p.Name)
			}
		}
	}
}

func podName(prefix string, n int) string {
	return prefix + "-" + string(rune('0'+n))
}
