//go:build e2e

package e2e_test

import (
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
)

// Static-stability e2e: bring the cluster to steady state, then stop
// the BigFleet shard and verify:
//
//  1. CapacityRequest CRs survive (control-plane state is in the
//     cluster's etcd, not in BigFleet's process memory).
//  2. Pods continue to exist (the kind kubelet does not depend on
//     BigFleet — that's the load-bearing safety property of the
//     architecture).
//
// We don't yet exercise restart-and-resume in this scenario; that
// becomes meaningful in M6 when the coordinator's persistent state
// matters. Here we prove the data plane survives.
func TestE2E_StaticStability_ShardOutage(t *testing.T) {
	env := startE2EEnv(t)
	env.addIdleMachines("gpu", 4, "a3-highgpu-8g", 8)

	for i := 0; i < 4; i++ {
		env.createUnschedulablePod("trainer-"+strconv.Itoa(i), 8, 1_000_000)
	}

	// Wait for steady state — at least one CR Acknowledged so we know
	// the pipeline ran end-to-end.
	env.waitFor(60*time.Second, func() bool {
		var list bfv1alpha1.CapacityRequestList
		if err := env.kc.List(env.ctx, &list); err != nil {
			return false
		}
		for _, cr := range list.Items {
			if cr.Status.Phase == bfv1alpha1.CapacityRequestAcknowledged {
				return true
			}
		}
		return false
	}, "at least one CR Acknowledged")

	beforePods := env.countPods()
	beforeCRs := env.countCRs()

	// Stop the shard's grpc server. The operator's stream will close;
	// the rollup loop will retry indefinitely. The cluster — including
	// its pods and CRs — must not be affected.
	env.shardSrv.Stop()

	// Give the system 5 seconds with no shard to do whatever damage
	// it's going to do.
	time.Sleep(5 * time.Second)

	if got := env.countPods(); got < beforePods {
		t.Errorf("pods lost while shard was down: before=%d after=%d", beforePods, got)
	}
	if got := env.countCRs(); got < beforeCRs {
		t.Errorf("CRs lost while shard was down: before=%d after=%d", beforeCRs, got)
	}

	// Verify the pods are still present and not in any errored phase.
	var pods corev1.PodList
	if err := env.kc.List(env.ctx, &pods); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodFailed {
			t.Errorf("pod %s entered Failed during shard outage", p.Name)
		}
	}
}

func (e *e2eEnv) countPods() int {
	var pods corev1.PodList
	if err := e.kc.List(e.ctx, &pods); err != nil {
		return -1
	}
	return len(pods.Items)
}

func (e *e2eEnv) countCRs() int {
	var list bfv1alpha1.CapacityRequestList
	if err := e.kc.List(e.ctx, &list); err != nil {
		return -1
	}
	return len(list.Items)
}
