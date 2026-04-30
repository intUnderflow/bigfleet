package operator

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// handleNodeStateUpdate upserts an UpcomingNode CR for the given
// machine. Phase mapping per the paper §6.3 — every transitional /
// stable MachineState maps to one UpcomingNode phase.
//
// Coalescing: the supersedes_key is checked at the shard's outbox layer.
// Operators apply frames in arrival order; if a stale frame races a
// fresh one, the resulting CR write is harmless (status will catch up
// on the next frame).
func (o *Operator) handleNodeStateUpdate(ctx context.Context, u *pb.NodeStateUpdate) error {
	if u == nil || u.GetMachineId() == "" {
		return nil
	}
	name := upcomingNodeName(u.GetMachineId())
	phase := upcomingNodePhase(u.GetState())

	var existing bfv1alpha1.UpcomingNode
	getErr := o.cfg.KubeClient.Get(ctx, types.NamespacedName{Name: name}, &existing)
	switch {
	case apierrors.IsNotFound(getErr):
		// Fresh — create.
		un := &bfv1alpha1.UpcomingNode{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: bfv1alpha1.UpcomingNodeSpec{
				// Resources / Labels / Taints arrive on subsequent
				// updates if the shard chooses to populate them.
				Resources: corev1.ResourceList{},
			},
		}
		if err := o.cfg.KubeClient.Create(ctx, un); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create UpcomingNode: %w", err)
		}
		// Re-fetch so the status update below operates on a fresh copy.
		if err := o.cfg.KubeClient.Get(ctx, types.NamespacedName{Name: name}, &existing); err != nil {
			return fmt.Errorf("re-fetch UpcomingNode after create: %w", err)
		}
	case getErr != nil:
		return fmt.Errorf("get UpcomingNode: %w", getErr)
	}

	existing.Status.Phase = phase
	if u.GetNodeName() != "" {
		existing.Status.NodeRef = &corev1.ObjectReference{Kind: "Node", Name: u.GetNodeName()}
	}
	if u.GetProviderId() != "" {
		existing.Status.ProviderID = u.GetProviderId()
	}
	if u.GetEstimatedReadyUnixNanos() != 0 {
		t := metav1.NewTime(time.Unix(0, u.GetEstimatedReadyUnixNanos()))
		existing.Status.EstimatedReadyTime = &t
	}
	if u.GetLastError() != "" {
		existing.Status.LastError = u.GetLastError()
	}
	if existing.Status.ProvisioningStartTime == nil {
		now := metav1.Now()
		existing.Status.ProvisioningStartTime = &now
	}
	if err := o.cfg.KubeClient.Status().Update(ctx, &existing); err != nil {
		return fmt.Errorf("update UpcomingNode status: %w", err)
	}
	return nil
}

// handleAvailableCapacityUpdate is the M4 stub for AvailableCapacity
// CR writes. The full implementation lands when the operator-side
// telemetry story is fleshed out — for M4 we log and drop, since the
// shard hasn't started emitting these yet.
func (o *Operator) handleAvailableCapacityUpdate(_ context.Context, u *pb.AvailableCapacityUpdate) error {
	if u == nil {
		return nil
	}
	o.log.Debug("AvailableCapacityUpdate received (no-op in M4)",
		"key", u.GetSupersedesKey(),
		"count", u.GetAvailableCount(),
		"confidence", u.GetConfidence(),
	)
	return nil
}

// upcomingNodeName produces a stable Kubernetes name for an
// UpcomingNode given a machine id. Kubernetes object names must be
// DNS-1123 (lowercase alnum + hyphens), which BigFleet machine IDs
// already are by convention.
func upcomingNodeName(machineID string) string {
	return "un-" + machineID
}

func upcomingNodePhase(state pb.MachineState) bfv1alpha1.UpcomingNodePhase {
	switch state {
	case pb.MachineState_MACHINE_STATE_CREATING, pb.MachineState_MACHINE_STATE_SPECULATIVE:
		return bfv1alpha1.UpcomingNodeProvisioning
	case pb.MachineState_MACHINE_STATE_IDLE:
		return bfv1alpha1.UpcomingNodeLaunched
	case pb.MachineState_MACHINE_STATE_CONFIGURING:
		return bfv1alpha1.UpcomingNodeRegistered
	case pb.MachineState_MACHINE_STATE_CONFIGURED:
		return bfv1alpha1.UpcomingNodeReady
	case pb.MachineState_MACHINE_STATE_FAILED:
		return bfv1alpha1.UpcomingNodeFailed
	}
	return bfv1alpha1.UpcomingNodeProvisioning
}
