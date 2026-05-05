package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// handleReclaimInstruction processes a ReclaimInstruction from the
// shard. M20: cordons each named node, walks UpcomingNode CR phase to
// Draining, sends ReclaimAck (cordon has taken effect), then drains
// in the background — evicting non-DaemonSet pods via the policy/v1
// eviction API which respects PodDisruptionBudgets. On drain
// completion the UpcomingNode walks to Drained; on grace-period
// timeout it walks to Failed with last_error populated.
//
// The ack fires after cordon but BEFORE drain completes — drain can
// take minutes per pod and the recv-loop must not block on it. The
// shard's reclamation accounting is the cordon, not the drain
// completion; "started" rather than "finished" is the contract.
func (o *Operator) handleReclaimInstruction(ctx context.Context, sess *session, instr *pb.ReclaimInstruction) error {
	o.log.Info("reclaim instruction",
		"id", instr.GetInstructionId(),
		"nodes", instr.GetNodes(),
		"grace_seconds", instr.GetGracePeriodSeconds(),
		"preemptor_priority", instr.GetPreemptorPriority(),
	)

	// 1. Cordon every named node. Patch is fast; do it inline so the
	// ack carries the post-cordon truth.
	for _, name := range instr.GetNodes() {
		if err := o.cordonNode(ctx, name); err != nil {
			o.log.Warn("cordon failed", "node", name, "err", err)
		}
		if err := o.markUpcomingNodeDraining(ctx, name); err != nil {
			o.log.Warn("UpcomingNode draining-phase patch failed", "node", name, "err", err)
		}
	}

	// 2. Ack synchronously: cordon is the post-condition the shard
	// cares about. Drain is on its own clock.
	ack := sess.enqueue(ctx, &pb.OperatorMessage{
		Payload: &pb.OperatorMessage_ReclaimAck{ReclaimAck: &pb.ReclaimAck{
			InstructionId: instr.GetInstructionId(),
			NodesStarted:  int32(len(instr.GetNodes())),
		}},
	})
	if ack != nil {
		// If we can't ack the shard will retry; drain still runs to
		// give workloads a chance to migrate.
		o.log.Warn("ReclaimAck enqueue failed; continuing drain", "err", ack)
	}

	// 3. Drain asynchronously. Bound the lifetime by the instruction's
	// grace_period so a stalled drain can't outlive the reclaim window
	// without resolution. We deliberately don't propagate the recv-
	// loop's ctx — that goroutine returns immediately after the ack.
	grace := time.Duration(instr.GetGracePeriodSeconds()) * time.Second
	if grace <= 0 {
		grace = 30 * time.Second
	}
	go o.drainNodesInBackground(instr.GetNodes(), grace)

	return ack
}

func (o *Operator) cordonNode(ctx context.Context, name string) error {
	patch := []byte(`{"spec":{"unschedulable":true}}`)
	target := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := o.cfg.KubeClient.Patch(ctx, target, client.RawPatch(types.MergePatchType, patch)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// markUpcomingNodeDraining patches the UpcomingNode CR whose
// status.nodeRef.name == nodeName to phase=Draining. UpcomingNodes
// are cluster-scoped, so we list all of them and match by NodeRef.
// At realistic operator scales (tens to a few hundred upcoming nodes
// in flight) this is cheap.
func (o *Operator) markUpcomingNodeDraining(ctx context.Context, nodeName string) error {
	un, err := o.findUpcomingNodeForNode(ctx, nodeName)
	if err != nil || un == nil {
		return err
	}
	return o.patchUpcomingNodePhase(ctx, un.Name, bfv1alpha1.UpcomingNodeDraining, "")
}

func (o *Operator) findUpcomingNodeForNode(ctx context.Context, nodeName string) (*bfv1alpha1.UpcomingNode, error) {
	var list bfv1alpha1.UpcomingNodeList
	if err := o.cfg.KubeClient.List(ctx, &list); err != nil {
		return nil, fmt.Errorf("list UpcomingNodes: %w", err)
	}
	for i := range list.Items {
		n := &list.Items[i]
		if n.Status.NodeRef != nil && n.Status.NodeRef.Name == nodeName {
			return n, nil
		}
	}
	return nil, nil
}

// patchUpcomingNodePhase JSON-merge-patches a single UpcomingNode CR's
// status.phase (and optional status.lastError). Mirrors the
// markAcknowledged pattern in rollup.go.
func (o *Operator) patchUpcomingNodePhase(ctx context.Context, name string, phase bfv1alpha1.UpcomingNodePhase, lastError string) error {
	body := map[string]any{"phase": string(phase)}
	if lastError != "" {
		body["lastError"] = lastError
	}
	patch, err := json.Marshal(map[string]any{"status": body})
	if err != nil {
		return err
	}
	target := &bfv1alpha1.UpcomingNode{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := o.cfg.KubeClient.Status().Patch(ctx, target, client.RawPatch(types.MergePatchType, patch)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// drainNodesInBackground evicts non-DaemonSet pods from each named
// node. Eviction goes through the policy/v1 eviction subresource so
// PDBs are respected (the apiserver returns 429 when an eviction would
// violate a PDB; we retry with backoff up to the grace deadline).
//
// On per-node completion: phase=Drained.
// On per-node grace timeout: phase=Failed with last_error populated.
func (o *Operator) drainNodesInBackground(nodes []string, grace time.Duration) {
	deadline := time.Now().Add(grace)
	for _, name := range nodes {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		err := o.drainOneNode(ctx, name)
		cancel()
		if err != nil {
			o.log.Warn("drain failed; UpcomingNode → Failed", "node", name, "err", err)
			updCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
			_ = o.patchUpcomingNodePhaseByNodeRef(updCtx, name, bfv1alpha1.UpcomingNodeFailed, err.Error())
			c()
			continue
		}
		updCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		_ = o.patchUpcomingNodePhaseByNodeRef(updCtx, name, bfv1alpha1.UpcomingNodeDrained, "")
		c()
	}
}

func (o *Operator) patchUpcomingNodePhaseByNodeRef(ctx context.Context, nodeName string, phase bfv1alpha1.UpcomingNodePhase, lastError string) error {
	un, err := o.findUpcomingNodeForNode(ctx, nodeName)
	if err != nil || un == nil {
		return err
	}
	return o.patchUpcomingNodePhase(ctx, un.Name, phase, lastError)
}

// drainOneNode evicts every non-DaemonSet pod on the node and waits
// for them to leave. Returns when all evictable pods are gone or
// when ctx expires. The latter surfaces as the grace-period-exceeded
// failure mode in drainNodesInBackground.
func (o *Operator) drainOneNode(ctx context.Context, nodeName string) error {
	for {
		var pods corev1.PodList
		if err := o.cfg.KubeClient.List(ctx, &pods, client.MatchingFields{"spec.nodeName": nodeName}); err != nil {
			// Field-indexer may not be installed; fall back to label-less full list + filter.
			if err := o.cfg.KubeClient.List(ctx, &pods); err != nil {
				return fmt.Errorf("list pods: %w", err)
			}
		}
		drained := true
		for i := range pods.Items {
			p := &pods.Items[i]
			if p.Spec.NodeName != nodeName {
				continue
			}
			if isDaemonSetPod(p) {
				continue
			}
			if p.DeletionTimestamp != nil {
				drained = false
				continue
			}
			drained = false
			if err := o.evictPod(ctx, p); err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				// 429 (PDB-blocked) and similar transient errors:
				// don't fail the whole drain — sleep and retry.
				o.log.Info("eviction transient failure; will retry", "pod", p.Namespace+"/"+p.Name, "err", err)
			}
		}
		if drained {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func (o *Operator) evictPod(ctx context.Context, p *corev1.Pod) error {
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: p.Namespace},
	}
	// SubResource("eviction").Create posts the eviction subresource.
	return o.cfg.KubeClient.SubResource("eviction").Create(ctx, p, eviction)
}

func isDaemonSetPod(p *corev1.Pod) bool {
	for _, ref := range p.OwnerReferences {
		if ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}
