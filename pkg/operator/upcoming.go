package operator

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// classifyWriteErr maps an error from a controller-runtime Client write
// to one of {success, conflict, error} for the UpcomingNode write
// counter. AlreadyExists on Create and Conflict on Update are common
// retryable signals that the cache disagreed with the apiserver — count
// them separately so a healthy cache lag doesn't look like an error.
func classifyWriteErr(err error) string {
	switch {
	case err == nil:
		return "success"
	case apierrors.IsAlreadyExists(err), apierrors.IsConflict(err):
		return "conflict"
	}
	return "error"
}

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
	start := time.Now()
	defer func() {
		metrics.OperatorNodeStateUpdateDuration.WithLabelValues(string(phase)).Observe(time.Since(start).Seconds())
	}()

	var existing bfv1alpha1.UpcomingNode
	getErr := o.cfg.KubeClient.Get(ctx, types.NamespacedName{Name: name}, &existing)
	switch {
	case apierrors.IsNotFound(getErr):
		// Fresh — create.
		un := &bfv1alpha1.UpcomingNode{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       upcomingNodeSpecFromUpdate(u),
		}
		createErr := o.cfg.KubeClient.Create(ctx, un)
		metrics.OperatorUpcomingNodeWrites.WithLabelValues("create", classifyWriteErr(createErr)).Inc()
		switch {
		case createErr == nil:
			// M44.4 Drop B: when Create succeeds, the controller-runtime
			// Client populates `un` with the apiserver's response (incl.
			// ResourceVersion). Use it directly instead of re-fetching;
			// the re-fetch was paying a cache miss + apiserver round-trip
			// per fresh handler.
			existing = *un
		case apierrors.IsAlreadyExists(createErr):
			// Lost the race to a concurrent handler. Fetch the existing
			// object so the status update below operates on the right
			// ResourceVersion.
			if err := o.cfg.KubeClient.Get(ctx, types.NamespacedName{Name: name}, &existing); err != nil {
				return fmt.Errorf("re-fetch UpcomingNode after AlreadyExists: %w", err)
			}
		default:
			return fmt.Errorf("create UpcomingNode: %w", createErr)
		}
	case getErr != nil:
		return fmt.Errorf("get UpcomingNode: %w", getErr)
	default:
		// ADR-0016: refresh Spec on every update so observers see the
		// latest labels/resources/taints. The shard always sends them
		// (or empty maps for pre-host-binding states); we always
		// write them through.
		freshSpec := upcomingNodeSpecFromUpdate(u)
		if !upcomingSpecEqual(existing.Spec, freshSpec) {
			specPatched := existing.DeepCopy()
			specPatched.Spec = freshSpec
			updErr := o.cfg.KubeClient.Update(ctx, specPatched)
			metrics.OperatorUpcomingNodeWrites.WithLabelValues("spec_update", classifyWriteErr(updErr)).Inc()
			if updErr != nil {
				return fmt.Errorf("update UpcomingNode spec: %w", updErr)
			}
			existing = *specPatched
		}
	}

	// Drained is the terminal post-drain phase — observed as a state
	// transition from DRAINING back to IDLE. Without the previous-phase
	// check we'd mis-map this as Launched (the default IDLE mapping).
	if phase == bfv1alpha1.UpcomingNodeLaunched && existing.Status.Phase == bfv1alpha1.UpcomingNodeDraining {
		phase = bfv1alpha1.UpcomingNodeDrained
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
	statusErr := o.cfg.KubeClient.Status().Update(ctx, &existing)
	metrics.OperatorUpcomingNodeWrites.WithLabelValues("status_update", classifyWriteErr(statusErr)).Inc()
	if statusErr != nil {
		return fmt.Errorf("update UpcomingNode status: %w", statusErr)
	}
	return nil
}

// handleAvailableCapacityUpdate upserts an AvailableCapacity CR for
// the profile carried in the update. The supersedes_key is the profile
// fingerprint, so the CR name is stable per profile and successive
// updates rewrite the same object.
func (o *Operator) handleAvailableCapacityUpdate(ctx context.Context, u *pb.AvailableCapacityUpdate) error {
	if u == nil || u.GetSupersedesKey() == "" {
		return nil
	}
	name := availableCapacityName(u.GetSupersedesKey())

	reqs := availableCapacityRequirements(u.GetRequirements())
	resources := availableCapacityResources(u.GetResources())
	availability := availableCapacityConfidence(u.GetConfidence())

	var existing bfv1alpha1.AvailableCapacity
	getErr := o.cfg.KubeClient.Get(ctx, types.NamespacedName{Name: name}, &existing)
	switch {
	case apierrors.IsNotFound(getErr):
		ac := &bfv1alpha1.AvailableCapacity{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: bfv1alpha1.AvailableCapacitySpec{
				Requirements:   reqs,
				Resources:      resources,
				AvailableCount: u.GetAvailableCount(),
				Availability:   availability,
			},
		}
		if err := o.cfg.KubeClient.Create(ctx, ac); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create AvailableCapacity: %w", err)
		}
		return nil
	case getErr != nil:
		return fmt.Errorf("get AvailableCapacity: %w", getErr)
	}

	existing.Spec.Requirements = reqs
	existing.Spec.Resources = resources
	existing.Spec.AvailableCount = u.GetAvailableCount()
	existing.Spec.Availability = availability
	if err := o.cfg.KubeClient.Update(ctx, &existing); err != nil {
		return fmt.Errorf("update AvailableCapacity: %w", err)
	}
	return nil
}

// availableCapacityName produces a DNS-1123 name from a profile
// fingerprint. The "ac-" prefix avoids collisions with user-named CRs.
func availableCapacityName(fingerprint string) string {
	return "ac-" + fingerprint
}

func availableCapacityRequirements(in []*pb.NodeSelectorRequirement) []corev1.NodeSelectorRequirement {
	if len(in) == 0 {
		return nil
	}
	out := make([]corev1.NodeSelectorRequirement, 0, len(in))
	for _, r := range in {
		out = append(out, corev1.NodeSelectorRequirement{
			Key:      r.GetKey(),
			Operator: corev1.NodeSelectorOperator(requirementOperatorString(r.GetOperator())),
			Values:   append([]string(nil), r.GetValues()...),
		})
	}
	return out
}

func availableCapacityResources(in *pb.Resources) corev1.ResourceList {
	out := corev1.ResourceList{}
	if in == nil {
		return out
	}
	for k, v := range in.GetResources() {
		q, err := resource.ParseQuantity(v)
		if err != nil {
			continue
		}
		out[corev1.ResourceName(k)] = q
	}
	return out
}

func availableCapacityConfidence(c pb.AvailableCapacityUpdate_Confidence) bfv1alpha1.Confidence {
	switch c {
	case pb.AvailableCapacityUpdate_CONFIDENCE_LOW:
		return bfv1alpha1.ConfidenceLow
	case pb.AvailableCapacityUpdate_CONFIDENCE_MEDIUM:
		return bfv1alpha1.ConfidenceMedium
	case pb.AvailableCapacityUpdate_CONFIDENCE_HIGH:
		return bfv1alpha1.ConfidenceHigh
	}
	return bfv1alpha1.ConfidenceNone
}

// upcomingNodeName produces a stable Kubernetes name for an
// UpcomingNode given a machine id. Kubernetes object names must be
// DNS-1123 (lowercase alnum + hyphens), which BigFleet machine IDs
// already are by convention.
func upcomingNodeName(machineID string) string {
	return "un-" + machineID
}

// upcomingNodeSpecFromUpdate builds an UpcomingNodeSpec from the
// shard's NodeStateUpdate per ADR-0016. Empty Labels/Resources/Taints
// are preserved as zero values; the spec round-trips faithfully.
func upcomingNodeSpecFromUpdate(u *pb.NodeStateUpdate) bfv1alpha1.UpcomingNodeSpec {
	spec := bfv1alpha1.UpcomingNodeSpec{
		Resources: corev1.ResourceList{},
	}
	if l := u.GetLabels(); len(l) > 0 {
		spec.Labels = make(map[string]string, len(l))
		for k, v := range l {
			spec.Labels[k] = v
		}
	}
	if r := u.GetResources(); r != nil {
		for k, v := range r.GetResources() {
			q, err := resource.ParseQuantity(v)
			if err != nil {
				// Skip malformed quantity rather than fail the
				// whole update; the shard should be sending valid
				// values, so this is a should-not-happen path.
				continue
			}
			spec.Resources[corev1.ResourceName(k)] = q
		}
	}
	if ts := u.GetTaints(); len(ts) > 0 {
		spec.Taints = make([]corev1.Taint, 0, len(ts))
		for _, t := range ts {
			spec.Taints = append(spec.Taints, corev1.Taint{
				Key:    t.GetKey(),
				Value:  t.GetValue(),
				Effect: corev1.TaintEffect(t.GetEffect()),
			})
		}
	}
	return spec
}

// upcomingSpecEqual compares two specs cheaply for the "do we need
// to write?" check. Any structural difference returns false.
func upcomingSpecEqual(a, b bfv1alpha1.UpcomingNodeSpec) bool {
	if len(a.Labels) != len(b.Labels) {
		return false
	}
	for k, v := range a.Labels {
		if b.Labels[k] != v {
			return false
		}
	}
	if len(a.Resources) != len(b.Resources) {
		return false
	}
	for k, v := range a.Resources {
		bv, ok := b.Resources[k]
		if !ok || v.Cmp(bv) != 0 {
			return false
		}
	}
	if len(a.Taints) != len(b.Taints) {
		return false
	}
	for i := range a.Taints {
		if a.Taints[i] != b.Taints[i] {
			return false
		}
	}
	return true
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
	case pb.MachineState_MACHINE_STATE_DRAINING:
		return bfv1alpha1.UpcomingNodeDraining
	case pb.MachineState_MACHINE_STATE_FAILED:
		return bfv1alpha1.UpcomingNodeFailed
	}
	return bfv1alpha1.UpcomingNodeProvisioning
}
