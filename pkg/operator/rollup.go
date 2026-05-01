package operator

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
	"github.com/intUnderflow/bigfleet/pkg/conv"
	"github.com/intUnderflow/bigfleet/pkg/metrics"
	"github.com/intUnderflow/bigfleet/pkg/needs"
	pb "github.com/intUnderflow/bigfleet/pkg/proto/bigfleet/v1alpha1"
)

// rollupLoop fires every Config.RollupInterval. Each iteration lists
// all CapacityRequest CRs in the cluster, aggregates them by Profile
// fingerprint, builds a ClusterCapacityNeeds proto, and enqueues it for
// the send goroutine. Pending CRs included in the rollup are then
// status-written to Acknowledged (single transition per CR, ever).
func (o *Operator) rollupLoop(ctx context.Context, sess *session) error {
	ticker := time.NewTicker(o.cfg.RollupInterval)
	defer ticker.Stop()
	// Fire one immediately on connect so the shard has fresh demand
	// without waiting for the first tick.
	if err := o.runRollup(ctx, sess); err != nil {
		o.log.Warn("initial rollup failed", "err", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := o.runRollup(ctx, sess); err != nil {
				o.log.Warn("rollup failed", "err", err)
			}
		}
	}
}

// runRollup performs one rollup cycle.
func (o *Operator) runRollup(ctx context.Context, sess *session) error {
	start := time.Now()
	defer func() {
		metrics.OperatorRollupDuration.Observe(time.Since(start).Seconds())
	}()
	crs, err := o.listCapacityRequests(ctx)
	if err != nil {
		return fmt.Errorf("list CapacityRequests: %w", err)
	}

	rollup, pending := o.buildRollup(crs)
	if err := sess.enqueue(ctx, &pb.OperatorMessage{
		Payload: &pb.OperatorMessage_Rollup{Rollup: rollup},
	}); err != nil {
		return fmt.Errorf("enqueue rollup: %w", err)
	}

	// Mark the included Pending CRs Acknowledged. Single status write
	// per CR, ever — the paper specifies the transition is one-way.
	// Run in parallel up to AcknowledgeConcurrency to absorb status
	// writes on large rollup batches without blocking the next cycle.
	if len(pending) > 0 {
		o.acknowledgeAll(ctx, pending)
	}
	return nil
}

func (o *Operator) acknowledgeAll(ctx context.Context, crs []bfv1alpha1.CapacityRequest) {
	type job struct {
		cr bfv1alpha1.CapacityRequest
	}
	jobs := make(chan job)
	var wg sync.WaitGroup
	concurrency := o.cfg.AcknowledgeConcurrency
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				if err := o.markAcknowledged(ctx, j.cr); err != nil {
					o.log.Warn("Acknowledge status update", "cr", j.cr.Name, "err", err)
				}
			}
		}()
	}
	for _, cr := range crs {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		case jobs <- job{cr: cr}:
		}
	}
	close(jobs)
	wg.Wait()
}

// listCapacityRequests fetches every CapacityRequest in the cluster.
func (o *Operator) listCapacityRequests(ctx context.Context) ([]bfv1alpha1.CapacityRequest, error) {
	var list bfv1alpha1.CapacityRequestList
	if err := o.cfg.KubeClient.List(ctx, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// buildRollup aggregates CRs by Profile fingerprint and builds the
// proto message. Returns the rollup plus the list of CRs that were
// Pending at observation time (for the Pending → Acknowledged write).
func (o *Operator) buildRollup(crs []bfv1alpha1.CapacityRequest) (*pb.ClusterCapacityNeeds, []bfv1alpha1.CapacityRequest) {
	pending := make([]bfv1alpha1.CapacityRequest, 0)
	rawNeeds := make([]needs.Need, 0, len(crs))
	for i := range crs {
		cr := &crs[i]
		profile, err := profileFromCapacityRequest(cr)
		if err != nil {
			o.log.Warn("skipping CR with bad profile", "cr", cr.Name, "ns", cr.Namespace, "err", err)
			continue
		}
		rawNeeds = append(rawNeeds, needs.Need{
			ClusterID: o.cfg.ClusterID,
			Profile:   profile,
			Count:     1,
		})
		if cr.Status.Phase == "" || cr.Status.Phase == bfv1alpha1.CapacityRequestPending {
			pending = append(pending, *cr)
		}
	}
	aggregated := needs.Aggregate(rawNeeds)

	out := &pb.ClusterCapacityNeeds{
		ClusterId:          string(o.cfg.ClusterID),
		TimestampUnixNanos: time.Now().UnixNano(),
		Needs:              make([]*pb.CapacityNeed, 0, len(aggregated)),
	}
	for _, n := range aggregated {
		out.Needs = append(out.Needs, profileToCapacityNeed(n.Profile, int32(n.Count)))
	}
	return out, pending
}

// profileFromCapacityRequest maps a CR's spec into a needs.Profile.
// Penalties are bucketed at this point; the cluster operator is the
// canonical place where raw dollar values become PenaltyBuckets.
func profileFromCapacityRequest(cr *bfv1alpha1.CapacityRequest) (needs.Profile, error) {
	reqs := make([]needs.Requirement, 0, len(cr.Spec.Requirements))
	for _, r := range cr.Spec.Requirements {
		op, err := requirementOperatorFromCore(r.Operator)
		if err != nil {
			return needs.Profile{}, err
		}
		reqs = append(reqs, needs.Requirement{
			Key:      r.Key,
			Operator: op,
			Values:   append([]string(nil), r.Values...),
		})
	}
	res := make([]needs.ResourceQty, 0, len(cr.Spec.Resources))
	for name, q := range cr.Spec.Resources {
		// String() returns the canonical form (e.g., "768Gi", "96").
		quantity := q
		res = append(res, needs.ResourceQty{Name: string(name), Quantity: quantity.String()})
	}
	spread := make([]needs.TopologySpread, 0, len(cr.Spec.TopologySpread))
	for _, s := range cr.Spec.TopologySpread {
		spread = append(spread, needs.TopologySpread{
			TopologyKey:       s.TopologyKey,
			MaxSkew:           s.MaxSkew,
			WhenUnsatisfiable: whenUnsatisfiableFromCore(s.WhenUnsatisfiable),
		})
	}
	intBucket := needs.PenaltyBucketUnspecified
	if cr.Spec.InterruptionPenalty != nil {
		v, _ := cr.Spec.InterruptionPenalty.AsInt64()
		intBucket = needs.BucketForDollars(float64(v))
	}
	recBucket := needs.PenaltyBucketUnspecified
	if cr.Spec.ReclamationPenalty != nil {
		v, _ := cr.Spec.ReclamationPenalty.AsInt64()
		recBucket = needs.BucketForDollars(float64(v))
	}
	return needs.NewProfile(reqs, res, spread, cr.Spec.Priority, intBucket, recBucket), nil
}

// requirementOperatorFromCore maps the core/v1 NodeSelectorRequirement
// operator into the needs domain enum. Standard operators only — Same
// is protobuf-only and the operator emits it later when it detects
// co-location signals (out of M4 scope).
func requirementOperatorFromCore(op corev1.NodeSelectorOperator) (needs.Operator, error) {
	switch op {
	case corev1.NodeSelectorOpIn:
		return needs.OperatorIn, nil
	case corev1.NodeSelectorOpNotIn:
		return needs.OperatorNotIn, nil
	case corev1.NodeSelectorOpExists:
		return needs.OperatorExists, nil
	case corev1.NodeSelectorOpDoesNotExist:
		return needs.OperatorDoesNotExist, nil
	}
	return 0, fmt.Errorf("unsupported NodeSelectorOperator: %v", op)
}

func whenUnsatisfiableFromCore(w corev1.UnsatisfiableConstraintAction) needs.WhenUnsatisfiable {
	switch w {
	case corev1.DoNotSchedule:
		return needs.WhenUnsatisfiableDoNotSchedule
	case corev1.ScheduleAnyway:
		return needs.WhenUnsatisfiableScheduleAnyway
	}
	return needs.WhenUnsatisfiableUnspecified
}

// profileToCapacityNeed builds a wire-format CapacityNeed from a domain
// Profile + count. Inverse of conv.NeedsFromRollup for the operator's
// outbound direction.
func profileToCapacityNeed(p needs.Profile, count int32) *pb.CapacityNeed {
	out := &pb.CapacityNeed{
		Requirements:              conv.RequirementsToProto(p.Requirements()),
		Priority:                  p.Priority(),
		Count:                     count,
		InterruptionPenaltyBucket: pb.PenaltyBucket(p.InterruptionPenaltyBucket()),
		ReclamationPenaltyBucket:  pb.PenaltyBucket(p.ReclamationPenaltyBucket()),
	}
	if res := p.Resources(); len(res) > 0 {
		out.Resources = make(map[string]string, len(res))
		for _, r := range res {
			out.Resources[r.Name] = r.Quantity
		}
	}
	for _, s := range p.Spread() {
		var w pb.TopologySpread_WhenUnsatisfiable
		switch s.WhenUnsatisfiable {
		case needs.WhenUnsatisfiableDoNotSchedule:
			w = pb.TopologySpread_WHEN_UNSATISFIABLE_DO_NOT_SCHEDULE
		case needs.WhenUnsatisfiableScheduleAnyway:
			w = pb.TopologySpread_WHEN_UNSATISFIABLE_SCHEDULE_ANYWAY
		default:
			w = pb.TopologySpread_WHEN_UNSATISFIABLE_UNSPECIFIED
		}
		out.Spread = append(out.Spread, &pb.TopologySpread{
			TopologyKey:       s.TopologyKey,
			MaxSkew:           s.MaxSkew,
			WhenUnsatisfiable: w,
		})
	}
	return out
}

// markAcknowledged transitions a CR from Pending to Acknowledged via
// status subresource. Idempotent: re-running on an already-Acknowledged
// CR is a no-op.
func (o *Operator) markAcknowledged(ctx context.Context, cr bfv1alpha1.CapacityRequest) error {
	if cr.Status.Phase == bfv1alpha1.CapacityRequestAcknowledged {
		return nil
	}
	// Refetch to avoid status conflicts with whichever controller
	// created the CR most recently.
	var fresh bfv1alpha1.CapacityRequest
	if err := o.cfg.KubeClient.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, &fresh); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // CR garbage-collected since the rollup; fine.
		}
		return err
	}
	if fresh.Status.Phase == bfv1alpha1.CapacityRequestAcknowledged {
		return nil
	}
	fresh.Status.Phase = bfv1alpha1.CapacityRequestAcknowledged
	now := metav1.Now()
	fresh.Status.AcknowledgedAt = &now
	if err := o.cfg.KubeClient.Status().Update(ctx, &fresh); err != nil {
		return err
	}
	metrics.OperatorAcknowledgedTotal.Inc()
	return nil
}

// (avoid unused import warnings if buildRollup doesn't reach client)
var _ client.Client = nil //nolint:unused
