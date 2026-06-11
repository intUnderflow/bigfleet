package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
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
	rollupStart := time.Now()
	listStart := rollupStart
	crs, err := o.listCapacityRequests(ctx)
	metrics.OperatorRollupPhaseDuration.WithLabelValues("list").Observe(time.Since(listStart).Seconds())
	if err != nil {
		metrics.OperatorRollupDuration.Observe(time.Since(rollupStart).Seconds())
		return fmt.Errorf("list CapacityRequests: %w", err)
	}
	buildStart := time.Now()
	rollup, pending := o.buildRollup(crs)
	metrics.OperatorRollupPhaseDuration.WithLabelValues("build").Observe(time.Since(buildStart).Seconds())

	// enqueueRollup atomically replaces any pending older rollup (each
	// rollup is full replacement per paper §3.1) so the operator never
	// queues two rollups behind a slow stream. Never errors — the
	// pending slot always accepts.
	enqueueStart := time.Now()
	sess.enqueueRollup(&pb.OperatorMessage{
		Payload: &pb.OperatorMessage_Rollup{Rollup: rollup},
	})
	metrics.OperatorRollupPhaseDuration.WithLabelValues("enqueue").Observe(time.Since(enqueueStart).Seconds())
	// Rollup hot path is done. Record before the (potentially long)
	// acknowledgement batch — its duration is exposed separately.
	metrics.OperatorRollupDuration.Observe(time.Since(rollupStart).Seconds())

	// Mark the included Pending CRs Acknowledged. Single status write
	// per CR, ever — the paper specifies the transition is one-way.
	// Run in parallel up to AcknowledgeConcurrency to absorb status
	// writes on large rollup batches without blocking the next cycle.
	if len(pending) > 0 {
		ackStart := time.Now()
		o.acknowledgeAll(ctx, pending)
		metrics.OperatorAcknowledgeDuration.Observe(time.Since(ackStart).Seconds())
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

// buildRollup aggregates CRs by Profile fingerprint and co-location
// group, then builds the proto message. Returns the rollup plus the
// list of CRs that were Pending at observation time.
//
// ADR-0027: each CR contributes one Pod's worth of demand. Its resource
// request is both the CR's AggregateResources and its MinUnit (one CR =
// one Pod = the atomic schedulable unit). needs.Aggregate then sums
// AggregateResources and maxes MinUnit across same-(Profile, Group)
// Needs, so each wire CapacityNeed carries the constraint set's total
// resource demand plus the largest single unit that must fit on one
// machine. No Pod count crosses the wire — machine count is the
// autoscaler's output.
//
// Co-location (paper §8, ADR-0024): a CR's co-location group is the
// canonical form of its Spec.CoLocation term — the projection of the
// source pod's required podAffinity. CRs with an equal term aggregate
// into one Need, and the operator appends a Same requirement on the
// term's TopologyKey; CRs with different terms (independent workloads)
// stay separate even when their profiles are identical, so each is
// co-located onto its own domain. CRs with no CoLocation get an empty
// group and aggregate purely by Profile fingerprint — the common case,
// which keeps the roll-up ~2KB regardless of fleet size (paper §11).
func (o *Operator) buildRollup(crs []bfv1alpha1.CapacityRequest) (*pb.ClusterCapacityNeeds, []bfv1alpha1.CapacityRequest) {
	pending := make([]bfv1alpha1.CapacityRequest, 0)
	rawNeeds := make([]needs.Need, 0, len(crs))
	for i := range crs {
		cr := &crs[i]
		profile, podResources, err := profileFromCapacityRequest(cr)
		if err != nil {
			o.log.Warn("skipping CR with bad profile", "cr", cr.Name, "ns", cr.Namespace, "err", err)
			continue
		}
		group := coLocationGroup(cr)
		if group != "" {
			profile = withSameRequirement(profile, cr.Spec.CoLocation.TopologyKey)
		}
		// One CR = one Pod: its resource request is both the unit of
		// aggregate demand and the atomic unit that must fit on a single
		// machine. needs.Aggregate sums the former and maxes the latter.
		rawNeeds = append(rawNeeds, needs.Need{
			ClusterID:          o.cfg.ClusterID,
			Profile:            profile,
			AggregateResources: podResources,
			MinUnit:            podResources,
			Group:              group,
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
		out.Needs = append(out.Needs, needToCapacityNeed(n))
	}
	return out, pending
}

// coLocationGroup returns the canonical aggregation key for a CR's
// co-location term, or "" when the CR declared no co-location (or a
// term with no topology key). CRs with an equal key aggregate into one
// Need and carry a Same requirement; CRs with different keys —
// independent workloads — stay separate. ADR-0024: derived from
// Spec.CoLocation (the source pod's required podAffinity projection),
// not from ownerReferences (which the UPC sets to the Pod, for GC).
func coLocationGroup(cr *bfv1alpha1.CapacityRequest) string {
	t := cr.Spec.CoLocation
	if t == nil || t.TopologyKey == "" {
		return ""
	}
	return t.TopologyKey + "\x00" + canonicalLabelSelector(t.LabelSelector)
}

// canonicalLabelSelector serialises a LabelSelector deterministically:
// matchLabels by sorted key, matchExpressions sorted by (key, operator,
// values). Two selectors that select the same set produce the same
// string regardless of map-iteration or slice order, so co-located
// pods of one workload always land in the same aggregation group.
func canonicalLabelSelector(sel *metav1.LabelSelector) string {
	if sel == nil {
		return ""
	}
	var b strings.Builder
	keys := make([]string, 0, len(sel.MatchLabels))
	for k := range sel.MatchLabels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(sel.MatchLabels[k])
		b.WriteByte(',')
	}
	exprs := append([]metav1.LabelSelectorRequirement(nil), sel.MatchExpressions...)
	for i := range exprs {
		vs := append([]string(nil), exprs[i].Values...)
		sort.Strings(vs)
		exprs[i].Values = vs
	}
	sort.Slice(exprs, func(i, j int) bool {
		if exprs[i].Key != exprs[j].Key {
			return exprs[i].Key < exprs[j].Key
		}
		if exprs[i].Operator != exprs[j].Operator {
			return exprs[i].Operator < exprs[j].Operator
		}
		return strings.Join(exprs[i].Values, ",") < strings.Join(exprs[j].Values, ",")
	})
	b.WriteByte(';')
	for _, e := range exprs {
		b.WriteString(e.Key)
		b.WriteByte(' ')
		b.WriteString(string(e.Operator))
		b.WriteByte(' ')
		b.WriteString(strings.Join(e.Values, ","))
		b.WriteByte(';')
	}
	return b.String()
}

// withSameRequirement returns a copy of profile with a Same requirement
// on key appended. Idempotent: if the profile already has a Same on
// the same key, returns it unchanged.
func withSameRequirement(p needs.Profile, key string) needs.Profile {
	for _, r := range p.Requirements() {
		if r.Operator == needs.OperatorSame && r.Key == key {
			return p
		}
	}
	reqs := append([]needs.Requirement(nil), p.Requirements()...)
	reqs = append(reqs, needs.Requirement{Key: key, Operator: needs.OperatorSame})
	return needs.NewProfile(reqs, p.Spread(), p.Priority(), p.InterruptionPenaltyBucket(), p.ReclamationPenaltyBucket())
}

// profileFromCapacityRequest maps a CR's spec into a needs.Profile (the
// aggregation identity) and the CR's per-Pod resource request vector.
// ADR-0027: the resource request is no longer part of the Profile — it
// is the demand the operator sums into a CapacityNeed. Penalties are
// bucketed here; the operator is the canonical place where raw dollar
// values become PenaltyBuckets.
func profileFromCapacityRequest(cr *bfv1alpha1.CapacityRequest) (needs.Profile, []needs.ResourceQty, error) {
	reqs := make([]needs.Requirement, 0, len(cr.Spec.Requirements))
	for _, r := range cr.Spec.Requirements {
		op, err := requirementOperatorFromCore(r.Operator)
		if err != nil {
			return needs.Profile{}, nil, err
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
	return needs.NewProfile(reqs, spread, cr.Spec.Priority, intBucket, recBucket), res, nil
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

// needToCapacityNeed builds a wire-format CapacityNeed from an
// aggregated domain Need. Inverse of conv.NeedsFromRollup for the
// operator's outbound direction (ADR-0027: a constrained aggregate
// resource request — requirements + aggregate_resources + min_unit).
func needToCapacityNeed(n needs.Need) *pb.CapacityNeed {
	p := n.Profile
	out := &pb.CapacityNeed{
		Requirements:              conv.RequirementsToProto(p.Requirements()),
		AggregateResources:        resourceQtyMap(n.AggregateResources),
		MinUnit:                   resourceQtyMap(n.MinUnit),
		Group:                     n.Group,
		Priority:                  p.Priority(),
		InterruptionPenaltyBucket: pb.PenaltyBucket(p.InterruptionPenaltyBucket()),
		ReclamationPenaltyBucket:  pb.PenaltyBucket(p.ReclamationPenaltyBucket()),
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

// resourceQtyMap converts a []needs.ResourceQty into the proto's
// name→quantity-string map shape. Returns nil for an empty vector so
// the wire message stays compact.
func resourceQtyMap(rs []needs.ResourceQty) map[string]string {
	if len(rs) == 0 {
		return nil
	}
	out := make(map[string]string, len(rs))
	for _, r := range rs {
		out[r.Name] = r.Quantity
	}
	return out
}

// markAcknowledged transitions a CR from Pending to Acknowledged via
// a JSON merge-patch on the status subresource. Idempotent: re-running
// on an already-Acknowledged CR is a no-op.
//
// Implementation note: this used to be a Get + Status().Update round
// trip (two apiserver calls per CR). For a 1K-CR ramp burst that
// doubled the apiserver load on the operator's path. The merge-patch
// version is one call per CR, with no resource-version conflicts
// since merge-patch doesn't carry a precondition. JSON merge-patch
// (not strategic merge) because CRDs don't support strategic merge.
func (o *Operator) markAcknowledged(ctx context.Context, cr bfv1alpha1.CapacityRequest) error {
	if cr.Status.Phase == bfv1alpha1.CapacityRequestAcknowledged {
		return nil
	}
	now := metav1.Now()
	patch, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"phase":          string(bfv1alpha1.CapacityRequestAcknowledged),
			"acknowledgedAt": now.Format(time.RFC3339),
		},
	})
	if err != nil {
		return err
	}
	target := &bfv1alpha1.CapacityRequest{
		ObjectMeta: metav1.ObjectMeta{Name: cr.Name, Namespace: cr.Namespace},
	}
	if err := o.cfg.KubeClient.Status().Patch(ctx, target, client.RawPatch(types.MergePatchType, patch)); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // CR garbage-collected since the rollup; fine.
		}
		return err
	}
	metrics.OperatorAcknowledgedTotal.Inc()
	return nil
}

// (avoid unused import warnings if buildRollup doesn't reach client)
var _ client.Client = nil //nolint:unused
