package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CapacityRequestPhase mirrors the two-phase lifecycle from the paper:
// Pending → Acknowledged. The single transition is written by the cluster
// operator the first time a CR is included in a roll-up.
type CapacityRequestPhase string

const (
	// CapacityRequestPending is the initial phase, set by whatever creates
	// the CR. The cluster operator transitions it to Acknowledged on the
	// first roll-up that includes the CR.
	CapacityRequestPending CapacityRequestPhase = "Pending"

	// CapacityRequestAcknowledged means the CR has been observed by the
	// operator and included in at least one roll-up to the shard. After
	// this, the status is never written again.
	CapacityRequestAcknowledged CapacityRequestPhase = "Acknowledged"
)

// CapacityRequestSpec is the user-facing description of a capacity need.
//
// One CR per pod. Withdrawal is implicit: deleting the pod garbage-collects
// the CR via ownerReferences, which causes the next roll-up to omit it.
type CapacityRequestSpec struct {
	// Requirements are node-selector style match expressions. Standard
	// operators only — In, NotIn, Exists, DoesNotExist. Co-location ("Same"
	// rack / zone) is not expressed here; the cluster operator translates
	// pod-level signals into the protobuf-only Same operator at roll-up.
	// +optional
	Requirements []corev1.NodeSelectorRequirement `json:"requirements,omitempty"`

	// Resources are the per-machine resource requests.
	Resources corev1.ResourceList `json:"resources"`

	// Priority is the pod's PriorityClass value. Higher = served first.
	Priority int32 `json:"priority"`

	// TopologySpread is copied from the source pod's topologySpreadConstraints.
	// Pass-through to the autoscaler.
	// +optional
	TopologySpread []TopologySpreadConstraint `json:"topologySpread,omitempty"`

	// CoLocation is the autoscaler-relevant projection of the source pod's
	// required podAffinity — the structured "co-location signal" the
	// cluster operator translates into the protobuf-only Same requirement
	// at roll-up (paper §8). Absent means the pod declared no co-location;
	// the CR then aggregates freely with other same-shaped CRs. Symmetric
	// to TopologySpread, which is co-location's dual.
	// +optional
	CoLocation *CoLocationTerm `json:"coLocation,omitempty"`

	// InterruptionPenalty is the dollar cost of interrupting the workload
	// once. Used in the autoscaler's effective_cost calculation:
	//
	//   effective_cost = price + (interruption_probability × interruption_penalty)
	//
	// The autoscaler buckets penalties to a coarse log scale at aggregation
	// time (powers of 2 between $0.50 and $10M); two CRs whose raw values
	// fall in the same bucket aggregate together. See the bucket boundaries
	// in api/proto/.../capacity.proto.
	// +optional
	InterruptionPenalty *resource.Quantity `json:"interruptionPenalty,omitempty"`

	// ReclamationPenalty is the dollar cost tied to *this specific* set of
	// machines (e.g., burned-in GPUs, warmed caches). Distinct from
	// interruption penalty. Used in victim scoring and idle-tiebreaks.
	// Bucketed identically to InterruptionPenalty.
	// +optional
	ReclamationPenalty *resource.Quantity `json:"reclamationPenalty,omitempty"`
}

// TopologySpreadConstraint mirrors the autoscaler-relevant subset of
// corev1.TopologySpreadConstraint. Only fields the autoscaler honours.
type TopologySpreadConstraint struct {
	TopologyKey       string                               `json:"topologyKey"`
	MaxSkew           int32                                `json:"maxSkew"`
	WhenUnsatisfiable corev1.UnsatisfiableConstraintAction `json:"whenUnsatisfiable"`
}

// CoLocationTerm is the autoscaler-relevant projection of one required
// podAffinity term: the peer set this pod must be scheduled with, and
// the topology domain they share. The cluster operator turns it into a
// Same requirement on the term's TopologyKey at roll-up; CRs with an
// equal CoLocationTerm aggregate into one Need, while CRs with
// different terms stay separate so each workload is co-located onto its
// own domain.
type CoLocationTerm struct {
	// LabelSelector selects the peer pods this pod co-locates with —
	// the labelSelector of the source pod's podAffinity term. Nil
	// selects nothing; in practice the source pod also carries the
	// labels it selects on, so its own workload's pods share the term.
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty"`

	// TopologyKey is the node-label key defining the shared domain
	// (e.g. topology.kubernetes.io/zone, or a rack key). It becomes the
	// key of the Same requirement the operator emits.
	TopologyKey string `json:"topologyKey"`
}

// CapacityRequestStatus is written exactly once by the cluster operator
// on the first roll-up that includes this CR.
type CapacityRequestStatus struct {
	// +kubebuilder:validation:Enum=Pending;Acknowledged
	Phase CapacityRequestPhase `json:"phase"`

	// AcknowledgedAt is set when Phase transitions to Acknowledged.
	// +optional
	AcknowledgedAt *metav1.Time `json:"acknowledgedAt,omitempty"`
}

// CapacityRequest declares a single pod's resource need.
//
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cr;crs,categories=fleet
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Priority",type=integer,JSONPath=`.spec.priority`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
// +kubebuilder:selectablefield:JSONPath=`.status.phase`
type CapacityRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   CapacityRequestSpec   `json:"spec"`
	Status CapacityRequestStatus `json:"status,omitempty"`
}

// CapacityRequestList is a list of CapacityRequests.
//
// +kubebuilder:object:root=true
type CapacityRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []CapacityRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CapacityRequest{}, &CapacityRequestList{})
}
