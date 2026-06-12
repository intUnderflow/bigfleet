package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Confidence is the qualitative signal on AvailableCapacity. Real capacity
// is inherently racy; consumers must treat AvailableCapacity as a hint,
// not a reservation.
type Confidence string

const (
	ConfidenceNone   Confidence = "None"
	ConfidenceLow    Confidence = "Low"
	ConfidenceMedium Confidence = "Medium"
	ConfidenceHigh   Confidence = "High"
)

// AvailableCapacitySpec is the operator-visible projection of the shard's
// view of what it could provision. Eventually consistent.
type AvailableCapacitySpec struct {
	// Requirements describe the matchable shape of this capacity.
	// +optional
	Requirements []corev1.NodeSelectorRequirement `json:"requirements,omitempty"`

	// Resources the capacity would expose per machine.
	Resources corev1.ResourceList `json:"resources"`

	// AvailableCount is the shard's hint about how many machines of this
	// shape it could provide soon.
	AvailableCount int32 `json:"availableCount"`

	// +kubebuilder:validation:Enum=None;Low;Medium;High
	Availability Confidence `json:"availability"`

	// Cost is the per-hour USD cost the shard expects to charge for this
	// capacity.
	Cost resource.Quantity `json:"cost"`

	// M68b (philosophy-conformance audit): supportsAtomicProvisioning,
	// estimatedProvisioningTime and nodeTemplate were removed — their
	// wire fields were deleted in M66.1 (AvailableCapacityUpdate
	// reserved 7–9), so no shard could ever populate them.
}

// AvailableCapacityStatus is reserved for future operator-side bookkeeping.
// In v1alpha1 the spec carries everything; status is intentionally empty.
type AvailableCapacityStatus struct{}

// AvailableCapacity is an eventually-consistent hint about provisionable
// capacity. Cluster-scoped, lives in fleet-system per the paper.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=avc,categories=fleet
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Availability",type=string,JSONPath=`.spec.availability`
// +kubebuilder:printcolumn:name="Available",type=integer,JSONPath=`.spec.availableCount`
// +kubebuilder:printcolumn:name="Cost",type=string,JSONPath=`.spec.cost`
type AvailableCapacity struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AvailableCapacitySpec   `json:"spec"`
	Status AvailableCapacityStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type AvailableCapacityList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AvailableCapacity `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AvailableCapacity{}, &AvailableCapacityList{})
}
