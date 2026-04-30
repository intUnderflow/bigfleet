// Package v1alpha1 contains the Kubernetes types for the bigfleet.lucy.sh
// API group: CapacityRequest, AvailableCapacity, UpcomingNode.
//
// +kubebuilder:object:generate=true
// +groupName=bigfleet.lucy.sh
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the API group / version for this package.
	GroupVersion = schema.GroupVersion{Group: "bigfleet.lucy.sh", Version: "v1alpha1"}

	// SchemeBuilder registers the types in this package with a scheme.
	// scheme.Builder is the canonical kubebuilder pattern; the SA1019
	// deprecation tag on it is upstream noise we don't want to chase.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion} //nolint:staticcheck

	// AddToScheme adds the types in this group/version to a scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
