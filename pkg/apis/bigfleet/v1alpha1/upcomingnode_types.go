package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// UpcomingNodePhase is the lifecycle phase of a node BigFleet is bringing
// online. The cluster operator drives transitions from NodeStateUpdate
// frames received over the Shard.Session stream.
type UpcomingNodePhase string

const (
	UpcomingNodeProvisioning UpcomingNodePhase = "Provisioning"
	UpcomingNodeLaunched     UpcomingNodePhase = "Launched"
	UpcomingNodeRegistered   UpcomingNodePhase = "Registered"
	UpcomingNodeReady        UpcomingNodePhase = "Ready"
	// UpcomingNodeDraining is set while the shard is draining workloads
	// off the node (machine state == DRAINING). M14: workloads that
	// `kubectl get upcomingnodes` during a Reclaim see this phase
	// during the drain window.
	UpcomingNodeDraining UpcomingNodePhase = "Draining"
	// UpcomingNodeDrained is the terminal phase set when drain
	// completes — observed as a NodeStateUpdate transition from
	// DRAINING back to IDLE. Distinguishes a freshly-released machine
	// from a fresh Provisioning Idle. The operator may delete the CR
	// shortly after, but the Drained window gives observers a final
	// "the machine has been released from this workload" signal.
	UpcomingNodeDrained UpcomingNodePhase = "Drained"
	UpcomingNodeFailed  UpcomingNodePhase = "Failed"
)

// UpcomingNodeSpec describes the node that's coming online. Set when the
// shard provisions a machine; updated only if the machine's identity
// changes (which it doesn't — machine_id is stable).
type UpcomingNodeSpec struct {
	// Labels the node will carry once it joins. Useful for scheduling
	// decisions that need to make a placement choice before the node Ready
	// transition (e.g., topology-spread bookkeeping).
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Resources the node will expose as allocatable.
	Resources corev1.ResourceList `json:"resources"`

	// Taints the node will register with.
	// +optional
	Taints []corev1.Taint `json:"taints,omitempty"`
}

type UpcomingNodeStatus struct {
	// +kubebuilder:validation:Enum=Provisioning;Launched;Registered;Ready;Draining;Drained;Failed
	Phase UpcomingNodePhase `json:"phase"`

	// NodeRef is set once the kubelet has registered and the Kubernetes
	// node object exists.
	// +optional
	NodeRef *corev1.ObjectReference `json:"nodeRef,omitempty"`

	// ProviderID echoes the cloud / bare-metal identifier (e.g.,
	// "aws:///us-east-1a/i-0abc123def456") for operator-side debugging.
	// +optional
	ProviderID string `json:"providerID,omitempty"`

	// ProvisioningStartTime is when the shard first told the operator
	// about this node.
	// +optional
	ProvisioningStartTime *metav1.Time `json:"provisioningStartTime,omitempty"`

	// M68b (philosophy-conformance audit): estimatedReadyTime was
	// removed — nothing ever populated it and the "age out stuck
	// upcoming nodes" consumer its doc comment named never existed.

	// LastError is populated when Phase == Failed.
	// +optional
	LastError string `json:"lastError,omitempty"`
}

// UpcomingNode is a placeholder Kubernetes object that signals "a node
// matching this spec is being provisioned". Cluster-scoped, lives in the
// fleet-system namespace per the paper's kubectl experience.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=upn,categories=fleet
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.status.nodeRef.name`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type UpcomingNode struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   UpcomingNodeSpec   `json:"spec"`
	Status UpcomingNodeStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type UpcomingNodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []UpcomingNode `json:"items"`
}

func init() {
	SchemeBuilder.Register(&UpcomingNode{}, &UpcomingNodeList{})
}
