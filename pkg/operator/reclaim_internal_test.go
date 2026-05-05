package operator

import (
	"context"
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
)

// TestCordonNode_PatchesUnschedulable: cordonNode flips the named
// node to Spec.Unschedulable=true via JSON merge-patch. Mirrors the
// real reclaim path.
func TestCordonNode_PatchesUnschedulable(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()

	o := &Operator{cfg: Config{KubeClient: c}, log: slog.Default()}
	if err := o.cordonNode(context.Background(), "node-a"); err != nil {
		t.Fatalf("cordonNode: %v", err)
	}

	var got corev1.Node
	if err := c.Get(context.Background(), types.NamespacedName{Name: "node-a"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.Spec.Unschedulable {
		t.Errorf("Spec.Unschedulable = false, want true after cordon")
	}
}

// TestCordonNode_NotFoundIsNoOp: cordoning a node that doesn't exist
// returns nil. The operator may receive a stale ReclaimInstruction
// referencing an already-deleted node; that's not a hard failure.
func TestCordonNode_NotFoundIsNoOp(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).Build()
	o := &Operator{cfg: Config{KubeClient: c}, log: slog.Default()}
	if err := o.cordonNode(context.Background(), "ghost"); err != nil {
		t.Errorf("cordonNode of absent node should be a no-op, got: %v", err)
	}
}

// TestMarkUpcomingNodeDraining_PatchesPhase: locating an UpcomingNode
// by NodeRef and patching its phase to Draining via the status
// subresource.
func TestMarkUpcomingNodeDraining_PatchesPhase(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := bfv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("bfv1alpha1.AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("corev1.AddToScheme: %v", err)
	}
	un := &bfv1alpha1.UpcomingNode{
		ObjectMeta: metav1.ObjectMeta{Name: "upn-machine-7"},
		Status: bfv1alpha1.UpcomingNodeStatus{
			Phase:   bfv1alpha1.UpcomingNodeReady,
			NodeRef: &corev1.ObjectReference{Kind: "Node", Name: "node-a"},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&bfv1alpha1.UpcomingNode{}).
		WithObjects(un).
		Build()

	o := &Operator{cfg: Config{KubeClient: c}, log: slog.Default()}
	if err := o.markUpcomingNodeDraining(context.Background(), "node-a"); err != nil {
		t.Fatalf("markUpcomingNodeDraining: %v", err)
	}

	var got bfv1alpha1.UpcomingNode
	if err := c.Get(context.Background(), client.ObjectKey{Name: "upn-machine-7"}, &got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status.Phase != bfv1alpha1.UpcomingNodeDraining {
		t.Errorf("Status.Phase = %q, want %q", got.Status.Phase, bfv1alpha1.UpcomingNodeDraining)
	}
}

// TestIsDaemonSetPod: filter-out logic for non-evictable pods.
func TestIsDaemonSetPod(t *testing.T) {
	cases := []struct {
		name string
		ref  string
		want bool
	}{
		{"daemonset", "DaemonSet", true},
		{"replicaset", "ReplicaSet", false},
		{"statefulset", "StatefulSet", false},
		{"no-owner", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pod := &corev1.Pod{}
			if c.ref != "" {
				pod.OwnerReferences = []metav1.OwnerReference{{Kind: c.ref}}
			}
			if got := isDaemonSetPod(pod); got != c.want {
				t.Errorf("isDaemonSetPod(%s) = %v, want %v", c.ref, got, c.want)
			}
		})
	}
}
