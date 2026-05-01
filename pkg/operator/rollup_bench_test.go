package operator

import (
	"log/slog"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	bfv1alpha1 "github.com/intUnderflow/bigfleet/pkg/apis/bigfleet/v1alpha1"
)

// BenchmarkBuildRollup measures the in-pod CPU cost of one rollup
// cycle's compute work — list iteration, profileFromCapacityRequest
// per CR, the needs.Aggregate fold, and proto-encoding. Excludes
// the apiserver round-trip; that's covered by the cache change in
// M11.11.
//
// Sub-benchmarks scale by CR count (10 / 100 / 1000 / 10000) so
// the resulting graph shows whether the walk is O(N), O(N log N),
// or worse. M11.12 follow-up: read the resulting numbers and the
// pprof CPU profile to find the dominant frame.
func BenchmarkBuildRollup(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		b.Run(name(n), func(b *testing.B) {
			crs := makeCapacityRequests(n)
			op := makeOperator(b, &bfv1alpha1.CapacityRequestList{Items: crs})
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = op.buildRollup(crs)
			}
		})
	}
}

// BenchmarkProfileFromCapacityRequest isolates the per-CR cost of
// turning a CapacityRequest into a needs.Profile. This is the
// inner-most loop body in buildRollup.
func BenchmarkProfileFromCapacityRequest(b *testing.B) {
	crs := makeCapacityRequests(1)
	cr := &crs[0]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = profileFromCapacityRequest(cr)
	}
}

// BenchmarkListCapacityRequests measures the cost of listCapacityRequests
// against the controller-runtime fake client (no apiserver, no
// network). Isolates the deep-copy cost the cache-backed client also
// pays on every read.
func BenchmarkListCapacityRequests(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		b.Run(name(n), func(b *testing.B) {
			crs := makeCapacityRequests(n)
			op := makeOperator(b, &bfv1alpha1.CapacityRequestList{Items: crs})
			ctx := b.Context()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = op.listCapacityRequests(ctx)
			}
		})
	}
}

// BenchmarkRollupListPlusBuild combines the two: this is what
// runRollup measures as OperatorRollupDuration (modulo the stream
// enqueue, which is bounded by network not CPU).
func BenchmarkRollupListPlusBuild(b *testing.B) {
	for _, n := range []int{10, 100, 1000, 10000} {
		b.Run(name(n), func(b *testing.B) {
			crs := makeCapacityRequests(n)
			op := makeOperator(b, &bfv1alpha1.CapacityRequestList{Items: crs})
			ctx := b.Context()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				crs, _ := op.listCapacityRequests(ctx)
				_, _ = op.buildRollup(crs)
			}
		})
	}
}

func name(n int) string {
	switch n {
	case 10:
		return "10crs"
	case 100:
		return "100crs"
	case 1000:
		return "1000crs"
	case 10000:
		return "10000crs"
	}
	return ""
}

func makeCapacityRequests(n int) []bfv1alpha1.CapacityRequest {
	intr := resource.MustParse("8192")
	recl := resource.MustParse("65536")
	out := make([]bfv1alpha1.CapacityRequest, n)
	for i := 0; i < n; i++ {
		out[i] = bfv1alpha1.CapacityRequest{
			ObjectMeta: metav1.ObjectMeta{
				Name:      crName(i),
				Namespace: "default",
			},
			Spec: bfv1alpha1.CapacityRequestSpec{
				Requirements: []corev1.NodeSelectorRequirement{{
					Key:      "node.kubernetes.io/instance-type",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"a3-highgpu-8g"},
				}},
				Resources: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("8"),
				},
				Priority:            1_000_000,
				InterruptionPenalty: &intr,
				ReclamationPenalty:  &recl,
			},
		}
	}
	return out
}

// crName generates 1000-distinct CR names cheaply.
func crName(i int) string {
	const hex = "0123456789abcdef"
	var buf [12]byte
	copy(buf[:6], "cr-")
	x := uint32(i)
	for j := 11; j >= 6; j-- {
		buf[j] = hex[x&0xf]
		x >>= 4
	}
	return string(buf[:])
}

func makeOperator(b *testing.B, list *bfv1alpha1.CapacityRequestList) *Operator {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(bfv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithLists(list).Build()
	op, err := New(Config{
		ClusterID:    "bench-cluster",
		ShardAddress: "127.0.0.1:7780",
		KubeClient:   c,
		Logger:       slog.New(slog.NewTextHandler(testWriter{b}, nil)),
	})
	if err != nil {
		b.Fatalf("operator.New: %v", err)
	}
	return op
}

type testWriter struct{ b *testing.B }

func (w testWriter) Write(p []byte) (int, error) { w.b.Log(string(p)); return len(p), nil }
