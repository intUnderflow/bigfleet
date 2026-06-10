package main

import (
	"math/rand"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
)

// fixedTemplate is a minimal pod template carrying the fields the UPC
// reads, used to assert the workload builders preserve them.
func fixedTemplate() corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{labelArchetype: "cpu-service"},
			Annotations: map[string]string{"bigfleet.lucy.sh/interruption-penalty": "100"},
		},
		Spec: corev1.PodSpec{
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{{}},
					},
				},
			},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{TopologyKey: "zone"}},
		},
	}
}

func TestBuildDeployment(t *testing.T) {
	name := "cluster-a-cpu-service-7"
	dep := buildDeployment(name, 12, fixedTemplate())

	if dep.Name != name || dep.Namespace != "default" {
		t.Fatalf("unexpected meta: %s/%s", dep.Namespace, dep.Name)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 12 {
		t.Fatalf("replicas = %v, want 12", dep.Spec.Replicas)
	}
	// Selector must match the template's per-object workload label.
	sel := dep.Spec.Selector.MatchLabels[labelWorkload]
	if sel != name {
		t.Fatalf("selector workload label = %q, want %q", sel, name)
	}
	tmplLabel := dep.Spec.Template.Labels[labelWorkload]
	if tmplLabel != name {
		t.Fatalf("template workload label = %q, want %q", tmplLabel, name)
	}
	if dep.Spec.Template.Labels[labelArchetype] != "cpu-service" {
		t.Fatalf("template missing archetype label: %v", dep.Spec.Template.Labels)
	}
	// The pod template must carry the affinity the UPC reads.
	if dep.Spec.Template.Spec.Affinity == nil || dep.Spec.Template.Spec.Affinity.NodeAffinity == nil {
		t.Fatal("template lost node affinity")
	}
	if len(dep.Spec.Template.Spec.TopologySpreadConstraints) != 1 {
		t.Fatal("template lost topology spread constraints")
	}
	if dep.Spec.Template.Annotations["bigfleet.lucy.sh/interruption-penalty"] != "100" {
		t.Fatal("template lost penalty annotation")
	}
}

func TestBuildStatefulSet(t *testing.T) {
	name := "cluster-a-stateful-db-3"
	ss := buildStatefulSet(name, 5, fixedTemplate())

	if ss.Name != name || ss.Namespace != "default" {
		t.Fatalf("unexpected meta: %s/%s", ss.Namespace, ss.Name)
	}
	if ss.Spec.Replicas == nil || *ss.Spec.Replicas != 5 {
		t.Fatalf("replicas = %v, want 5", ss.Spec.Replicas)
	}
	if ss.Spec.ServiceName != name {
		t.Fatalf("serviceName = %q, want %q", ss.Spec.ServiceName, name)
	}
	if ss.Spec.Selector.MatchLabels[labelWorkload] != name {
		t.Fatalf("selector workload label = %q, want %q", ss.Spec.Selector.MatchLabels[labelWorkload], name)
	}
	if ss.Spec.Template.Labels[labelWorkload] != name {
		t.Fatalf("template workload label = %q, want %q", ss.Spec.Template.Labels[labelWorkload], name)
	}
	// ADR-0038: StatefulSet carries no volumeClaimTemplates.
	if len(ss.Spec.VolumeClaimTemplates) != 0 {
		t.Fatalf("StatefulSet must not carry volumeClaimTemplates, got %d", len(ss.Spec.VolumeClaimTemplates))
	}
	if ss.Spec.Template.Spec.Affinity == nil {
		t.Fatal("template lost affinity")
	}
}

func TestIsStateful(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"stateful-db", true},
		{"memory-cache", true},
		{"cpu-service", false},
		{"gpu-training-large", false},
		{"tiny-stateless", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isStateful(c.name); got != c.want {
			t.Errorf("isStateful(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestPickReplicasWithinBuckets(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	// Every draw must land inside one of the declared buckets.
	for i := 0; i < 5000; i++ {
		n := pickReplicas(rng, false)
		if n < 1 {
			t.Fatalf("stateless draw %d < 1", n)
		}
		inBucket := false
		for _, b := range replicaDistribution {
			if n >= b.lo && n <= b.hi {
				inBucket = true
				break
			}
		}
		if !inBucket {
			t.Fatalf("stateless draw %d fell outside every bucket", n)
		}
	}
}

func TestPickReplicasStatefulCap(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 5000; i++ {
		n := pickReplicas(rng, true)
		if n < 1 {
			t.Fatalf("stateful draw %d < 1", n)
		}
		if n > statefulReplicaCap {
			t.Fatalf("stateful draw %d exceeds cap %d", n, statefulReplicaCap)
		}
	}
}

func TestPickReplicasHitsLargeBucket(t *testing.T) {
	// The heavy tail must be reachable: over many draws at least one
	// stateless service should land in the largest bucket.
	rng := rand.New(rand.NewSource(3))
	large := replicaDistribution[len(replicaDistribution)-1]
	hitLarge := false
	for i := 0; i < 20000 && !hitLarge; i++ {
		if n := pickReplicas(rng, false); n >= large.lo {
			hitLarge = true
		}
	}
	if !hitLarge {
		t.Fatal("never drew from the large-service bucket in 20000 draws")
	}
}

// TestBuildPodTemplateCarriesShape asserts buildPodTemplate produces a
// template with exactly one shape and every UPC-read field, for both an
// ordinary archetype and a sameRack one.
func TestBuildPodTemplate(t *testing.T) {
	d := &driver{rng: rand.New(rand.NewSource(4))}

	t.Run("plain archetype", func(t *testing.T) {
		a := &archetype.Archetype{
			Name:                "cpu-service",
			InstanceTypes:       []string{"n2-standard-8"},
			Zones:               []string{"z1"},
			Resources:           map[string]string{"cpu": "4"},
			PriorityClasses:     []int32{1000},
			InterruptionPenalty: 100,
			ReclamationPenalty:  200,
		}
		tmpl := d.buildPodTemplate(a)
		if tmpl.Labels[labelArchetype] != "cpu-service" {
			t.Fatalf("missing archetype label: %v", tmpl.Labels)
		}
		if tmpl.Spec.Affinity == nil || tmpl.Spec.Affinity.NodeAffinity == nil {
			t.Fatal("missing node affinity")
		}
		if tmpl.Spec.Priority == nil || *tmpl.Spec.Priority != 1000 {
			t.Fatalf("priority = %v, want 1000", tmpl.Spec.Priority)
		}
		if len(tmpl.Spec.Containers) != 1 {
			t.Fatalf("want exactly one container, got %d", len(tmpl.Spec.Containers))
		}
		if tmpl.Annotations["bigfleet.lucy.sh/interruption-penalty"] != "100" {
			t.Fatalf("missing/incorrect interruption penalty: %v", tmpl.Annotations)
		}
	})

	t.Run("sameRack archetype carries co-location group", func(t *testing.T) {
		a := &archetype.Archetype{
			Name:           "stateful-db",
			InstanceTypes:  []string{"n2-standard-8"},
			SameRack:       true,
			GroupSizeRange: [2]int{3, 3},
		}
		tmpl := d.buildPodTemplate(a)
		gid := tmpl.Labels[labelCoLocationGroup]
		if gid == "" {
			t.Fatal("sameRack template missing co-location-group label")
		}
		if tmpl.Spec.Affinity == nil || tmpl.Spec.Affinity.PodAffinity == nil {
			t.Fatal("sameRack template missing podAffinity")
		}
		terms := tmpl.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if len(terms) != 1 || terms[0].TopologyKey != topologyKeyRack {
			t.Fatalf("unexpected podAffinity terms: %+v", terms)
		}
		if terms[0].LabelSelector.MatchLabels[labelCoLocationGroup] != gid {
			t.Fatal("podAffinity selector does not match the template's group label")
		}
	})
}

// TestBuildPodTemplateLegacy asserts the no-catalog fallback still
// produces a usable GPU template.
func TestBuildPodTemplateLegacy(t *testing.T) {
	d := &driver{
		rng:  rand.New(rand.NewSource(5)),
		prof: profile{PriorityClasses: []int32{500}},
	}
	tmpl := d.buildPodTemplate(nil)
	if tmpl.Spec.Priority == nil || *tmpl.Spec.Priority != 500 {
		t.Fatalf("legacy priority = %v, want 500", tmpl.Spec.Priority)
	}
	if _, ok := tmpl.Spec.Containers[0].Resources.Requests["nvidia.com/gpu"]; !ok {
		t.Fatal("legacy template missing GPU request")
	}
	// buildDeployment writes labelWorkload into a non-nil map; ensure
	// the legacy template's label map is allocated.
	if tmpl.Labels == nil {
		t.Fatal("legacy template labels map is nil; buildDeployment would panic")
	}
}

// TestActiveCountSumsReplicas verifies activeCount returns Σreplicas
// across tracked workload objects (the ADR-0038 gauge semantics).
func TestActiveCountSumsReplicas(t *testing.T) {
	d := &driver{
		workloads: map[string]workloadMeta{
			"a": {kind: kindDeployment, archetype: "cpu-service", replicas: 10},
			"b": {kind: kindStatefulSet, archetype: "stateful-db", replicas: 5},
			"c": {kind: kindDeployment, archetype: "cpu-service", replicas: 3},
		},
	}
	if got := d.activeCount(); got != 18 {
		t.Fatalf("activeCount = %d, want 18", got)
	}
	by := d.replicasByArchetype()
	if by["cpu-service"] != 13 || by["stateful-db"] != 5 {
		t.Fatalf("replicasByArchetype = %v, want cpu-service:13 stateful-db:5", by)
	}
}

// TestRampToTermination verifies the Σreplicas ramp loop terminates
// once tracked replicas meet or exceed the target, without an apiserver.
// createWorkload is exercised indirectly via a local accumulation that
// mirrors rampTo's termination predicate: the loop draws replica counts
// (capped by remaining) and stops at Σ ≥ want.
func TestRampToTermination(t *testing.T) {
	rng := rand.New(rand.NewSource(6))
	for _, want := range []int{1, 7, 100, 1000} {
		sum := 0
		objects := 0
		for sum < want {
			remaining := want - sum
			n := drawReplicas(rng, nil, false, remaining)
			sum += n
			objects++
			if objects > want+1 {
				t.Fatalf("ramp to %d did not terminate (%d objects)", want, objects)
			}
		}
		if sum != want {
			t.Fatalf("ramp to %d landed on Σreplicas=%d; remaining-cap should make it exact", want, sum)
		}
	}
}

// ADR-0040 §3: sameRack workload objects draw replicas (= co-location
// gang size) from the archetype's GroupSizeRange, not the heavy-tailed
// service-size distribution; the remaining-cap still applies so the
// ramp lands on target (a truncated final group is fine — every Need
// is partial-fill-tolerant in v1).
func TestDrawReplicasSameRackUsesGroupSizeRange(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	a := &archetype.Archetype{
		Name:           "gpu-training",
		InstanceTypes:  []string{"a3-highgpu-8g"},
		SameRack:       true,
		GroupSizeRange: [2]int{3, 5},
	}
	for i := 0; i < 5000; i++ {
		n := drawReplicas(rng, a, false, 0)
		if n < 3 || n > 5 {
			t.Fatalf("sameRack draw %d outside GroupSizeRange [3, 5]", n)
		}
	}
	// remaining caps the final group below the range's minimum.
	if n := drawReplicas(rng, a, false, 2); n != 2 {
		t.Fatalf("sameRack draw with remaining=2 = %d, want 2 (truncated final group)", n)
	}
	// Non-sameRack archetypes keep the service-size distribution.
	plain := &archetype.Archetype{Name: "cpu-service", InstanceTypes: []string{"m5.large"}, GroupSizeRange: [2]int{3, 5}}
	sawOutsideRange := false
	for i := 0; i < 5000 && !sawOutsideRange; i++ {
		if n := drawReplicas(rng, plain, false, 0); n < 3 || n > 5 {
			sawOutsideRange = true
		}
	}
	if !sawOutsideRange {
		t.Fatal("non-sameRack archetype never drew outside GroupSizeRange; PickGroupSize must not be consulted for it")
	}
}

// ---- planPreBind (ADR-0040 rack-coherent pre-bind) ---------------------

// planPod builds an unbound Pod for planPreBind tests. gid == "" means
// a non-group Pod.
func planPod(name, arch, gid, cpu string) *corev1.Pod {
	labels := map[string]string{labelArchetype: arch}
	if gid != "" {
		labels[labelCoLocationGroup] = gid
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: "workload",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
				},
			}},
		},
	}
}

func planNode(name, rack string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{labelArchetype: "cpu-service", topologyKeyRack: rack},
	}}
}

func cpuList(cpu string) corev1.ResourceList {
	return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}
}

func TestPlanPreBind_GroupLandsOnOneRack(t *testing.T) {
	// rack-a holds one cpu-8 node (fits 2 of the group's 3 Pods);
	// rack-b holds two cpu-8 nodes (fits all 3). The group must land
	// entirely within rack-b, bin-packed across its nodes.
	n1 := planNode("n1", "rack-a")
	n2 := planNode("n2", "rack-b")
	n3 := planNode("n3", "rack-b")
	byArchetype := map[string][]*corev1.Node{"cpu-service": {n1, n2, n3}}
	remaining := map[string]corev1.ResourceList{
		"n1": cpuList("8"), "n2": cpuList("8"), "n3": cpuList("8"),
	}
	group := []*corev1.Pod{
		planPod("g-0", "cpu-service", "grp-1", "4"),
		planPod("g-1", "cpu-service", "grp-1", "4"),
		planPod("g-2", "cpu-service", "grp-1", "4"),
	}

	plan := planPreBind(group, byArchetype, remaining, nil)
	if len(plan) != 3 {
		t.Fatalf("planned %d of 3 group Pods", len(plan))
	}
	for _, a := range plan {
		if a.node != "n2" && a.node != "n3" {
			t.Errorf("pod %s planned onto %s; group must stay within rack-b", a.pod.Name, a.node)
		}
	}
}

func TestPlanPreBind_GroupNeverScatters(t *testing.T) {
	// Two racks of one cpu-8 node each; the group needs cpu 12. No
	// single rack fits → NONE of the group is planned and capacity is
	// untouched, so the trailing single still binds.
	n1 := planNode("n1", "rack-a")
	n2 := planNode("n2", "rack-b")
	byArchetype := map[string][]*corev1.Node{"cpu-service": {n1, n2}}
	remaining := map[string]corev1.ResourceList{"n1": cpuList("8"), "n2": cpuList("8")}
	pods := []*corev1.Pod{
		planPod("g-0", "cpu-service", "grp-1", "4"),
		planPod("g-1", "cpu-service", "grp-1", "4"),
		planPod("g-2", "cpu-service", "grp-1", "4"),
		planPod("single", "cpu-service", "", "4"),
	}

	plan := planPreBind(pods, byArchetype, remaining, nil)
	if len(plan) != 1 || plan[0].pod.Name != "single" {
		t.Fatalf("plan = %d assignments; want only the non-group Pod (groups are whole-rack or pending): %+v", len(plan), planNames(plan))
	}
}

func TestPlanPreBind_SinglesKeepFirstFit(t *testing.T) {
	// Non-group Pods keep the original first-fit walk and may span
	// racks freely.
	n1 := planNode("n1", "rack-a")
	n2 := planNode("n2", "rack-b")
	byArchetype := map[string][]*corev1.Node{"cpu-service": {n1, n2}}
	remaining := map[string]corev1.ResourceList{"n1": cpuList("4"), "n2": cpuList("4")}
	pods := []*corev1.Pod{
		planPod("s-0", "cpu-service", "", "4"),
		planPod("s-1", "cpu-service", "", "4"),
	}

	plan := planPreBind(pods, byArchetype, remaining, nil)
	if len(plan) != 2 {
		t.Fatalf("planned %d of 2 singles", len(plan))
	}
	if plan[0].node == plan[1].node {
		t.Errorf("both singles on %s; capacity tracking broken", plan[0].node)
	}
}

func TestPlanPreBind_PinnedRackConstrainsGroup(t *testing.T) {
	// grp-1 has a member already bound on rack-b (e.g. the ADR-0025
	// anchor): the unbound remainder must go to rack-b even though
	// rack-a also fits — and must stay pending when rack-b lacks room.
	n1 := planNode("n1", "rack-a")
	n2 := planNode("n2", "rack-b")
	byArchetype := map[string][]*corev1.Node{"cpu-service": {n1, n2}}
	remaining := map[string]corev1.ResourceList{"n1": cpuList("8"), "n2": cpuList("8")}
	group := []*corev1.Pod{
		planPod("g-0", "cpu-service", "grp-1", "4"),
		planPod("g-1", "cpu-service", "grp-1", "4"),
	}
	pinned := map[string]string{"grp-1": "rack-b"}

	plan := planPreBind(group, byArchetype, remaining, pinned)
	if len(plan) != 2 {
		t.Fatalf("planned %d of 2 group Pods", len(plan))
	}
	for _, a := range plan {
		if a.node != "n2" {
			t.Errorf("pod %s planned onto %s; group is pinned to rack-b", a.pod.Name, a.node)
		}
	}

	// Pinned rack out of capacity: the group waits rather than landing
	// on the unpinned rack.
	remaining = map[string]corev1.ResourceList{"n1": cpuList("8"), "n2": cpuList("4")}
	plan = planPreBind(group, byArchetype, remaining, pinned)
	if len(plan) != 0 {
		t.Fatalf("planned %d Pods onto a full pinned rack; want 0 (never scatter): %+v", len(plan), planNames(plan))
	}
}

func planNames(plan []assignment) []string {
	out := make([]string, 0, len(plan))
	for _, a := range plan {
		out = append(out, a.pod.Name+"→"+a.node)
	}
	return out
}
