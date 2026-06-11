package main

import (
	"math/rand"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/intUnderflow/bigfleet/pkg/scaletest/archetype"
	"github.com/intUnderflow/bigfleet/pkg/scaletest/preflight"
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

// PickReplicas bucket / cap / heavy-tail tests live with the
// distribution in pkg/scaletest/archetype (moved by ADR-0044 §2).

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

	t.Run("sameZone archetype carries zone-scope co-location group", func(t *testing.T) {
		// M66.2: zone-scope gangs — same group label as sameRack, but
		// the podAffinity TopologyKey is the standard zone key.
		a := &archetype.Archetype{
			Name:           "gpu-training-large",
			InstanceTypes:  []string{"a3-highgpu-8g"},
			SameZone:       true,
			GroupSizeRange: [2]int{64, 256},
		}
		tmpl := d.buildPodTemplate(a)
		gid := tmpl.Labels[labelCoLocationGroup]
		if gid == "" {
			t.Fatal("sameZone template missing co-location-group label")
		}
		if tmpl.Spec.Affinity == nil || tmpl.Spec.Affinity.PodAffinity == nil {
			t.Fatal("sameZone template missing podAffinity")
		}
		terms := tmpl.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		if len(terms) != 1 || terms[0].TopologyKey != topologyKeyZone {
			t.Fatalf("unexpected podAffinity terms: %+v", terms)
		}
		if terms[0].LabelSelector.MatchLabels[labelCoLocationGroup] != gid {
			t.Fatal("podAffinity selector does not match the template's group label")
		}
	})
}

// TestBuildPodTemplateLegacy asserts the no-catalog fallback produces
// a template matching the shared preflight shape tables.
func TestBuildPodTemplateLegacy(t *testing.T) {
	d := &driver{
		rng:  rand.New(rand.NewSource(5)),
		prof: profile{PriorityClasses: []int32{500}},
	}
	tmpl := d.buildPodTemplate(nil)
	if tmpl.Spec.Priority == nil || *tmpl.Spec.Priority != 500 {
		t.Fatalf("legacy priority = %v, want 500", tmpl.Spec.Priority)
	}
	for k, v := range preflight.LegacyDemandResources() {
		got, ok := tmpl.Spec.Containers[0].Resources.Requests[corev1.ResourceName(k)]
		if !ok || got.String() != v {
			t.Fatalf("legacy template request %s = %v, want %s", k, got, v)
		}
	}
	terms := tmpl.Spec.Affinity.NodeAffinity.
		RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || terms[0].MatchExpressions[0].Values[0] != preflight.LegacyDemandInstanceType {
		t.Fatalf("legacy template nodeAffinity = %+v, want %s", terms, preflight.LegacyDemandInstanceType)
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

// M66.2: sameZone gangs draw replicas from GroupSizeRange exactly like
// sameRack ones — drawReplicas keys off "is a gang", not the scope.
func TestDrawReplicasSameZoneUsesGroupSizeRange(t *testing.T) {
	rng := rand.New(rand.NewSource(8))
	a := &archetype.Archetype{
		Name:           "gpu-training-large",
		InstanceTypes:  []string{"a3-highgpu-8g"},
		SameZone:       true,
		GroupSizeRange: [2]int{64, 256},
	}
	for i := 0; i < 5000; i++ {
		n := drawReplicas(rng, a, false, 0)
		if n < 64 || n > 256 {
			t.Fatalf("sameZone draw %d outside GroupSizeRange [64, 256]", n)
		}
	}
	if n := drawReplicas(rng, a, false, 10); n != 10 {
		t.Fatalf("sameZone draw with remaining=10 = %d, want 10 (truncated final group)", n)
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

// planGangPod is planPod plus the required podAffinity term
// buildPodTemplate emits for gangs, with the given topology key. The
// planner derives each group's domain key from this term (M66.2).
func planGangPod(name, arch, gid, cpu, topoKey string) *corev1.Pod {
	p := planPod(name, arch, gid, cpu)
	p.Spec.Affinity = &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{labelCoLocationGroup: gid},
				},
				TopologyKey: topoKey,
			}},
		},
	}
	return p
}

// planZoneNode builds a node carrying the zone topology key but NOT the
// rack key — the shape a fake-Node has when its machine profile carries
// Zone but no rack label.
func planZoneNode(name, zone string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{labelArchetype: "gpu-training-large", topologyKeyZone: zone},
	}}
}

func TestPlanPreBind_ZoneGangLandsOnOneZone(t *testing.T) {
	// M66.2: a sameZone gang must land entirely within one zone, even
	// when its nodes carry no rack label at all. zone-a holds one cpu-8
	// node (fits 2 of 3); zone-b holds two (fits all 3) → zone-b wins.
	n1 := planZoneNode("n1", "zone-a")
	n2 := planZoneNode("n2", "zone-b")
	n3 := planZoneNode("n3", "zone-b")
	byArchetype := map[string][]*corev1.Node{"gpu-training-large": {n1, n2, n3}}
	remaining := map[string]corev1.ResourceList{
		"n1": cpuList("8"), "n2": cpuList("8"), "n3": cpuList("8"),
	}
	group := []*corev1.Pod{
		planGangPod("g-0", "gpu-training-large", "grp-z", "4", topologyKeyZone),
		planGangPod("g-1", "gpu-training-large", "grp-z", "4", topologyKeyZone),
		planGangPod("g-2", "gpu-training-large", "grp-z", "4", topologyKeyZone),
	}

	plan := planPreBind(group, byArchetype, remaining, nil)
	if len(plan) != 3 {
		t.Fatalf("planned %d of 3 zone-gang Pods: %+v", len(plan), planNames(plan))
	}
	for _, a := range plan {
		if a.node != "n2" && a.node != "n3" {
			t.Errorf("pod %s planned onto %s; gang must stay within zone-b", a.pod.Name, a.node)
		}
	}

	// No single zone fits the whole gang → nothing is planned, never
	// scattered across zones.
	remaining = map[string]corev1.ResourceList{
		"n1": cpuList("8"), "n2": cpuList("4"), "n3": cpuList("4"),
	}
	plan = planPreBind(group, byArchetype, remaining, nil)
	if len(plan) != 0 {
		t.Fatalf("planned %d Pods across zones; want 0 (one zone or nowhere): %+v", len(plan), planNames(plan))
	}
}

func TestPlanPreBind_ZoneGangHonoursPinnedZone(t *testing.T) {
	// A zone gang with a member already anchored in zone-b must plan
	// its remainder there, even though zone-a also fits.
	n1 := planZoneNode("n1", "zone-a")
	n2 := planZoneNode("n2", "zone-b")
	byArchetype := map[string][]*corev1.Node{"gpu-training-large": {n1, n2}}
	remaining := map[string]corev1.ResourceList{"n1": cpuList("8"), "n2": cpuList("8")}
	group := []*corev1.Pod{
		planGangPod("g-0", "gpu-training-large", "grp-z", "4", topologyKeyZone),
		planGangPod("g-1", "gpu-training-large", "grp-z", "4", topologyKeyZone),
	}
	pinned := map[string]string{"grp-z": "zone-b"}

	plan := planPreBind(group, byArchetype, remaining, pinned)
	if len(plan) != 2 {
		t.Fatalf("planned %d of 2 pinned zone-gang Pods", len(plan))
	}
	for _, a := range plan {
		if a.node != "n2" {
			t.Errorf("pod %s planned onto %s; gang is pinned to zone-b", a.pod.Name, a.node)
		}
	}
}

// ---- pickAnchorNode (ADR-0025 gang-scheduler stand-in) ------------------

// anchorNode builds a schedulable node with the given labels and cpu-8
// Allocatable for pickAnchorNode tests.
func anchorNode(name string, labels map[string]string) corev1.Node {
	labels[labelArchetype] = "gpu-training-large"
	return corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")},
		},
	}
}

func TestPickAnchorNode_DerivesKeyFromPodAffinity(t *testing.T) {
	// One node carries only the rack key, the other only the zone key.
	// Each gang's anchor must land on the node carrying ITS topology
	// key — a zone gang can anchor on a zone-labelled node that lacks
	// the rack key entirely (M66.2).
	nodes := []corev1.Node{
		anchorNode("rack-node", map[string]string{topologyKeyRack: "z1-rack-0"}),
		anchorNode("zone-node", map[string]string{topologyKeyZone: "zone-a"}),
	}

	zoneAnchor := planGangPod("z-0", "gpu-training-large", "grp-z", "4", topologyKeyZone)
	if n := pickAnchorNode(nodes, map[string]bool{}, zoneAnchor); n == nil || n.Name != "zone-node" {
		t.Fatalf("zone-gang anchor picked %v, want zone-node", n)
	}

	rackAnchor := planGangPod("r-0", "gpu-training-large", "grp-r", "4", topologyKeyRack)
	if n := pickAnchorNode(nodes, map[string]bool{}, rackAnchor); n == nil || n.Name != "rack-node" {
		t.Fatalf("rack-gang anchor picked %v, want rack-node", n)
	}

	// Affinity-less group pods keep the historical rack-key behaviour.
	legacyAnchor := planPod("l-0", "gpu-training-large", "grp-l", "4")
	if n := pickAnchorNode(nodes, map[string]bool{}, legacyAnchor); n == nil || n.Name != "rack-node" {
		t.Fatalf("affinity-less anchor picked %v, want rack-node (rack fallback)", n)
	}

	// Claimed and non-fitting nodes are skipped.
	if n := pickAnchorNode(nodes, map[string]bool{"zone-node": true}, zoneAnchor); n != nil {
		t.Fatalf("zone-gang anchor picked claimed node %s", n.Name)
	}
	big := planGangPod("z-big", "gpu-training-large", "grp-z", "16", topologyKeyZone)
	if n := pickAnchorNode(nodes, map[string]bool{}, big); n != nil {
		t.Fatalf("oversized anchor picked %s; node cannot fit it", n.Name)
	}
}

func planNames(plan []assignment) []string {
	out := make([]string, 0, len(plan))
	for _, a := range plan {
		out = append(out, a.pod.Name+"→"+a.node)
	}
	return out
}

// TestSteadyBindLatency covers the M66.3 watcher's classification: a
// Pod leaving the unbound watch selection counts as a steady-state
// bind only when it actually has a node (bound, not deleted) and was
// created after the steady cutoff (churn/burst, not initial fill).
func TestSteadyBindLatency(t *testing.T) {
	cutoff := time.Now().Add(-time.Minute)
	pod := func(node string, created time.Time) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.Time{Time: created}},
			Spec:       corev1.PodSpec{NodeName: node},
		}
	}

	if _, ok := steadyBindLatency(pod("", time.Now()), cutoff); ok {
		t.Fatal("deleted-while-unbound pod (no node) was counted as a bind")
	}
	if _, ok := steadyBindLatency(pod("fake-1", cutoff.Add(-time.Hour)), cutoff); ok {
		t.Fatal("initial-fill pod (created before cutoff) was counted as steady-state")
	}
	created := time.Now().Add(-30 * time.Second)
	lat, ok := steadyBindLatency(pod("fake-1", created), cutoff)
	if !ok {
		t.Fatal("post-cutoff bound pod was not counted")
	}
	if lat < 30*time.Second || lat > time.Minute {
		t.Fatalf("latency = %v, want ~30s (now - creationTimestamp)", lat)
	}
}
