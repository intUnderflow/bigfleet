package main

import (
	"bytes"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// helmShowOnly renders a single chart template via `helm template
// --show-only` with the given --set overrides and decodes the
// multi-document output into a slice of generic maps. Skips the test
// if helm isn't on PATH (local dev without the kubectl/helm stack).
func helmShowOnly(t *testing.T, template string, sets []string) []map[string]any {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not on PATH; skipping chart-template render test")
	}
	chart := filepath.Join(repoRoot(t), "test", "scaletest", "chart")
	args := []string{"template", "scaletest", chart, "--set", "runId=test", "--show-only", template}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template %s %v: %v\n%s", template, sets, err, out)
	}
	var docs []map[string]any
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var d map[string]any
		if err := dec.Decode(&d); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decode helm output: %v\n%s", err, out)
		}
		if len(d) > 0 {
			docs = append(docs, d)
		}
	}
	return docs
}

// helmTemplateAll renders the whole chart with the given --set
// overrides and decodes every document. Used where --show-only would
// error on an empty render (helm treats "template produced no output"
// as "could not find template").
func helmTemplateAll(t *testing.T, sets []string) []map[string]any {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not on PATH; skipping chart-template render test")
	}
	chart := filepath.Join(repoRoot(t), "test", "scaletest", "chart")
	args := []string{"template", "scaletest", chart, "--set", "runId=test"}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template (all) %v: %v\n%s", sets, err, out)
	}
	var docs []map[string]any
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var d map[string]any
		if err := dec.Decode(&d); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decode helm output: %v\n%s", err, out)
		}
		if len(d) > 0 {
			docs = append(docs, d)
		}
	}
	return docs
}

// svcByName finds a Service document by metadata.name.
func svcByName(docs []map[string]any, name string) map[string]any {
	for _, d := range docs {
		if d["kind"] != "Service" {
			continue
		}
		meta, _ := d["metadata"].(map[string]any)
		if meta != nil && meta["name"] == name {
			return d
		}
	}
	return nil
}

// firstNodePort returns spec.ports[0].nodePort for a Service doc.
func firstNodePort(t *testing.T, svc map[string]any) int {
	t.Helper()
	spec, _ := svc["spec"].(map[string]any)
	ports, _ := spec["ports"].([]any)
	if len(ports) == 0 {
		t.Fatalf("service has no ports: %#v", svc)
	}
	p0, _ := ports[0].(map[string]any)
	np, _ := p0["nodePort"].(int)
	return np
}

// selectorPodName returns spec.selector["statefulset.kubernetes.io/pod-name"].
func selectorPodName(svc map[string]any) string {
	spec, _ := svc["spec"].(map[string]any)
	sel, _ := spec["selector"].(map[string]any)
	s, _ := sel["statefulset.kubernetes.io/pod-name"].(string)
	return s
}

// TestNodePort_PerOrdinal pins the cross-host routing fix: with
// crossHost.expose and shard.replicas=2, the chart renders one NodePort
// Service per shard ordinal — each selecting exactly that ordinal's pod
// via the StatefulSet pod-name label, on nodePort = base + ordinal — so
// a satellite can tunnel to a SPECIFIC shard rather than round-robining
// across all of them. This is what makes the 2-shard failover profiles
// (shard-kill / partition / failover-soak) routable across the tunnel.
func TestNodePort_PerOrdinal(t *testing.T) {
	t.Parallel()
	docs := helmShowOnly(t, "templates/nodeport.yaml",
		[]string{"crossHost.expose=true", "shard.replicas=2"})

	s0 := svcByName(docs, "bigfleet-shard-0-nodeport")
	s1 := svcByName(docs, "bigfleet-shard-1-nodeport")
	if s0 == nil || s1 == nil {
		t.Fatalf("want both per-ordinal NodePort services; got docs: %#v", docs)
	}
	if got := firstNodePort(t, s0); got != 30780 {
		t.Errorf("shard-0 nodePort = %d, want 30780 (base)", got)
	}
	if got := firstNodePort(t, s1); got != 30781 {
		t.Errorf("shard-1 nodePort = %d, want 30781 (base+1)", got)
	}
	if got := selectorPodName(s0); got != "bigfleet-shard-0" {
		t.Errorf("shard-0 selector pod-name = %q, want bigfleet-shard-0", got)
	}
	if got := selectorPodName(s1); got != "bigfleet-shard-1" {
		t.Errorf("shard-1 selector pod-name = %q, want bigfleet-shard-1", got)
	}
	// Singularity properties: exactly `replicas` shard NodePorts and
	// exactly ONE coordinator NodePort (a regression that moved the
	// coordinator block inside the shard range would render N of them).
	shardN, coordN := countNodePorts(docs)
	if shardN != 2 {
		t.Errorf("shard NodePort count = %d, want 2 (one per ordinal)", shardN)
	}
	if coordN != 1 {
		t.Errorf("coordinator NodePort count = %d, want exactly 1 regardless of shard count", coordN)
	}
}

// countNodePorts returns (shard NodePort count, coordinator NodePort
// count) across the rendered Service docs, keyed by name.
func countNodePorts(docs []map[string]any) (shard, coord int) {
	for _, d := range docs {
		if d["kind"] != "Service" {
			continue
		}
		meta, _ := d["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		switch {
		case name == "bigfleet-coordinator-nodeport":
			coord++
		case strings.HasPrefix(name, "bigfleet-shard-") && strings.HasSuffix(name, "-nodeport"):
			shard++
		}
	}
	return shard, coord
}

// helmRenderErr runs `helm template --show-only` and returns the
// combined output, expecting a NON-nil error (used to assert the
// render-time fail guards fire). Skips if helm is absent.
func helmRenderErr(t *testing.T, template string, sets []string) string {
	t.Helper()
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not on PATH; skipping chart-template guard test")
	}
	chart := filepath.Join(repoRoot(t), "test", "scaletest", "chart")
	args := []string{"template", "scaletest", chart, "--set", "runId=test", "--show-only", template}
	for _, s := range sets {
		args = append(args, "--set", s)
	}
	out, err := exec.Command("helm", args...).CombinedOutput()
	if err == nil {
		t.Fatalf("helm template %s %v unexpectedly succeeded; want a fail-guard error\n%s", template, sets, out)
	}
	return string(out)
}

// TestCrossHostGuards pins the render-time fail guards added after the
// adversarial review: the silent shard-0 collapse (overrideHost with
// replicas<2) and the NodePort-range collisions (coordinator port,
// 32767 ceiling) must abort the render with a clear message rather than
// produce a deceptively-green deploy.
func TestCrossHostGuards(t *testing.T) {
	t.Parallel()

	t.Run("overrideHost-needs-replicas>=2", func(t *testing.T) {
		t.Parallel()
		out := helmRenderErr(t, "templates/kwok-clusters.yaml",
			[]string{"shard.overrideHost=172.17.0.1", "shard.replicas=1"})
		if !strings.Contains(out, "MULTI-shard") {
			t.Errorf("guard message missing the MULTI-shard hint:\n%s", out)
		}
	})

	t.Run("nodeport-collides-with-coordinator", func(t *testing.T) {
		t.Parallel()
		// base 30780 + (11-1) = 30790 == coordinatorNodePort default.
		out := helmRenderErr(t, "templates/nodeport.yaml",
			[]string{"crossHost.expose=true", "shard.replicas=11", "coordinator.enabled=true"})
		if !strings.Contains(out, "collides with coordinatorNodePort") {
			t.Errorf("guard message missing the coordinator-collision hint:\n%s", out)
		}
	})

	t.Run("nodeport-exceeds-ceiling", func(t *testing.T) {
		t.Parallel()
		// Disable the coordinator so the ceiling guard (not the collision
		// guard) is the one that fires; base 32760 + 9 = 32769 > 32767.
		out := helmRenderErr(t, "templates/nodeport.yaml",
			[]string{"crossHost.expose=true", "shard.replicas=10",
				"crossHost.shardNodePort=32760", "coordinator.enabled=false"})
		if !strings.Contains(out, "32767") {
			t.Errorf("guard message missing the 32767 ceiling hint:\n%s", out)
		}
	})
}

// TestNodePort_SingleShardBackwardCompat asserts the replicas=1 case is
// identical in reach to the legacy single-shard exposure: exactly one
// shard NodePort on the base port selecting bigfleet-shard-0, and no
// stray shard-1 service.
func TestNodePort_SingleShardBackwardCompat(t *testing.T) {
	t.Parallel()
	docs := helmShowOnly(t, "templates/nodeport.yaml",
		[]string{"crossHost.expose=true", "shard.replicas=1"})

	s0 := svcByName(docs, "bigfleet-shard-0-nodeport")
	if s0 == nil {
		t.Fatalf("want shard-0 NodePort; got docs: %#v", docs)
	}
	if got := firstNodePort(t, s0); got != 30780 {
		t.Errorf("shard-0 nodePort = %d, want 30780", got)
	}
	if svcByName(docs, "bigfleet-shard-1-nodeport") != nil {
		t.Errorf("replicas=1 rendered a stray shard-1 NodePort")
	}
}

// TestNodePort_DisabledByDefault confirms single-host profiles (no
// crossHost.expose) render no NodePort Services at all. Renders the
// whole chart because --show-only errors on an empty template render.
func TestNodePort_DisabledByDefault(t *testing.T) {
	t.Parallel()
	docs := helmTemplateAll(t, []string{"shard.replicas=2"})
	for _, d := range docs {
		if d["kind"] != "Service" {
			continue
		}
		meta, _ := d["metadata"].(map[string]any)
		name, _ := meta["name"].(string)
		if strings.Contains(name, "nodeport") {
			t.Errorf("crossHost.expose unset rendered a NodePort Service %q", name)
		}
	}
}

// envValue returns the value of the named env var on the named
// container in a kwok-cluster StatefulSet doc.
func kwokEnv(t *testing.T, docs []map[string]any, container, name string) (string, bool) {
	t.Helper()
	for _, d := range docs {
		if d["kind"] != "StatefulSet" {
			continue
		}
		meta, _ := d["metadata"].(map[string]any)
		if meta == nil || meta["name"] != "kwok-cluster" {
			continue
		}
		spec, _ := d["spec"].(map[string]any)
		tmpl, _ := spec["template"].(map[string]any)
		tspec, _ := tmpl["spec"].(map[string]any)
		containers, _ := tspec["containers"].([]any)
		for _, c := range containers {
			cm, _ := c.(map[string]any)
			if cm["name"] != container {
				continue
			}
			env, _ := cm["env"].([]any)
			for _, e := range env {
				em, _ := e.(map[string]any)
				if em["name"] == name {
					v, _ := em["value"].(string)
					return v, true
				}
			}
		}
	}
	return "", false
}

// TestKwokShardResolution_Precedence pins the three shard-endpoint
// resolution paths the workload entrypoint consumes, and their
// precedence: overrideHost (multi-shard satellite) wins over
// overrideAddr (single-shard satellite), which wins over the default
// in-cluster headless-DNS path. The entrypoint computes the ordinal
// from POD_NAME for the overrideHost and headless paths alike.
func TestKwokShardResolution_Precedence(t *testing.T) {
	t.Parallel()

	t.Run("overrideHost", func(t *testing.T) {
		t.Parallel()
		docs := helmShowOnly(t, "templates/kwok-clusters.yaml",
			[]string{"shard.overrideHost=172.17.0.1", "shard.replicas=2"})
		if v, ok := kwokEnv(t, docs, "workload", "BIGFLEET_SHARD_OVERRIDE_HOST"); !ok || v != "172.17.0.1" {
			t.Errorf("BIGFLEET_SHARD_OVERRIDE_HOST = %q (ok=%v), want 172.17.0.1", v, ok)
		}
		if v, ok := kwokEnv(t, docs, "workload", "BIGFLEET_SHARD_OVERRIDE_PORT_BASE"); !ok || v != "30780" {
			t.Errorf("BIGFLEET_SHARD_OVERRIDE_PORT_BASE = %q (ok=%v), want 30780", v, ok)
		}
		if v, ok := kwokEnv(t, docs, "workload", "BIGFLEET_SHARD_REPLICAS"); !ok || v != "2" {
			t.Errorf("BIGFLEET_SHARD_REPLICAS = %q (ok=%v), want 2", v, ok)
		}
		// overrideHost wins: no literal single-endpoint ADDR.
		if v, ok := kwokEnv(t, docs, "workload", "BIGFLEET_SHARD_ADDR"); ok {
			t.Errorf("BIGFLEET_SHARD_ADDR unexpectedly set to %q under overrideHost", v)
		}
	})

	t.Run("overrideAddr", func(t *testing.T) {
		t.Parallel()
		docs := helmShowOnly(t, "templates/kwok-clusters.yaml",
			[]string{"shard.overrideAddr=172.17.0.1:30780"})
		if v, ok := kwokEnv(t, docs, "workload", "BIGFLEET_SHARD_ADDR"); !ok || v != "172.17.0.1:30780" {
			t.Errorf("BIGFLEET_SHARD_ADDR = %q (ok=%v), want 172.17.0.1:30780", v, ok)
		}
		if _, ok := kwokEnv(t, docs, "workload", "BIGFLEET_SHARD_OVERRIDE_HOST"); ok {
			t.Errorf("BIGFLEET_SHARD_OVERRIDE_HOST set under plain overrideAddr")
		}
	})

	t.Run("default-headless", func(t *testing.T) {
		t.Parallel()
		docs := helmShowOnly(t, "templates/kwok-clusters.yaml", []string{"shard.replicas=2"})
		if v, ok := kwokEnv(t, docs, "workload", "BIGFLEET_SHARD_HEADLESS_DNS"); !ok || v == "" {
			t.Errorf("BIGFLEET_SHARD_HEADLESS_DNS = %q (ok=%v), want non-empty", v, ok)
		}
		if v, ok := kwokEnv(t, docs, "workload", "BIGFLEET_SHARD_REPLICAS"); !ok || v != "2" {
			t.Errorf("BIGFLEET_SHARD_REPLICAS = %q (ok=%v), want 2", v, ok)
		}
		if _, ok := kwokEnv(t, docs, "workload", "BIGFLEET_SHARD_OVERRIDE_HOST"); ok {
			t.Errorf("BIGFLEET_SHARD_OVERRIDE_HOST set on default in-cluster path")
		}
	})
}
