package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// repoRoot finds the bigfleet repo root via `git rev-parse`. Used to
// resolve test/scaletest/profiles/*.yaml and substrates/*.yaml from
// whatever cwd the tests run in.
func repoRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		t.Fatalf("git rev-parse: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// TestBYO_ProfileSubstrateMatrix is the Stage-4 integration smoke
// test: every committed BYO profile × every committed example
// substrate parses, validates, merges, and renders cleanly. If any
// substrate-agnostic profile drifts (missing field, wrong unit) or
// any example substrate breaks the merge math (zero capacity, etc.),
// this fails loudly before a real run pays the cost.
//
// Note: helm-template invocation is gated on helm being on PATH.
// The merge + render path runs unconditionally.
func TestBYO_ProfileSubstrateMatrix(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	profilesDir := filepath.Join(root, "test", "scaletest", "profiles")
	substratesDir := filepath.Join(root, "test", "scaletest", "substrates")

	profiles := []string{"5k", "50k", "500k", "1m", "5m"}
	substrates := []string{"example-fat-host", "example-mid-host", "example-kind-laptop"}

	helmAvailable := false
	if _, err := exec.LookPath("helm"); err == nil {
		helmAvailable = true
	}

	for _, pname := range profiles {
		for _, sname := range substrates {
			pname, sname := pname, sname
			t.Run(pname+"_x_"+sname, func(t *testing.T) {
				t.Parallel()
				pPath := filepath.Join(profilesDir, pname+".yaml")
				sPath := filepath.Join(substratesDir, sname+".yaml")

				p, err := readProfileV2(pPath)
				if err != nil {
					t.Fatalf("readProfileV2 %s: %v", pPath, err)
				}
				s, err := readSubstrate(sPath)
				if err != nil {
					t.Fatalf("readSubstrate %s: %v", sPath, err)
				}
				cfg, err := merge(p, s)
				if err != nil {
					t.Fatalf("merge: %v", err)
				}
				if cfg.ClusterCount <= 0 {
					t.Errorf("merged ClusterCount = %d (must be > 0)", cfg.ClusterCount)
				}
				if cfg.HostsNeeded <= 0 {
					t.Errorf("merged HostsNeeded = %d (must be > 0)", cfg.HostsNeeded)
				}

				values := renderHelmValues(p, s, cfg)
				if values == nil {
					t.Fatal("renderHelmValues returned nil")
				}

				out := t.TempDir()
				path, err := writeRenderedValues(values, out)
				if err != nil {
					t.Fatalf("writeRenderedValues: %v", err)
				}
				if _, err := os.Stat(path); err != nil {
					t.Errorf("rendered values not on disk: %v", err)
				}

				if !helmAvailable {
					t.Logf("helm not on PATH; skipped helm-template stage")
					return
				}
				chart := filepath.Join(root, "test", "scaletest", "chart")
				output, err := exec.Command("helm", "template", "scaletest", chart, "-f", path, "--set", "runId=test").CombinedOutput()
				if err != nil {
					t.Fatalf("helm template %s × %s: %v\n%s", pname, sname, err, output)
				}
			})
		}
	}
}
