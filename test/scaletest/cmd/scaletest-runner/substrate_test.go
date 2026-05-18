package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// validSubstrateYAML is the canonical example used across tests.
// Mirrors the schema documented in ADR-0034. All fields populated so
// validation has a positive baseline to round-trip against.
const validSubstrateYAML = `apiVersion: bigfleet.io/scaletest/v1
kind: Substrate
metadata:
  name: example-fat-host
  description: "80vCPU / 160GiB hosts with etcd-backed kwok apiservers."
host:
  vCPU: 80
  memoryGiB: 160
cluster:
  podsPerCluster: 25000
  clustersPerHost: 10
  storage: etcd
  bindThroughputPodsPerSec: 30
kwokPod:
  requests: { cpu: "2", memory: "4Gi" }
  limits:   { cpu: "8", memory: "32Gi" }
  sharedVolumeSizeLimit: "2Gi"
apiserver:
  extraFlags: []
costEstimate:
  perHostUsdPerHour: 0
  notes: ""
provisioning: |
  See substrate docs.
`

func TestSubstrateRoundTrip(t *testing.T) {
	t.Parallel()

	var s substrateFile
	if err := yaml.Unmarshal([]byte(validSubstrateYAML), &s); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got, want := s.Metadata.Name, "example-fat-host"; got != want {
		t.Errorf("metadata.name = %q, want %q", got, want)
	}
	if got, want := s.Host.VCPU, 80; got != want {
		t.Errorf("host.vCPU = %d, want %d", got, want)
	}
	if got, want := s.Cluster.PodsPerCluster, 25000; got != want {
		t.Errorf("cluster.podsPerCluster = %d, want %d", got, want)
	}
	if got, want := s.Cluster.Storage, "etcd"; got != want {
		t.Errorf("cluster.storage = %q, want %q", got, want)
	}
	if got, want := s.Cluster.BindThroughputPodsPerSec, 30; got != want {
		t.Errorf("cluster.bindThroughputPodsPerSec = %d, want %d", got, want)
	}
	if got, want := s.KwokPod.Requests.CPU, "2"; got != want {
		t.Errorf("kwokPod.requests.cpu = %q, want %q", got, want)
	}
	if got, want := s.KwokPod.Limits.Memory, "32Gi"; got != want {
		t.Errorf("kwokPod.limits.memory = %q, want %q", got, want)
	}
	if got, want := s.KwokPod.SharedVolumeSizeLimit, "2Gi"; got != want {
		t.Errorf("kwokPod.sharedVolumeSizeLimit = %q, want %q", got, want)
	}

	if err := s.validate(); err != nil {
		t.Errorf("validate(valid substrate): %v", err)
	}
}

func TestReadSubstrate(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "substrate.yaml")
	if err := os.WriteFile(path, []byte(validSubstrateYAML), 0o644); err != nil {
		t.Fatalf("write tempfile: %v", err)
	}

	s, err := readSubstrate(path)
	if err != nil {
		t.Fatalf("readSubstrate: %v", err)
	}
	if s.Metadata.Name != "example-fat-host" {
		t.Errorf("metadata.name = %q, want example-fat-host", s.Metadata.Name)
	}
}

func TestReadSubstrate_FileMissing(t *testing.T) {
	t.Parallel()
	_, err := readSubstrate(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

// TestSubstrateValidation covers each rejection path on validate(). The
// purpose is to fail loudly before any helm install if the user hands
// us a nonsense substrate — a slow apiserver or a zero-capacity host
// would still pass YAML parsing but cause silent surprises downstream.
func TestSubstrateValidation(t *testing.T) {
	t.Parallel()

	type mutator func(*substrateFile)
	cases := []struct {
		name    string
		mutate  mutator
		wantErr string
	}{
		{
			name:    "host vCPU zero",
			mutate:  func(s *substrateFile) { s.Host.VCPU = 0 },
			wantErr: "host.vCPU",
		},
		{
			name:    "host memory negative",
			mutate:  func(s *substrateFile) { s.Host.MemoryGiB = -1 },
			wantErr: "host.memoryGiB",
		},
		{
			name:    "podsPerCluster zero",
			mutate:  func(s *substrateFile) { s.Cluster.PodsPerCluster = 0 },
			wantErr: "cluster.podsPerCluster",
		},
		{
			name:    "clustersPerHost zero",
			mutate:  func(s *substrateFile) { s.Cluster.ClustersPerHost = 0 },
			wantErr: "cluster.clustersPerHost",
		},
		{
			name:    "unknown storage backend",
			mutate:  func(s *substrateFile) { s.Cluster.Storage = "rocksdb" },
			wantErr: "cluster.storage",
		},
		{
			name:    "negative bind throughput",
			mutate:  func(s *substrateFile) { s.Cluster.BindThroughputPodsPerSec = -1 },
			wantErr: "cluster.bindThroughputPodsPerSec",
		},
		{
			name:    "missing kwokPod requests cpu",
			mutate:  func(s *substrateFile) { s.KwokPod.Requests.CPU = "" },
			wantErr: "kwokPod.requests.cpu",
		},
		{
			name:    "missing kwokPod limits memory",
			mutate:  func(s *substrateFile) { s.KwokPod.Limits.Memory = "" },
			wantErr: "kwokPod.limits.memory",
		},
		{
			name:    "negative per-host cost",
			mutate:  func(s *substrateFile) { s.CostEstimate.PerHostUSDPerHour = -0.5 },
			wantErr: "costEstimate.perHostUsdPerHour",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var s substrateFile
			if err := yaml.Unmarshal([]byte(validSubstrateYAML), &s); err != nil {
				t.Fatalf("unmarshal baseline: %v", err)
			}
			tc.mutate(&s)
			err := s.validate()
			if err == nil {
				t.Fatalf("expected validate() to fail with %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.wantErr)
			}
			// All field errors should also include the substrate name
			// for user-facing clarity.
			if !strings.Contains(err.Error(), "example-fat-host") {
				t.Errorf("error %q does not include substrate name", err.Error())
			}
		})
	}
}

// TestSubstrateValidation_UnnamedFallback ensures a substrate with no
// metadata.name still produces an error message the user can act on
// (the file path lives outside this function, so we mark it
// "<unnamed>" so at least the user knows it's not the named one).
func TestSubstrateValidation_UnnamedFallback(t *testing.T) {
	t.Parallel()
	var s substrateFile
	if err := yaml.Unmarshal([]byte(validSubstrateYAML), &s); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	s.Metadata.Name = ""
	s.Host.VCPU = 0
	err := s.validate()
	if err == nil || !strings.Contains(err.Error(), "<unnamed>") {
		t.Errorf("expected unnamed-fallback error, got: %v", err)
	}
}
