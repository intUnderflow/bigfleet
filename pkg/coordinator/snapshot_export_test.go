package coordinator

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSnapshotExport_WritesMetaAndState brings up a single-node
// coordinator with SnapshotExportDir set, applies a few commands, runs
// the export logic, and asserts the export directory has both
// meta.json and state.
func TestSnapshotExport_WritesMetaAndState(t *testing.T) {
	exportDir := filepath.Join(t.TempDir(), "export")

	c, err := New(Config{
		NodeID:                 "node-1",
		DataDir:                filepath.Join(t.TempDir(), "raft"),
		RaftBindAddress:        freePortLocal(t),
		Bootstrap:              true,
		SnapshotExportDir:      exportDir,
		SnapshotExportInterval: time.Hour, // we drive the export manually
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(c.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.WaitForLeader(ctx); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}

	// Add some state so the snapshot has content.
	if err := c.Apply(ctx, MakeAddShardCommand(ShardEntry{ID: "shard-a", Address: "host:1"})); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if err := c.exportSnapshotOnce(); err != nil {
		t.Fatalf("exportSnapshotOnce: %v", err)
	}

	entries, err := os.ReadDir(exportDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	// Expect: one timestamped subdir + a `latest` symlink.
	var subdirs int
	var hasLatest bool
	for _, e := range entries {
		switch {
		case e.IsDir():
			subdirs++
			if _, err := os.Stat(filepath.Join(exportDir, e.Name(), "meta.json")); err != nil {
				t.Errorf("meta.json missing in %s: %v", e.Name(), err)
			}
			if _, err := os.Stat(filepath.Join(exportDir, e.Name(), "state")); err != nil {
				t.Errorf("state missing in %s: %v", e.Name(), err)
			}
		case e.Name() == "latest":
			hasLatest = true
		}
	}
	if subdirs != 1 {
		t.Errorf("expected 1 subdir, got %d", subdirs)
	}
	if !hasLatest {
		t.Errorf("expected `latest` symlink")
	}
}

// freePortLocal duplicates the helper from coordinator_test so this
// in-package test doesn't import its sibling.
func freePortLocal(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}
