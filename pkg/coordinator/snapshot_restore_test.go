package coordinator

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSnapshotRestore_RoundTrip is the offline DR path end to end
// (ADR-0047): apply state, export a snapshot, destroy the
// coordinator, restore the export into a fresh data dir, and assert
// the restored node elects itself (single-voter configuration), holds
// the old state, and accepts new writes.
func TestSnapshotRestore_RoundTrip(t *testing.T) {
	exportDir := filepath.Join(t.TempDir(), "export")
	c, err := New(Config{
		NodeID:                 "node-1",
		DataDir:                filepath.Join(t.TempDir(), "raft"),
		RaftBindAddress:        freePortLocal(t),
		Bootstrap:              true,
		SnapshotExportDir:      exportDir,
		SnapshotExportInterval: time.Hour, // driven manually
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.WaitForLeader(ctx); err != nil {
		t.Fatalf("WaitForLeader: %v", err)
	}
	if err := c.Apply(ctx, MakeAddShardCommand(ShardEntry{ID: "shard-a", Address: "host:1"})); err != nil {
		t.Fatalf("Apply AddShard: %v", err)
	}
	if err := c.Apply(ctx, MakeBindClusterCommand("cluster-x", "shard-a")); err != nil {
		t.Fatalf("Apply BindCluster: %v", err)
	}
	if err := c.exportSnapshotOnce(); err != nil {
		t.Fatalf("exportSnapshotOnce: %v", err)
	}
	// Total loss: the original data dir is gone.
	c.Close()

	// The export-dir layout restores via the `latest` symlink — the
	// path the DR runbook points operators at.
	meta, state, err := OpenSnapshotArchive(filepath.Join(exportDir, "latest"))
	if err != nil {
		t.Fatalf("OpenSnapshotArchive: %v", err)
	}
	defer state.Close()

	restoredAddr := freePortLocal(t)
	restoredDir := filepath.Join(t.TempDir(), "restored")
	if err := RestoreSnapshot(restoredDir, "node-restored", restoredAddr, meta, state); err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}

	r, err := New(Config{
		NodeID:          "node-restored",
		DataDir:         restoredDir,
		RaftBindAddress: restoredAddr,
		// No Bootstrap: the restored snapshot's single-voter
		// configuration is the membership.
	})
	if err != nil {
		t.Fatalf("New (restored): %v", err)
	}
	t.Cleanup(r.Close)
	if err := r.WaitForLeader(ctx); err != nil {
		t.Fatalf("restored node never elected itself: %v", err)
	}
	if !r.IsLeader() {
		t.Fatalf("restored node should be the single-voter leader")
	}

	if got, ok := r.State().ClusterShard("cluster-x"); !ok || got != "shard-a" {
		t.Errorf("restored ClusterShard(cluster-x) = %q ok=%v, want shard-a", got, ok)
	}
	if shards := r.State().Shards(); len(shards) != 1 || shards[0].ID != "shard-a" {
		t.Errorf("restored Shards = %+v, want [shard-a]", shards)
	}
	// Post-restore writes must commit — the cluster is alive, not a
	// read-only museum.
	if err := r.Apply(ctx, MakeAddShardCommand(ShardEntry{ID: "shard-b", Address: "host:2"})); err != nil {
		t.Errorf("Apply after restore: %v", err)
	}
}

// TestSnapshotArchive_TarRoundTrip pins the `bigfleetctl snapshot
// save` single-file format: WriteSnapshotArchive and
// OpenSnapshotArchive agree, and the state payload survives intact.
func TestSnapshotArchive_TarRoundTrip(t *testing.T) {
	t.Parallel() // pure file I/O, no Raft.
	dir := t.TempDir()
	metaJSON := []byte(`{"ID":"2-12-1749700000000","Index":12,"Term":2}`)
	statePath := filepath.Join(dir, "spooled-state")
	stateBytes := []byte(`{"shards":[{"id":"shard-a"}]}`)
	if err := os.WriteFile(statePath, stateBytes, 0o644); err != nil {
		t.Fatal(err)
	}

	archive := filepath.Join(dir, "backup.snap")
	if err := WriteSnapshotArchive(archive, metaJSON, statePath); err != nil {
		t.Fatalf("WriteSnapshotArchive: %v", err)
	}
	meta, state, err := OpenSnapshotArchive(archive)
	if err != nil {
		t.Fatalf("OpenSnapshotArchive: %v", err)
	}
	defer state.Close()
	if meta.Index != 12 || meta.Term != 2 {
		t.Errorf("meta = index %d term %d, want index 12 term 2", meta.Index, meta.Term)
	}
	got, err := io.ReadAll(state)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}
	if string(got) != string(stateBytes) {
		t.Errorf("state round-trip mismatch: got %q want %q", got, stateBytes)
	}
}
