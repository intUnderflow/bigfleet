package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
)

// snapshotExportLoop periodically exports the latest Raft snapshot to
// SnapshotExportDir. Only the leader exports — followers don't have a
// complete state until they catch up; once promoted they take over.
//
// Per export:
//
//  1. Trigger raft.Snapshot() so the on-disk snapshot is fresh.
//  2. List the snapshot store and find the most recent ID.
//  3. Open the snapshot, copy meta + data into
//     SnapshotExportDir/<timestamp>-<id>/.
//  4. Update SnapshotExportDir/latest -> <timestamp>-<id> (best-effort).
//
// Plan §10.8 calls for "S3-compatible" storage; this writes to a
// local path so operators can mount the destination from durable
// object storage (FUSE, sidecar uploader, etc.). The reference impl
// stays free of cloud SDK dependencies; production deployments wire
// the path to whatever backing store they trust.
func (c *Coordinator) snapshotExportLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.SnapshotExportInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if c.raft.State() != raft.Leader {
				continue
			}
			if err := c.exportSnapshotOnce(); err != nil {
				c.log.Warn("snapshot export failed", "err", err)
			}
		}
	}
}

// openLatestSnapshot triggers a fresh Raft snapshot and opens the
// most recent one from the store. Shared by the periodic export loop
// and the SnapshotSave RPC (M75 DR). Returns nil meta when the store
// is empty.
func (c *Coordinator) openLatestSnapshot() (*raft.SnapshotMeta, io.ReadCloser, error) {
	// Trigger a snapshot so the latest stored one reflects current
	// state. hashicorp/raft.Snapshot is async — it returns a Future
	// that we wait on (cheap; state is small). ErrNothingNewToSnapshot
	// means the stored snapshot already reflects every committed apply
	// — exactly what we want to hand out, not a failure.
	if err := c.raft.Snapshot().Error(); err != nil && !errors.Is(err, raft.ErrNothingNewToSnapshot) {
		return nil, nil, fmt.Errorf("trigger snapshot: %w", err)
	}

	snaps, err := c.snapshotStore.List()
	if err != nil {
		return nil, nil, fmt.Errorf("list snapshots: %w", err)
	}
	if len(snaps) == 0 {
		return nil, nil, nil
	}
	latest := snaps[0] // SnapshotStore.List returns most-recent-first.
	meta, src, err := c.snapshotStore.Open(latest.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("open snapshot: %w", err)
	}
	return meta, src, nil
}

func (c *Coordinator) exportSnapshotOnce() error {
	meta, src, err := c.openLatestSnapshot()
	if err != nil {
		return err
	}
	if meta == nil {
		return nil
	}
	defer src.Close()

	dest := filepath.Join(c.cfg.SnapshotExportDir, fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102-150405"), meta.ID))
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return fmt.Errorf("mkdir export dest: %w", err)
	}

	// Persist the snapshot's meta sidecar + the FSM-state payload. The
	// meta payload is small JSON; the state is whatever the FSM's
	// SnapshotPersist wrote.
	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dest, "meta.json"), metaBytes, 0o644); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}

	stateFile, err := os.Create(filepath.Join(dest, "state"))
	if err != nil {
		return fmt.Errorf("create state: %w", err)
	}
	defer stateFile.Close()
	if _, err := io.Copy(stateFile, src); err != nil {
		return fmt.Errorf("copy state: %w", err)
	}

	// Best-effort latest-symlink update so consumers can always find
	// the most recent export by a stable path.
	link := filepath.Join(c.cfg.SnapshotExportDir, "latest")
	tmpLink := link + ".tmp"
	_ = os.Remove(tmpLink)
	if err := os.Symlink(filepath.Base(dest), tmpLink); err == nil {
		_ = os.Rename(tmpLink, link)
	}
	c.log.Info("snapshot exported", "dest", dest, "index", meta.Index, "term", meta.Term)
	return nil
}
