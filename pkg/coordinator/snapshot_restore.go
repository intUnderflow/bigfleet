package coordinator

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Snapshot archives (M75 DR, ADR-0047). Two layouts hold the same two
// artefacts — the JSON-encoded raft.SnapshotMeta and the FSM state
// payload:
//
//   - a directory with meta.json + state, as written by the leader's
//     periodic snapshot-export loop (snapshot_export.go);
//   - a single tar file with the same two entries, as written by
//     `bigfleetctl snapshot save`.
//
// OpenSnapshotArchive reads either; RestoreSnapshot rebuilds a
// stopped coordinator's data dir from one.

const (
	archiveMetaName  = "meta.json"
	archiveStateName = "state"
)

// WriteSnapshotArchive writes a tar archive at path containing
// meta.json followed by the state payload. statePath must be a
// regular file (the caller spools the streamed RPC chunks to disk
// first — tar headers need the size up front).
func WriteSnapshotArchive(path string, metaJSON []byte, statePath string) (err error) {
	stateInfo, err := os.Stat(statePath)
	if err != nil {
		return fmt.Errorf("stat state: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()
	tw := tar.NewWriter(f)
	if err := tw.WriteHeader(&tar.Header{Name: archiveMetaName, Mode: 0o644, Size: int64(len(metaJSON))}); err != nil {
		return err
	}
	if _, err := tw.Write(metaJSON); err != nil {
		return err
	}
	if err := tw.WriteHeader(&tar.Header{Name: archiveStateName, Mode: 0o644, Size: stateInfo.Size()}); err != nil {
		return err
	}
	state, err := os.Open(statePath)
	if err != nil {
		return err
	}
	defer func() { _ = state.Close() }()
	if _, err := io.Copy(tw, state); err != nil {
		return err
	}
	return tw.Close()
}

// OpenSnapshotArchive opens a snapshot archive — either a
// `bigfleetctl snapshot save` tar file or a snapshot-export directory
// (meta.json + state) — and returns the decoded meta plus a reader
// over the state payload. The caller closes the reader.
func OpenSnapshotArchive(path string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}
	if info.IsDir() {
		metaBytes, err := os.ReadFile(filepath.Join(path, archiveMetaName))
		if err != nil {
			return nil, nil, fmt.Errorf("read %s: %w", archiveMetaName, err)
		}
		meta, err := decodeMeta(metaBytes)
		if err != nil {
			return nil, nil, err
		}
		state, err := os.Open(filepath.Join(path, archiveStateName))
		if err != nil {
			return nil, nil, fmt.Errorf("open state: %w", err)
		}
		return meta, state, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	tr := tar.NewReader(f)
	var meta *raft.SnapshotMeta
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = f.Close()
			return nil, nil, fmt.Errorf("read archive: %w", err)
		}
		switch hdr.Name {
		case archiveMetaName:
			metaBytes, err := io.ReadAll(tr)
			if err != nil {
				_ = f.Close()
				return nil, nil, err
			}
			if meta, err = decodeMeta(metaBytes); err != nil {
				_ = f.Close()
				return nil, nil, err
			}
		case archiveStateName:
			if meta == nil {
				_ = f.Close()
				return nil, nil, fmt.Errorf("archive %s: %s precedes %s", path, archiveStateName, archiveMetaName)
			}
			// The tar is written meta-first; hand back a reader
			// positioned at the state entry so restore streams it
			// without buffering the payload in memory.
			return meta, &tarEntryReader{tr: tr, f: f}, nil
		}
	}
	_ = f.Close()
	return nil, nil, fmt.Errorf("archive %s: missing %s entry", path, archiveStateName)
}

func decodeMeta(b []byte) (*raft.SnapshotMeta, error) {
	var meta raft.SnapshotMeta
	if err := json.Unmarshal(b, &meta); err != nil {
		return nil, fmt.Errorf("decode %s: %w", archiveMetaName, err)
	}
	if meta.Index == 0 {
		return nil, fmt.Errorf("%s: snapshot index 0 — not a valid coordinator snapshot", archiveMetaName)
	}
	return &meta, nil
}

type tarEntryReader struct {
	tr *tar.Reader
	f  *os.File
}

func (r *tarEntryReader) Read(p []byte) (int, error) { return r.tr.Read(p) }
func (r *tarEntryReader) Close() error               { return r.f.Close() }

// RestoreSnapshot rebuilds a STOPPED coordinator's data dir from a
// snapshot archive's meta + state (ADR-0047).
//
// hashicorp/raft restore semantics: on startup raft installs the
// newest snapshot in the store, taking both the FSM state and the
// membership configuration recorded in the snapshot's meta. We write
// the snapshot back with a SINGLE-VOTER configuration — just the
// restoring node — instead of the original membership, so the
// restored node elects itself immediately rather than waiting for
// quorum among peers whose data is gone. The other replicas must
// start with EMPTY data dirs and re-form the quorum through the
// ADR-0047 join path; this is the same single-survivor recovery shape
// as hashicorp's peers.json / RecoverCluster procedure.
//
// Any existing Raft state in dataDir is removed first: a snapshot at
// index N alongside an unrelated log/stable store is undefined
// behaviour, and restore exists precisely because that state is lost
// or corrupt.
func RestoreSnapshot(dataDir, nodeID, raftAdvertise string, meta *raft.SnapshotMeta, state io.Reader) error {
	if nodeID == "" || raftAdvertise == "" {
		return errors.New("restore: node id and raft advertise address required")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("restore: data dir: %w", err)
	}
	for _, name := range []string{"raft-log.db", "raft-stable.db", "snapshots"} {
		if err := os.RemoveAll(filepath.Join(dataDir, name)); err != nil {
			return fmt.Errorf("restore: clear %s: %w", name, err)
		}
	}

	snaps, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("restore: snapshot store: %w", err)
	}
	// FileSnapshotStore.Create only uses the transport to encode the
	// legacy Peers field; the in-memory transport's identity encoding
	// matches the TCP transport's.
	_, trans := raft.NewInmemTransport(raft.ServerAddress(raftAdvertise))
	conf := raft.Configuration{Servers: []raft.Server{{
		Suffrage: raft.Voter,
		ID:       raft.ServerID(nodeID),
		Address:  raft.ServerAddress(raftAdvertise),
	}}}
	sink, err := snaps.Create(raft.SnapshotVersionMax, meta.Index, meta.Term, conf, meta.Index, trans)
	if err != nil {
		return fmt.Errorf("restore: create snapshot: %w", err)
	}
	if _, err := io.Copy(sink, state); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("restore: write state: %w", err)
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("restore: finalise snapshot: %w", err)
	}

	// Seed CurrentTerm in the fresh stable store. With term 0 the
	// restored node would campaign at term 1 — below the snapshot's
	// term — and append post-restore entries with regressed terms.
	// A surviving member would have the term persisted; mirror that.
	stable, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft-stable.db"))
	if err != nil {
		return fmt.Errorf("restore: stable store: %w", err)
	}
	defer func() { _ = stable.Close() }()
	if err := stable.SetUint64([]byte("CurrentTerm"), meta.Term); err != nil {
		return fmt.Errorf("restore: seed current term: %w", err)
	}
	return nil
}
