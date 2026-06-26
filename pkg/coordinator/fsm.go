package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// CommandKind discriminates the operations the coordinator commits
// through Raft.
type CommandKind uint8

const (
	CmdUnspecified CommandKind = iota
	CmdAddShard
	CmdRemoveShard
	CmdBindCluster
	CmdAssignDomain
	CmdUnassignDomain
)

// Command is the wire form of a state mutation. Encoded as JSON for
// transport through Raft (small volumes; protobuf would be premature).
type Command struct {
	Kind CommandKind `json:"kind"`

	// Only the field matching Kind is populated. Embedded structs not
	// pointer fields so JSON round-trips cleanly.
	AddShard       *AddShardCmd       `json:"add_shard,omitempty"`
	RemoveShard    *RemoveShardCmd    `json:"remove_shard,omitempty"`
	BindCluster    *BindClusterCmd    `json:"bind_cluster,omitempty"`
	AssignDomain   *AssignDomainCmd   `json:"assign_domain,omitempty"`
	UnassignDomain *UnassignDomainCmd `json:"unassign_domain,omitempty"`
}

type AddShardCmd struct {
	Entry ShardEntry `json:"entry"`
}
type RemoveShardCmd struct {
	ID ShardID `json:"id"`
}
type BindClusterCmd struct {
	Cluster ClusterID `json:"cluster"`
	Shard   ShardID   `json:"shard"`
}
type AssignDomainCmd struct {
	Domain DomainKey `json:"domain"`
	Shard  ShardID   `json:"shard"`
}
type UnassignDomainCmd struct {
	Domain DomainKey `json:"domain"`
}

// FSM is the Raft FSM over State. Apply commits commands; Snapshot
// dumps the entire state for log compaction; Restore reloads from a
// snapshot. Concurrent reads against State are safe via its internal
// RWMutex; the FSM serialises Apply through Raft's single-goroutine
// Apply path.
type FSM struct {
	state *State

	// term + sequence track fencing tokens issued for outgoing
	// instructions. Both belong to the leader's process state and are
	// not Raft-replicated; on failover the new leader starts with a
	// fresh sequence under whatever term Raft assigns it.
	mu  sync.Mutex
	seq int64
}

// NewFSM constructs an FSM over a new empty State.
func NewFSM() *FSM { return &FSM{state: NewState()} }

// State exposes the underlying state. Read-only access from outside
// the Apply path is safe; writes must go through Apply via Raft.
func (f *FSM) State() *State { return f.state }

// NextSequence returns a monotonic sequence number for an outgoing
// instruction. Combined with Raft's term, this is the (term, seq)
// fencing token.
func (f *FSM) NextSequence() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	return f.seq
}

// Apply implements raft.FSM. Returns whatever the command's handler
// returns; nil on success, an error on failure.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("fsm: decode command: %w", err)
	}
	return f.applyCommand(cmd)
}

func (f *FSM) applyCommand(cmd Command) error {
	switch cmd.Kind {
	case CmdAddShard:
		if cmd.AddShard == nil {
			return errors.New("fsm: AddShard payload missing")
		}
		return f.state.AddShard(cmd.AddShard.Entry)
	case CmdRemoveShard:
		if cmd.RemoveShard == nil {
			return errors.New("fsm: RemoveShard payload missing")
		}
		return f.state.RemoveShard(cmd.RemoveShard.ID)
	case CmdBindCluster:
		if cmd.BindCluster == nil {
			return errors.New("fsm: BindCluster payload missing")
		}
		return f.state.BindCluster(cmd.BindCluster.Cluster, cmd.BindCluster.Shard)
	case CmdAssignDomain:
		if cmd.AssignDomain == nil {
			return errors.New("fsm: AssignDomain payload missing")
		}
		return f.state.AssignDomain(cmd.AssignDomain.Domain, cmd.AssignDomain.Shard)
	case CmdUnassignDomain:
		if cmd.UnassignDomain == nil {
			return errors.New("fsm: UnassignDomain payload missing")
		}
		f.state.UnassignDomain(cmd.UnassignDomain.Domain)
		return nil
	}
	return fmt.Errorf("fsm: unknown command kind %d", cmd.Kind)
}

// snapshot is the JSON-serialised form of the entire State. (Older
// snapshots may carry "quotas"/"providers" keys from the removed
// provider-registry + quota scaffolding; encoding/json silently drops
// them on Restore — they were always empty.)
type snapshot struct {
	Shards         []ShardEntry          `json:"shards"`
	ClusterToShard map[ClusterID]ShardID `json:"cluster_to_shard"`
	DomainToShard  []domainShardPair     `json:"domain_to_shard"`
}

type domainShardPair struct {
	Domain DomainKey `json:"domain"`
	Shard  ShardID   `json:"shard"`
}

// Snapshot implements raft.FSM. Returns a snapshot that can be
// persisted to durable storage by the Raft library.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	st := f.state
	st.mu.RLock()
	defer st.mu.RUnlock()

	snap := snapshot{
		ClusterToShard: make(map[ClusterID]ShardID, len(st.clusterToShard)),
	}
	for _, e := range st.shards {
		snap.Shards = append(snap.Shards, e)
	}
	for c, sh := range st.clusterToShard {
		snap.ClusterToShard[c] = sh
	}
	for d, sh := range st.domainToShard {
		snap.DomainToShard = append(snap.DomainToShard, domainShardPair{Domain: d, Shard: sh})
	}

	data, err := json.Marshal(&snap)
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore implements raft.FSM. Replaces the FSM's state with the
// contents of the supplied snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("fsm: read snapshot: %w", err)
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("fsm: decode snapshot: %w", err)
	}

	fresh := NewState()
	for _, e := range snap.Shards {
		_ = fresh.AddShard(e)
	}
	for c, sh := range snap.ClusterToShard {
		_ = fresh.BindCluster(c, sh)
	}
	for _, p := range snap.DomainToShard {
		_ = fresh.AssignDomain(p.Domain, p.Shard)
	}
	f.state = fresh
	return nil
}

// fsmSnapshot is a tiny helper that satisfies raft.FSMSnapshot over a
// pre-serialised byte slice.
type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// Encode marshals a Command for Raft. Convenience wrapper used by the
// coordinator to submit commands.
func (cmd *Command) Encode() ([]byte, error) {
	return json.Marshal(cmd)
}

// Convenience constructors.

func MakeAddShardCommand(e ShardEntry) Command {
	if e.RegisteredAt.IsZero() {
		e.RegisteredAt = time.Now().UTC()
	}
	return Command{Kind: CmdAddShard, AddShard: &AddShardCmd{Entry: e}}
}
func MakeRemoveShardCommand(id ShardID) Command {
	return Command{Kind: CmdRemoveShard, RemoveShard: &RemoveShardCmd{ID: id}}
}
func MakeBindClusterCommand(c ClusterID, sh ShardID) Command {
	return Command{Kind: CmdBindCluster, BindCluster: &BindClusterCmd{Cluster: c, Shard: sh}}
}
func MakeAssignDomainCommand(d DomainKey, sh ShardID) Command {
	return Command{Kind: CmdAssignDomain, AssignDomain: &AssignDomainCmd{Domain: d, Shard: sh}}
}
func MakeUnassignDomainCommand(d DomainKey) Command {
	return Command{Kind: CmdUnassignDomain, UnassignDomain: &UnassignDomainCmd{Domain: d}}
}
