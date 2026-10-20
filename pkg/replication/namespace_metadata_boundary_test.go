package replication

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type namespaceMetadataProbe struct {
	*StorageAdapter
	raft *RaftReplicator
	t    *testing.T
}

func (p *namespaceMetadataProbe) assertMetadataReleased() {
	p.t.Helper()
	if !p.raft.logMu.TryLock() {
		p.t.Error("storage callback entered while consensus log metadata is held")
		return
	}
	p.raft.logMu.Unlock()
	if !p.raft.mu.TryLock() {
		p.t.Error("storage callback entered while consensus role metadata is held")
		return
	}
	p.raft.mu.Unlock()
}
func (p *namespaceMetadataProbe) ApplyCommandContext(ctx context.Context, cmd *Command) error {
	p.assertMetadataReleased()
	return p.StorageAdapter.ApplyCommandContext(ctx, cmd)
}

// Checks the lock boundary itself, independent of a particular interleaving or
// database name. A new apply/discard route cannot silently reintroduce nesting.
func TestNamespaceRaftMetadataReleasedAtStorageBoundary(t *testing.T) {
	for _, mode := range []string{"leader", "follower", "discard", "single"} {
		t.Run(mode, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			defer adapter.Close()
			probe := &namespaceMetadataProbe{StorageAdapter: adapter, t: t}
			cfg := DefaultConfig()
			cfg.Mode, cfg.NodeID, cfg.Raft.Bootstrap = ModeRaft, "metadata-boundary", true
			if mode != "single" {
				cfg.Raft.Peers = []PeerConfig{{ID: "peer", Addr: "127.0.0.1:1"}}
			}
			raft, err := NewRaftReplicator(cfg, probe)
			require.NoError(t, err)
			probe.raft = raft
			raft.state, raft.currentTerm = StateLeader, 1
			data, err := encodeNodePayload(&storage.Node{ID: "boundary:write"})
			require.NoError(t, err)
			command := &Command{Type: CmdCreateNode, Data: data}
			switch mode {
			case "leader":
				raft.log = append(raft.log, &RaftLogEntry{Index: 1, Term: 1, Command: command})
				raft.matchIndex["peer"] = 1
				raft.advanceCommitIndex()
			case "follower":
				require.True(t, raft.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 1, LeaderID: "peer", Entries: []*RaftLogEntry{{Index: 1, Term: 1, Command: command}}, LeaderCommit: 1}).Success)
			case "single":
				future := &applyFuture{cmd: command, errCh: make(chan error, 1), doneCh: make(chan struct{})}
				raft.processApply(context.Background(), future)
				require.NoError(t, <-future.errCh)
			case "discard":
				op := storage.NewNamespaceDrop(nil)
				require.NoError(t, storage.PrepareNamespaceDrop(context.Background(), base, "boundary:", op))
				op.SetPublicationRelease(probe.assertMetadataReleased)
				data, err := encodeGob(namespaceDropCommand{Prefix: "boundary:"})
				require.NoError(t, err)
				raft.log = append(raft.log, &RaftLogEntry{Index: 1, Term: 1, Command: &Command{Type: CmdDeleteByPrefix, Data: data, namespaceDrop: op}})
				require.True(t, raft.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 2, LeaderID: "peer", Entries: []*RaftLogEntry{{Index: 1, Term: 2}}}).Success)
				<-op.Done()
			}
		})
	}
}
