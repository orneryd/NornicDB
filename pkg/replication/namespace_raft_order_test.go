package replication

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRaftNamespacedPartialPrefixDelete(t *testing.T) {
	for _, prefix := range []string{"del:", "del"} {
		t.Run(prefix, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			defer adapter.Close()
			cfg := DefaultConfig()
			cfg.Mode, cfg.NodeID = ModeRaft, "partial"
			cfg.Raft.Bootstrap = true
			raft, err := NewRaftReplicator(cfg, adapter)
			require.NoError(t, err)
			raft.state, raft.currentTerm = StateLeader, 1
			raft.started.Store(true)
			view := storage.NewNamespacedEngine(NewReplicatedEngine(base, raft, time.Second), "tenant")
			for _, id := range []storage.NodeID{"tenant:del:a", "tenant:del:b", "tenant:keep", "other:del:a"} {
				_, err := base.CreateNode(&storage.Node{ID: id, Labels: []string{"Item"}})
				require.NoError(t, err)
			}
			for _, edge := range []*storage.Edge{
				{ID: "tenant:del:edge", StartNode: "tenant:del:a", EndNode: "tenant:del:b", Type: "T"},
				{ID: "tenant:keep:edge", StartNode: "tenant:keep", EndNode: "tenant:keep", Type: "T"},
			} {
				require.NoError(t, base.CreateEdge(edge))
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				_, _, err := view.DeleteByPrefixContext(ctx, prefix)
				result <- err
			}()
			select {
			case future := <-raft.applyCh:
				raft.processApply(context.Background(), future)
			case <-ctx.Done():
				t.Fatal("partial deletion did not reach Raft")
			}
			require.NoError(t, <-result)
			for _, id := range []storage.NodeID{"tenant:del:a", "tenant:del:b"} {
				_, err := base.GetNode(id)
				require.ErrorIs(t, err, storage.ErrNotFound)
			}
			for _, id := range []storage.NodeID{"tenant:keep", "other:del:a"} {
				_, err := base.GetNode(id)
				require.NoError(t, err)
			}
			_, err = base.GetEdge("tenant:del:edge")
			require.ErrorIs(t, err, storage.ErrNotFound)
			_, err = base.GetEdge("tenant:keep:edge")
			require.NoError(t, err)
			_, err = base.CreateNode(&storage.Node{ID: "tenant:fresh"})
			require.NoError(t, err, "partial deletion must not retain a namespace fence")
		})
	}
}

type namespaceOrderStorage struct {
	*StorageAdapter
	writeEntered chan struct{}
	results      chan error
}

func (s *namespaceOrderStorage) ApplyCommandContext(ctx context.Context, cmd *Command) error {
	if cmd.Type == CmdCreateNode {
		close(s.writeEntered)
	}
	err := s.StorageAdapter.ApplyCommandContext(ctx, cmd)
	s.results <- err
	return err
}

func TestRaftNamespaceCompletionOrdersFollowingWrite(t *testing.T) {
	for _, mode := range []string{"single-node", "leader", "follower"} {
		t.Run(mode, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			_, err := base.CreateNode(&storage.Node{ID: "ordered:old"})
			require.NoError(t, err)
			adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			defer adapter.Close()
			target := &namespaceOrderStorage{adapter, make(chan struct{}), make(chan error, 2)}
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			defer release()
			unregister := storage.RegisterNamespaceRetirement(base, func(namespace string) error {
				if namespace == "ordered" {
					close(entered)
					<-resume
				}
				return nil
			})
			defer unregister()
			cfg := DefaultConfig()
			cfg.Mode, cfg.NodeID = ModeRaft, "ordered"
			cfg.Raft.Bootstrap = true
			if mode != "single-node" {
				cfg.Raft.Peers = []PeerConfig{{ID: "peer", Addr: "127.0.0.1:17001"}}
			}
			raft, err := NewRaftReplicator(cfg, target)
			require.NoError(t, err)
			raft.state, raft.currentTerm = StateLeader, 1
			data, err := encodeGob(namespaceDropCommand{Prefix: "ordered:"})
			require.NoError(t, err)
			drop := &Command{Type: CmdDeleteByPrefix, Data: data}
			node, err := encodeNodePayload(&storage.Node{ID: "ordered:fresh", Labels: []string{"Item"}})
			require.NoError(t, err)
			write := &Command{Type: CmdCreateNode, Data: node}
			entries := []*RaftLogEntry{{Index: 1, Term: 1, Command: drop}, {Index: 2, Term: 1, Command: write}}
			if mode == "leader" {
				raft.log = append(raft.log, entries...)
				raft.matchIndex["peer"] = 2
			}
			done := make(chan bool, 1)
			go func() {
				switch mode {
				case "single-node":
					for _, cmd := range []*Command{drop, write} {
						future := &applyFuture{cmd: cmd, errCh: make(chan error, 1), doneCh: make(chan struct{})}
						raft.processApply(context.Background(), future)
					}
				case "leader":
					raft.advanceCommitIndex()
				case "follower":
					response := raft.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 1, LeaderID: "peer", Entries: entries, LeaderCommit: 2})
					done <- response.Success
					return
				}
				done <- true
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("DROP did not enter retirement completion")
			}
			var overtook bool
			select {
			case <-target.writeEntered:
				overtook = true
			case <-time.After(30 * time.Millisecond):
			}
			release()
			select {
			case success := <-done:
				require.True(t, success)
			case <-time.After(time.Second):
				t.Fatal("Raft apply did not complete after retirement resumed")
			}
			select {
			case <-drop.namespaceDrop.Done():
			case <-time.After(time.Second):
				t.Fatal("namespace completion did not release its fence")
			}
			require.False(t, overtook, "later entries must wait for namespace completion")
			require.NoError(t, <-target.results)
			require.NoError(t, <-target.results)
			require.Equal(t, uint64(2), raft.lastApplied)
			_, err = base.GetNode("ordered:old")
			require.ErrorIs(t, err, storage.ErrNotFound)
			_, err = base.GetNode("ordered:fresh")
			require.NoError(t, err, "the same committed write must survive on every replica")
		})
	}
}
