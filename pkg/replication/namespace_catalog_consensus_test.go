package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamespaceCreateConsensusPublishesOnlyAfterCommit(t *testing.T) {
	for _, outcome := range []string{"commit", "follower-commit", "discard"} {
		t.Run(outcome, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			_, err := multidb.NewDatabaseManager(base, nil)
			require.NoError(t, err)
			adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			defer adapter.Close()
			cfg := DefaultConfig()
			cfg.Mode = ModeRaft
			cfg.NodeID = "catalog"
			cfg.AdvertiseAddr = "127.0.0.1:17000"
			cfg.Raft.Peers = []PeerConfig{{ID: "peer", Addr: "127.0.0.1:17001"}}
			cfg.Raft.CommitTimeout = time.Second
			raft, err := NewRaftReplicator(cfg, adapter)
			require.NoError(t, err)
			raft.state, raft.currentTerm = StateLeader, 1
			raft.started.Store(true)
			manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, raft, time.Second), nil)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- manager.CreateDatabaseContext(ctx, "consensus_create") }()
			var future *applyFuture
			select {
			case future = <-raft.applyCh:
			case <-time.After(time.Second):
				t.Fatal("CREATE did not reach consensus admission")
			}
			raft.processApply(context.Background(), future)
			require.False(t, manager.Exists("consensus_create"), "a proposal is not a committed database")
			require.Equal(t, CmdNamespaceCatalog, future.cmd.Type)
			_, err = manager.GetStorage("nornic")
			require.NoError(t, err, "peer catalog reads stay available during consensus")
			_, err = base.CreateNode(&storage.Node{ID: "peer_namespace:live"})
			require.NoError(t, err)
			cancel()
			require.ErrorIs(t, <-result, context.Canceled)
			op := future.cmd.namespaceDrop
			require.False(t, op.Committed())
			if outcome == "discard" {
				response := raft.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 2, LeaderID: "peer", Entries: []*RaftLogEntry{{Index: 1, Term: 2}}})
				require.True(t, response.Success)
			} else if outcome == "follower-commit" {
				response := raft.handleAppendEntriesRequest(&AppendEntriesRequest{Term: 2, LeaderID: "peer", PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 1})
				require.True(t, response.Success)
			} else {
				raft.peerMu.Lock()
				raft.matchIndex["peer"] = 1
				raft.peerMu.Unlock()
				raft.advanceCommitIndex()
			}
			select {
			case <-op.Done():
			case <-time.After(time.Second):
				t.Fatal("CREATE lifecycle did not complete")
			}
			require.Equal(t, outcome != "discard", manager.Exists("consensus_create"))
			require.Equal(t, outcome != "discard", op.Committed())
		})
	}
}
