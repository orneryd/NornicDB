package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDropConsensusWaitsForPriorApply(t *testing.T) {
	for _, cancelPreparation := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "cancel"}[cancelPreparation], func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
			require.NoError(t, err)
			defer adapter.Close()
			cfg := DefaultConfig()
			cfg.Mode, cfg.NodeID, cfg.AdvertiseAddr = ModeRaft, "frontier", "127.0.0.1:17000"
			cfg.Raft.Peers = []PeerConfig{{ID: "peer", Addr: "127.0.0.1:17001"}}
			cfg.Raft.CommitTimeout = time.Second
			raft, err := NewRaftReplicator(cfg, adapter)
			require.NoError(t, err)
			raft.state, raft.currentTerm = StateLeader, 1
			node, err := encodeNodePayload(&storage.Node{ID: "frontier_a:prior"})
			require.NoError(t, err)
			raft.log = append(raft.log, &RaftLogEntry{Index: 1, Term: 1, Command: &Command{Type: CmdCreateNode, Data: node}})
			data, err := encodeGob(struct{ Prefix string }{"frontier_a:"})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			op := storage.NewNamespaceDrop(nil)
			future := &applyFuture{caller: ctx, cmd: &Command{Type: CmdDeleteByPrefix, Data: data, namespaceDrop: op}, errCh: make(chan error, 1), doneCh: make(chan struct{})}
			prepared := make(chan struct{})
			go func() { raft.processApply(context.Background(), future); close(prepared) }()
			select {
			case <-prepared:
				t.Fatal("DROP installed its fence before the preceding entry applied")
			case <-time.After(30 * time.Millisecond):
			}
			if cancelPreparation {
				cancel()
				select {
				case <-prepared:
				case <-time.After(time.Second):
					t.Fatal("prior-apply admission ignored cancellation")
				}
				require.ErrorIs(t, <-future.errCh, context.Canceled)
				require.Len(t, raft.log, 2)
			}
			raft.peerMu.Lock()
			raft.matchIndex["peer"] = 1
			raft.peerMu.Unlock()
			raft.advanceCommitIndex()
			_, err = base.GetNode("frontier_a:prior")
			require.NoError(t, err, "DROP preparation must not fence preceding committed writes")
			if cancelPreparation {
				return
			}
			select {
			case <-prepared:
			case <-time.After(time.Second):
				t.Fatal("DROP did not progress after the fixed frontier applied")
			}
			raft.peerMu.Lock()
			raft.matchIndex["peer"] = 2
			raft.peerMu.Unlock()
			raft.advanceCommitIndex()
			select {
			case <-op.Done():
			case <-time.After(time.Second):
				t.Fatal("DROP did not complete")
			}
			_, err = base.GetNode("frontier_a:prior")
			require.ErrorIs(t, err, storage.ErrNotFound)
		})
	}
}
