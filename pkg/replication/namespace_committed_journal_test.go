package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDropCommittedReplayFinishesDespiteSecondaryJournalFailure(t *testing.T) {
	for _, mode := range []string{"raft", "wal"} {
		t.Run(mode, func(t *testing.T) {
			b := storage.NewMemoryEngine()
			defer b.Close()
			m, err := multidb.NewDatabaseManager(b, nil)
			require.NoError(t, err)
			require.NoError(t, m.CreateDatabase("committed_a"))
			_, err = b.CreateNode(&storage.Node{ID: "committed_a:old"})
			require.NoError(t, err)
			_, err = b.CreateNode(&storage.Node{ID: "committed_b:peer"})
			require.NoError(t, err)
			old, err := b.BeginTransaction()
			require.NoError(t, err)
			defer old.Rollback()
			require.NoError(t, old.SetNamespace("committed_a"))
			a, err := NewStorageAdapterWithWAL(b, t.TempDir())
			require.NoError(t, err)
			defer a.Close()
			data, err := encodeGob(namespaceDropCommand{Prefix: "committed_a:", Retirement: 7})
			require.NoError(t, err)
			cmd := &Command{Type: CmdDeleteByPrefix, Data: data}
			require.NoError(t, a.wal.Close())
			if mode == "wal" {
				_, err = NewWALApplier(a).ApplyBatch([]*WALEntry{{Position: 1, Command: cmd}})
			} else {
				cfg := DefaultConfig()
				cfg.Mode = ModeRaft
				cfg.NodeID = "committed"
				cfg.AdvertiseAddr = "127.0.0.1:17000"
				cfg.Raft.Peers = nil
				r, makeErr := NewRaftReplicator(cfg, a)
				require.NoError(t, makeErr)
				r.state, r.currentTerm = StateLeader, 1
				future := &applyFuture{caller: context.Background(), cmd: cmd, errCh: make(chan error, 1), doneCh: make(chan struct{})}
				r.processApply(context.Background(), future)
				select {
				case err = <-future.errCh:
				case <-time.After(time.Second):
					t.Fatal("committed DROP completion blocked")
				}
			}
			require.ErrorIs(t, err, storage.ErrWALClosed)
			require.True(t, storage.MutationApplied(err), "secondary journal failure cannot roll back an already committed entry")
			require.False(t, m.Exists("committed_a"))
			_, err = b.GetNode("committed_a:old")
			require.ErrorIs(t, err, storage.ErrNotFound)
			_, err = b.GetNode("committed_b:peer")
			require.NoError(t, err)
			require.ErrorIs(t, old.Commit(), storage.ErrConflict)
		})
	}
}
