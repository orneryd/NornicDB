package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamespaceCreateReplicationJournalFailurePublishesNothing(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	replicator := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, replicator.Start(context.Background()))
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, replicator, time.Second), nil)
	require.NoError(t, err)
	require.NoError(t, adapter.FlushWAL())
	require.NoError(t, adapter.wal.Close())
	err = manager.CreateDatabase("unadmitted_create")
	require.ErrorIs(t, err, storage.ErrWALClosed)
	require.False(t, storage.MutationApplied(err))
	require.False(t, manager.Exists("unadmitted_create"))
}

func TestNamespaceFencedReplicationWriteNeverEntersJournal(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	op := storage.NewNamespaceDrop(nil)
	require.NoError(t, storage.PrepareNamespaceDrop(context.Background(), base, "fenced_a:", op))
	defer op.Abort()
	before, err := adapter.GetWALPosition()
	require.NoError(t, err)
	data, err := encodeNodePayload(&storage.Node{ID: "fenced_a:ghost"})
	require.NoError(t, err)
	require.ErrorIs(t, adapter.ApplyCommand(&Command{Type: CmdCreateNode, Data: data}), storage.ErrConflict)
	require.NoError(t, adapter.FlushWAL())
	entries, err := adapter.GetWALEntries(before, 100)
	require.NoError(t, err)
	require.Empty(t, entries, "a rejected write must never reappear on a standby after DROP")
	peer, err := encodeNodePayload(&storage.Node{ID: "fenced_b:peer"})
	require.NoError(t, err)
	require.NoError(t, adapter.ApplyCommand(&Command{Type: CmdCreateNode, Data: peer}))
}
