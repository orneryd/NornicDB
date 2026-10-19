package multidb

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// Interpose a real base writer at the existing journal-before-commit seam.
// Policy/embedding writers do not acquire the manager's admission lease.
type xhighJournalRace struct {
	*storage.ManagedWriteTransaction
	competing func() error
}

func (x *xhighJournalRace) PrepareCommit(ctx context.Context) (func(), error) {
	release, err := x.ManagedWriteTransaction.PrepareCommit(ctx)
	if err != nil {
		return nil, err
	}
	if err = x.competing(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
func TestXHighAbortedManagedWriteDoesNotReplay(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	base := storage.NewMemoryEngine()
	defer base.Close()
	dir := t.TempDir()
	wal, err := storage.NewWAL(dir, storage.DefaultWALConfig())
	require.NoError(t, err)
	journal := storage.NewWALEngine(base, wal)
	defer journal.Close()
	manager, err := NewDatabaseManager(journal, nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("abort_wal"))
	view, err := manager.GetStorage("abort_wal")
	require.NoError(t, err)
	_, err = view.CreateNode(&storage.Node{ID: "n", Properties: map[string]any{"payload": "original"}})
	require.NoError(t, err)
	tracker := view.(*sizeTrackingEngine)
	attempted := &storage.Node{ID: "n", Properties: map[string]any{"payload": "must-not-recover"}}
	tx := &xhighJournalRace{ManagedWriteTransaction: storage.NewManagedWrite(tracker.Engine, "abort_wal", storage.Operation{Type: storage.OpUpdateNode, Node: attempted}), competing: func() error {
		return base.UpdateNode(&storage.Node{ID: "abort_wal:n", Properties: map[string]any{"payload": "winner"}})
	}}
	err = tracker.commitTransaction(context.Background(), tx)
	require.ErrorIs(t, err, storage.ErrConflict)
	before, err := storage.GetStoredNode(view, "n")
	require.NoError(t, err)
	require.Equal(t, "winner", before.Properties["payload"])
	require.NoError(t, wal.Sync())
	entries, err := storage.ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	recovered := storage.NewMemoryEngine()
	defer recovered.Close()
	result := storage.ReplayWALEntries(recovered, entries)
	require.False(t, result.HasCriticalErrors(), "%s", result.Summary())
	after, err := storage.GetStoredNode(storage.NewNamespacedEngine(recovered, "abort_wal"), "n")
	require.NoError(t, err)
	t.Logf("rejected update: beforeReplay=%v afterReplay=%v", before.Properties["payload"], after.Properties["payload"])
	require.NotEqual(t, "must-not-recover", after.Properties["payload"], "recovery must not publish the mutation rejected by transaction conflict")
}
