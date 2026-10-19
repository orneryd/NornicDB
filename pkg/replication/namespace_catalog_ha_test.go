package replication

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestNamespaceCreateHACancelAfterLocalCommit(t *testing.T) {
	base := storage.NewMemoryEngine()
	defer base.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	local := &solR3CancelAfterApply{StorageAdapter: adapter, cancel: cancel}
	cfg := DefaultConfig()
	cfg.HAStandby.SyncMode = SyncAsync
	replicator := &HAStandbyReplicator{config: cfg, storage: local, walStreamer: NewWALStreamer(adapter, 100)}
	replicator.started.Store(true)
	replicator.isPrimary.Store(true)
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(base, replicator, time.Second), nil)
	require.NoError(t, err)
	local.cancelType = CmdNamespaceCatalog
	cfg.HAStandby.SyncMode = SyncQuorum
	err = manager.CreateDatabaseContext(ctx, "ha_committed_create")
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, storage.MutationApplied(err), "stopping the ACK wait never rolls back an admitted CREATE")
	require.True(t, manager.Exists("ha_committed_create"))
	replica := storage.NewMemoryEngine()
	defer replica.Close()
	target, err := NewStorageAdapterWithWAL(replica, t.TempDir())
	require.NoError(t, err)
	defer target.Close()
	entries, err := adapter.GetWALEntries(0, 100)
	require.NoError(t, err)
	_, err = NewWALApplier(target).ApplyBatch(entries)
	require.NoError(t, err)
	standby, err := multidb.NewDatabaseManager(&namespaceCatalogStandby{replica}, nil)
	require.NoError(t, err)
	require.True(t, standby.Exists("ha_committed_create"))
}
