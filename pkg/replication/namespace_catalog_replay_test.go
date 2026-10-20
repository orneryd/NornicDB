package replication

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type namespaceCatalogStandby struct{ storage.Engine }

func (s *namespaceCatalogStandby) IsLeader() bool                 { return false }
func (s *namespaceCatalogStandby) GetInnerEngine() storage.Engine { return s.Engine }

func TestNamespaceDropRecreatePublishesReplicaCatalog(t *testing.T) {
	for _, mode := range []string{"stream", "different-history", "snapshot"} {
		t.Run(mode, func(t *testing.T) { testNamespaceCatalogReplay(t, mode) })
	}
}

func testNamespaceCatalogReplay(t *testing.T, mode string) {
	primary, standby := storage.NewMemoryEngine(), storage.NewMemoryEngine()
	defer primary.Close()
	defer standby.Close()
	if mode != "stream" {
		for i := 0; i < 2; i++ {
			_, _, err := primary.DeleteByPrefix("local_history:")
			require.NoError(t, err)
		}
	}
	if mode == "different-history" {
		for i := 0; i < 5; i++ {
			_, _, err := standby.DeleteByPrefix("local_history:")
			require.NoError(t, err)
		}
	}
	adapter, err := NewStorageAdapterWithWAL(primary, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	follower, err := NewStorageAdapterWithWAL(standby, t.TempDir())
	require.NoError(t, err)
	defer follower.Close()
	r := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, r.Start(context.Background()))
	manager, err := multidb.NewDatabaseManager(NewReplicatedEngine(primary, r, time.Second), nil)
	require.NoError(t, err)
	require.NoError(t, manager.CreateDatabase("catalog_a"))
	applier := NewWALApplier(follower)
	var cursor uint64
	syncReplica := func() {
		require.NoError(t, adapter.FlushWAL())
		entries, err := adapter.GetWALEntries(cursor, 100)
		require.NoError(t, err)
		_, err = applier.ApplyBatch(entries)
		require.NoError(t, err)
		if len(entries) != 0 {
			cursor = entries[len(entries)-1].Position
		}
	}
	if mode == "snapshot" {
		require.NoError(t, adapter.FlushWAL())
		var snapshot bytes.Buffer
		require.NoError(t, adapter.WriteSnapshot(&snapshot))
		require.NoError(t, follower.RestoreSnapshot(&snapshot))
		cursor, err = adapter.GetWALPosition()
		require.NoError(t, err)
	} else {
		syncReplica()
	}
	replicaManager, err := multidb.NewDatabaseManager(&namespaceCatalogStandby{standby}, nil)
	require.NoError(t, err)
	before, _ := replicaManager.DatabaseIdentity("catalog_a")
	require.NotEmpty(t, before)
	old, err := standby.BeginTransaction()
	require.NoError(t, err)
	defer old.Rollback()
	require.NoError(t, old.SetNamespace("catalog_a"))
	require.NoError(t, manager.DropDatabase("catalog_a"))
	require.NoError(t, manager.CreateDatabase("catalog_a"))
	after, _ := manager.DatabaseIdentity("catalog_a")
	require.NotEqual(t, before, after)
	syncReplica()
	require.ErrorIs(t, old.Commit(), storage.ErrConflict, "portable catalog revisions must not replace the local transaction lifetime")
	require.True(t, replicaManager.Exists("catalog_a"), "committed recreate must publish to a running standby catalog")
	actual, _ := replicaManager.DatabaseIdentity("catalog_a")
	require.Equal(t, after, actual, "replica catalog must bind the recreated logical lifetime")
}
