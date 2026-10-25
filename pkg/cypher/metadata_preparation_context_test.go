package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestStagedMemoryPreparationCancellation(t *testing.T) {
	resetTransactionMemory(t)
	ConfigureTransactionMemory(0, TransactionMemoryLimits{TransactionMax: 64 << 20})
	e := newTransactionMemoryExecutor(t)
	run := func(q string) {
		t.Helper()
		_, err := e.Execute(context.Background(), q, nil)
		require.NoError(t, err, q)
	}
	run("BEGIN")
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &ddlPreparationContext{Context: parent, owner: "StagedMemoryBytesWithContext", cancel: cancel}
	_, err := e.Execute(ctx, "UNWIND range(1,20) AS i CREATE (:P {k:i})", nil)
	t.Logf("owner=%s hits=%d execute=%v", ctx.owner, ctx.hits, err)
	require.GreaterOrEqual(t, ctx.hits, 2)
	require.ErrorIs(t, err, context.Canceled)
	_, err = e.Execute(context.Background(), "CREATE (:MustNotPublish)", nil)
	require.Error(t, err, "canceled statement must fail its explicit transaction")
	run("ROLLBACK")
	result, err := e.Execute(context.Background(), "MATCH (n:P) RETURN n.k", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
	run("BEGIN")
	run("UNWIND range(1,20) AS i CREATE (:P {k:i})")
	run("COMMIT")
	requirePoolsEmpty(t, "nornic")
}

func TestManagedMetadataPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"StoredEntityCountDeltaWithContext", "ChangesWithContext", "storageChangesUsageWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			manager, err := multidb.NewDatabaseManager(b, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			db := manager.DefaultDatabaseName()
			limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100}}
			if owner != "StoredEntityCountDeltaWithContext" {
				limits.Storage.MaxBytes = 1 << 30
			}
			require.NoError(t, manager.SetDatabaseLimits(db, limits))
			store, err := manager.GetStorage(db)
			require.NoError(t, err)
			e := NewStorageExecutor(store)
			e.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			run := func(q string) {
				t.Helper()
				_, err := e.Execute(context.Background(), q, nil)
				require.NoError(t, err, q)
			}
			prepare := func() { run("BEGIN"); run("UNWIND range(1,20) AS i CREATE (:P {k:i})") }
			prepare()
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			if owner == "storageChangesUsageWithContext" {
				ctx.ownerPackage = "/pkg/multidb."
			}
			_, err = e.Execute(ctx, "COMMIT", nil)
			t.Logf("owner=%s hits=%d commit=%v", owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2)
			require.ErrorIs(t, err, context.Canceled)
			_, _ = e.Execute(context.Background(), "ROLLBACK", nil)
			nodes, err := b.GetStoredNodesByLabel("P")
			require.NoError(t, err)
			require.Empty(t, nodes, "metadata preparation cannot accept a mutation")
			prepare()
			run("COMMIT")
			nodes, err = b.GetStoredNodesByLabel("P")
			require.NoError(t, err)
			require.Len(t, nodes, 20)
		})
	}
}

// The first write after enabling quotas must not hide its baseline scan before
// native preparation. Byte tracking can also be enabled on an existing count ledger.
func TestColdManagedSnapshotPreparationCancellation(t *testing.T) {
	for _, upgrade := range []bool{false, true} {
		t.Run(map[bool]string{false: "first count snapshot", true: "count to byte snapshot"}[upgrade], func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			manager, err := multidb.NewDatabaseManager(b, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			db := manager.DefaultDatabaseName()
			if upgrade {
				require.NoError(t, manager.SetDatabaseLimits(db, &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100}}))
			}
			store, err := manager.GetStorage(db)
			require.NoError(t, err)
			e := NewStorageExecutor(store)
			e.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			_, err = e.Execute(context.Background(), "UNWIND range(1,20) AS i CREATE (:Baseline {k:i})", nil)
			require.NoError(t, err)
			limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100}}
			if upgrade {
				limits.Storage.MaxBytes = 1 << 30
			}
			require.NoError(t, manager.SetDatabaseLimits(db, limits))
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: "ForEachWithContext.func1", cancel: cancel}
			_, err = e.Execute(ctx, "CREATE (:MustNotPublish)", nil)
			t.Logf("upgrade=%v owner=%s hits=%d execute=%v", upgrade, ctx.owner, ctx.hits, err)
			require.GreaterOrEqual(t, ctx.hits, 2, "cancel in cold stored-identity iteration")
			require.ErrorIs(t, err, context.Canceled)
			nodes, err := b.GetStoredNodesByLabel("MustNotPublish")
			require.NoError(t, err)
			require.Empty(t, nodes)
			_, err = e.Execute(context.Background(), "CREATE (:Healthy)", nil)
			require.NoError(t, err)
			nodes, err = b.GetStoredNodesByLabel("Baseline")
			require.NoError(t, err)
			require.Len(t, nodes, 20)
		})
	}
}
