package cypher

import (
	"context"
	"fmt"
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

// Author: #1667 implementation lane (Codex). Native public registry preparation,
// also consumed by replicated leader admission; no parser is involved here.
func TestManagedCommandPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"managedEntityOwnerForWithContext", "qualifyEntityOperationsWithContext", "BeginEntityTransactionWithContext", "prepareManagedDirect", "ApplyEntityChangeWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			manager, err := multidb.NewDatabaseManager(context.Background(), b, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			db := manager.DefaultDatabaseName()
			require.NoError(t, manager.SetDatabaseLimits(context.Background(), db, &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100}}))
			store, err := manager.GetStorage(db)
			require.NoError(t, err)
			baseline, err := b.AllNodes()
			require.NoError(t, err)
			baselineIDs := make([]storage.NodeID, len(baseline))
			for i, node := range baseline {
				baselineIDs[i] = node.ID
			}
			ops := make([]storage.Operation, 20)
			for i := range ops {
				ops[i] = storage.Operation{Type: storage.OpCreateNode, Node: &storage.Node{ID: storage.NodeID(fmt.Sprintf("p%d", i)), Labels: []string{"CancellationCandidate"}}}
			}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &ddlPreparationContext{Context: parent, owner: owner, cancel: cancel}
			if owner == "managedEntityOwnerForWithContext" || owner == "qualifyEntityOperationsWithContext" || owner == "prepareManagedDirect" {
				ctx.ownerPackage = "/pkg/multidb."
			}
			after := 2
			if owner == "BeginEntityTransactionWithContext" {
				ctx.after, after = 1, 1
			}
			prepared, settle, handled, err := storage.PrepareManagedEntityChanges(ctx, store, db, ops, storage.CommitModeImplicit)
			if prepared != nil {
				prepared.Abort()
				if settle != nil {
					settle(storage.CommitOutcome{State: storage.CommitDiscarded})
				}
			}
			t.Logf("owner=%s hits=%d prepare=%v", owner, ctx.hits, err)
			require.True(t, handled)
			require.GreaterOrEqual(t, ctx.hits, after)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, prepared)
			nodes, err := b.GetStoredNodesByLabel("CancellationCandidate")
			require.NoError(t, err)
			require.Empty(t, nodes)
			afterNodes, err := b.AllNodes()
			require.NoError(t, err)
			afterIDs := make([]storage.NodeID, len(afterNodes))
			for i, node := range afterNodes {
				afterIDs[i] = node.ID
			}
			require.ElementsMatch(t, baselineIDs, afterIDs, "cancellation preserves existing metadata and publishes no candidate")
		})
	}
}

func TestManagedMetadataPreparationCancellation(t *testing.T) {
	for _, owner := range []string{"StoredEntityCountDeltaWithContext", "ChangesWithContext", "storageChangesUsageWithContext"} {
		t.Run(owner, func(t *testing.T) {
			b, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			manager, err := multidb.NewDatabaseManager(context.Background(), b, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			db := manager.DefaultDatabaseName()
			limits := &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100}}
			if owner != "StoredEntityCountDeltaWithContext" {
				limits.Storage.MaxBytes = 1 << 30
			}
			require.NoError(t, manager.SetDatabaseLimits(context.Background(), db, limits))
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
			manager, err := multidb.NewDatabaseManager(context.Background(), b, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = manager.Close() })
			db := manager.DefaultDatabaseName()
			if upgrade {
				require.NoError(t, manager.SetDatabaseLimits(context.Background(), db, &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 100}}))
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
			require.NoError(t, manager.SetDatabaseLimits(context.Background(), db, limits))
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
