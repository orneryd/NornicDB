package storage_test

// Read-only transaction retention regression from the integrated review.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Read-only work in a transaction should not be charged per node read.
func TestReview1655R2ReadImageRetention(t *testing.T) {
	base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	manager, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	store, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	exec := cypher.NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	_, err = exec.Execute(ctx, "UNWIND range(1, 20000) AS i CREATE (:Item {v:i, pad:'xxxxxxxxxxxxxxxx'})", nil)
	require.NoError(t, err)

	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("nornic"))
	before := tx.StagedMemoryBytes()
	nodes, err := tx.GetNodesByLabel("Item")
	require.NoError(t, err)
	require.Len(t, nodes, 20000)
	after := tx.StagedMemoryBytes()
	require.Equal(t, before, after, "reads must not retain per-node transaction state")
	require.NoError(t, tx.Rollback())
	t.Logf("read-only transaction staged bytes: before=%d after reading %d nodes=%d (%.1f bytes/node)", before, len(nodes), after, float64(after-before)/float64(len(nodes)))

	cypher.SetDatabaseTransactionMemoryLimits("nornic", cypher.TransactionMemoryLimits{TransactionMax: 6 << 20})
	t.Cleanup(func() { cypher.SetDatabaseTransactionMemoryLimits("nornic", cypher.TransactionMemoryLimits{}) })
	explicit := cypher.NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	_, err = explicit.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	defer explicit.Execute(context.Background(), "ROLLBACK", nil)
	result, err := explicit.Execute(ctx, "MATCH (n:Item) WHERE n.v < 0 RETURN n.v AS v", nil)
	if result != nil {
		t.Logf("explicit read rows=%v", result.Rows)
	}
	require.NoError(t, err, "a one-row read-only statement over 20000 nodes must fit a 6 MiB transaction limit")
	_, err = explicit.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}
