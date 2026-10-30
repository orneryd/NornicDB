package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTransactionQueriesDoNotReadOtherNamespaces(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	for _, node := range []*storage.Node{
		{ID: "tenant:own", Labels: []string{"Shared"}, Properties: map[string]interface{}{"secret": "own"}},
		{ID: "tenantb:other", Labels: []string{"Shared"}, Properties: map[string]interface{}{"secret": "other"}},
		{ID: "system:admin", Labels: []string{"_User"}, Properties: map[string]interface{}{"password_hash": "sensitive"}},
	} {
		_, err := baseStore.CreateNode(node)
		require.NoError(t, err)
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "tenant"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"MATCH (n) RETURN count(n) AS c",
		"MATCH (n:Shared) RETURN count(n) AS c",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, countFromResult(t, result))
	}
	result, err := exec.Execute(ctx, "MATCH (n:_User) RETURN n.password_hash AS hash", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)

	result, err = exec.Execute(ctx, "MERGE (n:Shared {secret: 'other'}) RETURN n.secret AS secret", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	count, err := storage.NewNamespacedEngine(baseStore, "tenant").NodeCountByLabel("Shared")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

// TestExecuteInTransaction_ReusesExistingTransactionWrapper proves that
// recursive execution paths reuse the transaction wrapper already stored in
// context instead of re-wrapping the same Badger transaction and dropping the
// active namespace metadata.
func TestExecuteInTransaction_ReusesExistingTransactionWrapper(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "tenant")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)

	tx, ok := exec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok, "expected explicit transaction to use BadgerTransaction")

	txWrapper := &transactionStorageWrapper{
		tx:             tx,
		underlying:     exec.storage,
		namespace:      "tenant",
		separator:      ":",
		mutatedNodeIDs: make(map[string]struct{}),
	}
	txExec := exec.cloneWithStorage(txWrapper)
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, txWrapper)

	_, err = txExec.executeInTransaction(
		txCtx,
		"CREATE (:TxReuse {name: 'ctx-wrapper'})",
		strings.ToUpper("CREATE (:TxReuse {name: 'ctx-wrapper'})"),
	)
	require.NoError(t, err)
	require.Len(t, txWrapper.snapshotMutatedNodeIDs(), 1, "existing tx wrapper should record mutations")

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:TxReuse {name: 'ctx-wrapper'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, countFromResult(t, result))
}

func TestExecuteInTransaction_IgnoresStaleTransactionWrapper(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "tenant")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)

	currentTx, ok := exec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok, "expected explicit transaction to use BadgerTransaction")

	otherExec := NewStorageExecutor(storage.NewNamespacedEngine(baseStore, "tenant"))
	_, err = otherExec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	require.NotNil(t, otherExec.txContext)
	t.Cleanup(func() {
		if otherExec.txContext != nil && otherExec.txContext.active {
			_, _ = otherExec.Execute(ctx, "ROLLBACK", nil)
		}
	})

	staleTx, ok := otherExec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok, "expected stale transaction to use BadgerTransaction")
	require.NotEqual(t, staleTx, currentTx, "test requires distinct explicit transactions")

	staleWrapper := &transactionStorageWrapper{
		tx:             staleTx,
		underlying:     otherExec.storage,
		namespace:      "tenant",
		separator:      ":",
		mutatedNodeIDs: make(map[string]struct{}),
	}
	txCtx := context.WithValue(ctx, ctxKeyTxStorage, staleWrapper)

	_, err = exec.executeInTransaction(
		txCtx,
		"CREATE (:TxReuse {name: 'fresh-wrapper'})",
		strings.ToUpper("CREATE (:TxReuse {name: 'fresh-wrapper'})"),
	)
	require.NoError(t, err)
	require.Nil(t, staleWrapper.snapshotMutatedNodeIDs(), "stale wrapper should not be reused")

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:TxReuse {name: 'fresh-wrapper'}) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, countFromResult(t, result))
}

func TestExplicitTransaction_ReusesWrapperForRecursiveUnwindMatchMerge(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "tenant")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx,
		"MERGE (r:Repository {id: $repo_id}) SET r.name = $name, r.path = $path",
		map[string]any{
			"repo_id": "pcg-nornicdb-canonical",
			"name":    "pcg-nornicdb-canonical",
			"path":    "/tmp/pcg-nornicdb-canonical",
		},
	)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `UNWIND $rows AS row
MATCH (r:Repository {id: row.repo_id})
MERGE (d:Directory {path: row.path})
SET d.name = row.name, d.repo_id = row.repo_id,
    d.scope_id = row.scope_id, d.generation_id = row.generation_id
MERGE (r)-[:CONTAINS]->(d)`, map[string]any{
		"rows": []map[string]any{{
			"repo_id":       "pcg-nornicdb-canonical",
			"path":          "/tmp/pcg-nornicdb-canonical/src",
			"name":          "src",
			"scope_id":      "scope:pcg-nornicdb-canonical",
			"generation_id": "generation:pcg-nornicdb-canonical",
		}},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx,
		"MATCH (:Repository {id: $repo_id})-[:CONTAINS]->(d:Directory {path: $path}) RETURN count(*) AS c, max(d.scope_id) AS scope_id, max(d.generation_id) AS generation_id",
		map[string]any{
			"repo_id": "pcg-nornicdb-canonical",
			"path":    "/tmp/pcg-nornicdb-canonical/src",
		},
	)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.EqualValues(t, 1, countFromResult(t, result))
	require.Equal(t, "scope:pcg-nornicdb-canonical", result.Rows[0][1])
	require.Equal(t, "generation:pcg-nornicdb-canonical", result.Rows[0][2])
}
