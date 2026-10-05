package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindCallInTransactionsPipeline_BatchesSeeStatementParameters(t *testing.T) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
	_, err := exec.Execute(context.Background(), "UNWIND $rows AS x CALL { WITH x CREATE (:T {v: x, tag: $tag}) } IN TRANSACTIONS OF 2 ROWS", map[string]interface{}{
		"tag":  "batched",
		"rows": []interface{}{int64(1), int64(2), int64(3)},
	})
	require.NoError(t, err)

	result, err := exec.Execute(context.Background(), "MATCH (t:T {tag: 'batched'}) RETURN t.v ORDER BY t.v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, result.Rows)
}

func TestVariableScopeCallInTransactionsFallback_BatchesSeeStatementParameters(t *testing.T) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
	_, err := exec.Execute(context.Background(), "UNWIND range(1, 3) AS i CREATE (:X {id: i})", nil)
	require.NoError(t, err)
	seeds, err := exec.storage.GetNodesByLabel("X")
	require.NoError(t, err)

	ctx := withQueryParams(context.Background(), map[string]interface{}{"mark": "seen"})
	_, err = exec.executeVariableScopeCallInTransactions(ctx, seeds, "n", "WITH n SET n.mark = $mark", "", 2)
	require.NoError(t, err)

	result, err := exec.Execute(context.Background(), "MATCH (n:X {mark: 'seen'}) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
}
