package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type failFirstDeleteNodeEngine struct {
	*storage.MemoryEngine
	failuresRemaining int
}

func (e *failFirstDeleteNodeEngine) DeleteNode(id storage.NodeID) error {
	if e.failuresRemaining > 0 {
		e.failuresRemaining--
		return fmt.Errorf("forced delete failure for %s", id)
	}
	return e.MemoryEngine.DeleteNode(id)
}

func TestExecuteDeleteStreaming_UnsupportedMatchShapeReturnsEmpty(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "delete_stream_match_err_cov"))
	res, err := exec.Execute(context.Background(), "BOGUS"+" DELETE "+"n", getParamsFromContext(context.Background()))
	require.Error(t, err)
	require.Nil(t, res)
}

func TestExecuteDeleteStreaming_FallbackMapEdgeBranch(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_stream_fallback_map_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Ghost {id:'g1'})", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:Ghost)"+" DELETE "+"{_edgeId:'missing-edge'}", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.EqualValues(t, 0, res.Stats.NodesDeleted)
	require.EqualValues(t, 0, res.Stats.RelationshipsDeleted)
}

func TestExecuteDeleteStreaming_FallbackDeletesAfterInitialDeleteFailure(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &failFirstDeleteNodeEngine{MemoryEngine: base, failuresRemaining: 1}
	store := storage.NewNamespacedEngine(eng, "delete_stream_fallback_delete_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: storage.NodeID("n-retry"), Labels: []string{"Retry"}, Properties: map[string]interface{}{"id": "n-retry"}})
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "MATCH (n:Retry)"+" DELETE "+"n", getParamsFromContext(ctx))
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Stats.NodesDeleted)

	verify, err := exec.Execute(ctx, "MATCH (n:Retry) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])
}
