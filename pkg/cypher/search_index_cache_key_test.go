package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCachedSearchProcedureSeesIndexChanges: a cached read that calls a
// search procedure is recomputed once the search index changes, although the
// graph version doesn't move: a node's vector is indexed after its embedding
// is saved (#945).
func TestCachedSearchProcedureSeesIndexChanges(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	svc := search.NewServiceWithDimensions(store, 2)
	exec := NewStorageExecutor(store)
	exec.SetSearchService(svc)
	ctx := context.Background()
	node := &storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]any{"id": "source", "text": "source"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := store.CreateNode(node)
	require.NoError(t, err)
	const query = "CALL db.rretrieve({query: 'source', embedding: $v, limit: 10, minSimilarity: 0.01}) YIELD node RETURN node.id AS id"
	params := map[string]any{"v": []float32{1, 0}}
	rows := func() [][]interface{} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err)
		return result.Rows
	}

	require.Empty(t, rows(), "not indexed yet")
	require.Empty(t, rows(), "served from the cache")
	require.NoError(t, svc.IndexNode(node))
	require.Equal(t, [][]interface{}{{"source"}}, rows(), "indexed: the cached empty result no longer applies")
	require.NoError(t, svc.RemoveNode(node.ID))
	require.Empty(t, rows(), "removed from the index")
}
