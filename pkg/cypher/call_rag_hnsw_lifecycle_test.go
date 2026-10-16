package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// First-use retrieval uses exact scoring through the same eligibility and
// ordering path while graph construction starts in the background.
func TestCallRRetrieveFirstUseExactEligibility(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	executor := NewStorageExecutor(store)
	svc := search.NewServiceWithDimensions(store, 3)
	t.Cleanup(func() { _ = svc.Close() })
	for _, node := range []*storage.Node{
		{ID: "ineligible", Labels: []string{"Item"}, Properties: map[string]interface{}{"name": "excluded", "eligible": false}, ChunkEmbeddings: [][]float32{{1, 0, 0}}},
		{ID: "best", Labels: []string{"Item"}, Properties: map[string]interface{}{"name": "best", "eligible": true}, ChunkEmbeddings: [][]float32{{1, 0.1, 0}}},
		{ID: "next", Labels: []string{"Item"}, Properties: map[string]interface{}{"name": "next", "eligible": true}, ChunkEmbeddings: [][]float32{{1, 0.5, 0}}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}
	executor.SetSearchService(svc)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := executor.Execute(ctx, `CALL db.rretrieve({query: 'needle', embedding: [1.0, 0.0, 0.0], limit: 2, min_similarity: 0.0, where: 'node.eligible = true'})
	YIELD node, search_method RETURN node.name AS name, search_method`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	require.Equal(t, "best", result.Rows[0][0])
	require.Equal(t, "next", result.Rows[1][0])
	require.Equal(t, "rrf_hybrid", result.Rows[0][1])
}
