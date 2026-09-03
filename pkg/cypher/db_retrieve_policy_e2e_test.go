package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const documentedStrictRetrieveQuery = `CALL db.retrieve({
  query: 'zero-trust architecture',
  limit: 10,
  candidateTarget: 50,
  adaptiveOverfetch: false,
  rrfK: 60,
  vectorWeight: 1.0,
  bm25Weight: 1.0,
  minRRFScore: 0.0,
  fallbackEnabled: false,
  filters: {
    lifecycle: 'active',
    generation: [3, 4],
    artifact: ['source', 'summary']
  }
})
YIELD node, score, rrf_score, vector_rank, bm25_rank, search_method, fallback_triggered
RETURN node, score, rrf_score`

func TestE2E_DbRetrieve_DocumentedStrictPolicyQuery(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{1, 0}})

	nodes := []*storage.Node{
		{ID: "active-source", Labels: []string{"Document"}, Properties: map[string]interface{}{
			"content": "zero-trust architecture source", "embedding": []float32{1, 0},
			"lifecycle": "active", "generation": int64(3), "artifact": "source",
		}},
		{ID: "active-summary-array", Labels: []string{"Document"}, Properties: map[string]interface{}{
			"content": "zero-trust architecture summary", "embedding": []float32{0.99, 0.01},
			"lifecycle": "active", "generation": int64(4), "artifact": []string{"derived", "summary"},
		}},
		{ID: "archived", Labels: []string{"Document"}, Properties: map[string]interface{}{
			"content": "zero-trust architecture archived", "embedding": []float32{1, 0},
			"lifecycle": "archived", "generation": int64(3), "artifact": "source",
		}},
		{ID: "wrong-generation", Labels: []string{"Document"}, Properties: map[string]interface{}{
			"content": "zero-trust architecture old generation", "embedding": []float32{1, 0},
			"lifecycle": "active", "generation": int64(5), "artifact": "source",
		}},
		{ID: "wrong-artifact", Labels: []string{"Document"}, Properties: map[string]interface{}{
			"content": "zero-trust architecture derived", "embedding": []float32{1, 0},
			"lifecycle": "active", "generation": int64(3), "artifact": "derived",
		}},
	}
	for _, node := range nodes {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}

	service := search.NewServiceWithDimensions(store, 2)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)

	result, err := exec.Execute(ctx, documentedStrictRetrieveQuery, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"node", "score", "rrf_score"}, result.Columns)
	require.Len(t, result.Rows, 2)

	ids := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Len(t, row, 3)
		node, ok := row[0].(*storage.Node)
		require.True(t, ok)
		ids = append(ids, string(node.ID))
		require.IsType(t, float64(0), row[1])
		require.Positive(t, row[2])
	}
	require.ElementsMatch(t, []string{"active-source", "active-summary-array"}, ids)
}

func TestE2E_DbRetrieve_StrictPolicyFlag(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{1, 0}})

	nodes := []*storage.Node{
		{ID: "active-source", Labels: []string{"Document"}, Properties: map[string]interface{}{
			"content": "zero-trust architecture source", "embedding": []float32{1, 0},
			"lifecycle": "active", "generation": int64(3), "artifact": "source",
		}},
		{ID: "archived", Labels: []string{"Document"}, Properties: map[string]interface{}{
			"content": "zero-trust architecture archived", "embedding": []float32{1, 0},
			"lifecycle": "archived", "generation": int64(3), "artifact": "source",
		}},
	}
	for _, node := range nodes {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}

	service := search.NewServiceWithDimensions(store, 2)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)

	result, err := exec.Execute(ctx, `CALL db.retrieve({
  query: 'zero-trust architecture',
  limit: 10,
  strictPolicy: true,
  filters: {lifecycle: 'active'}
})
YIELD node, score, search_method, fallback_triggered
RETURN node, search_method, fallback_triggered`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	node, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	require.Equal(t, "active-source", string(node.ID))
	require.Equal(t, "rrf_hybrid", result.Rows[0][1])
	require.Equal(t, false, result.Rows[0][2])
}
