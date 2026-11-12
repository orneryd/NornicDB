package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// Index construction legitimately reads vectors. Subsequent result-cache
// misses must not repeatedly hydrate those vectors just to return node metadata,
// even when the full-node cache cannot retain the matching node.
func TestVectorQueryMissesUseMetadataAfterNodeCacheEviction(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		graph := newNOR413ColdDocGraph(t) // one-entry node cache
		spy := storagefault.NewQueryNodeReadSpy(graph.store)
		service := search.NewService(spy, 2048, nil)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		require.NoError(t, service.BuildIndexes(t.Context()))
		exec := NewStorageExecutor(spy, QueryCachePolicy{MaxEntries: 0})
		exec.SetSearchService(service)
		const query = "CALL db.index.vector.queryNodes('doc_chunks',1,$q) YIELD node,score " +
			"CALL { WITH node RETURN node.title AS title } RETURN title,score"
		for attempt := 0; attempt < 3; attempt++ {
			// Evict the selected document before every read. Distinct vectors
			// also prevent a service result key from hiding the hydration path.
			_, err := graph.store.GetNode(t.Context(), "hub")
			require.NoError(t, err)
			vector := make([]float64, 2048)
			vector[3], vector[0] = 1, float64(attempt+1)/100
			spy.Full.Store(0)
			spy.Query.Store(0)
			result, err := exec.Execute(t.Context(), query, map[string]interface{}{"q": vector})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, "doc 3", result.Rows[0][0])
			require.Greater(t, result.Rows[0][1].(float64), 0.99)
			require.Zero(t, spy.Full.Load(), "a metadata result must not decode evicted stored vectors")
			require.Positive(t, spy.Query.Load(), "each cache miss reaches the metadata reader")
		}
	})
}
