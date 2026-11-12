package cypher

import (
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestManagedVectorAdmissionPreservesPinnedSnapshot(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		g := newLargeEmbeddingGraph(t, 0)
		_, err := g.store.CreateNode(t.Context(), &storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "source"}})
		require.NoError(t, err)
		service := search.NewService(g.store, 3, nil)
		t.Cleanup(func() { service.Close() })
		g.exec.SetSearchService(service)
		g.run(t, "BEGIN", nil)
		require.Len(t, g.run(t, "MATCH (n:Doc) RETURN n.title", nil).Rows, 1)
		g.embed(t, "source", []float32{1, 0, 0})
		fresh, err := g.store.GetNode(t.Context(), "source")
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(fresh))
		const query = "CALL db.index.vector.queryNodes('default', 10, [1.0,0.0,0.0]) YIELD node, score RETURN node.title"
		result, err := g.exec.Execute(t.Context(), query, nil)
		require.NoError(t, err, "a pinned snapshot with no vectors must not misclassify a live managed population as a missing schema index")
		require.Empty(t, result.Rows, "the older snapshot must not see newly published vectors")
		g.run(t, "ROLLBACK", nil)
		g.run(t, "BEGIN", nil)
		defer g.exec.Execute(t.Context(), "ROLLBACK", nil)
		require.Len(t, g.run(t, query, nil).Rows, 1, "a fresh transaction sees the published vector")

	})
}
