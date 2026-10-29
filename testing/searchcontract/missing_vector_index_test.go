package searchcontract

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestMissingVectorIndexUsesManagedEmbeddings keeps the service's index-only
// fixture and the procedure's stored, lazily wired fixture as separate public
// paths over the same named-versus-chunk vector population.
func TestMissingVectorIndexUsesManagedEmbeddings(t *testing.T) {
	for _, route := range []struct {
		name, namespace, namedID, chunkID string
	}{
		{"service", "", "named_non_default", "chunk_fallback"},
		{"procedure", "test", "doc-managed", "doc-chunk"},
	} {
		t.Run(route.name, func(t *testing.T) {
			base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			var engine storage.Engine = base
			if route.namespace != "" {
				engine = storage.NewNamespacedEngine(base, route.namespace)
			}
			nodes := []*storage.Node{
				{ID: storage.NodeID(route.namedID), Labels: []string{"Doc"}, NamedEmbeddings: map[string][]float32{"managed": {1, 0, 0}}},
				{ID: storage.NodeID(route.chunkID), Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0.6, 0.4, 0}}},
			}
			ctx := context.Background()
			if route.name == "service" {
				svc := search.NewService(engine, 3, nil)
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
				for _, node := range nodes {
					require.NoError(t, svc.IndexNode(node))
				}
				hits, err := svc.VectorQueryNodes(ctx, []float32{1, 0, 0}, search.VectorQuerySpec{
					IndexName: "missing_idx", Label: "Doc", Property: "", Limit: 5,
				})
				require.NoError(t, err)
				require.NotEmpty(t, hits)
				require.Equal(t, route.namedID, hits[0].ID)
				return
			}
			for _, node := range nodes {
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
			}
			exec := cypher.NewStorageExecutor(engine)
			res, err := exec.Execute(ctx, "CALL db.index.vector.queryNodes('missing_idx', 5, [1.0, 0.0, 0.0]) YIELD node, score", nil)
			require.NoError(t, err)
			require.NotNil(t, res)
			require.NotEmpty(t, res.Rows)
			topNode, ok := res.Rows[0][0].(*storage.Node)
			require.True(t, ok)
			require.Equal(t, storage.NodeID(route.namedID), topNode.ID)
		})
	}
}
