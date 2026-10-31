package nornicdb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSol1482R3DeferredEdgeDrain(t *testing.T) {
	for _, warm := range []bool{false, true} {
		for _, remove := range []bool{false, true} {
			t.Run(fmt.Sprintf("warm=%t/remove=%t", warm, remove), func(t *testing.T) {
				base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
				t.Cleanup(func() { _ = base.Close() })
				store := storage.NewNamespacedEngine(base, "test")
				for _, id := range []storage.NodeID{"a", "b"} {
					_, err := store.CreateNode(&storage.Node{ID: id})
					require.NoError(t, err)
				}
				rel := &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "CITES", Properties: map[string]any{"vec": []float32{1, 0}}}
				require.NoError(t, store.CreateEdge(rel))
				svc := search.NewService(store, 2, nil)
				t.Cleanup(func() { _ = svc.Close() })
				if warm {
					require.NoError(t, svc.BuildIndexes(context.Background()))
				}
				entry := &dbSearchService{svc: svc}
				seedReservedSearchRefresh(entry, "r", search.RefreshRetry{Edge: entry.svc.ReserveEdgeMutation("r", storage.CaptureVectorWorkOrigin())})
				_, stale := entry.drainPending()
				rel.Properties["vec"] = []float32{0, 1}
				require.NoError(t, store.UpdateEdge(storage.EdgeUpdateRequest{After: rel}))
				require.NoError(t, svc.RefreshEdge("r", storage.CaptureVectorWorkOrigin()))
				if remove {
					require.NoError(t, store.DeleteEdge("r"))
					seedReservedSearchRefresh(entry, "r", search.RefreshRetry{Edge: entry.svc.ReserveEdgeMutation("r", storage.CaptureVectorWorkOrigin())})
					_, newer := entry.drainPending()
					require.NoError(t, svc.RefreshEdgeWithMutation(newer["r"]))
				}
				require.NoError(t, svc.RefreshEdgeWithMutation(stale["r"]))
				hits, err := svc.VectorQueryRelationships(context.Background(), []float32{0, 1}, search.RelationshipVectorQuerySpec{Type: "CITES", Property: "vec", Limit: 1})
				require.NoError(t, err)
				if remove {
					require.Empty(t, hits)
				} else {
					require.Len(t, hits, 1)
					require.InDelta(t, 1, hits[0].Score, 1e-6)
				}
			})
		}
	}
}

// Relationship text is optional; the live vector index still has to follow
// storage-event updates and creates (including replication) when it is off.
func TestSol1482R3EdgeEventsWithoutRelationshipText(t *testing.T) {
	ctx := context.Background()
	db, svc := openConsumerDB(t, false)
	v := make([]float64, db.config.Memory.EmbeddingDimensions)
	v[0] = 1
	w := make([]float32, len(v))
	w[1] = 1
	_, err := db.ExecuteCypher(ctx, "CREATE (:Doc)-[:CITES {vec:$v}]->(:Doc)", map[string]any{"v": v})
	require.NoError(t, err)
	edges, err := db.storage.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 1)
	edge := edges[0]
	require.True(t, svc.HasRelationshipVectorEntries("CITES", "vec"))
	edge.Properties["vec"] = w
	require.NoError(t, db.storage.UpdateEdge(storage.EdgeUpdateRequest{After: edge}))
	settleQueuedRefreshes()
	hits, err := svc.VectorQueryRelationships(ctx, w, search.RelationshipVectorQuerySpec{Type: "CITES", Property: "vec", Limit: 1})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.InDelta(t, 1, hits[0].Score, 1e-6, "a storage update did not refresh the edge's vector while relationship text is off")
	require.NoError(t, db.storage.DeleteEdge(edge.ID))
	settleQueuedRefreshes()
	require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"))
}
