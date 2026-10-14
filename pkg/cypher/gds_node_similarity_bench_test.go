package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkNodeSimilarity measures procedure execution on a projected graph
// with parallel relationships in two types, shared targets and distinct targets.
func BenchmarkNodeSimilarity(b *testing.B) {
	graphProjections = make(map[string]*GraphProjection)
	b.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "nsbench")
	for i := 0; i < 96; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%03d", i)), Labels: []string{"N"}})
		require.NoError(b, err)
	}
	for source := 0; source < 64; source++ {
		for target := 64; target < 80; target++ {
			for _, kind := range []string{"A", "Z"} {
				require.NoError(b, store.CreateEdge(&storage.Edge{
					ID:        storage.EdgeID(fmt.Sprintf("%s-%03d-%03d", kind, source, target)),
					StartNode: storage.NodeID(fmt.Sprintf("n%03d", source)), EndNode: storage.NodeID(fmt.Sprintf("n%03d", target)),
					Type: kind, Properties: map[string]interface{}{"w": 1.0},
				}))
			}
		}
	}
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CALL gds.graph.project('nsbench', 'N', {A: {properties:'w'}, Z: {properties:'w'}})", nil)
	require.NoError(b, err)
	for _, metric := range []string{"JACCARD", "OVERLAP", "COSINE"} {
		b.Run(metric, func(b *testing.B) {
			query := "CALL gds.nodeSimilarity.stream('nsbench', {relationshipWeightProperty:'w', similarityMetric:'" + metric + "'})"
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, query, map[string]interface{}{"nonce": i}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
