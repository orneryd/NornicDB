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
		targets := make([]int, 0, 17)
		for target := 64; target < 80; target++ {
			targets = append(targets, target)
		}
		targets = append(targets, 80+source%16)
		for _, target := range targets {
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

// BenchmarkGDSLinkPrediction measures the shared projection selection and the
// undirected relationship-degree reducer on 5,000 nodes and 20,000 edges.
func BenchmarkGDSLinkPrediction(b *testing.B) {
	for _, algorithm := range []string{"commonNeighbors", "adamicAdar"} {
		b.Run(algorithm, func(b *testing.B) {
			exec, ids := newGDSBenchmarkExecutor(b)
			ctx := context.Background()
			if _, err := exec.Execute(ctx, gdsBenchmarkScalarProjection, nil); err != nil {
				b.Fatal(err)
			}
			query := "CALL gds.linkPrediction." + algorithm + ".stream('gds-bench', {sourceNode:$source, topK:100})"
			params := map[string]interface{}{"source": ids[0], "nonce": 0}
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				params["nonce"] = i
				result, err := exec.Execute(ctx, query, params)
				if err != nil {
					b.Fatal(err)
				}
				gdsBenchmarkResult = result
			}
		})
	}
}
