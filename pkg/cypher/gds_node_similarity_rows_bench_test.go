package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkSimilarityRows measures one source's top 10 by resource
// allocation with a hub cutoff (B9): nornicdb.similarity.stream over
// source-centred rows against the same scoring written in Cypher, on
// documents holding 8 things each plus one hub every document holds.
func BenchmarkSimilarityRows(b *testing.B) {
	for _, documents := range []int{200, 2000} {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "nsrows")
		things := documents / 2
		for i := 0; i < documents; i++ {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("d%05d", i)), Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": i}})
			require.NoError(b, err)
		}
		for i := 0; i <= things; i++ {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("t%05d", i)), Labels: []string{"Thing"}})
			require.NoError(b, err)
		}
		seed := uint32(7)
		for i := 0; i < documents; i++ {
			held := map[int]bool{things: true} // the hub
			for len(held) < 9 {
				seed = seed*1664525 + 1013904223
				held[int(seed>>8)%things] = true
			}
			for thing := range held {
				require.NoError(b, store.CreateEdge(&storage.Edge{
					ID:        storage.EdgeID(fmt.Sprintf("h%05d-%05d", i, thing)),
					StartNode: storage.NodeID(fmt.Sprintf("d%05d", i)), EndNode: storage.NodeID(fmt.Sprintf("t%05d", thing)), Type: "H",
				}))
			}
		}
		exec := NewStorageExecutor(store)
		ctx := context.Background()
		queries := map[string]string{
			"rows": "MATCH (seed:Doc {id: $id})-[:H]->(thing)<-[:H]-(other:Doc) " +
				"WITH seed, collect(DISTINCT {source: other, thing: thing}) + [(seed)-[:H]->(t) | {source: seed, thing: t}] AS rows " +
				"CALL nornicdb.similarity.stream(rows, {sourceNode: seed, similarityMetric: 'RESOURCE_ALLOCATION', maxHolders: 100}) " +
				"YIELD node2, similarity RETURN node2, similarity",
			"cypher": "MATCH (seed:Doc {id: $id})-[:H]->(thing)<-[:H]-(other:Doc) WHERE other <> seed " +
				"WITH other, thing, COUNT { (thing)<-[:H]-(:Doc) } AS holders WHERE holders <= 100 " +
				"RETURN other.id AS node2, sum(1.0 / holders) AS similarity ORDER BY similarity DESC, node2 LIMIT 10",
		}
		for _, name := range []string{"rows", "cypher"} {
			b.Run(fmt.Sprintf("%s/docs=%d", name, documents), func(b *testing.B) {
				params := map[string]interface{}{"id": 0}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					params["id"] = i % documents
					result, err := exec.Execute(ctx, queries[name], params)
					if err != nil {
						b.Fatal(err)
					}
					gdsBenchmarkResult = result
				}
			})
		}
	}
}
