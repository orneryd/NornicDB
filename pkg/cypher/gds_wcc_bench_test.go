package cypher

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	wccBenchmarkNodes         = 10000
	wccBenchmarkRelationships = 15000
)

// BenchmarkWCC measures weakly connected components over 10,000 nodes and
// 15,000 relationships of three types (random ends, fixed seed: one large
// component and many small ones and singletons): apoc.algo.wcc over the
// stored graph, and gds.wcc.stream / gds.wcc.stats over a projection,
// whole and filtered to two types. It uses only the public Cypher API, so
// it also builds before gds.wcc existed; those cases skip there.
func BenchmarkWCC(b *testing.B) {
	graphProjections = make(map[string]*GraphProjection)
	b.Cleanup(func() { graphProjections = make(map[string]*GraphProjection) })
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "wccbench")
	for i := 0; i < wccBenchmarkNodes; i++ {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%05d", i)), Labels: []string{"BenchWCC"}})
		require.NoError(b, err)
	}
	random := rand.New(rand.NewSource(1))
	types := []string{"HAS_VERSION", "HAS_ORIGINAL", "SAME_DOCUMENT"}
	for i := 0; i < wccBenchmarkRelationships; i++ {
		require.NoError(b, store.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("r%05d", i)),
			StartNode: storage.NodeID(fmt.Sprintf("n%05d", random.Intn(wccBenchmarkNodes))),
			EndNode:   storage.NodeID(fmt.Sprintf("n%05d", random.Intn(wccBenchmarkNodes))),
			Type:      types[i%len(types)],
		}))
	}
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CALL gds.graph.project('wcc-bench', 'BenchWCC', ['HAS_VERSION', 'HAS_ORIGINAL', 'SAME_DOCUMENT'])", nil)
	require.NoError(b, err)
	for _, bench := range []struct{ name, query string }{
		{"apoc", "CALL apoc.algo.wcc('BenchWCC') YIELD node, componentId RETURN count(*)"},
		{"gds_stream", "CALL gds.wcc.stream('wcc-bench') YIELD nodeId, componentId RETURN count(*)"},
		{"gds_stream_types", "CALL gds.wcc.stream('wcc-bench', {relationshipTypes: ['HAS_VERSION', 'HAS_ORIGINAL']}) YIELD nodeId, componentId RETURN count(*)"},
		{"gds_stats", "CALL gds.wcc.stats('wcc-bench') YIELD componentCount RETURN componentCount"},
	} {
		b.Run(bench.name, func(b *testing.B) {
			if _, err := exec.Execute(ctx, bench.query, nil); err != nil {
				if strings.HasPrefix(bench.name, "gds") && strings.Contains(err.Error(), "gds.wcc") {
					b.Skip("this API has no gds.wcc")
				}
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, bench.query, map[string]interface{}{"nonce": i}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
