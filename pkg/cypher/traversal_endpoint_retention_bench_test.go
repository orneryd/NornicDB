package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Exercise the public scalar traversal with caller-owned unrelated bodies.
// The same fixture is frozen in both A/B arms; result caching is disabled.
func BenchmarkTraversalEndpointScalarRetention(b *testing.B) {
	store := &endpointRetentionEngine{NamespacedEngine: storage.NewNamespacedEngine(newTestMemoryEngine(b), "retained_bench"), payloadBytes: 32 << 10}
	_, err := store.CreateNode(&storage.Node{ID: "root", Labels: []string{"Seed"}, Properties: map[string]interface{}{"rank": int64(0)}})
	require.NoError(b, err)
	for i := 1; i <= 128; i++ {
		id := storage.NodeID(fmt.Sprintf("end-%03d", i))
		_, err = store.CreateNode(&storage.Node{ID: id, Labels: []string{"Endpoint"}, Properties: map[string]interface{}{"rank": int64(i)}})
		require.NoError(b, err)
		require.NoError(b, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("link-%03d", i)), StartNode: "root", EndNode: id, Type: "LINK"}))
	}
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	query := "MATCH (s:Seed)-[:LINK*1..2]->(e:Endpoint) RETURN sum(e.rank)"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(ctx, query, nil)
		if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != int64(8256) {
			b.Fatalf("scalar traversal changed: result=%v error=%v", result, err)
		}
	}
}
