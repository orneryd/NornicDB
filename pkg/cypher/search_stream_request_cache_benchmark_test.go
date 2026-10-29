package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func BenchmarkRRetrieveOneShotCache(b *testing.B) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"content": "library transcript"}})
		require.NoError(b, err)
	}
	svc := search.NewService(store, search.DefaultVectorDimensions, nil)
	b.Cleanup(func() { require.NoError(b, svc.Close()) })
	require.NoError(b, svc.BuildIndexes(context.Background()))
	for _, parameterized := range []bool{false, true} {
		name, query := "literal", "CALL db.rretrieve({query: 'transcript', limit: 2})"
		if parameterized {
			name, query = "parameterized", "CALL db.rretrieve($request)"
		}
		b.Run(name, func(b *testing.B) {
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 64, TTL: time.Hour})
			exec.SetSearchService(svc)
			params := map[string]any{"request": map[string]any{"query": "transcript", "limit": int64(2)}}
			_, err := exec.Execute(context.Background(), query, params)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := exec.Execute(context.Background(), query, params)
				if err != nil || len(result.Rows) != 2 {
					b.Fatalf("retrieve: %v, result=%v", err, result)
				}
			}
		})
	}
}
