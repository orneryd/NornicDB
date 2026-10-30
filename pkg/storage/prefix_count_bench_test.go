package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkBadgerEngine_NodeCountByPrefix_Warm measures the cached namespace
// count path after the cache has been populated by mutations.
func BenchmarkBadgerEngine_NodeCountByPrefix_Warm(b *testing.B) {
	badger, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	defer badger.Close()

	for index := 0; index < 100; index++ {
		_, err := badger.CreateNode(&Node{
			ID:         NodeID(fmt.Sprintf("nornic:n-%03d", index)),
			Labels:     []string{"Doc"},
			Properties: map[string]any{"seq": int64(index)},
		})
		require.NoError(b, err)
	}
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count, err := badger.NodeCountByPrefix("nornic:")
		if err != nil {
			b.Fatal(err)
		}
		if count != 100 {
			b.Fatalf("count = %d, want 100", count)
		}
	}
}

// BenchmarkBadgerEngine_EdgeCountByPrefix_Warm measures the cached edge count
// path after the cache has been populated by mutations.
func BenchmarkBadgerEngine_EdgeCountByPrefix_Warm(b *testing.B) {
	badger, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	defer badger.Close()

	for index := 0; index < 100; index++ {
		_, err := badger.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("nornic:n-%03d", index)),
			Labels: []string{"Doc"},
		})
		require.NoError(b, err)
	}
	for index := 0; index < 50; index++ {
		err := badger.CreateEdge(&Edge{
			ID:        EdgeID(fmt.Sprintf("nornic:e-%03d", index)),
			Type:      "R",
			StartNode: "nornic:n-000",
			EndNode:   "nornic:n-001",
		})
		require.NoError(b, err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count, err := badger.EdgeCountByPrefix("nornic:")
		if err != nil {
			b.Fatal(err)
		}
		if count != 50 {
			b.Fatalf("count = %d, want 50", count)
		}
	}
}
