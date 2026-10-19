package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchService_IndexNodeSkipsUnchangedVectorReindex(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)
	svc.hnswMu.Lock()
	svc.hnswIndex = NewHNSWIndex(4, DefaultHNSWConfig())
	svc.hnswMu.Unlock()

	node := &storage.Node{
		ID:     "entity-1",
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":           "FalkorDB",
			"summary":        "initial",
			"name_embedding": []float64{1, 0, 0, 0},
		},
	}
	require.NoError(t, svc.IndexNode(node))
	require.Equal(t, 1, svc.EmbeddingCount())
	initialHNSWSlots := hnswAllocatedSlots(svc.hnswIndex)

	unchangedVector := &storage.Node{
		ID:     "entity-1",
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":           "FalkorDB",
			"summary":        "scalar update only",
			"name_embedding": []float64{1, 0, 0, 0},
		},
	}
	require.NoError(t, svc.IndexNode(unchangedVector))
	require.Equal(t, 1, svc.EmbeddingCount())
	afterUnchangedSlots := hnswAllocatedSlots(svc.hnswIndex)
	require.Equal(t, initialHNSWSlots, afterUnchangedSlots, "unchanged vector writes must not mutate HNSW")

	changedVector := &storage.Node{
		ID:     "entity-1",
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":           "FalkorDB",
			"summary":        "vector update",
			"name_embedding": []float64{0, 1, 0, 0},
		},
	}
	require.NoError(t, svc.IndexNode(changedVector))
	require.Equal(t, 1, svc.EmbeddingCount())
	afterChangedSlots := hnswAllocatedSlots(svc.hnswIndex)
	require.Greater(t, afterChangedSlots, afterUnchangedSlots, "changed vector writes must still update HNSW state")
}

func hnswAllocatedSlots(index *HNSWIndex) int {
	index.mu.RLock()
	defer index.mu.RUnlock()
	return len(index.nodeLevel)
}

func BenchmarkSearchServiceIndexNodeUnchangedVector_CopyScaleHNSW(b *testing.B) {
	const (
		nodeCount = 1500
		dim       = 1024
	)
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, dim)
	for i := 0; i < nodeCount; i++ {
		node := &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("entity-%04d", i)),
			Labels: []string{"Entity"},
			Properties: map[string]any{
				"name":           fmt.Sprintf("entity %04d", i),
				"name_embedding": benchmarkUnitVector(i%dim, dim),
			},
		}
		require.NoError(b, svc.IndexNode(node))
	}
	_, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(b, err)
	requireHNSWReady(b, svc)

	target := &storage.Node{
		ID:     "entity-0000",
		Labels: []string{"Entity"},
		Properties: map[string]any{
			"name":           "entity 0000",
			"summary":        "same vector",
			"name_embedding": benchmarkUnitVector(0, dim),
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		target.Properties["summary"] = fmt.Sprintf("same vector %d", i)
		if err := svc.IndexNode(target); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkUnitVector(active, dim int) []float32 {
	vec := make([]float32, dim)
	vec[active%dim] = 1
	return vec
}
