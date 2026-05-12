package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type mvccSearchBenchFixture struct {
	service *Service
	engine  *storage.MemoryEngine
	query   []float32
	opts    *SearchOptions
}

func BenchmarkSearchService_VectorSearchCandidates_MVCCHistoryDepth(b *testing.B) {
	for _, historyDepth := range []int{1, 32, 128} {
		b.Run(fmt.Sprintf("history=%d", historyDepth), func(b *testing.B) {
			fixture := buildMVCCSearchBenchFixture(b, 2048, historyDepth, 32)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				candidates, err := fixture.service.VectorSearchCandidates(context.Background(), fixture.query, fixture.opts)
				if err != nil {
					b.Fatal(err)
				}
				if len(candidates) == 0 {
					b.Fatal("expected candidates")
				}
			}
		})
	}
}

func TestSearchService_VectorSearchCandidates_UnaffectedByMVCCPrune(t *testing.T) {
	fixture := buildMVCCSearchFixture(t, 30, 128, 32, false)
	fixture.opts = &SearchOptions{Limit: 5}
	before, err := fixture.service.VectorSearchCandidates(context.Background(), fixture.query, fixture.opts)
	require.NoError(t, err)
	require.NotEmpty(t, before)

	deleted, err := fixture.engine.PruneMVCCVersions(context.Background(), storage.MVCCPruneOptions{MaxVersionsPerKey: 100})
	require.NoError(t, err)
	require.Greater(t, deleted, int64(0))

	after, err := fixture.service.VectorSearchCandidates(context.Background(), fixture.query, fixture.opts)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func buildMVCCSearchBenchFixture(tb testing.TB, nodeCount, historyDepth, dimensions int) mvccSearchBenchFixture {
	return buildMVCCSearchFixture(tb, nodeCount, historyDepth, dimensions, true)
}

func buildMVCCSearchFixture(tb testing.TB, nodeCount, historyDepth, dimensions int, forceHNSW bool) mvccSearchBenchFixture {
	tb.Helper()

	// Use the history-retaining engine so UpdateNode archives prior bodies
	// into per-version MVCC records. The head-only default (NewMemoryEngine)
	// overwrites in place and the pruner has nothing to reclaim — which is
	// exactly what a test asserting `deleted > 0` needs to exercise.
	engine := storage.NewMemoryEngineWithMVCCHistory()
	tb.Cleanup(func() { _ = engine.Close() })

	ctx := context.Background()
	for nodeIndex := 0; nodeIndex < nodeCount; nodeIndex++ {
		nodeID := storage.NodeID(fmt.Sprintf("bench:search-mvcc-%05d", nodeIndex))
		vector := benchSearchEmbedding(dimensions, nodeIndex)
		_, err := engine.CreateNode(&storage.Node{
			ID:              nodeID,
			Labels:          []string{"Document"},
			Properties:      map[string]any{"title": fmt.Sprintf("document-%05d-v0", nodeIndex), "rank": nodeIndex},
			ChunkEmbeddings: [][]float32{vector},
		})
		if err != nil {
			tb.Fatal(err)
		}
		for version := 1; version < historyDepth; version++ {
			if err := engine.UpdateNode(&storage.Node{
				ID:              nodeID,
				Labels:          []string{"Document"},
				Properties:      map[string]any{"title": fmt.Sprintf("document-%05d-v%d", nodeIndex, version), "rank": nodeIndex, "version": version},
				ChunkEmbeddings: [][]float32{vector},
			}); err != nil {
				tb.Fatal(err)
			}
		}
	}

	svc := NewServiceWithDimensions(engine, dimensions)
	if err := svc.BuildIndexes(ctx); err != nil {
		tb.Fatal(err)
	}
	if svc.EmbeddingCount() != nodeCount {
		tb.Fatalf("expected %d indexed embeddings, got %d", nodeCount, svc.EmbeddingCount())
	}

	if forceHNSW {
		_, vi, vfs := svc.snapshotStrategyInputs()
		hnswIndex, err := svc.buildHNSWForTransition(ctx, dimensions, vi, vfs)
		if err != nil {
			tb.Fatal(err)
		}
		svc.indexMu.Lock()
		svc.applyTransitionSwapLocked(strategyModeHNSW, hnswIndex, vi, vfs)
		svc.indexMu.Unlock()
		if svc.currentPipelineStrategy() != strategyModeHNSW {
			tb.Fatalf("expected HNSW strategy, got %s", svc.currentPipelineStrategy())
		}
	}

	return mvccSearchBenchFixture{
		service: svc,
		engine:  engine,
		query:   benchSearchEmbedding(dimensions, 0),
		opts:    &SearchOptions{Limit: 10},
	}
}

func benchSearchEmbedding(dimensions, seed int) []float32 {
	vec := make([]float32, dimensions)
	for i := range vec {
		bucket := (seed*17 + i*13) % 31
		vec[i] = float32(bucket+1) / 31
	}
	return vec
}
