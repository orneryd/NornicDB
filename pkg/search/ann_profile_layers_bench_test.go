package search

import (
	"context"
	"encoding/binary"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkANNProfileLayers separates graph traversal, pipeline scoring and the
// public vector-query consumer using one persisted graph and identical queries.
// Set NORNICDB_PROFILE_DATA_DIR/DB/DIMS and NORNICDB_PROFILE_ANN_QUERIES to a
// little-endian float32 query matrix with the configured number of dimensions.
func BenchmarkANNProfileLayers(b *testing.B) {
	queryPath := os.Getenv("NORNICDB_PROFILE_ANN_QUERIES")
	if queryPath == "" || os.Getenv("NORNICDB_PROFILE_DATA_DIR") == "" {
		b.Skip("requires an explicit persisted ANN fixture and query matrix")
	}
	b.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	b.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	b.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	svc, _ := loadSearchProfileDiskFixture(b)
	b.Cleanup(func() { require.NoError(b, svc.Close()) })
	labels, types, named, properties, chunks, ok := svc.vectorFileStore.NodeQueryMetadataWithTypes()
	require.True(b, ok, "public-layer profiling requires persisted vector identity metadata")
	svc.mu.Lock()
	svc.nodeLabels, svc.nodeTypes, svc.nodeNamedVector, svc.nodePropVector, svc.nodeChunkVectors = labels, types, named, properties, chunks
	svc.mu.Unlock()
	raw, err := os.ReadFile(queryPath)
	require.NoError(b, err)
	dims := svc.VectorIndexDimensions()
	require.Positive(b, dims)
	require.NotEmpty(b, raw)
	require.Zero(b, len(raw)%(dims*4))
	queries := make([][]float32, len(raw)/(dims*4))
	for i := range queries {
		queries[i] = make([]float32, dims)
		for j := range queries[i] {
			queries[i][j] = math.Float32frombits(binary.LittleEndian.Uint32(raw[(i*dims+j)*4:]))
		}
	}
	b.Logf("fixture metadata: labels=%d named=%d properties=%d chunks=%d", len(labels), len(named), len(properties), len(chunks))
	ctx := context.Background()
	require.NoError(b, svc.ConfigureHNSWQuantization(ctx, svc.hnswIndex.Config().Quantization))
	pipeline, err := svc.getOrCreateVectorPipeline(ctx)
	require.NoError(b, err)
	require.IsType(b, &HNSWCandidateGen{}, pipeline.candidateGen)
	require.NotNil(b, svc.hnswIndex)
	// Mirror Qdrant Search: top-10 overfetches 15, with explicit fast ef=50.
	const candidates, beam = 15, 50
	ctx = withHNSWEf(ctx, beam)
	b.Run("graph", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			hits, err := svc.hnswIndex.SearchWithEf(ctx, queries[i%len(queries)], beam, -1, beam)
			if err != nil || len(hits) < 10 {
				b.Fatalf("graph: hits=%d error=%v", len(hits), err)
			}
		}
	})
	b.Run("pipeline", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			hits, err := pipeline.Search(ctx, queries[i%len(queries)], candidates, -1)
			if err != nil || len(hits) < 10 {
				b.Fatalf("pipeline: hits=%d error=%v", len(hits), err)
			}
		}
	})
	b.Run("public", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			hits, err := svc.VectorQueryNodesWithOptions(ctx, queries[i%len(queries)], VectorQuerySpec{Label: "QdrantPoint", Property: "default", Limit: 10, ScoreScale: VectorScoreSimilarity}, &SearchOptions{HNSWEf: beam})
			if err != nil || len(hits) != 10 {
				b.Fatalf("public: hits=%d error=%v", len(hits), err)
			}
		}
	})
}
