package search

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPropertyToString_SkipsDenseNumericVectors(t *testing.T) {
	got := propertyToString([]float32{0.1, 0.2, 0.3})
	require.Equal(t, "", got)

	got = propertyToString([]float64{0.1, 0.2, 0.3})
	require.Equal(t, "", got)

	denseAny := make([]any, 64)
	for i := range denseAny {
		denseAny[i] = float64(i) / 10.0
	}
	got = propertyToString(denseAny)
	require.Equal(t, "", got)

	smallAny := []any{1.0, 2.0, 3.0}
	got = propertyToString(smallAny)
	require.Equal(t, "1 2 3", got)
}

func TestVectorFromPropertyValue_DimensionAware(t *testing.T) {
	vec, ok := vectorFromPropertyValue([]float32{1, 2, 3}, 3)
	require.True(t, ok)
	require.Equal(t, []float32{1, 2, 3}, vec)

	_, ok = vectorFromPropertyValue([]float32{1, 2}, 3)
	require.False(t, ok)

	vec, ok = vectorFromPropertyValue([]any{1.0, 2.0, 3.0}, 3)
	require.True(t, ok)
	require.Equal(t, []float32{1, 2, 3}, vec)
}

func TestVectorFromPropertyValue_AdditionalBranches(t *testing.T) {
	_, ok := vectorFromPropertyValue([]float32{1, 2, 3}, 0)
	require.False(t, ok)

	vec, ok := vectorFromPropertyValue([]float64{1, 2, 3}, 3)
	require.True(t, ok)
	require.Equal(t, []float32{1, 2, 3}, vec)

	_, ok = vectorFromPropertyValue([]float64{1, 2}, 3)
	require.False(t, ok)

	vec, ok = vectorFromPropertyValue([]any{float32(1), int(2), int64(3)}, 3)
	require.True(t, ok)
	require.Equal(t, []float32{1, 2, 3}, vec)

	_, ok = vectorFromPropertyValue([]any{1, "bad", 3}, 3)
	require.False(t, ok)

	_, ok = vectorFromPropertyValue("not-a-vector", 3)
	require.False(t, ok)
}

func TestPropertyToString_AdditionalBranches(t *testing.T) {
	require.Equal(t, "hello", propertyToString("hello"))
	require.Equal(t, "a b", propertyToString([]string{"a", "b"}))
	require.Equal(t, "42", propertyToString(42))
	require.Equal(t, "true", propertyToString(true))
	require.Equal(t, "false", propertyToString(false))
	require.Equal(t, "", propertyToString(map[string]any{"k": "v"}))
}

func TestLooksLikeDenseNumericSlice_Branches(t *testing.T) {
	require.False(t, looksLikeDenseNumericSlice([]any{1, 2, 3}))

	mixed := make([]any, 40)
	for i := 0; i < 30; i++ {
		mixed[i] = float64(i)
	}
	for i := 30; i < len(mixed); i++ {
		mixed[i] = "x"
	}
	require.False(t, looksLikeDenseNumericSlice(mixed))

	dense := make([]any, 40)
	for i := range dense {
		dense[i] = float64(i)
	}
	require.True(t, looksLikeDenseNumericSlice(dense))
}

func TestBuildIndexes_IndexesNamedChunkAndPropertyVectors(t *testing.T) {
	t.Parallel()

	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 3)

	node := &storage.Node{
		ID:     "nornic:doc-vectors",
		Labels: []string{"Doc"},
		Properties: map[string]any{
			"title":     "vectorized doc",
			"customVec": []float32{0, 1, 0},
		},
		NamedEmbeddings: map[string][]float32{
			"titleVec": {1, 0, 0},
		},
		ChunkEmbeddings: [][]float32{
			{0, 0, 1},
			{0, 1, 0},
		},
	}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	require.NoError(t, svc.BuildIndexes(context.Background()))

	// Expected vectors:
	// - named: "nornic:doc-vectors-named-titleVec"
	// - chunks: chunk 0 under the main id + chunk-1
	// - custom property vector: "nornic:doc-vectors-prop-customVec"
	require.Equal(t, 4, svc.EmbeddingCount())

	named := svc.nodeNamedVector["nornic:doc-vectors"]
	require.NotNil(t, named)
	require.Equal(t, "nornic:doc-vectors-named-titleVec", named["titleVec"])

	props := svc.nodePropVector["nornic:doc-vectors"]
	require.NotNil(t, props)
	require.Equal(t, "nornic:doc-vectors-prop-customVec", props["customVec"])

	chunks := svc.nodeChunkVectors["nornic:doc-vectors"]
	require.Contains(t, chunks, "nornic:doc-vectors")
	require.NotContains(t, chunks, "nornic:doc-vectors-chunk-0")
	require.Contains(t, chunks, "nornic:doc-vectors-chunk-1")
}

func TestBuildIndexes_IndexesRelationshipPropertyVectors(t *testing.T) {
	t.Parallel()

	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 3)

	_, err := engine.CreateNode(&storage.Node{ID: "nornic:a"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:b"})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{
		ID:        "nornic:r1",
		StartNode: "nornic:a",
		EndNode:   "nornic:b",
		Type:      "RELATES_TO",
		Properties: map[string]any{
			"fact_embedding": []float32{1, 0, 0},
			"fact":           "alpha fact",
		},
	}))

	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.True(t, svc.HasRelationshipVectorEntries("RELATES_TO", "fact_embedding"))

	hits, err := svc.VectorQueryRelationships(context.Background(), []float32{1, 0, 0}, RelationshipVectorQuerySpec{
		Type:       "RELATES_TO",
		Property:   "fact_embedding",
		Similarity: "cosine",
		Limit:      5,
	})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "nornic:r1", hits[0].ID)
	require.Greater(t, hits[0].Score, 0.99)
}

func TestIndexEdge_ReplacesExistingRelationshipVectors(t *testing.T) {
	t.Parallel()

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	edge := &storage.Edge{
		ID:        "r1",
		StartNode: "a",
		EndNode:   "b",
		Type:      "RELATES_TO",
		Properties: map[string]any{
			"fact_embedding": []float32{1, 0},
		},
	}
	require.NoError(t, svc.IndexEdge(edge))

	edge.Properties["fact_embedding"] = []float32{0, 1}
	require.NoError(t, svc.IndexEdge(edge))

	hits, err := svc.VectorQueryRelationships(context.Background(), []float32{1, 0}, RelationshipVectorQuerySpec{
		Type:       "RELATES_TO",
		Property:   "fact_embedding",
		Similarity: "cosine",
		Limit:      5,
	})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "r1", hits[0].ID)
	// The replaced vector is orthogonal to the query: Neo4j's cosine score
	// 0.5 (the old one would score 1).
	require.Equal(t, 0.5, hits[0].Score)
}

func BenchmarkRelationshipVectorQueryIndexedVsStorageScan(b *testing.B) {
	const (
		dim       = 32
		edgeCount = 8192
		limit     = 50
	)

	engine, err := storage.NewBadgerEngineInMemory()
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })

	_, err = engine.CreateNode(&storage.Node{ID: "nornic:a"})
	require.NoError(b, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:b"})
	require.NoError(b, err)
	for i := 0; i < edgeCount; i++ {
		require.NoError(b, engine.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID("nornic:rel-" + formatBenchID(i)),
			StartNode: "nornic:a",
			EndNode:   "nornic:b",
			Type:      "RELATES_TO",
			Properties: map[string]any{
				"fact_embedding": benchmarkRelationshipVector(i, dim),
				"fact":           "relationship vector benchmark",
			},
		}))
	}

	ctx := context.Background()
	query := benchmarkRelationshipVector(0, dim)
	svc := NewServiceWithDimensions(engine, dim)
	require.NoError(b, svc.BuildIndexes(ctx))
	require.True(b, svc.HasRelationshipVectorEntries("RELATES_TO", "fact_embedding"))

	b.ReportAllocs()
	b.Run("storage_scan_baseline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			hits := benchmarkRelationshipStorageScan(b, ctx, engine, query, limit)
			if len(hits) == 0 {
				b.Fatal("expected relationship hits")
			}
		}
	})

	b.Run("indexed_relationship_vectors", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			hits, err := svc.VectorQueryRelationships(ctx, query, RelationshipVectorQuerySpec{
				Type:       "RELATES_TO",
				Property:   "fact_embedding",
				Similarity: "cosine",
				Limit:      limit,
			})
			require.NoError(b, err)
			if len(hits) == 0 {
				b.Fatal("expected relationship hits")
			}
		}
	})
}

func benchmarkRelationshipStorageScan(tb testing.TB, ctx context.Context, engine storage.Engine, query []float32, limit int) []RelationshipVectorQueryHit {
	tb.Helper()
	hits := make([]RelationshipVectorQueryHit, 0, limit)
	err := storage.StreamEdgesWithFallback(ctx, engine, 1000, func(edge *storage.Edge) error {
		if edge.Type != "RELATES_TO" {
			return nil
		}
		vec, ok := vectorFromPropertyValue(edge.Properties["fact_embedding"], len(query))
		if !ok {
			return nil
		}
		hits = append(hits, RelationshipVectorQueryHit{
			ID:    string(edge.ID),
			Score: vector.CosineSimilarity(query, vec),
		})
		return nil
	})
	require.NoError(tb, err)
	sort.Slice(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
	if len(hits) > limit {
		hits = hits[:limit]
	}
	return hits
}

func benchmarkRelationshipVector(seed, dim int) []float32 {
	vec := make([]float32, dim)
	base := seed % dim
	vec[base] = 1
	vec[(base+3)%dim] = 0.35
	vec[(base+7)%dim] = 0.2
	vector.NormalizeInPlace(vec)
	return vec
}

func formatBenchID(i int) string {
	const width = 8
	var buf [width]byte
	for pos := width - 1; pos >= 0; pos-- {
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[:])
}
