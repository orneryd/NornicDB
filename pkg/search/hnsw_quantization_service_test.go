package search

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/math/vector"
	"math"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQuantizedHNSWPublicServicePreservesLexicalSeedsAndExactResults(t *testing.T) {
	for _, mode := range []string{"", "none", "scalar", "binary", "pq"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
			t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
			t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "fast")
			t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", mode)
			engine := newNamespacedEngine(t)
			for i := 0; i < 256; i++ {
				angle := float64(i) * 2 * math.Pi / 256
				_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprint(i)), Properties: map[string]any{"text": "lexical alpha beta"}, NamedEmbeddings: map[string][]float32{"default": {float32(math.Cos(angle)), float32(math.Sin(angle))}}})
				require.NoError(t, err)
			}
			svc := NewServiceWithDimensions(engine, 2)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			require.NoError(t, svc.BuildIndexes(context.Background()))
			hits, err := svc.VectorQueryNodesWithOptions(context.Background(), []float32{1, 0}, VectorQuerySpec{Property: "default", Limit: 1, ScoreScale: VectorScoreSimilarity}, &SearchOptions{HNSWEf: 256, QuantizationOversampling: 256})
			require.NoError(t, err)
			require.Len(t, hits, 1)
			require.Equal(t, "0", hits[0].ID)
			require.InDelta(t, 1, hits[0].Score, 1e-6)
			require.NotNil(t, svc.hnswIndex)
			if mode == "none" {
				require.Zero(t, svc.hnswIndex.QuantizationBytes())
			} else {
				require.Positive(t, svc.hnswIndex.QuantizationBytes())
			}
			svc.hnswIndex.mu.RLock()
			defer svc.hnswIndex.mu.RUnlock()
			expected := HNSWQuantization(mode)
			if mode == "" {
				expected = HNSWQuantizationScalar
			}
			if mode == "none" {
				expected = HNSWQuantizationNone
			}
			require.Equal(t, expected, svc.hnswIndex.config.Quantization.Mode)
			require.NotEmpty(t, svc.hnswIndex.buildLexicalHints)
			for _, hint := range svc.hnswIndex.buildLexicalHints {
				require.NotZero(t, hint.Signature)
			}
		})
	}
}

func TestHNSWPQActivatesWhenTrainingPopulationReady(t *testing.T) {
	cfg := DefaultHNSWConfig()
	idx := NewHNSWIndex(2, cfg)
	require.NoError(t, idx.SetQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}))
	for i := 0; i < 256; i++ {
		require.NoError(t, idx.Add(fmt.Sprint(i), []float32{float32(i + 1), 1}))
	}
	_, err := idx.Search(context.Background(), []float32{1, 0}, 10, -1)
	require.NoError(t, err)
	require.Positive(t, idx.QuantizationBytes())
}

func TestHNSWQuantizationTrainingPrioritizesLexicalSeeds(t *testing.T) {
	idx, _ := quantizationTestIndex(t)
	idx.SetBuildLexicalHints([]LexicalSeedHint{{ID: "200", Rank: 0, Signature: 1}, {ID: "100", Rank: 1, Signature: 2}})
	idx.mu.RLock()
	ids := idx.quantizationTrainingIDsLocked(16)
	require.Equal(t, "200", idx.internalToID[ids[0]])
	require.Equal(t, "100", idx.internalToID[ids[1]])
	require.Len(t, ids, 16)
	unique := map[uint32]bool{}
	for _, id := range ids {
		require.False(t, unique[id])
		unique[id] = true
	}
	require.Equal(t, ids, idx.quantizationTrainingIDsLocked(16))
	idx.mu.RUnlock()
}

func TestHNSWQuantizationConfigurationAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		env  string
		mode HNSWQuantization
	}{
		{"none", HNSWQuantizationNone}, {"int8", HNSWQuantizationScalar}, {"scalar", HNSWQuantizationScalar}, {"product", HNSWQuantizationProduct}, {"binary", HNSWQuantizationBinary},
	} {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", tc.env)
			require.Equal(t, tc.mode, HNSWConfigFromEnv().Quantization.Mode)
		})
	}
	idx, _ := quantizationTestIndex(t)
	require.NoError(t, idx.SetQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}))
	original := idx.quantized
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, idx.SetQuantization(ctx, HNSWQuantizationConfig{Mode: HNSWQuantizationBinary}), context.Canceled)
	require.Same(t, original, idx.quantized)
	require.Error(t, idx.SetQuantization(context.Background(), HNSWQuantizationConfig{Mode: "invalid"}))
	require.Same(t, original, idx.quantized)
	require.Error(t, idx.SetQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationProduct, PQCompression: 7}))
	require.Same(t, original, idx.quantized)
	require.NoError(t, idx.SetQuantization(context.Background(), HNSWQuantizationConfig{}))
	require.Zero(t, idx.QuantizationBytes())
	require.Equal(t, "HNSW", idx.quantizedStrategyName())
	_, err := normalizeHNSWQuantizationConfig(HNSWQuantizationConfig{Mode: HNSWQuantizationBinary, BinaryQueryBits: 3})
	require.Error(t, err)
	for _, ratio := range []int{4, 8, 16, 32, 64} {
		cfg, err := normalizeHNSWQuantizationConfig(HNSWQuantizationConfig{Mode: HNSWQuantizationProduct, PQCompression: ratio})
		require.NoError(t, err)
		require.Equal(t, ratio, cfg.PQCompression)
		require.Positive(t, cfg.TrainingSampleMax)
	}
}

func TestQuantizationRescoreFalseReachesPropertySupplement(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "binary")
	engine := newNamespacedEngine(t)
	_, err := engine.CreateNode(&storage.Node{ID: "property", Properties: map[string]any{"features": []float32{1, .1}}})
	require.NoError(t, err)
	svc := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	no := false
	hits, err := svc.VectorQueryNodesWithOptions(context.Background(), []float32{1, -1}, VectorQuerySpec{Property: "features", Limit: 1, ScoreScale: VectorScoreSimilarity}, &SearchOptions{QuantizationRescore: &no})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "property", hits[0].ID)
	require.InDelta(t, 0, hits[0].Score, 1e-6, "a property supplement must not overwrite the requested approximate score with an exact cosine")
	hits, err = svc.VectorQueryNodesWithOptions(context.Background(), []float32{1, -1}, VectorQuerySpec{Property: "features", Limit: 1, ScoreScale: VectorScoreSimilarity}, nil)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.InDelta(t, .9/math.Sqrt(2.02), hits[0].Score, 1e-6)
}

func TestConfigureHNSWQuantizationAppliesDatabasePolicyAndInvalidatesPipeline(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "none")
	engine := newNamespacedEngine(t)
	_, err := engine.CreateNode(&storage.Node{ID: "one", NamedEmbeddings: map[string][]float32{"default": {1, .1}}})
	require.NoError(t, err)
	svc := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	cfg := HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}
	require.NoError(t, svc.ConfigureHNSWQuantization(nil, cfg))
	require.NoError(t, svc.BuildIndexes(context.Background()))
	hits, err := svc.VectorQueryNodesWithOptions(context.Background(), []float32{1, 0}, VectorQuerySpec{Property: "default", Limit: 1}, nil)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Positive(t, svc.hnswIndex.QuantizationBytes())
	previous := svc.vectorPipeline
	require.NoError(t, svc.ConfigureHNSWQuantization(nil, cfg))
	require.Same(t, previous, svc.vectorPipeline)
	cfg = HNSWQuantizationConfig{Mode: HNSWQuantizationBinary}
	require.NoError(t, svc.ConfigureHNSWQuantization(nil, cfg))
	require.Nil(t, svc.vectorPipeline)
	require.Equal(t, HNSWQuantizationBinary, svc.hnswConfig().Quantization.Mode)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, svc.ConfigureHNSWQuantization(ctx, HNSWQuantizationConfig{}), context.Canceled)
	require.Error(t, svc.ConfigureHNSWQuantization(nil, HNSWQuantizationConfig{Mode: "invalid"}))
	require.Equal(t, HNSWQuantizationBinary, svc.hnswConfig().Quantization.Mode)
}

func TestQuantizationPendingPQPropertyQueryUsesApproximateScoresOnActivation(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "pq")
	ctx := context.Background()
	engine := newNamespacedEngine(t)
	nodes := make([]*storage.Node, 512)
	for i := range nodes {
		angle := float64(i) * 2 * math.Pi / 512
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprint(i)), Properties: map[string]any{"features": []float32{float32(math.Cos(angle)), float32(math.Sin(angle))}}}
	}
	for _, node := range nodes[:255] {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	svc := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(ctx))
	for _, node := range nodes[255:] {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}
	require.Zero(t, svc.hnswIndex.QuantizationBytes(), "growth has not yet triggered the pending model's first query")
	// A separate checkpoint reader gives the expected compressed scores without
	// activating the service's pending model before its public request.
	path := filepath.Join(t.TempDir(), "pending")
	require.NoError(t, svc.hnswIndex.Save(path))
	reference, err := LoadHNSWIndex(path, svc.vectorIndex.getVectorRef)
	require.NoError(t, err)
	require.NotNil(t, reference)
	reference.SetBuildLexicalHints(lexicalHintValues(svc.hnswIndex.buildLexicalHints))
	require.NoError(t, reference.SetQuantization(ctx, svc.hnswIndex.Config().Quantization))
	var query []float32
	var expected float32
	for _, node := range nodes {
		candidate := vector.Normalize(node.Properties["features"].([]float32))
		scorer := reference.quantized.query(candidate)
		best := float32(-2)
		for id := range reference.internalToID {
			if score := scorer.dot(uint32(id)); score > best {
				best = score
			}
		}
		scorer.release()
		if best < .99999 {
			query, expected = candidate, best
			break
		}
	}
	require.NotNil(t, query, "the fixture must distinguish approximate scores from exact self-similarity")
	no := false
	hits, err := svc.VectorQueryNodesWithOptions(ctx, query, VectorQuerySpec{Property: "features", Limit: 1, ScoreScale: VectorScoreSimilarity}, &SearchOptions{HNSWEf: 512, QuantizationRescore: &no})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.InDelta(t, expected, hits[0].Score, 1e-6, "property supplements must use the newly activated model on its first query")
	require.Positive(t, svc.hnswIndex.QuantizationBytes())
}

func TestHNSWQuantizationCanceledReencodingKeepsPublishedModel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vectors := map[string][]float32{}
	idx := NewHNSWIndex(2, DefaultHNSWConfig())
	cancelReads := false
	idx.SetVectorLookup(func(id string) ([]float32, bool) {
		if cancelReads {
			cancel()
		}
		v, ok := vectors[id]
		return v, ok
	})
	for i := 0; i < 300; i++ {
		id := fmt.Sprint(i)
		vectors[id] = vector.Normalize([]float32{1, float32(i)})
		require.NoError(t, idx.Add(id, vectors[id]))
	}
	require.NoError(t, idx.SetQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}))
	published := idx.quantized
	cancelReads = true
	require.ErrorIs(t, idx.SetQuantization(ctx, HNSWQuantizationConfig{Mode: HNSWQuantizationBinary}), context.Canceled)
	require.Same(t, published, idx.quantized)
	require.Equal(t, HNSWQuantizationScalar, idx.Config().Quantization.Mode)
}

func TestHNSWDefaultScalarPreservesQualityAndCollectionOverride(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "")
	for _, quality := range []string{"fast", "balanced", "accurate"} {
		t.Run(quality, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", quality)
			cfg := HNSWConfigFromEnv()
			require.Equal(t, HNSWQuantizationScalar, cfg.Quantization.Mode)
			require.Equal(t, presetDefaults(HNSWQualityPreset(quality)).EfSearch, cfg.EfSearch)
		})
	}
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 2)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.Equal(t, HNSWQuantizationScalar, svc.hnswConfig().Quantization.Mode)
	require.NoError(t, svc.ConfigureHNSWQuantization(context.Background(), HNSWQuantizationConfig{}))
	require.Equal(t, HNSWQuantizationNone, svc.hnswConfig().Quantization.Mode)
}
