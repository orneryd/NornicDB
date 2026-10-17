package search

import (
	"cmp"
	"context"
	"slices"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHNSWLexicalSeedsFollowVectorIdentity(t *testing.T) {
	ids := []string{"other", "second-named-default", "first-prop-features", "first-chunk-0", "literal-named-doc"}
	hints := map[string]LexicalSeedHint{
		"first":             {ID: "first", Rank: 0, Signature: 1},
		"second":            {ID: "second", Rank: 1, Signature: 2},
		"literal-named-doc": {ID: "literal-named-doc", Rank: 2, Signature: 3},
	}
	check := func(t *testing.T, pairs []hnswBuildPair) {
		t.Helper()
		got := make([]string, len(pairs))
		for i, pair := range pairs {
			got[i] = pair.id
		}
		require.Equal(t, []string{"first-chunk-0", "first-prop-features", "second-named-default", "literal-named-doc", "other"}, got, "ranked document hints must prioritize every embedding exactly once")
		config := DefaultHNSWConfig()
		config.UseGPUBuild = true
		config.GPUBuildBatchSize = 2
		built, _, err := buildHNSWWithOptionalGPU(context.Background(), 2, config, nil, len(pairs), hnswPairSliceIterator(pairs), NewCPUHNSWBuildAccelerator())
		require.NoError(t, err)
		built.mu.RLock()
		defer built.mu.RUnlock()
		for _, pair := range pairs {
			if !pair.hasHint {
				continue
			}
			require.Contains(t, hints, pair.hint.ID, "iterator metadata keeps its BM25 document identity")
			bound, ok := built.buildLexicalHints[pair.id]
			require.True(t, ok, "construction must bind the hint to embedding %s", pair.id)
			require.Equal(t, pair.id, bound.ID)
			require.Equal(t, pair.hint.Signature, bound.Signature)
		}
	}
	t.Run("chunked file iterator", func(t *testing.T) {
		iterate := func(_ int, fn func([]string, [][]float32) error) error {
			return fn(ids, [][]float32{{1, 0}, {0, 1}, {0.5, 0.5}, {0.2, 0.8}, {0.8, 0.2}})
		}
		var pairs []hnswBuildPair
		err := hnswVectorChunkIterator(iterate, hints)(2, func(batch []hnswBuildPair) error {
			pairs = append(pairs, batch...)
			return nil
		})
		require.NoError(t, err)
		check(t, pairs)
	})
	t.Run("memory source", func(t *testing.T) {
		index := NewVectorIndex(2)
		for _, id := range ids {
			require.NoError(t, index.Add(id, []float32{1, 0}))
		}
		check(t, hnswOrderedVectorIndexPairs(index, hints))
	})
}

func TestANNPublicBuildUsesNamedVectorLexicalSeeds(t *testing.T) {
	for _, clustered := range []bool{false, true} {
		name := "HNSW"
		if clustered {
			name = "kmeans"
		}
		t.Run(name, func(t *testing.T) {
			t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
			t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
			t.Setenv("NORNICDB_KMEANS_SEED_DOCS_PER_TERM", "2")
			engine := newNamespacedEngine(t)
			for i, id := range []string{"first", "second"} {
				vec := make([]float32, 2)
				vec[i] = 1
				_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Properties: map[string]any{"text": "alpha beta"}, NamedEmbeddings: map[string][]float32{"default": vec}})
				require.NoError(t, err)
			}
			svc := NewServiceWithDimensions(engine, 2)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			if clustered {
				svc.EnableClustering(nil, 2)
				svc.SetMinEmbeddingsForClustering(1)
			}
			require.NoError(t, svc.BuildIndexes(context.Background()))
			if clustered {
				// Cluster consumes its preferred seeds. Reapply the same seed owner
				// to inspect the resolved IDs after the public build completed.
				svc.applyBM25SeedHints()
				require.Len(t, preferredSeedsForTest(t, svc.clusterIndex), 2, "BM25 document seeds must reach their named embeddings")
				require.Len(t, svc.clusterLexicalProfiles, 2)
			} else {
				require.NotNil(t, svc.hnswIndex)
				ranked := lexicalHintValues(svc.hnswBuildLexicalSeedHints(svc.fulltext(), svc.hnswIndex.Config().M))
				slices.SortFunc(ranked, func(a, b LexicalSeedHint) int {
					if n := cmp.Compare(a.Rank, b.Rank); n != 0 {
						return n
					}
					return cmp.Compare(a.ID, b.ID)
				})
				expected := make([]string, 0, 2)
				for _, hint := range ranked {
					if hint.ID == "first-named-default" || hint.ID == "second-named-default" {
						expected = append(expected, hint.ID)
					}
				}
				require.Len(t, expected, 2)
				svc.hnswIndex.mu.RLock()
				defer svc.hnswIndex.mu.RUnlock()
				require.Equal(t, expected, svc.hnswIndex.internalToID, "public construction must preserve ranked embedding insertion order")
				for _, id := range []string{"first-named-default", "second-named-default"} {
					hint, ok := svc.hnswIndex.buildLexicalHints[id]
					require.True(t, ok, "public index build must bind the lexical hint to %s", id)
					require.NotZero(t, hint.Signature)
				}
			}
		})
	}
}

func TestBM25SeedEmbeddingIDsUsesIndexedMetadata(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 2)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.IndexNode(&storage.Node{
		ID: "seed", Properties: map[string]any{"text": "alpha", "features": []float32{1, 0}},
		NamedEmbeddings: map[string][]float32{"default": {1, 0}, "alternate": {0, 1}},
		ChunkEmbeddings: [][]float32{{1, 0}, {0, 1}},
	}))
	require.Equal(t, []string{"missing", "seed"}, svc.bm25SeedEmbeddingIDs([]string{"missing", "seed"}))
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "encoded", Properties: map[string]any{"features": []float32{0, 1}}, NamedEmbeddings: map[string][]float32{"default": {1, 0}, "alternate": {0, 1}}}))
	require.Equal(t, []string{"encoded-named-alternate", "encoded-named-default", "encoded-prop-features"}, svc.bm25SeedEmbeddingIDs([]string{"encoded"}))
}
