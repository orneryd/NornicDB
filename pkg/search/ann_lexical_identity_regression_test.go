package search

import (
	"context"
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
	want := []string{"first-chunk-0", "first-prop-features", "second-named-default", "literal-named-doc", "other"}
	check := func(t *testing.T, pairs []hnswBuildPair) {
		t.Helper()
		got := make([]string, len(pairs))
		for i, pair := range pairs {
			got[i] = pair.id
			if pair.id != "other" {
				require.True(t, pair.hasHint)
				require.Equal(t, pair.id, pair.hint.ID, "the graph stores hints under the embedding ID")
			}
		}
		require.Equal(t, want, got)
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
				svc.hnswIndex.mu.RLock()
				defer svc.hnswIndex.mu.RUnlock()
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
	require.Equal(t, []string{"missing", "seed", "seed-chunk-1", "seed-named-alternate", "seed-named-default", "seed-prop-features"}, svc.bm25SeedEmbeddingIDs([]string{"missing", "seed"}))
}
