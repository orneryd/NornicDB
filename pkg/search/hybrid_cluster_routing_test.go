package search

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type seedOverrideFulltext struct {
	*FulltextIndexV2
	seedIDs []string
}

func (s *seedOverrideFulltext) LexicalSeedDocIDs(maxTerms, perTerm int) []string {
	return append([]string(nil), s.seedIDs...)
}

func (s *seedOverrideFulltext) LexicalSeedHints(maxTerms, perTerm int) []LexicalSeedHint {
	hints := make([]LexicalSeedHint, 0, len(s.seedIDs))
	for i, id := range s.seedIDs {
		hints = append(hints, LexicalSeedHint{ID: id, Rank: uint32(i), Signature: stableLexicalHash(id)})
	}
	return hints
}

func preferredSeedsForTest(t *testing.T, ci interface{}) []int {
	t.Helper()
	v := reflect.ValueOf(ci)
	require.Equal(t, reflect.Ptr, v.Kind())
	elem := v.Elem()
	field := elem.FieldByName("preferredSeedIndices")
	require.True(t, field.IsValid(), "preferredSeedIndices field must exist")
	out := make([]int, field.Len())
	for i := 0; i < field.Len(); i++ {
		out[i] = int(field.Index(i).Int())
	}
	return out
}

// TestEnableClustering_DefaultMaxIterationsIsFive is the canonical clustering
// configuration table owner: the default iteration budget, the re-entrant CPU
// mode, the environment overrides and the nil vector index guard.
func TestEnableClustering_DefaultMaxIterationsIsFive(t *testing.T) {
	t.Run("default max iterations is five", func(t *testing.T) {
		t.Setenv("NORNICDB_KMEANS_MAX_ITERATIONS", "")
		svc := NewService(newTestMemoryEngine(t), 2, nil)
		svc.EnableClustering(nil, 2)
		require.NotNil(t, svc.clusterIndex)
		require.Equal(t, 5, svc.clusterIndex.Config().MaxIterations)
	})

	t.Run("reentrant same mode keeps existing cluster index", func(t *testing.T) {
		svc := NewService(newTestMemoryEngine(t), 2, nil)
		svc.EnableClustering(nil, 2)
		require.NotNil(t, svc.clusterIndex)
		first := svc.clusterIndex

		// Same CPU mode: second call should be a no-op and preserve pointer.
		svc.EnableClustering(nil, 9)
		require.Same(t, first, svc.clusterIndex)
	})

	t.Run("env overrides cluster count and clamps low max iterations", func(t *testing.T) {
		t.Setenv("NORNICDB_KMEANS_NUM_CLUSTERS", "11")
		t.Setenv("NORNICDB_KMEANS_MAX_ITERATIONS", "1")

		svc := NewService(newTestMemoryEngine(t), 2, nil)
		svc.EnableClustering(nil, 0)
		require.NotNil(t, svc.clusterIndex)
		cfg := svc.clusterIndex.Config()
		require.Equal(t, 11, cfg.NumClusters)
		require.False(t, cfg.AutoK)
		require.Equal(t, 5, cfg.MaxIterations)
	})

	t.Run("auto cluster count and max iteration upper clamp", func(t *testing.T) {
		t.Setenv("NORNICDB_KMEANS_NUM_CLUSTERS", "")
		t.Setenv("NORNICDB_KMEANS_MAX_ITERATIONS", "9999")

		svc := NewService(newTestMemoryEngine(t), 2, nil)
		svc.EnableClustering(nil, 0)
		require.NotNil(t, svc.clusterIndex)
		cfg := svc.clusterIndex.Config()
		require.Equal(t, 0, cfg.NumClusters)
		require.True(t, cfg.AutoK)
		require.Equal(t, 500, cfg.MaxIterations)
	})

	t.Run("nil vector index is ignored safely", func(t *testing.T) {
		svc := &Service{}
		svc.EnableClustering(nil, 3)
		require.Nil(t, svc.clusterIndex)
		require.False(t, svc.clusterEnabled)
	})
}

func TestSelectHybridClusters_UsesLexicalSignal(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_HYBRID_ROUTING_W_SEM", "0.1")
	t.Setenv("NORNICDB_VECTOR_HYBRID_ROUTING_W_LEX", "0.9")
	t.Setenv("NORNICDB_KMEANS_MAX_ITERATIONS", "5")

	svc := NewService(newTestMemoryEngine(t), 2, nil)
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(1)

	// Two clear semantic groups.
	require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID("a1"), Properties: map[string]interface{}{"text": "alpha topic"}, ChunkEmbeddings: [][]float32{{1, 0}}}))
	require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID("a2"), Properties: map[string]interface{}{"text": "alpha graph"}, ChunkEmbeddings: [][]float32{{0.9, 0.1}}}))
	require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID("b1"), Properties: map[string]interface{}{"text": "beta api"}, ChunkEmbeddings: [][]float32{{0, 1}}}))
	require.NoError(t, svc.IndexNode(&storage.Node{ID: storage.NodeID("b2"), Properties: map[string]interface{}{"text": "beta auth"}, ChunkEmbeddings: [][]float32{{0.1, 0.9}}}))

	require.NoError(t, svc.TriggerClustering(context.Background()))
	require.True(t, svc.clusterIndex.IsClustered())

	// Semantic prefers first cluster for query [1,0].
	sem := svc.clusterIndex.FindNearestClusters([]float32{1, 0}, 2)
	require.Len(t, sem, 2)
	semFirst, semSecond := sem[0], sem[1]

	// Force lexical preference to second cluster via profile weights.
	setClusterProfilesForTest(t, svc, map[int]map[string]float64{
		semFirst:  {"alpha": 1.0},
		semSecond: {"beta": 1.0},
	})

	out := svc.selectHybridClusters(withQueryText(context.Background(), "beta auth"), []float32{1, 0}, 1)
	require.Len(t, out, 1)
	require.Equal(t, semSecond, out[0], "lexical signal should override semantic top cluster when weighted higher")
}

// setClusterProfilesForTest replaces the published generation's lexical
// routing profiles.
func setClusterProfilesForTest(t *testing.T, svc *Service, profiles map[int]map[string]float64) {
	t.Helper()
	svc.clusterStateMu.Lock()
	defer svc.clusterStateMu.Unlock()
	require.NotNil(t, svc.clusterGen, "a published cluster generation")
	svc.clusterGen.profiles = profiles
}

func TestTopNTokenWeights_Branches(t *testing.T) {
	in := map[string]float64{
		"a": 0.9,
		"b": 0.5,
		"c": 0.8,
	}
	// len(weights) <= n returns original map directly.
	require.Equal(t, in, topNTokenWeights(in, 3))

	trimmed := topNTokenWeights(in, 2)
	require.Len(t, trimmed, 2)
	_, hasA := trimmed["a"]
	_, hasC := trimmed["c"]
	require.True(t, hasA)
	require.True(t, hasC)
}

func TestQueryTextContextHelpers(t *testing.T) {
	ctx := withQueryText(context.Background(), "  hello world  ")
	require.Equal(t, "hello world", queryTextFromContext(ctx))

	// Missing value branch.
	require.Equal(t, "", queryTextFromContext(context.Background()))
}

func BenchmarkSelectHybridClusters(b *testing.B) {
	_ = os.Setenv("NORNICDB_VECTOR_HYBRID_ROUTING_W_SEM", "0.7")
	_ = os.Setenv("NORNICDB_VECTOR_HYBRID_ROUTING_W_LEX", "0.3")
	defer os.Unsetenv("NORNICDB_VECTOR_HYBRID_ROUTING_W_SEM")
	defer os.Unsetenv("NORNICDB_VECTOR_HYBRID_ROUTING_W_LEX")

	svc := NewService(fixtureowner.Own(b, fixtureowner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)), 2, nil)
	svc.EnableClustering(nil, 8)
	svc.SetMinEmbeddingsForClustering(1)
	for i := 0; i < 256; i++ {
		id := storage.NodeID(fmt.Sprintf("n-%d", i))
		vec := []float32{float32(i % 2), float32((i + 1) % 2)}
		_ = svc.IndexNode(&storage.Node{ID: id, Properties: map[string]interface{}{"text": "alpha beta gamma"}, ChunkEmbeddings: [][]float32{vec}})
	}
	_ = svc.TriggerClustering(context.Background())

	ctx := withQueryText(context.Background(), "alpha beta")
	query := []float32{1, 0}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = svc.selectHybridClusters(ctx, query, 3)
	}
}

func TestApplyBM25SeedHints_PreferredSeeds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		seedIDs []string
		want    []int
	}{
		{
			name:    "resolvable seeds become preferred cluster seeds",
			seedIDs: []string{"doc-b", "missing-id"},
			want:    []int{1},
		},
		{
			name:    "no seeds is a no-op",
			seedIDs: nil,
			want:    []int{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := NewService(newTestMemoryEngine(t), 2, nil)
			svc.EnableClustering(nil, 2)
			require.NotNil(t, svc.clusterIndex)

			require.NoError(t, svc.clusterIndex.Add("doc-a", []float32{1, 0}))
			require.NoError(t, svc.clusterIndex.Add("doc-b", []float32{0, 1}))

			svc.setFulltext(&seedOverrideFulltext{
				FulltextIndexV2: NewFulltextIndexV2WithAnalyzer(nil),
				seedIDs:         tc.seedIDs,
			})

			svc.applyBM25SeedHints()

			require.Equal(t, tc.want, preferredSeedsForTest(t, svc.clusterIndex))
		})
	}
}
