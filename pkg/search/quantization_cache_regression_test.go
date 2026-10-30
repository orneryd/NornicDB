package search

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func quantizationCachedService(t *testing.T) *Service {
	t.Helper()
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "binary")
	t.Setenv(EnvSearchResultCacheEnabled, "true")
	store := newNamespacedEngine(t)
	_, err := store.CreateNode(&storage.Node{ID: "one", NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}}})
	require.NoError(t, err)
	svc := NewService(store, 4, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.NotNil(t, svc.resultCache)
	return svc
}

func TestSearchCacheSeparatesQuantizationControlsBothOrders(t *testing.T) {
	yes, no := true, false
	defaults := *DefaultSearchOptions()
	defaults.Limit = 1
	defaults.SkipMatches = true
	approximate := defaults
	approximate.QuantizationRescore = &no
	explicitRescore := defaults
	explicitRescore.QuantizationRescore = &yes
	ignore := approximate
	ignore.QuantizationIgnore = true
	oversample := defaults
	oversample.QuantizationOversampling = 2
	beam := defaults
	beam.HNSWEf = 128
	exact := 1 / math.Sqrt(2)
	for _, tc := range []struct {
		name          string
		first, second SearchOptions
		a, b          float64
	}{
		{"unset-rescore", approximate, defaults, .5, exact},
		{"explicit-rescore", approximate, explicitRescore, .5, exact},
		{"ignore", approximate, ignore, .5, exact},
		{"oversampling", defaults, oversample, exact, exact},
		{"beam", defaults, beam, exact, exact},
	} {
		for _, reverse := range []bool{false, true} {
			name := tc.name + "/forward"
			if reverse {
				name = tc.name + "/reverse"
			}
			t.Run(name, func(t *testing.T) {
				svc := quantizationCachedService(t)
				a, b := tc.first, tc.second
				wantA, wantB := tc.a, tc.b
				if reverse {
					a, b = b, a
					wantA, wantB = wantB, wantA
				}
				query := []float32{1, 1, 0, 0}
				first, err := svc.Search(context.Background(), "", query, &a)
				require.NoError(t, err)
				require.Len(t, first.Results, 1)
				require.InDelta(t, wantA, first.Results[0].Score, 1e-6)
				cached, err := svc.Search(context.Background(), "", query, &a)
				require.NoError(t, err)
				require.Equal(t, first, cached, "the real result cache preserves the response value")
				require.NotSame(t, first, cached, "cache reads return caller-owned responses")
				second, err := svc.Search(context.Background(), "", query, &b)
				require.NoError(t, err)
				require.Len(t, second.Results, 1)
				require.InDelta(t, wantB, second.Results[0].Score, 1e-6)
				require.NotSame(t, first, second, "different ANN controls must execute their own public search")
				again, err := svc.Search(context.Background(), "", query, &a)
				require.NoError(t, err)
				require.Equal(t, first, again)
				require.NotSame(t, first, again)
			})
		}
	}
}

func TestSearchCacheRetiresChangedQuantizationPolicy(t *testing.T) {
	svc := quantizationCachedService(t)
	no := false
	opts := DefaultSearchOptions()
	opts.Limit = 1
	opts.SkipMatches = true
	opts.QuantizationRescore = &no
	query := []float32{1, 1, 0, 0}
	for _, mode := range []HNSWQuantization{HNSWQuantizationBinary, HNSWQuantizationNone, HNSWQuantizationBinary} {
		require.NoError(t, svc.ConfigureHNSWQuantization(context.Background(), HNSWQuantizationConfig{Mode: mode}))
		svc.resultCache.mu.Lock()
		entries := len(svc.resultCache.entries)
		svc.resultCache.mu.Unlock()
		require.Zero(t, entries, "a new policy must retire cached responses")
		got, err := svc.Search(context.Background(), "", query, opts)
		require.NoError(t, err)
		require.Len(t, got.Results, 1)
		want := .5
		if mode == HNSWQuantizationNone {
			want = 1 / math.Sqrt(2)
		}
		require.InDelta(t, want, got.Results[0].Score, 1e-6)
	}
}

func TestClearIndexesRetiresOnlyCollectionCompressionOverride(t *testing.T) {
	for _, global := range []string{"none", "scalar"} {
		t.Run(global, func(t *testing.T) {
			svc := quantizationCachedService(t)
			t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", global)
			require.NoError(t, svc.ConfigureHNSWQuantization(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationBinary}))
			require.NoError(t, svc.ClearVectorIndex())
			require.Equal(t, HNSWQuantizationBinary, svc.hnswConfig().Quantization.Mode, "embedding regeneration retains the collection policy")
			require.NoError(t, svc.ClearIndexes())
			require.Nil(t, svc.hnswQuantizationOverride)
			require.Equal(t, HNSWQuantizationConfigFromEnv(), svc.hnswConfig().Quantization)
		})
	}
}
