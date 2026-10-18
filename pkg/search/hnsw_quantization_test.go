package search

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/stretchr/testify/require"
)

func TestHNSWQuantizedCodecs(t *testing.T) {
	for _, dim := range []int{1, 3, 4, 7, 15, 16, 17, 63, 64, 65, 1024} {
		for _, mode := range []HNSWQuantization{HNSWQuantizationScalar, HNSWQuantizationBinary, HNSWQuantizationProduct} {
			t.Run(fmt.Sprintf("%s/%d", mode, dim), func(t *testing.T) {
				v := make([]float32, dim)
				for i := range v {
					v[i] = float32(math.Sin(float64(i + 1)))
				}
				v = vector.Normalize(v)
				q, err := newHNSWQuantizedVectors(context.Background(), HNSWQuantizationConfig{Mode: mode, PQCompression: 16}, dim, [][]float32{v})
				require.NoError(t, err)
				q.set(0, v)
				query := q.query(v)
				require.InDelta(t, 1, query.dot(0), 0.015)
				require.Positive(t, q.bytes())
				if mode == HNSWQuantizationScalar {
					require.Equal(t, dim+4, q.bytes())
				}
				if mode == HNSWQuantizationBinary {
					require.Equal(t, 8*((dim+63)/64), q.bytes())
				}
				if mode == HNSWQuantizationProduct {
					require.Equal(t, (dim+3)/4, len(q.codes))
				}
			})
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := newHNSWQuantizedVectors(ctx, HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}, 2, nil)
	require.ErrorIs(t, err, context.Canceled)
	_, err = newHNSWQuantizedVectors(context.Background(), HNSWQuantizationConfig{Mode: "unsupported"}, 2, nil)
	require.Error(t, err)
	_, err = newHNSWQuantizedVectors(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationProduct, PQCompression: 3}, 2, [][]float32{{1, 0}})
	require.Error(t, err)
}

func quantizationTestIndex(t *testing.T) (*HNSWIndex, *VectorIndex) {
	t.Helper()
	cfg := DefaultHNSWConfig()
	cfg.M = 8
	cfg.EfConstruction = 64
	cfg.EfSearch = 300
	idx := NewHNSWIndex(2, cfg)
	originals := NewVectorIndex(2)
	for i := 0; i < 256; i++ {
		angle := float64(i) * 2 * math.Pi / 256
		id := fmt.Sprint(i)
		v := []float32{float32(math.Cos(angle)), float32(math.Sin(angle))}
		require.NoError(t, originals.Add(id, v))
		require.NoError(t, idx.Add(id, v))
	}
	return idx, originals
}

func TestHNSWQuantizationPublicPipelineMutationsAndReload(t *testing.T) {
	for _, cfg := range []HNSWQuantizationConfig{{Mode: HNSWQuantizationScalar}, {Mode: HNSWQuantizationBinary}, {Mode: HNSWQuantizationBinary, BinaryQueryBits: 4}, {Mode: HNSWQuantizationBinary, BinaryQueryBits: 8}, {Mode: HNSWQuantizationProduct, PQCompression: 4}, {Mode: HNSWQuantizationProduct, PQCompression: 8}, {Mode: HNSWQuantizationProduct, PQCompression: 16}, {Mode: HNSWQuantizationProduct, PQCompression: 32}, {Mode: HNSWQuantizationProduct, PQCompression: 64}} {
		t.Run(fmt.Sprintf("%s/pq%d/query%d", cfg.Mode, cfg.PQCompression, cfg.BinaryQueryBits), func(t *testing.T) {
			idx, originals := quantizationTestIndex(t)
			ctx := context.Background()
			require.NoError(t, idx.SetQuantization(ctx, cfg))
			pipeline := NewVectorSearchPipeline(NewHNSWCandidateGen(idx), NewCPUExactScorer(originals))
			check := func(want string) {
				t.Helper()
				results, err := pipeline.Search(ctx, []float32{1, 0}, 300, 0.99999)
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Equal(t, want, results[0].ID)
				require.InDelta(t, 1, results[0].Score, 1e-6)
			}
			check("0")
			// Replace the original nearest vector; it must not retain its old code.
			require.NoError(t, originals.Add("0", []float32{-1, 0}))
			require.NoError(t, idx.Add("0", []float32{-1, 0}))
			require.NoError(t, originals.Add("new", []float32{1, 0}))
			require.NoError(t, idx.Add("new", []float32{1, 0}))
			check("new")
			idx.Remove("new")
			originals.Remove("new")
			results, err := pipeline.Search(ctx, []float32{1, 0}, 300, 0.99999)
			require.NoError(t, err)
			require.Empty(t, results)
			path := filepath.Join(t.TempDir(), "hnsw.idx")
			require.NoError(t, idx.Save(path))
			loaded, err := LoadHNSWIndex(path, originals.getVectorRef)
			require.NoError(t, err)
			require.NotNil(t, loaded)
			require.Positive(t, loaded.QuantizationBytes(), "reload retains compressed codes/codebooks, not a silent full-precision mode")
			require.NoError(t, loaded.Add("new", []float32{1, 0}))
			require.NoError(t, originals.Add("new", []float32{1, 0}))
			pipeline = NewVectorSearchPipeline(NewHNSWCandidateGen(loaded), NewCPUExactScorer(originals))
			check("new")
			loaded.Clear()
			require.NoError(t, loaded.Add("after-clear", []float32{1, 0}))
			require.NoError(t, originals.Add("after-clear", []float32{1, 0}))
			pipeline = NewVectorSearchPipeline(NewHNSWCandidateGen(loaded), NewCPUExactScorer(originals))
			check("after-clear")
		})
	}
}

func TestQuantizedHNSWPreservesLexicalEntriesAndConstruction(t *testing.T) {
	for _, mode := range []HNSWQuantization{HNSWQuantizationScalar, HNSWQuantizationBinary, HNSWQuantizationProduct} {
		t.Run(string(mode), func(t *testing.T) {
			idx, _ := quantizationTestIndex(t)
			require.NoError(t, idx.SetQuantization(context.Background(), HNSWQuantizationConfig{Mode: mode}))
			idx.mu.Lock()
			idx.entryPoint = idx.idToInternal["0"]
			idx.maxLevel = 0
			for id := range idx.internalToID {
				idx.setNeighborsAtLevelLocked(uint32(id), 0, nil)
			}
			idx.mu.Unlock()
			got, _, err := idx.searchWithEfExhaustionFromEntries(context.Background(), []float32{-1, 0}, 1, -1, 1, []string{"missing", "128", "128"})
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, "128", got[0].ID)
		})
	}
}

func TestHNSWBinaryScalarQueriesMatchDecodedReference(t *testing.T) {
	for _, width := range []int{4, 8} {
		for _, dim := range []int{3, 64, 65, 1024} {
			t.Run(fmt.Sprintf("%d/%d", width, dim), func(t *testing.T) {
				stored, query := make([]float32, dim), make([]float32, dim)
				for i := range stored {
					stored[i] = float32(math.Sin(float64(i + 1)))
					query[i] = float32(math.Cos(float64(i + 2)))
				}
				query = vector.Normalize(query)
				q, err := newHNSWQuantizedVectors(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationBinary, BinaryQueryBits: width}, dim, nil)
				require.NoError(t, err)
				q.set(0, stored)
				scorer := q.query(query)
				var reference float32
				for i, x := range query {
					value := float32(math.Round(float64(x/scorer.scale))) * scorer.scale
					if stored[i] <= 0 {
						value = -value
					}
					reference += value
				}
				reference /= float32(math.Sqrt(float64(dim)))
				require.InDelta(t, reference, scorer.dot(0), 1e-6)
				require.Equal(t, 8*((dim+63)/64), q.bytes(), "query precision must not inflate stored coordinates")
				require.Zero(t, q.query(make([]float32, dim)).dot(0))
			})
		}
	}
	// Hamming cannot distinguish these equidistant sign patterns; a precise
	// query retains the dimension carrying most of its weight.
	for _, width := range []int{1, 4, 8} {
		q, err := newHNSWQuantizedVectors(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationBinary, BinaryQueryBits: width}, 4, nil)
		require.NoError(t, err)
		q.set(0, []float32{1, 1, 1, -1})
		q.set(1, []float32{-1, 1, 1, 1})
		scorer := q.query(vector.Normalize([]float32{.9, .1, .1, .1}))
		if width == 1 {
			require.Equal(t, scorer.dot(0), scorer.dot(1))
		} else {
			require.Greater(t, scorer.dot(0), scorer.dot(1))
		}
	}
}

func TestHNSWQuantizationConcurrentQueriesAndMutations(t *testing.T) {
	for _, cfg := range []HNSWQuantizationConfig{{Mode: HNSWQuantizationScalar}, {Mode: HNSWQuantizationBinary, BinaryQueryBits: 8}, {Mode: HNSWQuantizationProduct}} {
		t.Run(string(cfg.Mode), func(t *testing.T) {
			idx, originals := quantizationTestIndex(t)
			require.NoError(t, idx.SetQuantization(context.Background(), cfg))
			pipeline := NewVectorSearchPipeline(NewHNSWCandidateGen(idx), NewCPUExactScorer(originals))
			errors := make(chan error, 5)
			for worker := 0; worker < 4; worker++ {
				go func() {
					for i := 0; i < 25; i++ {
						if _, err := pipeline.Search(context.Background(), []float32{1, 0}, 16, -1); err != nil {
							errors <- err
							return
						}
					}
					errors <- nil
				}()
			}
			go func() {
				for i := 0; i < 25; i++ {
					id := fmt.Sprintf("live-%d", i)
					if err := originals.Add(id, []float32{1, float32(i)}); err != nil {
						errors <- err
						return
					}
					if err := idx.Add(id, []float32{1, float32(i)}); err != nil {
						errors <- err
						return
					}
					if i > 0 {
						idx.Remove(fmt.Sprintf("live-%d", i-1))
						originals.Remove(fmt.Sprintf("live-%d", i-1))
					}
				}
				errors <- nil
			}()
			for i := 0; i < 5; i++ {
				require.NoError(t, <-errors)
			}
		})
	}
}

func TestQuantizedQueryPoolKeepsActiveQueriesIndependent(t *testing.T) {
	for _, cfg := range []HNSWQuantizationConfig{{Mode: HNSWQuantizationScalar}, {Mode: HNSWQuantizationBinary}, {Mode: HNSWQuantizationBinary, BinaryQueryBits: 4}, {Mode: HNSWQuantizationBinary, BinaryQueryBits: 8}, {Mode: HNSWQuantizationProduct}} {
		q, err := newHNSWQuantizedVectors(context.Background(), cfg, 3, [][]float32{{1, 0, 0}, {-1, 0, 0}})
		require.NoError(t, err)
		q.set(0, []float32{1, 0, 0})
		for i := 0; i < 10; i++ {
			positive := q.query([]float32{1, 0, 0})
			before := positive.dot(0)
			negative := q.query([]float32{-1, 0, 0})
			require.Less(t, negative.dot(0), before)
			require.Equal(t, before, positive.dot(0))
			negative.release()
			positive.release()
			again := q.query([]float32{1, 0, 0})
			require.Equal(t, before, again.dot(0))
			again.release()
		}
	}
}
