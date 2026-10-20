package gpu

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmbeddingIndexSearchWithPopulation(t *testing.T) {
	cfg := DefaultEmbeddingIndexConfig(2)
	cfg.GPUEnabled, cfg.AutoSync = false, false
	index := NewEmbeddingIndex(nil, cfg)
	for _, id := range []string{"a", "b", "c"} {
		require.NoError(t, index.Add(id, []float32{1, 0}))
	}
	for _, depth := range []int{1, 3, 4} {
		rows, population, err := index.SearchWithPopulation([]float32{1, 0}, depth)
		require.NoError(t, err)
		require.Equal(t, 3, population)
		require.Len(t, rows, min(depth, population))
		legacy, err := index.Search([]float32{1, 0}, depth)
		require.NoError(t, err)
		require.Equal(t, legacy, rows, "Search and its coverage receipt use the same scorer")
	}
	rows, population, err := index.SearchWithPopulation([]float32{1}, 3)
	require.ErrorIs(t, err, ErrInvalidDimensions)
	require.Nil(t, rows)
	require.Zero(t, population, "failed scans do not provide a coverage receipt")
	empty := NewEmbeddingIndex(nil, cfg)
	rows, population, err = empty.SearchWithPopulation([]float32{1, 0}, 1)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.Zero(t, population)
}

// A full returned page can exhaust a threshold-qualified population even
// though other stored vectors do not qualify. A qualifying witness beyond the
// window must still report an omitted match.
func TestEmbeddingIndexSearchWithThresholdPopulation(t *testing.T) {
	cfg := DefaultEmbeddingIndexConfig(2)
	cfg.GPUEnabled, cfg.AutoSync = false, false
	index := NewEmbeddingIndex(nil, cfg)
	for i := 0; i < 10; i++ {
		v := []float32{1, 0}
		if i >= 8 {
			v = []float32{-1, 0}
		}
		require.NoError(t, index.Add(string(rune('a'+i)), v))
	}
	for _, minimum := range []float64{0, 1, 2} {
		eligible := 8
		if minimum > 1 {
			eligible = 0
		}
		for _, depth := range []int{7, 8, 9, 16} {
			rows, population, err := index.SearchWithThresholdPopulation([]float32{1, 0}, depth, minimum)
			require.NoError(t, err)
			require.Len(t, rows, min(depth, eligible))
			require.Equal(t, min(depth+1, eligible), population)
			for _, row := range rows {
				require.GreaterOrEqual(t, float64(row.Score), minimum)
			}
		}
	}
	_, population, err := index.SearchWithPopulation([]float32{1, 0}, 7)
	require.NoError(t, err)
	require.Equal(t, 10, population, "the unfiltered API retains its stored-population contract")
	rows, population, err := index.SearchWithThresholdPopulation([]float32{1}, 8, 0)
	require.ErrorIs(t, err, ErrInvalidDimensions)
	require.Nil(t, rows)
	require.Zero(t, population)
}
