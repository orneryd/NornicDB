package search

import (
	"context"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestHNSWQuantizationRejectsIncompleteCheckpointModels(t *testing.T) {
	tests := []struct {
		name   string
		cfg    HNSWQuantizationConfig
		damage func(*hnswQuantizationSnapshot)
	}{
		{"dimensions", HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}, func(s *hnswQuantizationSnapshot) { s.Dimensions++ }},
		{"scalar stride", HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}, func(s *hnswQuantizationSnapshot) { s.Stride++ }},
		{"scalar rows", HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}, func(s *hnswQuantizationSnapshot) { s.Codes = nil }},
		{"scalar scales", HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}, func(s *hnswQuantizationSnapshot) { s.Scales = nil }},
		{"binary stride", HNSWQuantizationConfig{Mode: HNSWQuantizationBinary}, func(s *hnswQuantizationSnapshot) { s.Stride++ }},
		{"binary rows", HNSWQuantizationConfig{Mode: HNSWQuantizationBinary}, func(s *hnswQuantizationSnapshot) { s.Binary = nil }},
		{"product ratio", HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}, func(s *hnswQuantizationSnapshot) { s.Config.PQCompression = 3 }},
		{"product stride", HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}, func(s *hnswQuantizationSnapshot) { s.Stride++ }},
		{"product rows", HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}, func(s *hnswQuantizationSnapshot) { s.Codes = nil }},
		{"product books", HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}, func(s *hnswQuantizationSnapshot) { s.Books = nil }},
		{"product book dimensions", HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}, func(s *hnswQuantizationSnapshot) { s.Books[0].SubDim++ }},
		{"product empty book", HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}, func(s *hnswQuantizationSnapshot) { s.Books[0].Codeword = nil }},
		{"product center dimensions", HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}, func(s *hnswQuantizationSnapshot) { s.Books[0].Codeword[0] = nil }},
		{"product code reference", HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}, func(s *hnswQuantizationSnapshot) { s.Codes[0] = 255 }},
		{"unknown mode", HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}, func(s *hnswQuantizationSnapshot) { s.Config.Mode = "unknown" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q, err := newHNSWQuantizedVectors(context.Background(), tc.cfg, 3, [][]float32{{1, 0, 0}})
			require.NoError(t, err)
			q.set(0, []float32{1, 0, 0})
			snapshot := q.snapshot()
			tc.damage(snapshot)
			path := filepath.Join(t.TempDir(), "index")
			writeHNSWSnapshot(t, path, &hnswIndexSnapshot{Version: hnswIndexFormatVersionGraphOnly, Dimensions: 3, InternalToID: []string{"a"}, Quantization: snapshot})
			loaded, err := LoadHNSWIndex(path, func(string) ([]float32, bool) { return []float32{1, 0, 0}, true })
			require.NoError(t, err)
			require.Nil(t, loaded, "an incomplete compressed model must request a rebuild, not publish a graph under a compressed label")
		})
	}
}

func TestHNSWQuantizationConstructorInputBoundaries(t *testing.T) {
	_, err := newHNSWQuantizedVectors(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}, 0, nil)
	require.ErrorIs(t, err, ErrDimensionMismatch)
	_, err = newHNSWQuantizedVectors(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}, 3, nil)
	require.Error(t, err)
	_, err = newHNSWQuantizedVectors(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}, 3, [][]float32{{1, 0}})
	require.ErrorIs(t, err, ErrDimensionMismatch)
	// Zero coordinates retain zero scales and do not introduce spurious int8 hits.
	q, err := newHNSWQuantizedVectors(context.Background(), HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}, 3, nil)
	require.NoError(t, err)
	q.set(0, []float32{0, 0, 0})
	require.Zero(t, q.query([]float32{1, 0, 0}).dot(0))
}
