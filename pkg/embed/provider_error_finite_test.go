package embed

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckEmbeddingsRejectsNonFinite(t *testing.T) {
	for name, value := range map[string]float32{"nan": float32(math.NaN()), "positive_infinity": float32(math.Inf(1)), "negative_infinity": float32(math.Inf(-1))} {
		t.Run(name, func(t *testing.T) {
			vectors := [][]float32{{1, 0, 0}, {0, value, 1}}
			err := CheckEmbeddings(vectors, 2, 3)
			require.Error(t, err, "the shared usability checker must reject every non-finite coordinate")
			require.Error(t, CheckEmbeddings(vectors, 2, 0), "unknown dimension does not admit non-finite coordinates")
			require.False(t, IsRetryableError(err))
			base := &flakyEmbedder{batches: [][][]float32{vectors, {{1, 0, 0}, {0, 1, 0}}}}
			cached := NewCachedEmbedder(base, 8)
			_, err = cached.EmbedBatch(context.Background(), []string{"a", "b"})
			require.Error(t, err)
			got, err := cached.EmbedBatch(context.Background(), []string{"a", "b"})
			require.NoError(t, err)
			require.Equal(t, [][]float32{{1, 0, 0}, {0, 1, 0}}, got)
			require.Equal(t, 2, base.calls, "unusable values must not enter the independent query cache")
		})
	}
	require.NoError(t, CheckEmbeddings([][]float32{{0, 0}, {math.MaxFloat32, -math.MaxFloat32}}, 2, 2))
}
