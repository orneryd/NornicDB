package search

import (
	"context"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
)

func TestProductCodebookRetainsSubvectorMagnitude(t *testing.T) {
	training := [][]float32{{1}, {1.1}, {10}, {10.1}}
	centers, err := ivfpqTrainPQCodebook(context.Background(), training, 2, 20, rand.New(rand.NewSource(42)))
	require.NoError(t, err)
	for _, v := range training {
		best := float32(1000)
		for _, center := range centers {
			delta := v[0] - center[0]
			if delta*delta < best {
				best = delta * delta
			}
		}
		require.Less(t, best, float32(.02), "PQ encoding uses squared distance, so codebook training must retain subvector magnitude")
	}
}
