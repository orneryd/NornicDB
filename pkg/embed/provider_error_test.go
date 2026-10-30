package embed

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProviderErrorsClassifyRetryableFailures(t *testing.T) {
	for _, status := range []int{408, 409, 425, 429, 500, 503} {
		require.True(t, IsRetryableError(&ProviderError{StatusCode: status}), "status %d", status)
	}
	for _, status := range []int{400, 401, 403, 404, 422} {
		require.False(t, IsRetryableError(&ProviderError{StatusCode: status}), "status %d", status)
	}
	require.True(t, IsRetryableError(errors.New("connection reset")))
	require.False(t, IsRetryableError(nil))
}

// A provider answer is usable only as one non-empty vector per input, of the
// embedder's dimension; an unusable one is a retryable failure, never cached.
func TestCheckEmbeddingsAndCacheRefusesUnusable(t *testing.T) {
	require.NoError(t, CheckEmbeddings([][]float32{{1, 2}, {3, 4}}, 2, 2))
	require.NoError(t, CheckEmbeddings([][]float32{{1, 2, 3}}, 1, 0))
	require.EqualError(t, CheckEmbeddings([][]float32{{1}}, 2, 0), "embedding provider returned 1 vectors for 2 inputs")
	require.EqualError(t, CheckEmbeddings([][]float32{{1}, {}}, 2, 0), "embedding provider returned an empty vector for input 1")
	err := CheckEmbeddings([][]float32{{1, 2}}, 1, 3)
	require.EqualError(t, err, "embedding provider returned 2 dimensions for input 0; the embedder has 3")
	require.False(t, IsRetryableError(err), "the worker isolates the input instead of retrying the batch")

	base := &flakyEmbedder{answers: [][]float32{{}, {1, 2, 3}}}
	cached := NewCachedEmbedder(base, 10)
	_, err = cached.Embed(context.Background(), "q")
	require.ErrorContains(t, err, "empty vector")
	vec, err := cached.Embed(context.Background(), "q")
	require.NoError(t, err, "the unusable answer wasn't cached, so the provider is asked again")
	require.Equal(t, []float32{1, 2, 3}, vec)
	require.Equal(t, 2, base.calls)

	batchBase := &flakyEmbedder{batches: [][][]float32{{{1, 2, 3}, {}}, {{1, 2, 3}, {4, 5, 6}}}}
	batchCached := NewCachedEmbedder(batchBase, 10)
	_, err = batchCached.EmbedBatch(context.Background(), []string{"a", "b"})
	require.ErrorContains(t, err, "empty vector for input 1")
	vecs, err := batchCached.EmbedBatch(context.Background(), []string{"a", "b"})
	require.NoError(t, err)
	require.Len(t, vecs, 2)
	require.Equal(t, 2, batchBase.calls, "nothing from the unusable batch was cached")
}

type flakyEmbedder struct {
	mockEmbedder
	answers [][]float32
	batches [][][]float32
	calls   int
}

func (f *flakyEmbedder) Embed(context.Context, string) ([]float32, error) {
	answer := f.answers[f.calls]
	f.calls++
	return answer, nil
}

func (f *flakyEmbedder) EmbedBatch(context.Context, []string) ([][]float32, error) {
	answer := f.batches[f.calls]
	f.calls++
	return answer, nil
}

func (f *flakyEmbedder) Dimensions() int { return 3 }
