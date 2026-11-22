package embed

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type ownershipRacingEmbedder struct {
	mockEmbedder
	started chan int
	release [2]chan struct{}
	next    atomic.Int32
}

func (m *ownershipRacingEmbedder) Embed(ctx context.Context, _ string) ([]float32, error) {
	i := int(m.next.Add(1)) - 1
	select {
	case m.started <- i:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-m.release[i]:
		return []float32{1, 2, 3}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// The second miss reaches the existing-entry double check deterministically.
// Vector mutations happen only after both calls have returned.
func TestCachedEmbeddingOwnershipConcurrentMiss(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	base := &ownershipRacingEmbedder{started: make(chan int, 2), release: [2]chan struct{}{make(chan struct{}), make(chan struct{})}}
	cached := NewCachedEmbedder(base, 4)
	t.Cleanup(func() { require.NoError(t, cached.Close()) })
	type answer struct {
		vector []float32
		err    error
	}
	answers := make(chan answer, 2)
	for range 2 {
		go func() { v, err := cached.Embed(ctx, "same"); answers <- answer{v, err} }()
	}
	for range 2 {
		select {
		case <-base.started:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(base.release[0])
	first := <-answers
	require.NoError(t, first.err)
	close(base.release[1])
	second := <-answers
	require.NoError(t, second.err)
	require.Equal(t, []float32{1, 2, 3}, first.vector)
	require.Equal(t, first.vector, second.vector)
	first.vector[0] = 99
	require.Equal(t, []float32{1, 2, 3}, second.vector)
	second.vector[1] = 88
	retained, err := cached.Embed(ctx, "same")
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2, 3}, retained)
}

type ownershipAliasedBatchEmbedder struct{ mockEmbedder }

func (*ownershipAliasedBatchEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	shared := []float32{1, 2, 3}
	results := make([][]float32, len(texts))
	for i := range results {
		results[i] = shared
	}
	return results, nil
}

func TestCachedEmbeddingOwnershipBatchMissResults(t *testing.T) {
	cached := NewCachedEmbedder(&ownershipAliasedBatchEmbedder{}, 4)
	t.Cleanup(func() { require.NoError(t, cached.Close()) })
	results, err := cached.EmbedBatch(t.Context(), []string{"same", "same", "other"})
	require.NoError(t, err)
	require.Len(t, results, 3)
	results[0][0] = 99
	require.Equal(t, []float32{1, 2, 3}, results[1])
	require.Equal(t, []float32{1, 2, 3}, results[2])
	hits, err := cached.EmbedBatch(t.Context(), []string{"same", "other"})
	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 2, 3}, {1, 2, 3}}, hits)
}
