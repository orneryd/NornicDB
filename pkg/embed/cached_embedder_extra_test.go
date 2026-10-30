package embed

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCachedEmbedder_Dimensions(t *testing.T) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, 10)
	assert.Equal(t, 3, cached.Dimensions())
}

func TestCachedEmbedder_Model(t *testing.T) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, 10)
	assert.Equal(t, "mock", cached.Model())
}

func TestCachedEmbedder_Clear(t *testing.T) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, 10)
	ctx := context.Background()

	// Fill the cache
	_, err := cached.Embed(ctx, "entry1")
	require.NoError(t, err)
	_, err = cached.Embed(ctx, "entry2")
	require.NoError(t, err)

	stats := cached.Stats()
	assert.Equal(t, 2, stats.Size)

	// Clear should empty the cache
	cached.Clear()

	stats = cached.Stats()
	assert.Equal(t, 0, stats.Size)

	// Subsequent embeds should be cache misses (calls increase)
	prevCalls := mock.CallCount()
	_, err = cached.Embed(ctx, "entry1")
	require.NoError(t, err)
	assert.Equal(t, prevCalls+1, mock.CallCount())
}

func TestCachedEmbedder_Stats_HitRate(t *testing.T) {
	mock := &mockEmbedder{}
	cached := NewCachedEmbedder(mock, 100)
	ctx := context.Background()

	// 1 miss (cold)
	_, _ = cached.Embed(ctx, "query")
	// 3 hits
	_, _ = cached.Embed(ctx, "query")
	_, _ = cached.Embed(ctx, "query")
	_, _ = cached.Embed(ctx, "query")

	stats := cached.Stats()
	assert.Equal(t, uint64(1), stats.Misses)
	assert.Equal(t, uint64(3), stats.Hits)
	assert.InDelta(t, 75.0, stats.HitRate, 0.1)
}
