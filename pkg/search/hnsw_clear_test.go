package search

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSWIndex_Clear(t *testing.T) {
	dim := 4
	idx := NewHNSWIndex(dim, DefaultHNSWConfig())

	// Add some vectors
	require.NoError(t, idx.Add("a", []float32{1, 0, 0, 0}))
	require.NoError(t, idx.Add("b", []float32{0, 1, 0, 0}))
	require.NoError(t, idx.Add("c", []float32{0, 0, 1, 0}))

	assert.Equal(t, 3, idx.Size())

	// Remove one to create a tombstone
	idx.Remove("b")
	assert.Equal(t, 2, idx.Size())
	assert.Greater(t, idx.TombstoneRatio(), 0.0)

	// Clear should reset everything
	idx.Clear()
	assert.Equal(t, 0, idx.Size())
	assert.Equal(t, 0.0, idx.TombstoneRatio())
	assert.False(t, idx.ShouldRebuild())

	// Should be able to add vectors again after clear
	require.NoError(t, idx.Add("d", []float32{0, 0, 0, 1}))
	assert.Equal(t, 1, idx.Size())
}

func TestHNSWIndex_Add_InPlaceUpdate_NoTombstoneGrowth(t *testing.T) {
	dim := 4
	idx := NewHNSWIndex(dim, DefaultHNSWConfig())

	require.NoError(t, idx.Add("a", []float32{1, 0, 0, 0}))
	assert.Equal(t, 1, idx.Size())
	assert.Equal(t, 0.0, idx.TombstoneRatio())

	// Updating the same ID should overwrite the stored vector without creating tombstones.
	require.NoError(t, idx.Add("a", []float32{0, 1, 0, 0}))
	assert.Equal(t, 1, idx.Size())
	assert.Equal(t, 0.0, idx.TombstoneRatio())
}

func TestHNSWIndex_TombstoneRatio(t *testing.T) {
	dim := 4
	idx := NewHNSWIndex(dim, DefaultHNSWConfig())

	// Empty index
	assert.Equal(t, 0.0, idx.TombstoneRatio())
	assert.False(t, idx.ShouldRebuild())

	// Add vectors
	for i := 0; i < 10; i++ {
		require.NoError(t, idx.Add(string(rune('a'+i)), []float32{1, 0, 0, 0}))
	}
	assert.Equal(t, 10, idx.Size())
	assert.Equal(t, 0.0, idx.TombstoneRatio())

	// Delete half
	for i := 0; i < 5; i++ {
		idx.Remove(string(rune('a' + i)))
	}
	assert.Equal(t, 5, idx.Size())
	assert.InDelta(t, 0.5, idx.TombstoneRatio(), 0.01)
	assert.False(t, idx.ShouldRebuild()) // Exactly 50% doesn't trigger

	// Delete one more (now >50%)
	idx.Remove("f")
	assert.Equal(t, 4, idx.Size())
	assert.Greater(t, idx.TombstoneRatio(), 0.5)
	assert.True(t, idx.ShouldRebuild())
}

func TestHNSWIndex_ClearFreesMemory(t *testing.T) {
	dim := 128
	idx := NewHNSWIndex(dim, DefaultHNSWConfig())

	// Add many vectors with unique IDs
	ids := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		ids[i] = fmt.Sprintf("vec-%d", i)
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = float32(i+j) / 1000.0
		}
		require.NoError(t, idx.Add(ids[i], vec))
	}

	assert.Equal(t, 1000, idx.Size())

	// Delete most of them (creates many tombstones)
	for i := 0; i < 900; i++ {
		idx.Remove(ids[i])
	}

	assert.Equal(t, 100, idx.Size())
	assert.Greater(t, idx.TombstoneRatio(), 0.8)
	assert.True(t, idx.ShouldRebuild())

	// Clear should free all memory
	idx.Clear()
	assert.Equal(t, 0, idx.Size())
	assert.Equal(t, 0.0, idx.TombstoneRatio())
	assert.False(t, idx.ShouldRebuild())
}
