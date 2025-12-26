package search

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHNSWConfigFromEnv(t *testing.T) {
	t.Run("default balanced preset", func(t *testing.T) {
		os.Unsetenv("NORNICDB_VECTOR_ANN_QUALITY")
		config := HNSWConfigFromEnv()

		// Balanced defaults
		assert.Equal(t, 16, config.M)
		assert.Equal(t, 200, config.EfConstruction)
		assert.Equal(t, 100, config.EfSearch)
	})

	t.Run("fast preset", func(t *testing.T) {
		os.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "fast")
		defer os.Unsetenv("NORNICDB_VECTOR_ANN_QUALITY")

		config := HNSWConfigFromEnv()
		assert.Equal(t, 16, config.M)
		assert.Equal(t, 100, config.EfConstruction)
		assert.Equal(t, 50, config.EfSearch)
	})

	t.Run("accurate preset", func(t *testing.T) {
		os.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "accurate")
		defer os.Unsetenv("NORNICDB_VECTOR_ANN_QUALITY")

		config := HNSWConfigFromEnv()
		assert.Equal(t, 32, config.M)
		assert.Equal(t, 400, config.EfConstruction)
		assert.Equal(t, 200, config.EfSearch)
	})

	t.Run("advanced overrides", func(t *testing.T) {
		os.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "balanced")
		os.Setenv("NORNICDB_VECTOR_HNSW_M", "24")
		os.Setenv("NORNICDB_VECTOR_HNSW_EF_SEARCH", "150")
		defer func() {
			os.Unsetenv("NORNICDB_VECTOR_ANN_QUALITY")
			os.Unsetenv("NORNICDB_VECTOR_HNSW_M")
			os.Unsetenv("NORNICDB_VECTOR_HNSW_EF_SEARCH")
		}()

		config := HNSWConfigFromEnv()
		assert.Equal(t, 24, config.M)              // Overridden
		assert.Equal(t, 200, config.EfConstruction) // From preset
		assert.Equal(t, 150, config.EfSearch)      // Overridden
	})

	t.Run("invalid preset defaults to balanced", func(t *testing.T) {
		os.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "invalid")
		defer os.Unsetenv("NORNICDB_VECTOR_ANN_QUALITY")

		config := HNSWConfigFromEnv()
		// Should default to balanced
		assert.Equal(t, 16, config.M)
		assert.Equal(t, 200, config.EfConstruction)
		assert.Equal(t, 100, config.EfSearch)
	})
}

func TestHNSWUpdate(t *testing.T) {
	idx := NewHNSWIndex(4, DefaultHNSWConfig())

	// Add initial vector
	require.NoError(t, idx.Add("vec1", []float32{1, 0, 0, 0}))
	assert.Equal(t, 1, idx.Size())

	// Update with new vector
	require.NoError(t, idx.Update("vec1", []float32{0, 1, 0, 0}))
	assert.Equal(t, 1, idx.Size()) // Size should remain 1

	// Verify the vector was updated
	results, err := idx.Search(context.Background(), []float32{0, 1, 0, 0}, 1, 0.0)
	require.NoError(t, err)
	require.Equal(t, 1, len(results))
	assert.Equal(t, "vec1", results[0].ID)
	assert.Greater(t, results[0].Score, 0.9) // Should be very similar to new vector
}

