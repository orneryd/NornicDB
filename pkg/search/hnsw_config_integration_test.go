package search

import (
	"context"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHNSWConfigIntegration verifies that HNSW configuration from environment
// variables is properly applied when creating search services.
func TestHNSWConfigIntegration(t *testing.T) {
	t.Run("fast preset applied", func(t *testing.T) {
		os.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "fast")
		defer os.Unsetenv("NORNICDB_VECTOR_ANN_QUALITY")

		engine := storage.NewMemoryEngine()
		svc := NewServiceWithDimensions(engine, 4)

		// Add enough nodes to trigger HNSW creation (>= NSmallMax)
		for i := 0; i < NSmallMax+10; i++ {
			node := &storage.Node{
				ID:              storage.NodeID(string(rune('a'+i%26)) + string(rune(i))),
				ChunkEmbeddings: [][]float32{{float32(i % 4), float32((i + 1) % 4), 0, 0}},
			}
			require.NoError(t, svc.IndexNode(node))
		}

		// Trigger HNSW creation by performing a search
		query := []float32{1, 0, 0, 0}
		opts := DefaultSearchOptions()
		opts.Limit = 10
		_, err := svc.VectorSearchCandidates(context.Background(), query, opts)
		require.NoError(t, err)

		// Verify HNSW was created with fast preset
		svc.hnswMu.RLock()
		if svc.hnswIndex != nil {
			config := svc.hnswIndex.config
			assert.Equal(t, 16, config.M)
			assert.Equal(t, 100, config.EfConstruction)
			assert.Equal(t, 50, config.EfSearch)
		}
		svc.hnswMu.RUnlock()
	})

	t.Run("accurate preset applied", func(t *testing.T) {
		os.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "accurate")
		defer os.Unsetenv("NORNICDB_VECTOR_ANN_QUALITY")

		engine := storage.NewMemoryEngine()
		svc := NewServiceWithDimensions(engine, 4)

		// Add enough nodes to trigger HNSW creation
		for i := 0; i < NSmallMax+10; i++ {
			node := &storage.Node{
				ID:              storage.NodeID(string(rune('a'+i%26)) + string(rune(i))),
				ChunkEmbeddings: [][]float32{{float32(i % 4), float32((i + 1) % 4), 0, 0}},
			}
			require.NoError(t, svc.IndexNode(node))
		}

		// Trigger HNSW creation
		query := []float32{1, 0, 0, 0}
		opts := DefaultSearchOptions()
		opts.Limit = 10
		_, err := svc.VectorSearchCandidates(context.Background(), query, opts)
		require.NoError(t, err)

		// Verify HNSW was created with accurate preset
		svc.hnswMu.RLock()
		if svc.hnswIndex != nil {
			config := svc.hnswIndex.config
			assert.Equal(t, 32, config.M)
			assert.Equal(t, 400, config.EfConstruction)
			assert.Equal(t, 200, config.EfSearch)
		}
		svc.hnswMu.RUnlock()
	})

	t.Run("advanced overrides applied", func(t *testing.T) {
		os.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "balanced")
		os.Setenv("NORNICDB_VECTOR_HNSW_M", "24")
		os.Setenv("NORNICDB_VECTOR_HNSW_EF_SEARCH", "150")
		defer func() {
			os.Unsetenv("NORNICDB_VECTOR_ANN_QUALITY")
			os.Unsetenv("NORNICDB_VECTOR_HNSW_M")
			os.Unsetenv("NORNICDB_VECTOR_HNSW_EF_SEARCH")
		}()

		engine := storage.NewMemoryEngine()
		svc := NewServiceWithDimensions(engine, 4)

		// Add enough nodes to trigger HNSW creation
		for i := 0; i < NSmallMax+10; i++ {
			node := &storage.Node{
				ID:              storage.NodeID(string(rune('a'+i%26)) + string(rune(i))),
				ChunkEmbeddings: [][]float32{{float32(i % 4), float32((i + 1) % 4), 0, 0}},
			}
			require.NoError(t, svc.IndexNode(node))
		}

		// Trigger HNSW creation
		query := []float32{1, 0, 0, 0}
		opts := DefaultSearchOptions()
		opts.Limit = 10
		_, err := svc.VectorSearchCandidates(context.Background(), query, opts)
		require.NoError(t, err)

		// Verify HNSW was created with overrides
		svc.hnswMu.RLock()
		if svc.hnswIndex != nil {
			config := svc.hnswIndex.config
			assert.Equal(t, 24, config.M)              // Overridden
			assert.Equal(t, 200, config.EfConstruction) // From balanced preset
			assert.Equal(t, 150, config.EfSearch)      // Overridden
		}
		svc.hnswMu.RUnlock()
	})
}

