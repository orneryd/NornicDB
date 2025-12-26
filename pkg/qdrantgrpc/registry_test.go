package qdrantgrpc

import (
	"context"
	"fmt"
	"testing"

	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestPersistentCollectionRegistry_CreateAndGet(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryEngine()

	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer registry.Close()

	t.Run("create collection successfully", func(t *testing.T) {
		err := registry.CreateCollection(ctx, "test_collection", 1024, qpb.Distance_Cosine)
		require.NoError(t, err)

		// Verify it exists
		meta, err := registry.GetCollection(ctx, "test_collection")
		require.NoError(t, err)
		assert.Equal(t, "test_collection", meta.Name)
		assert.Equal(t, 1024, meta.Dimensions)
		assert.Equal(t, qpb.Distance_Cosine, meta.Distance)
		assert.Equal(t, qpb.CollectionStatus_Green, meta.Status)
	})

	t.Run("error on duplicate collection", func(t *testing.T) {
		err := registry.CreateCollection(ctx, "test_collection", 512, qpb.Distance_Dot)
		require.Error(t, err)
	})

	t.Run("get non-existent collection", func(t *testing.T) {
		_, err := registry.GetCollection(ctx, "not_found")
		require.Error(t, err)
	})
}

func TestPersistentCollectionRegistry_ListAndDelete(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryEngine()

	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer registry.Close()

	// Create multiple collections
	_ = registry.CreateCollection(ctx, "col_a", 128, qpb.Distance_Cosine)
	_ = registry.CreateCollection(ctx, "col_b", 256, qpb.Distance_Dot)
	_ = registry.CreateCollection(ctx, "col_c", 512, qpb.Distance_Euclid)

	t.Run("list all collections", func(t *testing.T) {
		names, err := registry.ListCollections(ctx)
		require.NoError(t, err)
		assert.Len(t, names, 3)

		nameSet := make(map[string]bool)
		for _, name := range names {
			nameSet[name] = true
		}
		assert.True(t, nameSet["col_a"])
		assert.True(t, nameSet["col_b"])
		assert.True(t, nameSet["col_c"])
	})

	t.Run("delete collection", func(t *testing.T) {
		err := registry.DeleteCollection(ctx, "col_b")
		require.NoError(t, err)

		// Verify it's gone
		_, err = registry.GetCollection(ctx, "col_b")
		require.Error(t, err)

		// Other collections still exist
		names, _ := registry.ListCollections(ctx)
		assert.Len(t, names, 2)
	})

	t.Run("delete non-existent collection", func(t *testing.T) {
		err := registry.DeleteCollection(ctx, "not_found")
		require.Error(t, err)
	})
}

func TestPersistentCollectionRegistry_PointCount(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryEngine()

	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer registry.Close()

	_ = registry.CreateCollection(ctx, "count_test", 4, qpb.Distance_Cosine)

	t.Run("initially zero points", func(t *testing.T) {
		count, err := registry.GetPointCount(ctx, "count_test")
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("count increases with stored points", func(t *testing.T) {
		// Add points directly to storage
		for i := 0; i < 3; i++ {
			node := &storage.Node{
				ID:              storage.NodeID(fmt.Sprintf("qdrant:count_test:point%d", i)),
				Labels:          []string{QdrantPointLabel, "count_test"},
				ChunkEmbeddings: [][]float32{{float32(i), 0, 0, 0}},
			}
			_, _ = store.CreateNode(node)
		}

		count, err := registry.GetPointCount(ctx, "count_test")
		require.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	t.Run("count for non-existent collection", func(t *testing.T) {
		_, err := registry.GetPointCount(ctx, "not_found")
		require.Error(t, err)
	})
}

func TestPersistentCollectionRegistry_Persistence(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryEngine()

	// Create first registry and add a collection
	registry1, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)

	err = registry1.CreateCollection(ctx, "persistent_col", 256, qpb.Distance_Dot)
	require.NoError(t, err)

	// Add a point to storage
	testNode := &storage.Node{
		ID:              storage.NodeID("qdrant:persistent_col:point1"),
		Labels:          []string{QdrantPointLabel, "persistent_col"},
		Properties:      map[string]any{"data": "test"},
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3, 0.4}},
	}
	_, _ = store.CreateNode(testNode)

	registry1.Close()

	// Create second registry from same storage - should reload
	registry2, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer registry2.Close()

	t.Run("collection persisted and reloaded", func(t *testing.T) {
		meta, err := registry2.GetCollection(ctx, "persistent_col")
		require.NoError(t, err)
		assert.Equal(t, "persistent_col", meta.Name)
		assert.Equal(t, 256, meta.Dimensions)
		assert.Equal(t, qpb.Distance_Dot, meta.Distance)
	})

	t.Run("list shows reloaded collection", func(t *testing.T) {
		names, err := registry2.ListCollections(ctx)
		require.NoError(t, err)
		assert.Contains(t, names, "persistent_col")
	})

	t.Run("point count shows persisted points", func(t *testing.T) {
		count, err := registry2.GetPointCount(ctx, "persistent_col")
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})
}

func TestPersistentCollectionRegistry_CollectionExists(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryEngine()

	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer registry.Close()

	_ = registry.CreateCollection(ctx, "exists_test", 64, qpb.Distance_Cosine)

	t.Run("existing collection", func(t *testing.T) {
		assert.True(t, registry.CollectionExists("exists_test"))
	})

	t.Run("non-existing collection", func(t *testing.T) {
		assert.False(t, registry.CollectionExists("not_found"))
	})
}

func TestPersistentCollectionRegistry_GetAllCollections(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryEngine()

	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer registry.Close()

	_ = registry.CreateCollection(ctx, "all_a", 64, qpb.Distance_Cosine)
	_ = registry.CreateCollection(ctx, "all_b", 128, qpb.Distance_Dot)

	t.Run("get all collections metadata", func(t *testing.T) {
		all, err := registry.GetAllCollections(ctx)
		require.NoError(t, err)
		assert.Len(t, all, 2)

		names := make(map[string]bool)
		for _, meta := range all {
			names[meta.Name] = true
		}
		assert.True(t, names["all_a"])
		assert.True(t, names["all_b"])
	})
}

func TestPersistentCollectionRegistry_ExportImport(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryEngine()

	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer registry.Close()

	_ = registry.CreateCollection(ctx, "export_test", 512, qpb.Distance_Euclid)

	t.Run("export collection metadata", func(t *testing.T) {
		data, err := registry.ExportCollectionMeta("export_test")
		require.NoError(t, err)
		assert.NotEmpty(t, data)
		assert.Contains(t, string(data), "export_test")
		assert.Contains(t, string(data), "512")
	})

	t.Run("import collection metadata", func(t *testing.T) {
		data := []byte(`{"name":"imported_col","dimensions":256,"distance":1,"created_at":1234567890}`)
		err := registry.ImportCollectionMeta(ctx, data)
		require.NoError(t, err)

		meta, err := registry.GetCollection(ctx, "imported_col")
		require.NoError(t, err)
		assert.Equal(t, "imported_col", meta.Name)
		assert.Equal(t, 256, meta.Dimensions)
		assert.Equal(t, qpb.Distance_Cosine, meta.Distance) // 1 = COSINE
	})

	t.Run("import invalid JSON", func(t *testing.T) {
		err := registry.ImportCollectionMeta(ctx, []byte("not json"))
		require.Error(t, err)
	})
}

func TestPersistentCollectionRegistry_NilStorage(t *testing.T) {
	_, err := NewPersistentCollectionRegistry(nil)
	require.Error(t, err)
}

func TestPersistentCollectionRegistry_DeleteWithPoints(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryEngine()

	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer registry.Close()

	_ = registry.CreateCollection(ctx, "delete_with_points", 4, qpb.Distance_Cosine)

	// Add points
	for i := 0; i < 5; i++ {
		node := &storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("qdrant:delete_with_points:point%d", i)),
			Labels:          []string{QdrantPointLabel, "delete_with_points"},
			ChunkEmbeddings: [][]float32{{float32(i), 0, 0, 0}},
		}
		_, _ = store.CreateNode(node)
	}

	// Verify points exist
	count, _ := registry.GetPointCount(ctx, "delete_with_points")
	assert.Equal(t, 5, count)

	// Delete collection
	err = registry.DeleteCollection(ctx, "delete_with_points")
	require.NoError(t, err)

	// Verify collection gone
	assert.False(t, registry.CollectionExists("delete_with_points"))

	// Verify points deleted
	nodes, err := store.GetNodesByLabel("delete_with_points")
	if err == nil {
		// Filter to only Qdrant points
		pointCount := 0
		for _, n := range nodes {
			for _, label := range n.Labels {
				if label == QdrantPointLabel {
					pointCount++
				}
			}
		}
		assert.Equal(t, 0, pointCount)
	}
}

// =============================================================================
// MEMORY REGISTRY TESTS (for testing only)
// =============================================================================

func TestMemoryCollectionRegistry(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryCollectionRegistry()

	t.Run("create and get collection", func(t *testing.T) {
		err := registry.CreateCollection(ctx, "mem_test", 128, qpb.Distance_Cosine)
		require.NoError(t, err)

		meta, err := registry.GetCollection(ctx, "mem_test")
		require.NoError(t, err)
		assert.Equal(t, "mem_test", meta.Name)
		assert.Equal(t, 128, meta.Dimensions)
	})

	t.Run("point count tracking", func(t *testing.T) {
		count, err := registry.GetPointCount(ctx, "mem_test")
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		// Increment count
		registry.IncrementPointCount("mem_test", 3)

		count, err = registry.GetPointCount(ctx, "mem_test")
		require.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	t.Run("delete collection", func(t *testing.T) {
		err := registry.DeleteCollection(ctx, "mem_test")
		require.NoError(t, err)

		_, err = registry.GetCollection(ctx, "mem_test")
		require.Error(t, err)
	})

	t.Run("list collections", func(t *testing.T) {
		_ = registry.CreateCollection(ctx, "list_a", 64, qpb.Distance_Dot)
		_ = registry.CreateCollection(ctx, "list_b", 64, qpb.Distance_Dot)

		names, err := registry.ListCollections(ctx)
		require.NoError(t, err)
		assert.Len(t, names, 2)
	})
}
