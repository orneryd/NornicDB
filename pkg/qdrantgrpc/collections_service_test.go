package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/orneryd/nornicdb/pkg/qdrantgrpc/gen"
)

func TestCollectionsService_CreateCollection(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryCollectionRegistry()
	service := NewCollectionsService(registry)

	t.Run("create collection successfully", func(t *testing.T) {
		req := &pb.CreateCollectionRequest{
			CollectionName: "test_collection",
			VectorsConfig: &pb.VectorsConfig{
				Config: &pb.VectorsConfig_Params{
					Params: &pb.VectorParams{
						Size:     1024,
						Distance: pb.Distance_COSINE,
					},
				},
			},
		}

		resp, err := service.CreateCollection(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Result)
		assert.Greater(t, resp.Time, float64(0))
	})

	t.Run("error on duplicate collection", func(t *testing.T) {
		req := &pb.CreateCollectionRequest{
			CollectionName: "test_collection",
			VectorsConfig: &pb.VectorsConfig{
				Config: &pb.VectorsConfig_Params{
					Params: &pb.VectorParams{
						Size:     1024,
						Distance: pb.Distance_COSINE,
					},
				},
			},
		}

		_, err := service.CreateCollection(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &pb.CreateCollectionRequest{
			CollectionName: "",
			VectorsConfig: &pb.VectorsConfig{
				Config: &pb.VectorsConfig_Params{
					Params: &pb.VectorParams{
						Size:     1024,
						Distance: pb.Distance_COSINE,
					},
				},
			},
		}

		_, err := service.CreateCollection(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on missing vectors config", func(t *testing.T) {
		req := &pb.CreateCollectionRequest{
			CollectionName: "no_config",
			VectorsConfig:  nil,
		}

		_, err := service.CreateCollection(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on zero dimensions", func(t *testing.T) {
		req := &pb.CreateCollectionRequest{
			CollectionName: "zero_dim",
			VectorsConfig: &pb.VectorsConfig{
				Config: &pb.VectorsConfig_Params{
					Params: &pb.VectorParams{
						Size:     0,
						Distance: pb.Distance_COSINE,
					},
				},
			},
		}

		_, err := service.CreateCollection(ctx, req)
		require.Error(t, err)
	})
}

func TestCollectionsService_GetCollectionInfo(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryCollectionRegistry()
	service := NewCollectionsService(registry)

	// Create a test collection first
	_ = registry.CreateCollection(ctx, "my_collection", 512, pb.Distance_DOT)

	t.Run("get existing collection", func(t *testing.T) {
		req := &pb.GetCollectionInfoRequest{
			CollectionName: "my_collection",
		}

		resp, err := service.GetCollectionInfo(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp.Result)
		assert.Equal(t, pb.CollectionStatus_GREEN, resp.Result.Status)
		assert.Equal(t, uint64(0), resp.Result.PointsCount)

		// Check config
		require.NotNil(t, resp.Result.Config)
		params := resp.Result.Config.GetParams()
		require.NotNil(t, params)
		assert.Equal(t, uint64(512), params.Size)
		assert.Equal(t, pb.Distance_DOT, params.Distance)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		req := &pb.GetCollectionInfoRequest{
			CollectionName: "not_found",
		}

		_, err := service.GetCollectionInfo(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &pb.GetCollectionInfoRequest{
			CollectionName: "",
		}

		_, err := service.GetCollectionInfo(ctx, req)
		require.Error(t, err)
	})
}

func TestCollectionsService_ListCollections(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryCollectionRegistry()
	service := NewCollectionsService(registry)

	t.Run("list empty collections", func(t *testing.T) {
		req := &pb.ListCollectionsRequest{}

		resp, err := service.ListCollections(ctx, req)
		require.NoError(t, err)
		assert.Empty(t, resp.Collections)
	})

	t.Run("list multiple collections", func(t *testing.T) {
		// Create some collections
		_ = registry.CreateCollection(ctx, "collection_a", 128, pb.Distance_COSINE)
		_ = registry.CreateCollection(ctx, "collection_b", 256, pb.Distance_EUCLID)
		_ = registry.CreateCollection(ctx, "collection_c", 512, pb.Distance_DOT)

		req := &pb.ListCollectionsRequest{}

		resp, err := service.ListCollections(ctx, req)
		require.NoError(t, err)
		assert.Len(t, resp.Collections, 3)

		// Check all names are present
		names := make(map[string]bool)
		for _, c := range resp.Collections {
			names[c.Name] = true
		}
		assert.True(t, names["collection_a"])
		assert.True(t, names["collection_b"])
		assert.True(t, names["collection_c"])
	})
}

func TestCollectionsService_DeleteCollection(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryCollectionRegistry()
	service := NewCollectionsService(registry)

	// Create a collection first
	_ = registry.CreateCollection(ctx, "to_delete", 128, pb.Distance_COSINE)

	t.Run("delete existing collection", func(t *testing.T) {
		req := &pb.DeleteCollectionRequest{
			CollectionName: "to_delete",
		}

		resp, err := service.DeleteCollection(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Result)

		// Verify it's gone
		_, err = registry.GetCollection(ctx, "to_delete")
		require.Error(t, err)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		req := &pb.DeleteCollectionRequest{
			CollectionName: "not_found",
		}

		_, err := service.DeleteCollection(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &pb.DeleteCollectionRequest{
			CollectionName: "",
		}

		_, err := service.DeleteCollection(ctx, req)
		require.Error(t, err)
	})
}

// MemoryCollectionRegistry tests are in registry_test.go

