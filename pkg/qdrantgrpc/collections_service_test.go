package qdrantgrpc

import (
	"context"
	"testing"

	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectionsService_Create(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryCollectionRegistry()
	service := NewCollectionsService(registry, nil, nil, newVectorIndexCache())

	t.Run("create collection successfully", func(t *testing.T) {
		req := &qpb.CreateCollection{
			CollectionName: "test_collection",
			VectorsConfig: &qpb.VectorsConfig{
				Config: &qpb.VectorsConfig_Params{
					Params: &qpb.VectorParams{
						Size:     1024,
						Distance: qpb.Distance_Cosine,
					},
				},
			},
		}

		resp, err := service.Create(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Result)
		assert.Greater(t, resp.Time, float64(0))
	})

	t.Run("error on duplicate collection", func(t *testing.T) {
		req := &qpb.CreateCollection{
			CollectionName: "test_collection",
			VectorsConfig: &qpb.VectorsConfig{
				Config: &qpb.VectorsConfig_Params{
					Params: &qpb.VectorParams{
						Size:     1024,
						Distance: qpb.Distance_Cosine,
					},
				},
			},
		}

		_, err := service.Create(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &qpb.CreateCollection{
			CollectionName: "",
			VectorsConfig: &qpb.VectorsConfig{
				Config: &qpb.VectorsConfig_Params{
					Params: &qpb.VectorParams{
						Size:     1024,
						Distance: qpb.Distance_Cosine,
					},
				},
			},
		}

		_, err := service.Create(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on missing vectors config", func(t *testing.T) {
		req := &qpb.CreateCollection{
			CollectionName: "no_config",
			VectorsConfig:  nil,
		}

		_, err := service.Create(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on zero dimensions", func(t *testing.T) {
		req := &qpb.CreateCollection{
			CollectionName: "zero_dim",
			VectorsConfig: &qpb.VectorsConfig{
				Config: &qpb.VectorsConfig_Params{
					Params: &qpb.VectorParams{
						Size:     0,
						Distance: qpb.Distance_Cosine,
					},
				},
			},
		}

		_, err := service.Create(ctx, req)
		require.Error(t, err)
	})
}

func TestCollectionsService_Get(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryCollectionRegistry()
	service := NewCollectionsService(registry, nil, nil, newVectorIndexCache())

	// Create a test collection first
	_ = registry.CreateCollection(ctx, "my_collection", 512, qpb.Distance_Dot)

	t.Run("get existing collection", func(t *testing.T) {
		req := &qpb.GetCollectionInfoRequest{
			CollectionName: "my_collection",
		}

		resp, err := service.Get(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp.Result)
		assert.Equal(t, qpb.CollectionStatus_Green, resp.Result.Status)
		require.NotNil(t, resp.Result.PointsCount)
		assert.Equal(t, uint64(0), *resp.Result.PointsCount)

		// Check config
		require.NotNil(t, resp.Result.Config)
		params := resp.Result.Config.GetParams()
		require.NotNil(t, params)
		require.NotNil(t, params.VectorsConfig)
		require.NotNil(t, params.VectorsConfig.GetParams())
		assert.Equal(t, uint64(512), params.VectorsConfig.GetParams().Size)
		assert.Equal(t, qpb.Distance_Dot, params.VectorsConfig.GetParams().Distance)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		req := &qpb.GetCollectionInfoRequest{
			CollectionName: "not_found",
		}

		_, err := service.Get(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &qpb.GetCollectionInfoRequest{
			CollectionName: "",
		}

		_, err := service.Get(ctx, req)
		require.Error(t, err)
	})
}

func TestCollectionsService_List(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryCollectionRegistry()
	service := NewCollectionsService(registry, nil, nil, newVectorIndexCache())

	t.Run("list empty collections", func(t *testing.T) {
		req := &qpb.ListCollectionsRequest{}

		resp, err := service.List(ctx, req)
		require.NoError(t, err)
		assert.Empty(t, resp.Collections)
	})

	t.Run("list multiple collections", func(t *testing.T) {
		// Create some collections
		_ = registry.CreateCollection(ctx, "collection_a", 128, qpb.Distance_Cosine)
		_ = registry.CreateCollection(ctx, "collection_b", 256, qpb.Distance_Euclid)
		_ = registry.CreateCollection(ctx, "collection_c", 512, qpb.Distance_Dot)

		req := &qpb.ListCollectionsRequest{}

		resp, err := service.List(ctx, req)
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

func TestCollectionsService_Delete(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryCollectionRegistry()
	service := NewCollectionsService(registry, nil, nil, newVectorIndexCache())

	// Create a collection first
	_ = registry.CreateCollection(ctx, "to_delete", 128, qpb.Distance_Cosine)

	t.Run("delete existing collection", func(t *testing.T) {
		req := &qpb.DeleteCollection{
			CollectionName: "to_delete",
		}

		resp, err := service.Delete(ctx, req)
		require.NoError(t, err)
		assert.True(t, resp.Result)

		// Verify it's gone
		_, err = registry.GetCollection(ctx, "to_delete")
		require.Error(t, err)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		req := &qpb.DeleteCollection{
			CollectionName: "not_found",
		}

		_, err := service.Delete(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &qpb.DeleteCollection{
			CollectionName: "",
		}

		_, err := service.Delete(ctx, req)
		require.Error(t, err)
	})
}

// MemoryCollectionRegistry tests are in registry_test.go
