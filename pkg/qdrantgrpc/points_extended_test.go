package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/orneryd/nornicdb/pkg/qdrantgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// setupExtendedTest creates a test environment with a collection and some points.
func setupExtendedTest(t *testing.T) (*PointsService, *PersistentCollectionRegistry, func()) {
	store := storage.NewMemoryEngine()
	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)

	config := &Config{
		ListenAddr:     ":6334",
		MaxVectorDim:   4096,
		MaxBatchPoints: 1000,
		MaxTopK:        1000,
	}

	service := NewPointsService(config, store, registry, nil)

	// Create test collection
	ctx := context.Background()
	err = registry.CreateCollection(ctx, "test_collection", 4, pb.Distance_COSINE)
	require.NoError(t, err)

	// Insert some test points
	points := []*pb.PointStruct{
		{
			Id:      &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}},
			Vectors: &pb.Vectors{VectorsOptions: &pb.Vectors_Vector{Vector: &pb.Vector{Data: []float32{1, 0, 0, 0}}}},
			Payload: map[string]*pb.Value{
				"category": {Kind: &pb.Value_StringValue{StringValue: "A"}},
				"score":    {Kind: &pb.Value_IntegerValue{IntegerValue: 10}},
			},
		},
		{
			Id:      &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "point2"}},
			Vectors: &pb.Vectors{VectorsOptions: &pb.Vectors_Vector{Vector: &pb.Vector{Data: []float32{0, 1, 0, 0}}}},
			Payload: map[string]*pb.Value{
				"category": {Kind: &pb.Value_StringValue{StringValue: "A"}},
				"score":    {Kind: &pb.Value_IntegerValue{IntegerValue: 20}},
			},
		},
		{
			Id:      &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "point3"}},
			Vectors: &pb.Vectors{VectorsOptions: &pb.Vectors_Vector{Vector: &pb.Vector{Data: []float32{0, 0, 1, 0}}}},
			Payload: map[string]*pb.Value{
				"category": {Kind: &pb.Value_StringValue{StringValue: "B"}},
				"score":    {Kind: &pb.Value_IntegerValue{IntegerValue: 30}},
			},
		},
		{
			Id:      &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "point4"}},
			Vectors: &pb.Vectors{VectorsOptions: &pb.Vectors_Vector{Vector: &pb.Vector{Data: []float32{0, 0, 0, 1}}}},
			Payload: map[string]*pb.Value{
				"category": {Kind: &pb.Value_StringValue{StringValue: "B"}},
				"score":    {Kind: &pb.Value_IntegerValue{IntegerValue: 40}},
			},
		},
	}

	_, err = service.Upsert(ctx, &pb.UpsertPointsRequest{
		CollectionName: "test_collection",
		Points:         points,
	})
	require.NoError(t, err)

	cleanup := func() {
		store.Close()
	}

	return service, registry, cleanup
}

func TestPointsService_Scroll(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("scroll all points with default limit", func(t *testing.T) {
		resp, err := service.Scroll(ctx, &pb.ScrollPointsRequest{
			CollectionName: "test_collection",
		})
		require.NoError(t, err)
		assert.Len(t, resp.Result, 4) // All points with default limit 10
	})

	t.Run("scroll with limit", func(t *testing.T) {
		limit := uint32(2)
		resp, err := service.Scroll(ctx, &pb.ScrollPointsRequest{
			CollectionName: "test_collection",
			Limit:          &limit,
		})
		require.NoError(t, err)
		assert.Len(t, resp.Result, 2)
		assert.NotNil(t, resp.NextPageOffset)
	})

	t.Run("scroll with payload", func(t *testing.T) {
		resp, err := service.Scroll(ctx, &pb.ScrollPointsRequest{
			CollectionName: "test_collection",
			WithPayload: &pb.WithPayloadSelector{
				SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true},
			},
		})
		require.NoError(t, err)
		assert.Len(t, resp.Result, 4)
		for _, point := range resp.Result {
			assert.NotNil(t, point.Payload)
		}
	})

	t.Run("scroll with vectors", func(t *testing.T) {
		resp, err := service.Scroll(ctx, &pb.ScrollPointsRequest{
			CollectionName: "test_collection",
			WithVectors: &pb.WithVectorsSelector{
				SelectorOptions: &pb.WithVectorsSelector_Enable{Enable: true},
			},
		})
		require.NoError(t, err)
		assert.Len(t, resp.Result, 4)
		for _, point := range resp.Result {
			assert.NotNil(t, point.Vectors)
		}
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.Scroll(ctx, &pb.ScrollPointsRequest{})
		assert.Error(t, err)
	})
}

func TestPointsService_Recommend(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("recommend based on positive example", func(t *testing.T) {
		resp, err := service.Recommend(ctx, &pb.RecommendPointsRequest{
			CollectionName: "test_collection",
			Positive:       []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}}},
			Limit:          3,
		})
		require.NoError(t, err)
		// Should return similar points (not including point1)
		assert.True(t, len(resp.Result) > 0)
		for _, point := range resp.Result {
			assert.NotEqual(t, "point1", point.Id.GetUuid())
		}
	})

	t.Run("recommend with positive and negative", func(t *testing.T) {
		resp, err := service.Recommend(ctx, &pb.RecommendPointsRequest{
			CollectionName: "test_collection",
			Positive:       []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}}},
			Negative:       []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point4"}}},
			Limit:          3,
		})
		require.NoError(t, err)
		// Should return results excluding both positive and negative examples
		for _, point := range resp.Result {
			id := point.Id.GetUuid()
			assert.NotEqual(t, "point1", id)
			assert.NotEqual(t, "point4", id)
		}
	})

	t.Run("error on no positive examples", func(t *testing.T) {
		_, err := service.Recommend(ctx, &pb.RecommendPointsRequest{
			CollectionName: "test_collection",
			Limit:          3,
		})
		assert.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.Recommend(ctx, &pb.RecommendPointsRequest{
			Positive: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}}},
			Limit:    3,
		})
		assert.Error(t, err)
	})
}

func TestPointsService_RecommendBatch(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("batch recommend multiple requests", func(t *testing.T) {
		resp, err := service.RecommendBatch(ctx, &pb.RecommendBatchPointsRequest{
			CollectionName: "test_collection",
			RecommendRequests: []*pb.RecommendPointsRequest{
				{
					Positive: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}}},
					Limit:    2,
				},
				{
					Positive: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point3"}}},
					Limit:    2,
				},
			},
		})
		require.NoError(t, err)
		assert.Len(t, resp.Result, 2)
	})
}

func TestPointsService_SearchGroups(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("search groups by category", func(t *testing.T) {
		resp, err := service.SearchGroups(ctx, &pb.SearchPointGroupsRequest{
			CollectionName: "test_collection",
			Vector:         []float32{0.5, 0.5, 0, 0},
			Limit:          2,
			GroupBy:        "category",
			GroupSize:      2,
		})
		require.NoError(t, err)
		// Should have groups for categories A and B
		assert.True(t, len(resp.Result) > 0)
	})

	t.Run("error on missing group_by", func(t *testing.T) {
		_, err := service.SearchGroups(ctx, &pb.SearchPointGroupsRequest{
			CollectionName: "test_collection",
			Vector:         []float32{0.5, 0.5, 0, 0},
			Limit:          2,
		})
		assert.Error(t, err)
	})

	t.Run("error on empty vector", func(t *testing.T) {
		_, err := service.SearchGroups(ctx, &pb.SearchPointGroupsRequest{
			CollectionName: "test_collection",
			GroupBy:        "category",
			Limit:          2,
		})
		assert.Error(t, err)
	})
}

func TestPointsService_SetPayload(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("set payload on specific points", func(t *testing.T) {
		resp, err := service.SetPayload(ctx, &pb.SetPayloadPointsRequest{
			CollectionName: "test_collection",
			Payload: map[string]*pb.Value{
				"new_field": {Kind: &pb.Value_StringValue{StringValue: "new_value"}},
			},
			PointsSelector: &pb.PointsSelector{
				PointsSelectorOneOf: &pb.PointsSelector_Points{
					Points: &pb.PointsIdsList{
						Ids: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}}},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)

		// Verify the payload was merged
		getResp, err := service.Get(ctx, &pb.GetPointsRequest{
			CollectionName: "test_collection",
			Ids:            []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}}},
			WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
		})
		require.NoError(t, err)
		require.Len(t, getResp.Result, 1)
		assert.Equal(t, "new_value", getResp.Result[0].Payload["new_field"].GetStringValue())
		// Original field should still exist
		assert.Equal(t, "A", getResp.Result[0].Payload["category"].GetStringValue())
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.SetPayload(ctx, &pb.SetPayloadPointsRequest{
			Payload: map[string]*pb.Value{"x": {Kind: &pb.Value_StringValue{StringValue: "y"}}},
		})
		assert.Error(t, err)
	})

	t.Run("error on empty payload", func(t *testing.T) {
		_, err := service.SetPayload(ctx, &pb.SetPayloadPointsRequest{
			CollectionName: "test_collection",
		})
		assert.Error(t, err)
	})
}

func TestPointsService_OverwritePayload(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("overwrite payload completely", func(t *testing.T) {
		resp, err := service.OverwritePayload(ctx, &pb.SetPayloadPointsRequest{
			CollectionName: "test_collection",
			Payload: map[string]*pb.Value{
				"only_field": {Kind: &pb.Value_StringValue{StringValue: "only_value"}},
			},
			PointsSelector: &pb.PointsSelector{
				PointsSelectorOneOf: &pb.PointsSelector_Points{
					Points: &pb.PointsIdsList{
						Ids: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point2"}}},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)

		// Verify the payload was replaced
		getResp, err := service.Get(ctx, &pb.GetPointsRequest{
			CollectionName: "test_collection",
			Ids:            []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point2"}}},
			WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
		})
		require.NoError(t, err)
		require.Len(t, getResp.Result, 1)
		assert.Equal(t, "only_value", getResp.Result[0].Payload["only_field"].GetStringValue())
		// Original fields should be gone
		assert.Nil(t, getResp.Result[0].Payload["category"])
	})
}

func TestPointsService_DeletePayload(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("delete specific payload keys", func(t *testing.T) {
		resp, err := service.DeletePayload(ctx, &pb.DeletePayloadPointsRequest{
			CollectionName: "test_collection",
			Keys:           []string{"score"},
			PointsSelector: &pb.PointsSelector{
				PointsSelectorOneOf: &pb.PointsSelector_Points{
					Points: &pb.PointsIdsList{
						Ids: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point3"}}},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)

		// Verify the key was deleted
		getResp, err := service.Get(ctx, &pb.GetPointsRequest{
			CollectionName: "test_collection",
			Ids:            []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point3"}}},
			WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
		})
		require.NoError(t, err)
		require.Len(t, getResp.Result, 1)
		assert.Nil(t, getResp.Result[0].Payload["score"])
		// Other keys should remain
		assert.Equal(t, "B", getResp.Result[0].Payload["category"].GetStringValue())
	})

	t.Run("error on empty keys", func(t *testing.T) {
		_, err := service.DeletePayload(ctx, &pb.DeletePayloadPointsRequest{
			CollectionName: "test_collection",
		})
		assert.Error(t, err)
	})
}

func TestPointsService_ClearPayload(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("clear all payload", func(t *testing.T) {
		resp, err := service.ClearPayload(ctx, &pb.ClearPayloadPointsRequest{
			CollectionName: "test_collection",
			Points: &pb.PointsSelector{
				PointsSelectorOneOf: &pb.PointsSelector_Points{
					Points: &pb.PointsIdsList{
						Ids: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point4"}}},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)

		// Verify payload was cleared
		getResp, err := service.Get(ctx, &pb.GetPointsRequest{
			CollectionName: "test_collection",
			Ids:            []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point4"}}},
			WithPayload:    &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
		})
		require.NoError(t, err)
		require.Len(t, getResp.Result, 1)
		assert.Empty(t, getResp.Result[0].Payload)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.ClearPayload(ctx, &pb.ClearPayloadPointsRequest{})
		assert.Error(t, err)
	})
}

func TestPointsService_UpdateVectors(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("update vector for existing point", func(t *testing.T) {
		newVector := []float32{0.25, 0.25, 0.25, 0.25}
		resp, err := service.UpdateVectors(ctx, &pb.UpdatePointVectorsRequest{
			CollectionName: "test_collection",
			Points: []*pb.PointVectors{
				{
					Id:      &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}},
					Vectors: &pb.Vectors{VectorsOptions: &pb.Vectors_Vector{Vector: &pb.Vector{Data: newVector}}},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)

		// Verify vector was updated
		getResp, err := service.Get(ctx, &pb.GetPointsRequest{
			CollectionName: "test_collection",
			Ids:            []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}}},
			WithVectors:    &pb.WithVectorsSelector{SelectorOptions: &pb.WithVectorsSelector_Enable{Enable: true}},
		})
		require.NoError(t, err)
		require.Len(t, getResp.Result, 1)
		assert.Equal(t, newVector, getResp.Result[0].Vectors.GetVector().Data)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.UpdateVectors(ctx, &pb.UpdatePointVectorsRequest{
			Points: []*pb.PointVectors{
				{
					Id:      &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "point1"}},
					Vectors: &pb.Vectors{VectorsOptions: &pb.Vectors_Vector{Vector: &pb.Vector{Data: []float32{1, 1, 1, 1}}}},
				},
			},
		})
		assert.Error(t, err)
	})

	t.Run("error on empty points", func(t *testing.T) {
		_, err := service.UpdateVectors(ctx, &pb.UpdatePointVectorsRequest{
			CollectionName: "test_collection",
		})
		assert.Error(t, err)
	})
}

func TestPointsService_DeleteVectors(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("delete vectors from points", func(t *testing.T) {
		resp, err := service.DeleteVectors(ctx, &pb.DeletePointVectorsRequest{
			CollectionName: "test_collection",
			PointsSelector: &pb.PointsSelector{
				PointsSelectorOneOf: &pb.PointsSelector_Points{
					Points: &pb.PointsIdsList{
						Ids: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point2"}}},
					},
				},
			},
		})
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)

		// Verify vector was deleted
		getResp, err := service.Get(ctx, &pb.GetPointsRequest{
			CollectionName: "test_collection",
			Ids:            []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "point2"}}},
			WithVectors:    &pb.WithVectorsSelector{SelectorOptions: &pb.WithVectorsSelector_Enable{Enable: true}},
		})
		require.NoError(t, err)
		require.Len(t, getResp.Result, 1)
		// Vector should be nil or empty after deletion
		if getResp.Result[0].Vectors != nil {
			vec := getResp.Result[0].Vectors.GetVector()
			if vec != nil {
				assert.Empty(t, vec.Data)
			}
		}
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.DeleteVectors(ctx, &pb.DeletePointVectorsRequest{})
		assert.Error(t, err)
	})
}

func TestPointsService_CreateFieldIndex(t *testing.T) {
	service, registry, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("create field index", func(t *testing.T) {
		resp, err := service.CreateFieldIndex(ctx, &pb.CreateFieldIndexCollectionRequest{
			CollectionName: "test_collection",
			FieldName:      "category",
		})
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.CreateFieldIndex(ctx, &pb.CreateFieldIndexCollectionRequest{
			FieldName: "category",
		})
		assert.Error(t, err)
	})

	t.Run("error on empty field name", func(t *testing.T) {
		_, err := service.CreateFieldIndex(ctx, &pb.CreateFieldIndexCollectionRequest{
			CollectionName: "test_collection",
		})
		assert.Error(t, err)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		_, err := service.CreateFieldIndex(ctx, &pb.CreateFieldIndexCollectionRequest{
			CollectionName: "non_existent",
			FieldName:      "category",
		})
		assert.Error(t, err)
	})

	// Silence unused variable
	_ = registry
}

func TestPointsService_DeleteFieldIndex(t *testing.T) {
	service, _, cleanup := setupExtendedTest(t)
	defer cleanup()
	ctx := context.Background()

	t.Run("delete field index", func(t *testing.T) {
		// First create an index
		_, err := service.CreateFieldIndex(ctx, &pb.CreateFieldIndexCollectionRequest{
			CollectionName: "test_collection",
			FieldName:      "score",
		})
		require.NoError(t, err)

		// Then delete it
		resp, err := service.DeleteFieldIndex(ctx, &pb.DeleteFieldIndexCollectionRequest{
			CollectionName: "test_collection",
			FieldName:      "score",
		})
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.DeleteFieldIndex(ctx, &pb.DeleteFieldIndexCollectionRequest{
			FieldName: "score",
		})
		assert.Error(t, err)
	})

	t.Run("error on empty field name", func(t *testing.T) {
		_, err := service.DeleteFieldIndex(ctx, &pb.DeleteFieldIndexCollectionRequest{
			CollectionName: "test_collection",
		})
		assert.Error(t, err)
	})
}

func TestCollectionsService_UpdateCollection(t *testing.T) {
	store := storage.NewMemoryEngine()
	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer store.Close()

	service := NewCollectionsService(registry)
	ctx := context.Background()

	// Create a collection
	err = registry.CreateCollection(ctx, "test_col", 128, pb.Distance_COSINE)
	require.NoError(t, err)

	t.Run("update existing collection", func(t *testing.T) {
		resp, err := service.UpdateCollection(ctx, &pb.UpdateCollectionRequest{
			CollectionName: "test_col",
		})
		require.NoError(t, err)
		assert.True(t, resp.Result)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		_, err := service.UpdateCollection(ctx, &pb.UpdateCollectionRequest{
			CollectionName: "non_existent",
		})
		assert.Error(t, err)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.UpdateCollection(ctx, &pb.UpdateCollectionRequest{})
		assert.Error(t, err)
	})
}

func TestCollectionsService_CollectionExists(t *testing.T) {
	store := storage.NewMemoryEngine()
	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer store.Close()

	service := NewCollectionsService(registry)
	ctx := context.Background()

	// Create a collection
	err = registry.CreateCollection(ctx, "existing_col", 128, pb.Distance_COSINE)
	require.NoError(t, err)

	t.Run("check existing collection", func(t *testing.T) {
		resp, err := service.CollectionExists(ctx, &pb.CollectionExistsRequest{
			CollectionName: "existing_col",
		})
		require.NoError(t, err)
		assert.True(t, resp.Result)
	})

	t.Run("check non-existent collection", func(t *testing.T) {
		resp, err := service.CollectionExists(ctx, &pb.CollectionExistsRequest{
			CollectionName: "non_existent",
		})
		require.NoError(t, err)
		assert.False(t, resp.Result)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		_, err := service.CollectionExists(ctx, &pb.CollectionExistsRequest{})
		assert.Error(t, err)
	})
}

