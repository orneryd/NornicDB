package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/orneryd/nornicdb/pkg/qdrantgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupPointsTest(t *testing.T) (*PointsService, *MemoryCollectionRegistry, storage.Engine) {
	t.Helper()

	registry := NewMemoryCollectionRegistry()
	store := storage.NewMemoryEngine()
	config := DefaultConfig()
	service := NewPointsService(config, store, registry, nil) // nil searchService for testing

	// Create a test collection
	ctx := context.Background()
	err := registry.CreateCollection(ctx, "test_vectors", 4, pb.Distance_COSINE)
	require.NoError(t, err)

	return service, registry, store
}

func TestPointsService_Upsert(t *testing.T) {
	ctx := context.Background()
	service, registry, store := setupPointsTest(t)

	t.Run("upsert single point", func(t *testing.T) {
		req := &pb.UpsertPointsRequest{
			CollectionName: "test_vectors",
			Points: []*pb.PointStruct{
				{
					Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "point-1"}},
					Vectors: &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{Data: []float32{0.1, 0.2, 0.3, 0.4}},
						},
					},
					Payload: map[string]*pb.Value{
						"name":  {Kind: &pb.Value_StringValue{StringValue: "test"}},
						"count": {Kind: &pb.Value_IntegerValue{IntegerValue: 42}},
					},
				},
			},
		}

		resp, err := service.Upsert(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)

		// Verify point was stored
		nodeID := storage.NodeID("qdrant:test_vectors:point-1")
		node, err := store.GetNode(nodeID)
		require.NoError(t, err)
		assert.NotNil(t, node)
		assert.Equal(t, []string{QdrantPointLabel, "test_vectors"}, node.Labels)
	})

	t.Run("upsert multiple points", func(t *testing.T) {
		// Create a new collection for this test
		_ = registry.CreateCollection(ctx, "multi_vectors", 4, pb.Distance_COSINE)

		req := &pb.UpsertPointsRequest{
			CollectionName: "multi_vectors",
			Points: []*pb.PointStruct{
				{
					Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}},
					Vectors: &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{Data: []float32{1.0, 0.0, 0.0, 0.0}},
						},
					},
				},
				{
					Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "p2"}},
					Vectors: &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{Data: []float32{0.0, 1.0, 0.0, 0.0}},
						},
					},
				},
				{
					Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "p3"}},
					Vectors: &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{Data: []float32{0.0, 0.0, 1.0, 0.0}},
						},
					},
				},
			},
		}

		resp, err := service.Upsert(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)

		// Verify all points were stored
		nodes, err := store.GetNodesByLabel("multi_vectors")
		require.NoError(t, err)
		// Count only QdrantPoints
		count := 0
		for _, n := range nodes {
			for _, label := range n.Labels {
				if label == QdrantPointLabel {
					count++
					break
				}
			}
		}
		assert.Equal(t, 3, count)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &pb.UpsertPointsRequest{
			CollectionName: "",
			Points: []*pb.PointStruct{
				{
					Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "x"}},
					Vectors: &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{Data: []float32{1, 2, 3, 4}},
						},
					},
				},
			},
		}

		_, err := service.Upsert(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty points", func(t *testing.T) {
		req := &pb.UpsertPointsRequest{
			CollectionName: "test_vectors",
			Points:         []*pb.PointStruct{},
		}

		_, err := service.Upsert(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		req := &pb.UpsertPointsRequest{
			CollectionName: "not_found",
			Points: []*pb.PointStruct{
				{
					Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "x"}},
					Vectors: &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{Data: []float32{1, 2, 3, 4}},
						},
					},
				},
			},
		}

		_, err := service.Upsert(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on dimension mismatch", func(t *testing.T) {
		req := &pb.UpsertPointsRequest{
			CollectionName: "test_vectors", // Expects 4 dimensions
			Points: []*pb.PointStruct{
				{
					Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "bad_dim"}},
					Vectors: &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{Data: []float32{1, 2}}, // Only 2 dims
						},
					},
				},
			},
		}

		_, err := service.Upsert(ctx, req)
		require.Error(t, err)
	})

	t.Run("upsert with numeric point ID", func(t *testing.T) {
		_ = registry.CreateCollection(ctx, "numeric_ids", 4, pb.Distance_COSINE)

		req := &pb.UpsertPointsRequest{
			CollectionName: "numeric_ids",
			Points: []*pb.PointStruct{
				{
					Id: &pb.PointId{PointIdOptions: &pb.PointId_Num{Num: 12345}},
					Vectors: &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{Data: []float32{1, 0, 0, 0}},
						},
					},
				},
			},
		}

		resp, err := service.Upsert(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)
	})
}

func TestPointsService_Get(t *testing.T) {
	ctx := context.Background()
	service, registry, _ := setupPointsTest(t)

	// Insert some test data first
	_ = registry.CreateCollection(ctx, "get_test", 4, pb.Distance_COSINE)

	upsertReq := &pb.UpsertPointsRequest{
		CollectionName: "get_test",
		Points: []*pb.PointStruct{
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "get-1"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{Data: []float32{1, 2, 3, 4}},
					},
				},
				Payload: map[string]*pb.Value{
					"color": {Kind: &pb.Value_StringValue{StringValue: "red"}},
				},
			},
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "get-2"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{Data: []float32{5, 6, 7, 8}},
					},
				},
				Payload: map[string]*pb.Value{
					"color": {Kind: &pb.Value_StringValue{StringValue: "blue"}},
				},
			},
		},
	}
	_, err := service.Upsert(ctx, upsertReq)
	require.NoError(t, err)

	t.Run("get existing points with payload", func(t *testing.T) {
		req := &pb.GetPointsRequest{
			CollectionName: "get_test",
			Ids: []*pb.PointId{
				{PointIdOptions: &pb.PointId_Uuid{Uuid: "get-1"}},
				{PointIdOptions: &pb.PointId_Uuid{Uuid: "get-2"}},
			},
			WithPayload: &pb.WithPayloadSelector{
				SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true},
			},
		}

		resp, err := service.Get(ctx, req)
		require.NoError(t, err)
		assert.Len(t, resp.Result, 2)

		// Verify payload is included
		for _, point := range resp.Result {
			require.NotNil(t, point.Payload)
			assert.Contains(t, point.Payload, "color")
		}
	})

	t.Run("get points with vectors", func(t *testing.T) {
		req := &pb.GetPointsRequest{
			CollectionName: "get_test",
			Ids: []*pb.PointId{
				{PointIdOptions: &pb.PointId_Uuid{Uuid: "get-1"}},
			},
			WithVectors: &pb.WithVectorsSelector{
				SelectorOptions: &pb.WithVectorsSelector_Enable{Enable: true},
			},
		}

		resp, err := service.Get(ctx, req)
		require.NoError(t, err)
		assert.Len(t, resp.Result, 1)

		// Verify vector is included
		require.NotNil(t, resp.Result[0].Vectors)
		vec := resp.Result[0].Vectors.GetVector()
		require.NotNil(t, vec)
		assert.Len(t, vec.Data, 4)
	})

	t.Run("get non-existent point returns empty", func(t *testing.T) {
		req := &pb.GetPointsRequest{
			CollectionName: "get_test",
			Ids: []*pb.PointId{
				{PointIdOptions: &pb.PointId_Uuid{Uuid: "not-found"}},
			},
		}

		resp, err := service.Get(ctx, req)
		require.NoError(t, err)
		assert.Empty(t, resp.Result)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &pb.GetPointsRequest{
			CollectionName: "",
			Ids:            []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "x"}}},
		}

		_, err := service.Get(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty ids", func(t *testing.T) {
		req := &pb.GetPointsRequest{
			CollectionName: "get_test",
			Ids:            []*pb.PointId{},
		}

		_, err := service.Get(ctx, req)
		require.Error(t, err)
	})
}

func TestPointsService_Delete(t *testing.T) {
	ctx := context.Background()
	service, registry, store := setupPointsTest(t)

	// Create collection and add points
	_ = registry.CreateCollection(ctx, "delete_test", 4, pb.Distance_COSINE)

	upsertReq := &pb.UpsertPointsRequest{
		CollectionName: "delete_test",
		Points: []*pb.PointStruct{
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "del-1"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{Data: []float32{1, 0, 0, 0}},
					},
				},
			},
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "del-2"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{Data: []float32{0, 1, 0, 0}},
					},
				},
			},
		},
	}
	_, err := service.Upsert(ctx, upsertReq)
	require.NoError(t, err)

	t.Run("delete by IDs", func(t *testing.T) {
		req := &pb.DeletePointsRequest{
			CollectionName: "delete_test",
			Points: &pb.PointsSelector{
				PointsSelectorOneOf: &pb.PointsSelector_Points{
					Points: &pb.PointsIdsList{
						Ids: []*pb.PointId{
							{PointIdOptions: &pb.PointId_Uuid{Uuid: "del-1"}},
						},
					},
				},
			},
		}

		resp, err := service.Delete(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, pb.UpdateStatus_COMPLETED, resp.Result.Status)

		// Verify only one point remains in storage
		nodes, err := store.GetNodesByLabel("delete_test")
		require.NoError(t, err)
		count := 0
		for _, n := range nodes {
			for _, label := range n.Labels {
				if label == QdrantPointLabel {
					count++
					break
				}
			}
		}
		assert.Equal(t, 1, count)
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &pb.DeletePointsRequest{
			CollectionName: "",
			Points: &pb.PointsSelector{
				PointsSelectorOneOf: &pb.PointsSelector_Points{
					Points: &pb.PointsIdsList{
						Ids: []*pb.PointId{{PointIdOptions: &pb.PointId_Uuid{Uuid: "x"}}},
					},
				},
			},
		}

		_, err := service.Delete(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on nil points selector", func(t *testing.T) {
		req := &pb.DeletePointsRequest{
			CollectionName: "delete_test",
			Points:         nil,
		}

		_, err := service.Delete(ctx, req)
		require.Error(t, err)
	})
}

func TestPointsService_Search(t *testing.T) {
	ctx := context.Background()
	service, registry, _ := setupPointsTest(t)

	// Create collection and add test vectors
	_ = registry.CreateCollection(ctx, "search_test", 4, pb.Distance_COSINE)

	upsertReq := &pb.UpsertPointsRequest{
		CollectionName: "search_test",
		Points: []*pb.PointStruct{
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "s1"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{Data: []float32{1, 0, 0, 0}},
					},
				},
				Payload: map[string]*pb.Value{
					"category": {Kind: &pb.Value_StringValue{StringValue: "A"}},
				},
			},
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "s2"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{Data: []float32{0.9, 0.1, 0, 0}},
					},
				},
				Payload: map[string]*pb.Value{
					"category": {Kind: &pb.Value_StringValue{StringValue: "A"}},
				},
			},
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "s3"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{Data: []float32{0, 0, 1, 0}},
					},
				},
				Payload: map[string]*pb.Value{
					"category": {Kind: &pb.Value_StringValue{StringValue: "B"}},
				},
			},
		},
	}
	_, err := service.Upsert(ctx, upsertReq)
	require.NoError(t, err)

	t.Run("basic search", func(t *testing.T) {
		req := &pb.SearchPointsRequest{
			CollectionName: "search_test",
			Vector:         []float32{1, 0, 0, 0},
			Limit:          10,
		}

		resp, err := service.Search(ctx, req)
		require.NoError(t, err)
		assert.NotEmpty(t, resp.Result)

		// First result should be most similar
		assert.Greater(t, resp.Result[0].Score, float32(0))
	})

	t.Run("search with payload", func(t *testing.T) {
		req := &pb.SearchPointsRequest{
			CollectionName: "search_test",
			Vector:         []float32{1, 0, 0, 0},
			Limit:          2,
			WithPayload: &pb.WithPayloadSelector{
				SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true},
			},
		}

		resp, err := service.Search(ctx, req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Result)

		// Verify payload is returned
		for _, point := range resp.Result {
			require.NotNil(t, point.Payload)
			assert.Contains(t, point.Payload, "category")
		}
	})

	t.Run("search with vectors", func(t *testing.T) {
		req := &pb.SearchPointsRequest{
			CollectionName: "search_test",
			Vector:         []float32{1, 0, 0, 0},
			Limit:          1,
			WithVectors: &pb.WithVectorsSelector{
				SelectorOptions: &pb.WithVectorsSelector_Enable{Enable: true},
			},
		}

		resp, err := service.Search(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.Result, 1)

		// Verify vector is returned
		require.NotNil(t, resp.Result[0].Vectors)
	})

	t.Run("search with score threshold", func(t *testing.T) {
		threshold := float32(0.5)
		req := &pb.SearchPointsRequest{
			CollectionName: "search_test",
			Vector:         []float32{1, 0, 0, 0},
			Limit:          10,
			ScoreThreshold: &threshold,
		}

		resp, err := service.Search(ctx, req)
		require.NoError(t, err)

		// All results should be above threshold
		for _, point := range resp.Result {
			assert.GreaterOrEqual(t, point.Score, threshold)
		}
	})

	t.Run("error on empty collection name", func(t *testing.T) {
		req := &pb.SearchPointsRequest{
			CollectionName: "",
			Vector:         []float32{1, 0, 0, 0},
			Limit:          10,
		}

		_, err := service.Search(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on empty vector", func(t *testing.T) {
		req := &pb.SearchPointsRequest{
			CollectionName: "search_test",
			Vector:         []float32{},
			Limit:          10,
		}

		_, err := service.Search(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on dimension mismatch", func(t *testing.T) {
		req := &pb.SearchPointsRequest{
			CollectionName: "search_test",
			Vector:         []float32{1, 0}, // Wrong dimensions
			Limit:          10,
		}

		_, err := service.Search(ctx, req)
		require.Error(t, err)
	})

	t.Run("error on limit too large", func(t *testing.T) {
		req := &pb.SearchPointsRequest{
			CollectionName: "search_test",
			Vector:         []float32{1, 0, 0, 0},
			Limit:          10000, // Exceeds MaxTopK
		}

		_, err := service.Search(ctx, req)
		require.Error(t, err)
	})
}

func TestPointsService_SearchBatch(t *testing.T) {
	ctx := context.Background()
	service, registry, _ := setupPointsTest(t)

	// Create collection and add test vectors
	_ = registry.CreateCollection(ctx, "batch_search", 4, pb.Distance_COSINE)

	upsertReq := &pb.UpsertPointsRequest{
		CollectionName: "batch_search",
		Points: []*pb.PointStruct{
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "b1"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{Data: []float32{1, 0, 0, 0}},
					},
				},
			},
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "b2"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vector{
						Vector: &pb.Vector{Data: []float32{0, 1, 0, 0}},
					},
				},
			},
		},
	}
	_, err := service.Upsert(ctx, upsertReq)
	require.NoError(t, err)

	t.Run("batch search multiple queries", func(t *testing.T) {
		req := &pb.SearchBatchPointsRequest{
			CollectionName: "batch_search",
			SearchRequests: []*pb.SearchPointsRequest{
				{Vector: []float32{1, 0, 0, 0}, Limit: 1},
				{Vector: []float32{0, 1, 0, 0}, Limit: 1},
			},
		}

		resp, err := service.SearchBatch(ctx, req)
		require.NoError(t, err)
		assert.Len(t, resp.Result, 2)
	})
}

func TestPointsService_NamedVectors(t *testing.T) {
	ctx := context.Background()
	service, registry, store := setupPointsTest(t)

	err := registry.CreateCollection(ctx, "named_vectors", 4, pb.Distance_COSINE)
	require.NoError(t, err)

	_, err = service.Upsert(ctx, &pb.UpsertPointsRequest{
		CollectionName: "named_vectors",
		Points: []*pb.PointStruct{
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vectors{
						Vectors: &pb.NamedVectors{
							Vectors: map[string]*pb.Vector{
								"a": {Data: []float32{1, 0, 0, 0}},
								"b": {Data: []float32{0, 1, 0, 0}},
							},
						},
					},
				},
				Payload: map[string]*pb.Value{
					"tag": {Kind: &pb.Value_StringValue{StringValue: "first"}},
				},
			},
			{
				Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "p2"}},
				Vectors: &pb.Vectors{
					VectorsOptions: &pb.Vectors_Vectors{
						Vectors: &pb.NamedVectors{
							Vectors: map[string]*pb.Vector{
								"a": {Data: []float32{0, 1, 0, 0}},
								"b": {Data: []float32{1, 0, 0, 0}},
							},
						},
					},
				},
				Payload: map[string]*pb.Value{
					"tag": {Kind: &pb.Value_StringValue{StringValue: "second"}},
				},
			},
		},
	})
	require.NoError(t, err)

	t.Run("stored as chunk embeddings with name mapping", func(t *testing.T) {
		node, err := store.GetNode(storage.NodeID("qdrant:named_vectors:p1"))
		require.NoError(t, err)
		require.Len(t, node.ChunkEmbeddings, 2)
		require.NotNil(t, node.Properties)
		_, ok := node.Properties[qdrantVectorNamesKey]
		require.True(t, ok)
	})

	t.Run("search respects vector_name", func(t *testing.T) {
		vnA := "a"
		resp, err := service.Search(ctx, &pb.SearchPointsRequest{
			CollectionName: "named_vectors",
			Vector:         []float32{1, 0, 0, 0},
			Limit:          1,
			VectorName:     &vnA,
		})
		require.NoError(t, err)
		require.Len(t, resp.Result, 1)
		require.Equal(t, "p1", resp.Result[0].GetId().GetUuid())

		vnB := "b"
		resp, err = service.Search(ctx, &pb.SearchPointsRequest{
			CollectionName: "named_vectors",
			Vector:         []float32{1, 0, 0, 0},
			Limit:          1,
			VectorName:     &vnB,
		})
		require.NoError(t, err)
		require.Len(t, resp.Result, 1)
		require.Equal(t, "p2", resp.Result[0].GetId().GetUuid())
	})

	t.Run("get can include a subset of named vectors", func(t *testing.T) {
		resp, err := service.Get(ctx, &pb.GetPointsRequest{
			CollectionName: "named_vectors",
			Ids: []*pb.PointId{
				{PointIdOptions: &pb.PointId_Uuid{Uuid: "p1"}},
			},
			WithVectors: &pb.WithVectorsSelector{
				SelectorOptions: &pb.WithVectorsSelector_Include{
					Include: &pb.VectorsSelector{Names: []string{"b"}},
				},
			},
			WithPayload: &pb.WithPayloadSelector{SelectorOptions: &pb.WithPayloadSelector_Enable{Enable: true}},
		})
		require.NoError(t, err)
		require.Len(t, resp.Result, 1)

		point := resp.Result[0]
		require.NotNil(t, point.Vectors)
		nv := point.Vectors.GetVectors()
		require.NotNil(t, nv)
		require.Len(t, nv.Vectors, 1)
		require.NotNil(t, nv.Vectors["b"])
	})
}

func TestPointsService_Count(t *testing.T) {
	ctx := context.Background()

	// Use PersistentCollectionRegistry for count tests since it counts from storage
	store := storage.NewMemoryEngine()
	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	defer registry.Close()

	config := DefaultConfig()
	service := NewPointsService(config, store, registry, nil)

	// Create collection
	err = registry.CreateCollection(ctx, "count_test", 4, pb.Distance_COSINE)
	require.NoError(t, err)

	t.Run("count empty collection", func(t *testing.T) {
		req := &pb.CountPointsRequest{
			CollectionName: "count_test",
		}

		resp, err := service.Count(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), resp.Result.Count)
	})

	t.Run("count after inserts", func(t *testing.T) {
		// Insert some points
		upsertReq := &pb.UpsertPointsRequest{
			CollectionName: "count_test",
			Points: []*pb.PointStruct{
				{
					Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "c1"}},
					Vectors: &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{Data: []float32{1, 0, 0, 0}},
						},
					},
				},
				{
					Id: &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "c2"}},
					Vectors: &pb.Vectors{
						VectorsOptions: &pb.Vectors_Vector{
							Vector: &pb.Vector{Data: []float32{0, 1, 0, 0}},
						},
					},
				},
			},
		}
		_, err := service.Upsert(ctx, upsertReq)
		require.NoError(t, err)

		req := &pb.CountPointsRequest{
			CollectionName: "count_test",
		}

		resp, err := service.Count(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), resp.Result.Count)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		req := &pb.CountPointsRequest{
			CollectionName: "not_found",
		}

		_, err := service.Count(ctx, req)
		require.Error(t, err)
	})
}

func TestPayloadConversion(t *testing.T) {
	t.Run("convert payload to properties", func(t *testing.T) {
		payload := map[string]*pb.Value{
			"string": {Kind: &pb.Value_StringValue{StringValue: "hello"}},
			"int":    {Kind: &pb.Value_IntegerValue{IntegerValue: 42}},
			"float":  {Kind: &pb.Value_DoubleValue{DoubleValue: 3.14}},
			"bool":   {Kind: &pb.Value_BoolValue{BoolValue: true}},
			"null":   {Kind: &pb.Value_NullValue{NullValue: pb.NullValue_NULL_VALUE}},
		}

		props := payloadToProperties(payload)

		assert.Equal(t, "hello", props["string"])
		assert.Equal(t, int64(42), props["int"])
		assert.Equal(t, 3.14, props["float"])
		assert.Equal(t, true, props["bool"])
		assert.Nil(t, props["null"])
	})

	t.Run("convert properties to payload", func(t *testing.T) {
		props := map[string]any{
			"string": "world",
			"int":    int64(100),
			"float":  2.71,
			"bool":   false,
		}

		payload := propertiesToPayload(props)

		assert.Equal(t, "world", payload["string"].GetStringValue())
		assert.Equal(t, int64(100), payload["int"].GetIntegerValue())
		assert.Equal(t, 2.71, payload["float"].GetDoubleValue())
		assert.Equal(t, false, payload["bool"].GetBoolValue())
	})

	t.Run("convert nested structures", func(t *testing.T) {
		payload := map[string]*pb.Value{
			"list": {Kind: &pb.Value_ListValue{ListValue: &pb.ListValue{
				Values: []*pb.Value{
					{Kind: &pb.Value_IntegerValue{IntegerValue: 1}},
					{Kind: &pb.Value_IntegerValue{IntegerValue: 2}},
				},
			}}},
			"struct": {Kind: &pb.Value_StructValue{StructValue: &pb.Struct{
				Fields: map[string]*pb.Value{
					"nested": {Kind: &pb.Value_StringValue{StringValue: "value"}},
				},
			}}},
		}

		props := payloadToProperties(payload)

		// Check list
		list, ok := props["list"].([]any)
		require.True(t, ok)
		assert.Len(t, list, 2)

		// Check struct
		m, ok := props["struct"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "value", m["nested"])
	})
}

func TestPointIDConversion(t *testing.T) {
	t.Run("uuid point id", func(t *testing.T) {
		id := &pb.PointId{PointIdOptions: &pb.PointId_Uuid{Uuid: "abc-123"}}
		nodeID := pointIDToNodeID("mycol", id)
		assert.Equal(t, storage.NodeID("qdrant:mycol:abc-123"), nodeID)
	})

	t.Run("numeric point id", func(t *testing.T) {
		id := &pb.PointId{PointIdOptions: &pb.PointId_Num{Num: 999}}
		nodeID := pointIDToNodeID("mycol", id)
		assert.Equal(t, storage.NodeID("qdrant:mycol:999"), nodeID)
	})

	t.Run("nil point id", func(t *testing.T) {
		nodeID := pointIDToNodeID("mycol", nil)
		assert.Equal(t, storage.NodeID(""), nodeID)
	})
}
