package qdrantgrpc

import (
	"context"
	"testing"

	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupExtendedPointsService(t *testing.T) (*PointsService, storage.Engine) {
	t.Helper()

	ctx := context.Background()
	store := storage.NewMemoryEngine()
	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	t.Cleanup(func() { _ = registry.Close() })

	cfg := DefaultConfig()
	cfg.AllowVectorMutations = true
	cfg.EmbedQuery = func(ctx context.Context, text string) ([]float32, error) {
		_ = ctx
		_ = text
		return []float32{0.5, 0.5, 0, 0}, nil
	}

	require.NoError(t, registry.CreateCollection(ctx, "test_collection", 4, qpb.Distance_Cosine))

	svc := NewPointsService(cfg, store, registry, nil, newVectorIndexCache())

	_, err = svc.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_collection",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"category": {Kind: &qpb.Value_StringValue{StringValue: "A"}},
				},
			},
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point2"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0.9, 0.1, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"category": {Kind: &qpb.Value_StringValue{StringValue: "A"}},
				},
			},
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point3"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 1, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"category": {Kind: &qpb.Value_StringValue{StringValue: "B"}},
				},
			},
		},
	})
	require.NoError(t, err)

	return svc, store
}

func TestPointsService_PayloadOps(t *testing.T) {
	ctx := context.Background()
	svc, _ := setupExtendedPointsService(t)

	_, err := svc.SetPayload(ctx, &qpb.SetPayloadPoints{
		CollectionName: "test_collection",
		Payload:        map[string]*qpb.Value{"new_field": {Kind: &qpb.Value_StringValue{StringValue: "new_value"}}},
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}}}},
			},
		},
	})
	require.NoError(t, err)

	getResp, err := svc.Get(ctx, &qpb.GetPoints{
		CollectionName: "test_collection",
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}}},
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.Equal(t, "new_value", getResp.Result[0].Payload["new_field"].GetStringValue())
	require.Equal(t, "A", getResp.Result[0].Payload["category"].GetStringValue())

	_, err = svc.DeletePayload(ctx, &qpb.DeletePayloadPoints{
		CollectionName: "test_collection",
		Keys:           []string{"new_field"},
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}}}},
			},
		},
	})
	require.NoError(t, err)

	getResp, err = svc.Get(ctx, &qpb.GetPoints{
		CollectionName: "test_collection",
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}}},
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.NotContains(t, getResp.Result[0].Payload, "new_field")

	_, err = svc.ClearPayload(ctx, &qpb.ClearPayloadPoints{
		CollectionName: "test_collection",
		Points: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}}}},
			},
		},
	})
	require.NoError(t, err)
}

func TestPointsService_VectorOps_NamedVectors(t *testing.T) {
	ctx := context.Background()
	svc, _ := setupExtendedPointsService(t)

	_, err := svc.UpdateVectors(ctx, &qpb.UpdatePointVectors{
		CollectionName: "test_collection",
		Points: []*qpb.PointVectors{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vectors{
						Vectors: &qpb.NamedVectors{
							Vectors: map[string]*qpb.Vector{
								"a": {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
								"b": {Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 1, 0, 0}}}},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	searchResp, err := svc.Search(ctx, &qpb.SearchPoints{
		CollectionName: "test_collection",
		Vector:         []float32{0, 1, 0, 0},
		VectorName:     ptrString("b"),
		Limit:          1,
	})
	require.NoError(t, err)
	require.Len(t, searchResp.Result, 1)
	require.Equal(t, "point1", searchResp.Result[0].GetId().GetUuid())

	_, err = svc.DeleteVectors(ctx, &qpb.DeletePointVectors{
		CollectionName: "test_collection",
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}}}},
			},
		},
		Vectors: &qpb.VectorsSelector{Names: []string{"b"}},
	})
	require.NoError(t, err)
}

func TestPointsService_RecommendAndGroupsAndFieldIndex(t *testing.T) {
	ctx := context.Background()
	svc, store := setupExtendedPointsService(t)

	// Recommend
	recResp, err := svc.Recommend(ctx, &qpb.RecommendPoints{
		CollectionName: "test_collection",
		Positive:       []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}}},
		Limit:          2,
	})
	require.NoError(t, err)
	require.NotEmpty(t, recResp.Result)

	// SearchGroups
	groupsResp, err := svc.SearchGroups(ctx, &qpb.SearchPointGroups{
		CollectionName: "test_collection",
		Vector:         []float32{0.5, 0.5, 0, 0},
		Limit:          2,
		GroupBy:        "category",
		GroupSize:      2,
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.NotNil(t, groupsResp.Result)
	require.NotEmpty(t, groupsResp.Result.Groups)

	// Field index ops
	_, err = svc.CreateFieldIndex(ctx, &qpb.CreateFieldIndexCollection{
		CollectionName: "test_collection",
		FieldName:      "category",
	})
	require.NoError(t, err)
	require.NotNil(t, store.GetSchema())

	_, err = svc.DeleteFieldIndex(ctx, &qpb.DeleteFieldIndexCollection{
		CollectionName: "test_collection",
		FieldName:      "category",
	})
	require.NoError(t, err)
}
