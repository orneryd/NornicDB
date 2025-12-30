package qdrantgrpc

import (
	"context"
	"testing"

	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestOfficialQdrantGRPC_BasicFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := storage.NewMemoryEngine()
	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	t.Cleanup(func() { _ = registry.Close() })

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false

	srv, err := NewServer(cfg, store, registry, nil, nil)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	collections := qpb.NewCollectionsClient(conn)
	points := qpb.NewPointsClient(conn)

	_, err = collections.Create(ctx, &qpb.CreateCollection{
		CollectionName: "test",
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{
					Size:     4,
					Distance: qpb.Distance_Cosine,
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{
							Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}},
						},
					},
				},
				Payload: map[string]*qpb.Value{
					"tag":   {Kind: &qpb.Value_StringValue{StringValue: "first"}},
					"score": {Kind: &qpb.Value_IntegerValue{IntegerValue: 10}},
				},
			},
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p2"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{
							Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 1, 0, 0}}},
						},
					},
				},
				Payload: map[string]*qpb.Value{
					"tag":   {Kind: &qpb.Value_StringValue{StringValue: "second"}},
					"score": {Kind: &qpb.Value_IntegerValue{IntegerValue: 20}},
				},
			},
		},
	})
	require.NoError(t, err)

	getResp, err := points.Get(ctx, &qpb.GetPoints{
		CollectionName: "test",
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}}},
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
		WithVectors:    &qpb.WithVectorsSelector{SelectorOptions: &qpb.WithVectorsSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.Len(t, getResp.Result, 1)
	require.NotNil(t, getResp.Result[0].Vectors)

	countResp, err := points.Count(ctx, &qpb.CountPoints{
		CollectionName: "test",
		Exact:          ptrBool(true),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), countResp.Result.Count)

	// Filtered count (tag == "first")
	countResp, err = points.Count(ctx, &qpb.CountPoints{
		CollectionName: "test",
		Exact:          ptrBool(true),
		Filter: &qpb.Filter{
			Must: []*qpb.Condition{
				{
					ConditionOneOf: &qpb.Condition_Field{
						Field: &qpb.FieldCondition{
							Key: "tag",
							Match: &qpb.Match{
								MatchValue: &qpb.Match_Keyword{Keyword: "first"},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), countResp.Result.Count)

	searchResp, err := points.Search(ctx, &qpb.SearchPoints{
		CollectionName: "test",
		Vector:         []float32{1, 0, 0, 0},
		Limit:          3,
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, searchResp.Result)

	scrollResp, err := points.Scroll(ctx, &qpb.ScrollPoints{
		CollectionName: "test",
		Limit:          ptrU32(1),
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
		WithVectors:    &qpb.WithVectorsSelector{SelectorOptions: &qpb.WithVectorsSelector_Enable{Enable: false}},
	})
	require.NoError(t, err)
	require.Len(t, scrollResp.Result, 1)

	_, err = points.Delete(ctx, &qpb.DeletePoints{
		CollectionName: "test",
		Points: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Filter{
				Filter: &qpb.Filter{
					Must: []*qpb.Condition{
						{
							ConditionOneOf: &qpb.Condition_Field{
								Field: &qpb.FieldCondition{
									Key: "tag",
									Match: &qpb.Match{
										MatchValue: &qpb.Match_Keyword{Keyword: "second"},
									},
								},
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	countResp, err = points.Count(ctx, &qpb.CountPoints{
		CollectionName: "test",
		Exact:          ptrBool(true),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), countResp.Result.Count)
}

func TestOfficialQdrantGRPC_NamedVectorsRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := storage.NewMemoryEngine()
	registry, err := NewPersistentCollectionRegistry(store)
	require.NoError(t, err)
	t.Cleanup(func() { _ = registry.Close() })

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false

	srv, err := NewServer(cfg, store, registry, nil, nil)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	collections := qpb.NewCollectionsClient(conn)
	points := qpb.NewPointsClient(conn)

	_, err = collections.Create(ctx, &qpb.CreateCollection{
		CollectionName: "mv",
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{Size: 4, Distance: qpb.Distance_Cosine},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "mv",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p"}},
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
				Payload: map[string]*qpb.Value{"tag": {Kind: &qpb.Value_StringValue{StringValue: "mv"}}},
			},
		},
	})
	require.NoError(t, err)

	// Search using the named vector "b".
	searchResp, err := points.Search(ctx, &qpb.SearchPoints{
		CollectionName: "mv",
		Vector:         []float32{0, 1, 0, 0},
		VectorName:     ptrString("b"),
		Limit:          1,
	})
	require.NoError(t, err)
	require.Len(t, searchResp.Result, 1)
	require.Equal(t, "p", searchResp.Result[0].GetId().GetUuid())

	// Get with vectors enabled should include both names.
	getResp, err := points.Get(ctx, &qpb.GetPoints{
		CollectionName: "mv",
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p"}}},
		WithVectors:    &qpb.WithVectorsSelector{SelectorOptions: &qpb.WithVectorsSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.Len(t, getResp.Result, 1)
	require.NotNil(t, getResp.Result[0].Vectors)
}

func ptrBool(v bool) *bool    { return &v }
func ptrU32(v uint32) *uint32 { return &v }
