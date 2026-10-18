package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestOfficialQdrantGRPC_BasicFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false

	srv, err := NewServerWithDatabaseManager(cfg, dbm, base, sharedSearchProvider(), nil)
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
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false

	srv, err := NewServerWithDatabaseManager(cfg, dbm, base, sharedSearchProvider(), nil)
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

func TestOfficialQdrantGRPC_AdvancedFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)

	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.EnableReflection = false
	cfg.AllowVectorMutations = true

	srv, err := NewServerWithDatabaseManager(cfg, dbm, base, sharedSearchProvider(), nil)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	collections := qpb.NewCollectionsClient(conn)
	points := qpb.NewPointsClient(conn)
	snapshots := qpb.NewSnapshotsClient(conn)

	_, err = collections.Create(ctx, &qpb.CreateCollection{
		CollectionName: "adv",
		VectorsConfig: &qpb.VectorsConfig{
			Config: &qpb.VectorsConfig_Params{
				Params: &qpb.VectorParams{Size: 4, Distance: qpb.Distance_Cosine},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "adv",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"group": {Kind: &qpb.Value_StringValue{StringValue: "g1"}},
					"score": {Kind: &qpb.Value_IntegerValue{IntegerValue: 10}},
				},
			},
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a2"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0.9, 0.1, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"group": {Kind: &qpb.Value_StringValue{StringValue: "g1"}},
					"score": {Kind: &qpb.Value_IntegerValue{IntegerValue: 20}},
				},
			},
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a3"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 1, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"group": {Kind: &qpb.Value_StringValue{StringValue: "g2"}},
					"score": {Kind: &qpb.Value_IntegerValue{IntegerValue: 30}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.SetPayload(ctx, &qpb.SetPayloadPoints{
		CollectionName: "adv",
		Payload: map[string]*qpb.Value{
			"newf": {Kind: &qpb.Value_StringValue{StringValue: "v"}},
		},
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{
					Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a1"}}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.OverwritePayload(ctx, &qpb.SetPayloadPoints{
		CollectionName: "adv",
		Payload: map[string]*qpb.Value{
			"only": {Kind: &qpb.Value_BoolValue{BoolValue: true}},
		},
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{
					Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a2"}}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.DeletePayload(ctx, &qpb.DeletePayloadPoints{
		CollectionName: "adv",
		Keys:           []string{"newf"},
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{
					Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a1"}}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.ClearPayload(ctx, &qpb.ClearPayloadPoints{
		CollectionName: "adv",
		Points: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{
					Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a3"}}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = points.UpdateVectors(ctx, &qpb.UpdatePointVectors{
		CollectionName: "adv",
		Points: []*qpb.PointVectors{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a1"}},
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

	_, err = points.DeleteVectors(ctx, &qpb.DeletePointVectors{
		CollectionName: "adv",
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{
					Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a1"}}},
				},
			},
		},
		Vectors: &qpb.VectorsSelector{Names: []string{"b"}},
	})
	require.NoError(t, err)

	sb, err := points.SearchBatch(ctx, &qpb.SearchBatchPoints{
		CollectionName: "adv",
		SearchPoints: []*qpb.SearchPoints{
			{Vector: []float32{0, 1, 0, 0}, Limit: 1},
			{Vector: []float32{1, 0, 0, 0}, Limit: 2},
		},
	})
	require.NoError(t, err)
	require.Len(t, sb.Result, 2)

	rr, err := points.Recommend(ctx, &qpb.RecommendPoints{
		CollectionName: "adv",
		Positive:       []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a1"}}},
		Limit:          2,
		Using:          ptrString("a"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, rr.Result)

	rb, err := points.RecommendBatch(ctx, &qpb.RecommendBatchPoints{
		CollectionName: "adv",
		RecommendPoints: []*qpb.RecommendPoints{
			{Positive: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a1"}}}, Limit: 1, Using: ptrString("a")},
			{Positive: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a2"}}}, Limit: 1},
		},
	})
	require.NoError(t, err)
	require.Len(t, rb.Result, 2)

	qr, err := points.Query(ctx, &qpb.QueryPoints{
		CollectionName: "adv",
		Query: &qpb.Query{
			Variant: &qpb.Query_Nearest{
				Nearest: &qpb.VectorInput{
					Variant: &qpb.VectorInput_Dense{
						Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}},
					},
				},
			},
		},
		Limit: ptrU64(2),
	})
	require.NoError(t, err)
	require.NotEmpty(t, qr.Result)

	qb, err := points.QueryBatch(ctx, &qpb.QueryBatchPoints{
		CollectionName: "adv",
		QueryPoints: []*qpb.QueryPoints{
			{
				CollectionName: "adv",
				Query: &qpb.Query{
					Variant: &qpb.Query_Nearest{
						Nearest: &qpb.VectorInput{
							Variant: &qpb.VectorInput_Dense{
								Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}},
							},
						},
					},
				},
				Limit: ptrU64(1),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, qb.Result, 1)

	sg, err := points.SearchGroups(ctx, &qpb.SearchPointGroups{
		CollectionName: "adv",
		Vector:         []float32{1, 0, 0, 0},
		GroupBy:        "group",
		Limit:          2,
		GroupSize:      2,
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.NotNil(t, sg.Result)

	_, err = points.CreateFieldIndex(ctx, &qpb.CreateFieldIndexCollection{
		CollectionName: "adv",
		FieldName:      "group",
	})
	require.NoError(t, err)
	_, err = points.DeleteFieldIndex(ctx, &qpb.DeleteFieldIndexCollection{
		CollectionName: "adv",
		FieldName:      "group",
	})
	require.NoError(t, err)

	_, err = collections.Update(ctx, &qpb.UpdateCollection{CollectionName: "adv"})
	require.NoError(t, err)
	ce, err := collections.CollectionExists(ctx, &qpb.CollectionExistsRequest{CollectionName: "adv"})
	require.NoError(t, err)
	require.True(t, ce.Result.Exists)

	cs, err := snapshots.Create(ctx, &qpb.CreateSnapshotRequest{CollectionName: "adv"})
	require.NoError(t, err)
	require.NotNil(t, cs.SnapshotDescription)
	ls, err := snapshots.List(ctx, &qpb.ListSnapshotsRequest{CollectionName: "adv"})
	require.NoError(t, err)
	require.NotEmpty(t, ls.SnapshotDescriptions)
	_, err = snapshots.Delete(ctx, &qpb.DeleteSnapshotRequest{
		CollectionName: "adv",
		SnapshotName:   cs.SnapshotDescription.Name,
	})
	require.NoError(t, err)

	cf, err := snapshots.CreateFull(ctx, &qpb.CreateFullSnapshotRequest{})
	require.NoError(t, err)
	require.NotNil(t, cf.SnapshotDescription)
	lf, err := snapshots.ListFull(ctx, &qpb.ListFullSnapshotsRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, lf.SnapshotDescriptions)
	_, err = snapshots.DeleteFull(ctx, &qpb.DeleteFullSnapshotRequest{SnapshotName: cf.SnapshotDescription.Name})
	require.NoError(t, err)

	_, err = collections.Delete(ctx, &qpb.DeleteCollection{CollectionName: "adv"})
	require.NoError(t, err)
}

func ptrBool(v bool) *bool    { return &v }
func ptrU32(v uint32) *uint32 { return &v }
