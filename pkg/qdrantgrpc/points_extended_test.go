package qdrantgrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupExtendedPointsService(t *testing.T) (*PointsService, storage.Engine) {
	t.Helper()

	ctx := context.Background()
	base := storage.NewMemoryEngine()
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	vecIndex := newVectorIndexCache()
	collections, err := NewDatabaseCollectionStore(dbm, vecIndex)
	require.NoError(t, err)

	cfg := DefaultConfig()
	cfg.AllowVectorMutations = true
	cfg.EmbedQuery = func(ctx context.Context, text string) ([]float32, error) {
		_ = ctx
		_ = text
		return []float32{0.5, 0.5, 0, 0}, nil
	}

	require.NoError(t, collections.Create(ctx, "test_collection", 4, qpb.Distance_Cosine))

	svc := NewPointsService(cfg, collections, nil, vecIndex, nil)

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

	return svc, base
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

	// Deleted vector "b" must not be accessible.
	searchResp, err = svc.Search(ctx, &qpb.SearchPoints{
		CollectionName: "test_collection",
		Vector:         []float32{0, 1, 0, 0},
		VectorName:     ptrString("b"),
		Limit:          10,
	})
	require.NoError(t, err)
	require.Len(t, searchResp.Result, 0)

	getResp, err := svc.Get(ctx, &qpb.GetPoints{
		CollectionName: "test_collection",
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}}},
		WithVectors: &qpb.WithVectorsSelector{
			SelectorOptions: &qpb.WithVectorsSelector_Enable{Enable: true},
		},
	})
	require.NoError(t, err)
	require.Len(t, getResp.Result, 1)
	require.NotNil(t, getResp.Result[0].Vectors)
	// Should still have vector "a" but not "b".
	namedOut := getResp.Result[0].GetVectors().GetVectors()
	require.NotNil(t, namedOut)
	require.Contains(t, namedOut.Vectors, "a")
	require.NotContains(t, namedOut.Vectors, "b")
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

	_, err = svc.CreateFieldIndex(ctx, &qpb.CreateFieldIndexCollection{
		CollectionName: "",
		FieldName:      "category",
	})
	require.Error(t, err)
	_, err = svc.CreateFieldIndex(ctx, &qpb.CreateFieldIndexCollection{
		CollectionName: "test_collection",
		FieldName:      "",
	})
	require.Error(t, err)
	_, err = svc.CreateFieldIndex(ctx, &qpb.CreateFieldIndexCollection{
		CollectionName: "missing_collection",
		FieldName:      "x",
	})
	require.Error(t, err)

	_, err = svc.DeleteFieldIndex(ctx, &qpb.DeleteFieldIndexCollection{
		CollectionName: "",
		FieldName:      "category",
	})
	require.Error(t, err)
	_, err = svc.DeleteFieldIndex(ctx, &qpb.DeleteFieldIndexCollection{
		CollectionName: "test_collection",
		FieldName:      "",
	})
	require.Error(t, err)
	_, err = svc.DeleteFieldIndex(ctx, &qpb.DeleteFieldIndexCollection{
		CollectionName: "missing_collection",
		FieldName:      "x",
	})
	require.Error(t, err)
}

func TestVectorIndexCache_BruteIndexOperations(t *testing.T) {
	t.Parallel()

	cache := newVectorIndexCache()
	idx := cache.getOrCreate("c1", "v1", 3, qpb.Distance_Dot)
	require.NotNil(t, idx)
	require.Equal(t, 3, idx.dimensions())
	require.Equal(t, qpb.Distance_Dot, idx.distance())

	// Wrong-dimension upsert is ignored.
	idx.upsert("p-bad", []float32{1, 2})

	idx.upsert("p1", []float32{1, 0, 0})
	idx.upsert("p2", []float32{0, 1, 0})
	results := idx.search(context.Background(), []float32{1, 0, 0}, 5, -1, 0)
	require.NotEmpty(t, results)

	idx.remove("p1")
	results = idx.search(context.Background(), []float32{1, 0, 0}, 5, -1, 0)
	for _, r := range results {
		require.NotEqual(t, "p1", r.ID)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Nil(t, idx.search(cancelCtx, []float32{1, 0, 0}, 5, -1, 0))

	// Cache-level helpers.
	require.NotNil(t, cache.search(context.Background(), "c1", 3, qpb.Distance_Dot, "v1", []float32{1, 0, 0}, 3, -1, 0))
	cache.deletePoint("c1", "p2", []string{"v1"})
	cache.deleteCollection("c1")

	// search validation branches
	require.Nil(t, cache.search(context.Background(), "c1", 3, qpb.Distance_Dot, "v1", []float32{1, 0}, 3, -1, 0))
	require.Nil(t, cache.search(context.Background(), "c1", 3, qpb.Distance_Dot, "v1", []float32{1, 0, 0}, 0, -1, 0))

	// hnsw nil receiver and nil index branches
	var hNil *hnswVectorIndex
	hNil.Clear()
	hNil.remove("x")
	hNil.upsert("x", []float32{1, 0, 0})
	require.Nil(t, hNil.search(context.Background(), []float32{1, 0, 0}, 1, -1, 0))

	h := &hnswVectorIndex{dim: 3, dist: qpb.Distance_Cosine}
	h.remove("x")
	h.upsert("x", []float32{1, 0, 0})
	require.Nil(t, h.search(context.Background(), []float32{1, 0, 0}, 1, -1, 0))

	require.Equal(t, "qdrant:point:x", expandPointID("qdrant:point:x"))
}

func TestVectorScoreHelpers(t *testing.T) {
	t.Parallel()

	a := []float32{1, 0, 0}
	b := []float32{1, 0, 0}

	require.Greater(t, scoreVectorIndex(qpb.Distance_Dot, a, b), 0.9)
	require.Greater(t, scoreVectorIndex(qpb.Distance_Cosine, a, b), 0.9)
	require.Greater(t, scoreVectorIndex(qpb.Distance_Euclid, a, b), 0.9)
	require.Greater(t, scoreVectorIndex(qpb.Distance_UnknownDistance, a, b), 0.9)
}

func TestFilterAndVectorCacheHelperBranches(t *testing.T) {
	t.Parallel()

	node := &storage.Node{
		ID: "qdrant:point:10",
		Properties: map[string]any{
			"s": "x",
			"i": int64(5),
			"f": float64(4.5),
			"b": true,
		},
	}
	require.True(t, matchesCondition(node, nil))
	require.True(t, matchesFilter(node, nil))

	require.False(t, matchesCondition(node, &qpb.Condition{
		ConditionOneOf: &qpb.Condition_Filter{Filter: &qpb.Filter{
			Must: []*qpb.Condition{
				{ConditionOneOf: &qpb.Condition_Field{
					Field: &qpb.FieldCondition{
						Key:   "s",
						Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "nope"}},
					},
				}},
			},
		}},
	}))

	require.False(t, matchesFilter(node, &qpb.Filter{
		MustNot: []*qpb.Condition{
			{ConditionOneOf: &qpb.Condition_Field{
				Field: &qpb.FieldCondition{
					Key:   "s",
					Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "x"}},
				},
			}},
		},
	}))

	require.False(t, matchesFilter(node, &qpb.Filter{
		Should: []*qpb.Condition{
			{ConditionOneOf: &qpb.Condition_Field{
				Field: &qpb.FieldCondition{
					Key:   "s",
					Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "none"}},
				},
			}},
		},
	}))

	require.False(t, pointIDsEqual(&qpb.PointId{}, &qpb.PointId{}))
	require.True(t, pointIDsEqual(
		&qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "u1"}},
		&qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "u1"}},
	))
	require.False(t, matchesFieldCondition(nil, nil))
	require.False(t, matchesFieldCondition(&storage.Node{}, nil))
	require.False(t, matchesFieldCondition(&storage.Node{Properties: map[string]any{}}, &qpb.FieldCondition{Key: "missing"}))

	cache := newVectorIndexCache()
	// Search input validation branches.
	require.Nil(t, cache.search(context.Background(), "c", 3, qpb.Distance_Dot, "", []float32{1, 0}, 3, -1, 0))
	require.Nil(t, cache.search(context.Background(), "c", 3, qpb.Distance_Dot, "", []float32{1, 0, 0}, 0, -1, 0))

	// replacePoint validation branch.
	err := cache.replacePoint("c", 3, qpb.Distance_Dot, "p", []string{"a"}, []string{"a", "b"}, [][]float32{{1, 0, 0}})
	require.Error(t, err)
}

func TestPointVectorHelperBranches(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(42), pointIDFromCompactString("42").GetNum())
	require.Equal(t, "not-a-number", pointIDFromCompactString("not-a-number").GetUuid())

	n := &storage.Node{}
	upsertNodeVectors(nil, []string{""}, [][]float32{{1}})
	upsertNodeVectors(n, nil, nil)
	upsertNodeVectors(n, []string{""}, [][]float32{{1, 0}})
	require.Equal(t, []float32{1, 0}, n.NamedEmbeddings["default"])
	upsertNodeVectors(n, []string{"a"}, [][]float32{{0, 1}})
	require.Equal(t, []float32{0, 1}, n.NamedEmbeddings["a"])

	out := vectorsOutputFromNode(nil, nil)
	require.Nil(t, out)
	out = vectorsOutputFromNode(&storage.Node{}, nil)
	require.Nil(t, out)

	onlyDefault := &storage.Node{NamedEmbeddings: map[string][]float32{"default": {1, 0}}}
	out = vectorsOutputFromNode(onlyDefault, nil)
	require.NotNil(t, out.GetVector())
	require.NotNil(t, out.GetVector().GetDense())

	named := &storage.Node{NamedEmbeddings: map[string][]float32{"a": {1, 0}, "b": {0, 1}}}
	out = vectorsOutputFromNode(named, []string{"a"})
	require.NotNil(t, out.GetVectors())
	require.Contains(t, out.GetVectors().Vectors, "a")
	require.NotContains(t, out.GetVectors().Vectors, "b")

	deleteNodeVectors(nil, []string{"a"})
	deleteNodeVectors(named, nil)
	deleteNodeVectors(named, []string{"a", "b"})
	require.Nil(t, named.NamedEmbeddings)

	require.False(t, matchesRange(int64(5), &qpb.Range{Lt: ptrF64(5)}))
	require.False(t, matchesRange(int64(5), &qpb.Range{Gt: ptrF64(5)}))
	require.False(t, matchesRange(int64(5), &qpb.Range{Gte: ptrF64(6)}))
	require.False(t, matchesRange(int64(5), &qpb.Range{Lte: ptrF64(4)}))

	// ID conversion helper branches.
	require.Equal(t, storage.NodeID("qdrant:point:9"), pointIDToNodeID(&qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 9}}))
	require.Equal(t, "raw-id", nodeIDToPointID(storage.NodeID("raw-id")).GetUuid())
	require.Equal(t, uint64(77), nodeIDToPointID(storage.NodeID("qdrant:point:77")).GetNum())

	// extractVectors branches.
	_, _, err := extractVectors(context.Background(), nil, nil)
	require.Error(t, err)
	_, _, err = extractVectors(context.Background(), nil, &qpb.Vectors{})
	require.Error(t, err)
	_, _, err = extractVectors(context.Background(), nil, &qpb.Vectors{
		VectorsOptions: &qpb.Vectors_Vectors{
			Vectors: &qpb.NamedVectors{Vectors: map[string]*qpb.Vector{}},
		},
	})
	require.Error(t, err)
	_, _, err = extractVectors(context.Background(), nil, &qpb.Vectors{
		VectorsOptions: &qpb.Vectors_Vectors{
			Vectors: &qpb.NamedVectors{
				Vectors: map[string]*qpb.Vector{
					"bad": {},
				},
			},
		},
	})
	require.Error(t, err)
}

func TestBruteVectorIndex_SearchBranches(t *testing.T) {
	t.Parallel()

	var nilIdx *bruteVectorIndex
	require.Nil(t, nilIdx.search(context.Background(), []float32{1, 0}, 1, -1, 0))

	idx := newBruteVectorIndex(2, qpb.Distance_Dot)
	idx.upsert("a", []float32{1, 0})
	idx.upsert("b", []float32{0, 1})
	idx.upsert("c", []float32{1, 1})

	require.Nil(t, idx.search(context.Background(), []float32{1, 0}, 0, -1, 0))
	require.Nil(t, idx.search(context.Background(), []float32{1, 0, 0}, 1, -1, 0))

	// minScore branch prunes low scores.
	out := idx.search(context.Background(), []float32{1, 0}, 2, 1.5, 0)
	require.Empty(t, out)

	// top replacement branch with limited top-k.
	out = idx.search(context.Background(), []float32{1, 0}, 1, -1, 0)
	require.Len(t, out, 1)

	// Cosine normalization branch + canceled context branch.
	cos := newBruteVectorIndex(2, qpb.Distance_Cosine)
	cos.upsert("c1", []float32{1, 1})
	out = cos.search(context.Background(), []float32{1, 1}, 1, -1, 0)
	require.Len(t, out, 1)

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Nil(t, cos.search(cancelCtx, []float32{1, 1}, 1, -1, 0))
}

func TestVectorIndexCacheExtraBranches(t *testing.T) {
	t.Parallel()

	var nilCache *vectorIndexCache
	require.Nil(t, nilCache.search(context.Background(), "c", 2, qpb.Distance_Dot, "", []float32{1, 0}, 1, -1, 0))
	require.NoError(t, nilCache.replacePoint("c", 2, qpb.Distance_Dot, "p", nil, nil, nil))
	nilCache.deletePoint("c", "p", nil)

	cache := newVectorIndexCache()
	require.NoError(t, cache.replacePoint("c", 2, qpb.Distance_Dot, "qdrant:point:p1", nil, []string{""}, [][]float32{{1, 0}}))
	results := cache.search(context.Background(), "c", 2, qpb.Distance_Dot, "", []float32{1, 0}, 5, -1, 0)
	require.Len(t, results, 1)
	require.Equal(t, "p1", results[0].ID)

	cache.deletePoint("c", "qdrant:point:p1", nil)
	require.Empty(t, cache.search(context.Background(), "c", 2, qpb.Distance_Dot, "", []float32{1, 0}, 5, -1, 0))

	require.ErrorContains(t, cache.replacePoint("c", 2, qpb.Distance_Dot, "p2", nil, []string{""}, [][]float32{{1, 0, 0}}), "vector dim mismatch")

	first := cache.getOrCreate("c", "named", 2, qpb.Distance_Euclid)
	second := cache.getOrCreate("c", "named", 3, qpb.Distance_Euclid)
	require.NotSame(t, first, second)
	cache.deleteCollection("c")
	require.Empty(t, cache.indexes)

	require.Equal(t, "abc", compactPointID("c", "qdrant:point:abc"))
	require.Equal(t, "raw", compactPointID("c", "raw"))
	require.Equal(t, "qdrant:point:raw", expandPointID("raw"))
	require.Equal(t, "qdrant:custom", expandPointID("qdrant:custom"))
}

func TestCollectionStorePointCountBranches(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestCollectionStore(t)
	dbStore := store.(*databaseCollectionStore)

	require.NoError(t, store.Create(ctx, "counted", 2, qpb.Distance_Cosine))
	engine, _, err := store.Open(ctx, "counted")
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "qdrant:point:a", Labels: []string{QdrantPointLabel}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "not-a-point", Labels: []string{"Other"}})
	require.NoError(t, err)

	count, err := store.PointCount(ctx, "counted")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	require.False(t, dbStore.Exists(""))
	require.False(t, dbStore.Exists("missing"))
	_, err = dbStore.PointCount(ctx, "missing")
	require.ErrorIs(t, err, ErrCollectionNotFound)
}

func TestFilterMatchRangeExtraBranches(t *testing.T) {
	t.Parallel()

	require.True(t, matchesMatch("anything", nil))
	require.False(t, matchesMatch("tag", &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "other"}}))
	require.False(t, matchesMatch(int64(5), &qpb.Match{MatchValue: &qpb.Match_Integer{Integer: 6}}))
	require.False(t, matchesMatch(false, &qpb.Match{MatchValue: &qpb.Match_Boolean{Boolean: true}}))
	require.False(t, matchesMatch("x", &qpb.Match{}))

	require.True(t, matchesRange(3, nil))
	require.True(t, matchesRange(float64(3), &qpb.Range{Gte: ptrF64(3), Lte: ptrF64(3)}))
	require.False(t, matchesFieldCondition(&storage.Node{Properties: map[string]interface{}{"k": "v"}}, &qpb.FieldCondition{
		Key:   "k",
		Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "v"}},
		Range: &qpb.Range{Gt: ptrF64(1)},
	}))
}

func TestPointsService_ScrollBatchAndGroupsBranches(t *testing.T) {
	ctx := context.Background()
	svc, _ := setupExtendedPointsService(t)

	scrollResp, err := svc.Scroll(ctx, &qpb.ScrollPoints{
		CollectionName: "test_collection",
		Limit:          ptrU32Ext(1),
	})
	require.NoError(t, err)
	require.Len(t, scrollResp.Result, 1)
	require.NotNil(t, scrollResp.NextPageOffset)

	scrollResp, err = svc.Scroll(ctx, &qpb.ScrollPoints{
		CollectionName: "test_collection",
		Limit:          ptrU32Ext(1),
		Offset:         scrollResp.NextPageOffset,
	})
	require.NoError(t, err)
	require.Len(t, scrollResp.Result, 1)

	// Offset not found branch.
	scrollResp, err = svc.Scroll(ctx, &qpb.ScrollPoints{
		CollectionName: "test_collection",
		Limit:          ptrU32Ext(1),
		Offset:         &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "missing"}},
	})
	require.NoError(t, err)
	require.Len(t, scrollResp.Result, 1)

	// SearchBatch with nil item.
	sb, err := svc.SearchBatch(ctx, &qpb.SearchBatchPoints{
		CollectionName: "test_collection",
		SearchPoints: []*qpb.SearchPoints{
			nil,
			{Vector: []float32{1, 0, 0, 0}, Limit: 1},
		},
	})
	require.NoError(t, err)
	require.Len(t, sb.Result, 2)
	require.Nil(t, sb.Result[0].Result)

	// SearchGroups default group limit/size and payload/group key handling.
	grp, err := svc.SearchGroups(ctx, &qpb.SearchPointGroups{
		CollectionName: "test_collection",
		Vector:         []float32{1, 0, 0, 0},
		GroupBy:        "missing_field",
	})
	require.NoError(t, err)
	require.NotNil(t, grp.Result)
}

func ptrU32Ext(v uint32) *uint32 { return &v }

func TestPointsService_RecommendQueryVectorBranches(t *testing.T) {
	ctx := context.Background()
	svc, _ := setupExtendedPointsService(t)

	_, err := svc.recommendQueryVector(ctx, "missing_collection", "", nil, nil, nil, nil)
	require.Error(t, err)

	// Positive + negative dimension mismatch branch.
	_, err = svc.recommendQueryVector(ctx, "test_collection", "",
		nil, nil,
		[]*qpb.Vector{{Data: []float32{1, 0, 0, 0}}},
		[]*qpb.Vector{{Data: []float32{1, 0, 0}}},
	)
	require.Error(t, err)

	// vectorFromVector unsupported vector kind branch via recommendQueryVector.
	_, err = svc.recommendQueryVector(ctx, "test_collection", "",
		nil, nil,
		[]*qpb.Vector{{}},
		nil,
	)
	require.Error(t, err)

	// ID-based positive/negative branches.
	vec, err := svc.recommendQueryVector(ctx, "test_collection", "",
		[]*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}}},
		[]*qpb.PointId{
			{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point2"}},
			{PointIdOptions: &qpb.PointId_Uuid{Uuid: "missing-id"}},
		},
		nil, nil,
	)
	require.NoError(t, err)
	require.Len(t, vec, 4)
}

func TestPointsService_VectorFromInputMoreBranches(t *testing.T) {
	ctx := context.Background()
	svc, _ := setupExtendedPointsService(t)

	// Missing named vector on existing point.
	_, err := svc.vectorFromInput(ctx, "test_collection", "missing_name", &qpb.VectorInput{
		Variant: &qpb.VectorInput_Id{
			Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "point1"}},
		},
	})
	require.Error(t, err)

	// Embedder error branch.
	svc.config.EmbedQuery = func(ctx context.Context, text string) ([]float32, error) {
		_ = ctx
		_ = text
		return nil, errors.New("embed failed")
	}
	_, err = svc.vectorFromInput(ctx, "test_collection", "", &qpb.VectorInput{
		Variant: &qpb.VectorInput_Document{
			Document: &qpb.Document{Text: "boom"},
		},
	})
	require.Error(t, err)

	// Unimplemented variant branch (nil variant in message).
	_, err = svc.vectorFromInput(ctx, "test_collection", "", &qpb.VectorInput{})
	require.Error(t, err)

	_, err = svc.vectorFromInput(ctx, "test_collection", "", &qpb.VectorInput{
		Variant: &qpb.VectorInput_Sparse{
			Sparse: &qpb.SparseVector{},
		},
	})
	require.Error(t, err)
}
