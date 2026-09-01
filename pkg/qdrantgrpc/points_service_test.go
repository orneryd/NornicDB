package qdrantgrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupPointsService(t *testing.T) (*PointsService, CollectionStore, storage.Engine) {
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
		return []float32{1, 0, 0, 0}, nil
	}

	err = collections.Create(ctx, "test_vectors", 4, qpb.Distance_Cosine)
	require.NoError(t, err)

	return NewPointsService(cfg, collections, nil, vecIndex, nil), collections, base
}

func TestPointsService_UpsertGetDeleteCountSearch(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	upsertResp, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
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
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 1, 0, 0}}}},
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
	require.Equal(t, qpb.UpdateStatus_Completed, upsertResp.GetResult().GetStatus())

	getResp, err := service.Get(ctx, &qpb.GetPoints{
		CollectionName: "test_vectors",
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}}},
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.Len(t, getResp.Result, 1)
	require.Equal(t, "first", getResp.Result[0].Payload["tag"].GetStringValue())

	countResp, err := service.Count(ctx, &qpb.CountPoints{CollectionName: "test_vectors", Exact: ptrBool(true)})
	require.NoError(t, err)
	require.Equal(t, uint64(2), countResp.Result.Count)

	countResp, err = service.Count(ctx, &qpb.CountPoints{
		CollectionName: "test_vectors",
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

	searchResp, err := service.Search(ctx, &qpb.SearchPoints{
		CollectionName: "test_vectors",
		Vector:         []float32{1, 0, 0, 0},
		Limit:          3,
		WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, searchResp.Result)

	_, err = service.Delete(ctx, &qpb.DeletePoints{
		CollectionName: "test_vectors",
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

	countResp, err = service.Count(ctx, &qpb.CountPoints{CollectionName: "test_vectors", Exact: ptrBool(true)})
	require.NoError(t, err)
	require.Equal(t, uint64(1), countResp.Result.Count)
}

func TestPointsService_SearchBatch(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	_, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	resp, err := service.SearchBatch(ctx, &qpb.SearchBatchPoints{
		CollectionName: "test_vectors",
		SearchPoints: []*qpb.SearchPoints{
			{Vector: []float32{1, 0, 0, 0}, Limit: 1},
			{Vector: []float32{0, 1, 0, 0}, Limit: 1},
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.Result, 2)
}

type recordingVectorIndex struct {
	dim  int
	dist qpb.Distance

	lastEf int
}

func (r *recordingVectorIndex) dimensions() int        { return r.dim }
func (r *recordingVectorIndex) distance() qpb.Distance { return r.dist }
func (r *recordingVectorIndex) remove(id string)       { _ = id }
func (r *recordingVectorIndex) upsert(id string, vec []float32) {
	_ = id
	_ = vec
}
func (r *recordingVectorIndex) search(ctx context.Context, query []float32, limit int, minScore float64, ef int) []searchResult {
	_ = ctx
	_ = query
	_ = limit
	_ = minScore
	r.lastEf = ef
	return []searchResult{{ID: "p1", Score: 0.99}}
}

type staticVectorIndex struct {
	dim   int
	dist  qpb.Distance
	cands []searchResult
}

func (s *staticVectorIndex) dimensions() int        { return s.dim }
func (s *staticVectorIndex) distance() qpb.Distance { return s.dist }
func (s *staticVectorIndex) remove(id string)       { _ = id }
func (s *staticVectorIndex) upsert(id string, vec []float32) {
	_ = id
	_ = vec
}
func (s *staticVectorIndex) search(ctx context.Context, query []float32, limit int, minScore float64, ef int) []searchResult {
	_ = ctx
	_ = query
	_ = limit
	_ = minScore
	_ = ef
	return append([]searchResult(nil), s.cands...)
}

func TestPointsService_Search_RespectsHnswEf(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	rec := &recordingVectorIndex{dim: 4, dist: qpb.Distance_Cosine}
	cache := newVectorIndexCache()
	cache.indexes[indexKey{collection: "test_vectors", vectorName: ""}] = rec
	service.vecIndex = cache

	ef := uint64(77)
	resp, err := service.Search(ctx, &qpb.SearchPoints{
		CollectionName: "test_vectors",
		Vector:         []float32{1, 0, 0, 0},
		Limit:          3,
		Params:         &qpb.SearchParams{HnswEf: &ef},
	})
	require.NoError(t, err)
	require.Len(t, resp.Result, 1)
	assert.Equal(t, 77, rec.lastEf)
	assert.Equal(t, "p1", resp.Result[0].GetId().GetUuid())
}

func TestPointsService_SearchCollection_VecIndexBranches(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	_, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{"tag": {Kind: &qpb.Value_StringValue{StringValue: "keep"}}},
			},
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "b"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 1, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{"tag": {Kind: &qpb.Value_StringValue{StringValue: "drop"}}},
			},
		},
	})
	require.NoError(t, err)

	store, meta, err := service.openCollection(ctx, "test_vectors")
	require.NoError(t, err)

	cache := newVectorIndexCache()
	cache.indexes[indexKey{collection: "test_vectors", vectorName: ""}] = &staticVectorIndex{
		dim:  4,
		dist: meta.Distance,
		cands: []searchResult{
			{ID: "a", Score: 0.99},
			{ID: "missing", Score: 0.98},
			{ID: "b", Score: 0.97},
		},
	}
	service.vecIndex = cache

	// No filter path with offset trimming.
	res := service.searchCollection(ctx, store, nil, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, nil, 1, 5, -1, 0)
	require.Nil(t, res)

	res = service.searchCollection(ctx, store, nil, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, nil, 1, 1, -1, 0)
	require.Len(t, res, 1)

	// Filter path drops missing/non-matching candidates and can return nil when offset exceeds.
	res = service.searchCollection(ctx, store, nil, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, &qpb.Filter{
		Must: []*qpb.Condition{
			{
				ConditionOneOf: &qpb.Condition_Field{
					Field: &qpb.FieldCondition{
						Key:   "tag",
						Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "keep"}},
					},
				},
			},
		},
	}, 1, 1, -1, 0)
	require.Nil(t, res)
}

func TestPointsService_SearchCollection_BruteForceBranches(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)
	service.vecIndex = nil

	_, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "bf1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{"tag": {Kind: &qpb.Value_StringValue{StringValue: "x"}}},
			},
		},
	})
	require.NoError(t, err)

	store, meta, err := service.openCollection(ctx, "test_vectors")
	require.NoError(t, err)

	// High score threshold prunes all brute-force candidates.
	res := service.searchCollection(ctx, store, nil, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, nil, 2, 0, 2.0, 0)
	require.Nil(t, res)

	// Offset beyond results branch.
	res = service.searchCollection(ctx, store, nil, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, nil, 1, 10, -1, 0)
	require.Nil(t, res)
}

func TestPointsService_Query_Document(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	_, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "p1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	resp, err := service.Query(ctx, &qpb.QueryPoints{
		CollectionName: "test_vectors",
		Query: &qpb.Query{
			Variant: &qpb.Query_Nearest{
				Nearest: &qpb.VectorInput{
					Variant: &qpb.VectorInput_Document{
						Document: &qpb.Document{Text: "hello world"},
					},
				},
			},
		},
		Limit: ptrU64(1),
	})
	require.NoError(t, err)
	assert.Len(t, resp.Result, 1)
}

func ptrU64(v uint64) *uint64 { return &v }

func ptrF64(v float64) *float64 { return &v }

// TestAverageVectors verifies that averageVectors correctly computes averages
// and handles vectors with mismatched dimensions.
func TestAverageVectors(t *testing.T) {
	t.Run("normal averaging", func(t *testing.T) {
		vectors := [][]float32{
			{1.0, 2.0, 3.0},
			{4.0, 5.0, 6.0},
			{7.0, 8.0, 9.0},
		}
		result := averageVectors(vectors)
		require.NotNil(t, result)
		assert.Equal(t, []float32{4.0, 5.0, 6.0}, result) // (1+4+7)/3, (2+5+8)/3, (3+6+9)/3
	})

	t.Run("skips mismatched dimensions", func(t *testing.T) {
		vectors := [][]float32{
			{1.0, 2.0, 3.0},
			{4.0, 5.0}, // Wrong dimension - should be skipped
			{7.0, 8.0, 9.0},
			{10.0, 11.0, 12.0, 13.0}, // Wrong dimension - should be skipped
		}
		result := averageVectors(vectors)
		require.NotNil(t, result)
		// Should average only the 2 vectors with correct dimension: [1,2,3] and [7,8,9]
		// Average: (1+7)/2, (2+8)/2, (3+9)/2 = [4.0, 5.0, 6.0]
		assert.Equal(t, []float32{4.0, 5.0, 6.0}, result)
	})

	t.Run("all vectors have mismatched dimensions", func(t *testing.T) {
		vectors := [][]float32{
			{1.0, 2.0, 3.0},      // First vector determines dimension (3)
			{4.0, 5.0},           // Wrong dimension - skipped
			{6.0, 7.0, 8.0, 9.0}, // Wrong dimension - skipped
		}
		// First vector determines dimension (3), but only first vector matches
		result := averageVectors(vectors)
		require.NotNil(t, result)
		// Should return the first vector (averaged by itself)
		assert.Equal(t, []float32{1.0, 2.0, 3.0}, result)
	})

	t.Run("no vectors match reference dimension", func(t *testing.T) {
		vectors := [][]float32{
			{1.0, 2.0, 3.0}, // First vector determines dimension (3)
			{4.0, 5.0},      // Wrong dimension - skipped
			{6.0, 7.0},      // Wrong dimension - skipped
		}
		// First vector determines dimension (3), but no other vectors match
		// Only the first vector matches, so it's averaged by itself
		result := averageVectors(vectors)
		require.NotNil(t, result)
		assert.Equal(t, []float32{1.0, 2.0, 3.0}, result)
	})

	t.Run("empty input", func(t *testing.T) {
		result := averageVectors(nil)
		assert.Nil(t, result)
		result = averageVectors([][]float32{})
		assert.Nil(t, result)
	})

	t.Run("single vector", func(t *testing.T) {
		vectors := [][]float32{
			{1.0, 2.0, 3.0},
		}
		result := averageVectors(vectors)
		require.NotNil(t, result)
		assert.Equal(t, []float32{1.0, 2.0, 3.0}, result)
	})

	t.Run("mixed dimensions with correct average", func(t *testing.T) {
		// This test demonstrates the bug fix: if we have 3 vectors but only 2 match,
		// we should divide by 2, not 3
		vectors := [][]float32{
			{1.0, 2.0, 3.0}, // First vector determines dimension (3) - included
			{10.0, 20.0},    // Wrong dimension - skipped
			{4.0, 5.0, 6.0}, // Included
		}
		result := averageVectors(vectors)
		require.NotNil(t, result)
		// Should be (1+4)/2, (2+5)/2, (3+6)/2 = [2.5, 3.5, 4.5]
		// NOT (1+4)/3, (2+5)/3, (3+6)/3 = [1.67, 2.33, 3.0] (wrong!)
		assert.Equal(t, []float32{2.5, 3.5, 4.5}, result)
	})
}

func TestPointsService_HelperCoverage(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	t.Run("query batch nearest", func(t *testing.T) {
		_, err := service.Upsert(ctx, &qpb.UpsertPoints{
			CollectionName: "test_vectors",
			Points: []*qpb.PointStruct{
				{
					Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "qb1"}},
					Vectors: &qpb.Vectors{
						VectorsOptions: &qpb.Vectors_Vector{
							Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		resp, err := service.QueryBatch(ctx, &qpb.QueryBatchPoints{
			CollectionName: "test_vectors",
			QueryPoints: []*qpb.QueryPoints{
				{
					CollectionName: "test_vectors",
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
		require.Len(t, resp.Result, 1)

		_, err = service.Query(ctx, &qpb.QueryPoints{
			CollectionName: "test_vectors",
			Query: &qpb.Query{
				Variant: &qpb.Query_Fusion{
					Fusion: qpb.Fusion_RRF,
				},
			},
		})
		require.Error(t, err)
	})

	t.Run("vector from vector variants", func(t *testing.T) {
		_, err := service.vectorFromVector(ctx, nil)
		require.Error(t, err)

		vec, err := service.vectorFromVector(ctx, &qpb.Vector{
			Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 2, 3, 4}}},
		})
		require.NoError(t, err)
		require.Len(t, vec, 4)

		vec, err = service.vectorFromVector(ctx, &qpb.Vector{Data: []float32{5, 6, 7, 8}})
		require.NoError(t, err)
		require.Len(t, vec, 4)

		noEmbedSvc, _, _ := setupPointsService(t)
		noEmbedSvc.config.EmbedQuery = nil
		_, err = noEmbedSvc.vectorFromVector(ctx, &qpb.Vector{
			Vector: &qpb.Vector_Document{Document: &qpb.Document{Text: "hello"}},
		})
		require.Error(t, err)

		vec, err = service.vectorFromVector(ctx, &qpb.Vector{
			Vector: &qpb.Vector_Document{Document: &qpb.Document{Text: "hello"}},
		})
		require.NoError(t, err)
		require.NotEmpty(t, vec)
	})

	t.Run("point id and filter helpers", func(t *testing.T) {
		idNum1 := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 7}}
		idNum2 := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 7}}
		idUUID := &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "x"}}
		require.True(t, pointIDsEqual(idNum1, idNum2))
		require.False(t, pointIDsEqual(idNum1, idUUID))
		require.False(t, pointIDsEqual(nil, idUUID))

		require.True(t, matchesRange(int64(10), &qpb.Range{Gte: ptrF64(10), Lte: ptrF64(11)}))
		require.True(t, matchesRange(float32(9.5), &qpb.Range{Gt: ptrF64(9), Lt: ptrF64(10)}))
		require.False(t, matchesRange("bad", &qpb.Range{Gt: ptrF64(1)}))

		require.True(t, matchesMatch("tag", &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "tag"}}))
		require.True(t, matchesMatch(int64(5), &qpb.Match{MatchValue: &qpb.Match_Integer{Integer: 5}}))
		require.True(t, matchesMatch(true, &qpb.Match{MatchValue: &qpb.Match_Boolean{Boolean: true}}))
		require.False(t, matchesMatch(1, &qpb.Match{MatchValue: &qpb.Match_Boolean{Boolean: true}}))

		node := &storage.Node{
			ID: "qdrant:point:7",
			Properties: map[string]any{
				"tag":   "alpha",
				"score": float64(10),
				"flag":  true,
			},
		}
		fieldCond := &qpb.Condition{
			ConditionOneOf: &qpb.Condition_Field{
				Field: &qpb.FieldCondition{
					Key:   "tag",
					Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "alpha"}},
				},
			},
		}
		require.True(t, matchesCondition(node, fieldCond))

		idCond := &qpb.Condition{
			ConditionOneOf: &qpb.Condition_HasId{
				HasId: &qpb.HasIdCondition{HasId: []*qpb.PointId{idNum1}},
			},
		}
		require.True(t, matchesCondition(node, idCond))

		filter := &qpb.Filter{
			Must: []*qpb.Condition{fieldCond},
			Should: []*qpb.Condition{
				{
					ConditionOneOf: &qpb.Condition_Field{
						Field: &qpb.FieldCondition{
							Key:   "flag",
							Match: &qpb.Match{MatchValue: &qpb.Match_Boolean{Boolean: true}},
						},
					},
				},
			},
		}
		require.True(t, matchesFilter(node, filter))
	})

	t.Run("score vector distances", func(t *testing.T) {
		a := []float32{1, 0, 0, 0}
		b := []float32{1, 0, 0, 0}
		require.Greater(t, scoreVector(qpb.Distance_Dot, a, b), 0.9)
		require.Greater(t, scoreVector(qpb.Distance_Cosine, a, b), 0.9)
		require.Greater(t, scoreVector(qpb.Distance_Euclid, a, b), 0.9)
	})
}

func TestPointsService_OverwritePayloadAndRecommendBatch(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	_, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "ow1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"old": {Kind: &qpb.Value_StringValue{StringValue: "value"}},
				},
			},
		},
	})
	require.NoError(t, err)

	t.Run("overwrite payload validates input", func(t *testing.T) {
		_, err := service.OverwritePayload(ctx, &qpb.SetPayloadPoints{})
		require.Error(t, err)

		_, err = service.OverwritePayload(ctx, &qpb.SetPayloadPoints{
			CollectionName: "test_vectors",
			PointsSelector: &qpb.PointsSelector{
				PointsSelectorOneOf: &qpb.PointsSelector_Points{
					Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "ow1"}}}},
				},
			},
		})
		require.Error(t, err)
	})

	t.Run("overwrite payload replaces existing properties", func(t *testing.T) {
		_, err := service.OverwritePayload(ctx, &qpb.SetPayloadPoints{
			CollectionName: "test_vectors",
			Payload: map[string]*qpb.Value{
				"new_only": {Kind: &qpb.Value_BoolValue{BoolValue: true}},
			},
			PointsSelector: &qpb.PointsSelector{
				PointsSelectorOneOf: &qpb.PointsSelector_Points{
					Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "ow1"}}}},
				},
			},
		})
		require.NoError(t, err)

		getResp, err := service.Get(ctx, &qpb.GetPoints{
			CollectionName: "test_vectors",
			Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "ow1"}}},
			WithPayload:    &qpb.WithPayloadSelector{SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: true}},
		})
		require.NoError(t, err)
		require.Len(t, getResp.Result, 1)
		require.Contains(t, getResp.Result[0].Payload, "new_only")
		require.NotContains(t, getResp.Result[0].Payload, "old")
	})

	t.Run("recommend batch validates input", func(t *testing.T) {
		_, err := service.RecommendBatch(ctx, &qpb.RecommendBatchPoints{})
		require.Error(t, err)

		_, err = service.RecommendBatch(ctx, &qpb.RecommendBatchPoints{
			CollectionName:  "test_vectors",
			RecommendPoints: nil,
		})
		require.Error(t, err)
	})

	t.Run("recommend batch handles nil element and valid request", func(t *testing.T) {
		resp, err := service.RecommendBatch(ctx, &qpb.RecommendBatchPoints{
			CollectionName: "test_vectors",
			RecommendPoints: []*qpb.RecommendPoints{
				nil,
				{
					Positive: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "ow1"}}},
					Limit:    1,
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, resp.Result, 2)
		require.Nil(t, resp.Result[0].Result)
		require.NotEmpty(t, resp.Result[1].Result)
	})
}

func TestPointsService_HelperFunctions_MoreCoverage(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	_, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "vh1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	t.Run("vectorFromInput covers id and document branches", func(t *testing.T) {
		_, err := service.vectorFromInput(ctx, "test_vectors", "", nil)
		require.Error(t, err)

		_, err = service.vectorFromInput(ctx, "test_vectors", "", &qpb.VectorInput{
			Variant: &qpb.VectorInput_Id{},
		})
		require.Error(t, err)

		_, err = service.vectorFromInput(ctx, "test_vectors", "", &qpb.VectorInput{
			Variant: &qpb.VectorInput_Id{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "missing"}},
			},
		})
		require.Error(t, err)

		vec, err := service.vectorFromInput(ctx, "test_vectors", "", &qpb.VectorInput{
			Variant: &qpb.VectorInput_Id{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "vh1"}},
			},
		})
		require.NoError(t, err)
		require.Len(t, vec, 4)

		noEmbedSvc, _, _ := setupPointsService(t)
		noEmbedSvc.config.EmbedQuery = nil
		_, err = noEmbedSvc.vectorFromInput(ctx, "test_vectors", "", &qpb.VectorInput{
			Variant: &qpb.VectorInput_Document{Document: &qpb.Document{}},
		})
		require.Error(t, err)
	})

	t.Run("recommendQueryVector covers error and mix branches", func(t *testing.T) {
		_, err := service.recommendQueryVector(ctx, "test_vectors", "", nil, nil, nil, nil)
		require.Error(t, err)

		_, err = service.recommendQueryVector(ctx, "test_vectors", "",
			nil, nil,
			[]*qpb.Vector{{Data: []float32{1, 1, 0, 0}}},
			[]*qpb.Vector{{Data: []float32{0.5, 0.5, 0, 0}}},
		)
		require.NoError(t, err)
	})

	t.Run("payload and vector selector helpers", func(t *testing.T) {
		props := map[string]any{
			"a":          int64(1),
			"b":          "x",
			"_qdrant_id": "internal",
		}

		require.Nil(t, withPayloadSelection(nil, nil))
		require.Nil(t, withPayloadSelection(props, &qpb.WithPayloadSelector{
			SelectorOptions: &qpb.WithPayloadSelector_Enable{Enable: false},
		}))

		clean := withPayloadSelection(props, nil)
		require.Contains(t, clean, "a")
		require.NotContains(t, clean, "_qdrant_id")

		included := withPayloadSelection(props, &qpb.WithPayloadSelector{
			SelectorOptions: &qpb.WithPayloadSelector_Include{
				Include: &qpb.PayloadIncludeSelector{Fields: []string{"b"}},
			},
		})
		require.Equal(t, "x", included["b"])
		require.NotContains(t, included, "a")

		excluded := withPayloadSelection(props, &qpb.WithPayloadSelector{
			SelectorOptions: &qpb.WithPayloadSelector_Exclude{
				Exclude: &qpb.PayloadExcludeSelector{Fields: []string{"a"}},
			},
		})
		require.NotContains(t, excluded, "a")

		include, names := withVectorsSelection(nil)
		require.False(t, include)
		require.Nil(t, names)

		include, names = withVectorsSelection(&qpb.WithVectorsSelector{
			SelectorOptions: &qpb.WithVectorsSelector_Enable{Enable: true},
		})
		require.True(t, include)
		require.Nil(t, names)

		include, names = withVectorsSelection(&qpb.WithVectorsSelector{
			SelectorOptions: &qpb.WithVectorsSelector_Include{
				Include: &qpb.VectorsSelector{Names: []string{"v1", "v2"}},
			},
		})
		require.True(t, include)
		require.Equal(t, []string{"v1", "v2"}, names)

		include, names = withVectorsSelection(&qpb.WithVectorsSelector{
			SelectorOptions: &qpb.WithVectorsSelector_Include{},
		})
		require.True(t, include)
		require.Nil(t, names)
	})

	t.Run("value conversion helpers", func(t *testing.T) {
		vals := []any{
			nil,
			true,
			int(7),
			int64(9),
			float32(1.5),
			float64(2.5),
			"txt",
			[]any{int64(1), "x"},
			map[string]any{"k": int64(3)},
			struct{}{},
		}
		for _, in := range vals {
			v := anyToValue(in)
			require.NotNil(t, v)
			_ = valueToAny(v)
		}
		require.Nil(t, valueToAny(nil))
	})
}

func TestPointsService_SearchCollectionAndLowLevelHelpers(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	_, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "sc1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"group": {Kind: &qpb.Value_StringValue{StringValue: "a"}},
				},
			},
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "sc2"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0, 1, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"group": {Kind: &qpb.Value_StringValue{StringValue: "b"}},
				},
			},
		},
	})
	require.NoError(t, err)

	store, meta, err := service.openCollection(ctx, "test_vectors")
	require.NoError(t, err)

	t.Run("vec index path without filter", func(t *testing.T) {
		results := service.searchCollection(ctx, store, nil, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, nil, 1, 0, -1, 0)
		require.Len(t, results, 1)

		none := service.searchCollection(ctx, store, nil, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, nil, 1, 99, -1, 0)
		require.Nil(t, none)
	})

	t.Run("vec index path with filter", func(t *testing.T) {
		results := service.searchCollection(ctx, store, nil, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, &qpb.Filter{
			Must: []*qpb.Condition{
				{
					ConditionOneOf: &qpb.Condition_Field{
						Field: &qpb.FieldCondition{
							Key:   "group",
							Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "a"}},
						},
					},
				},
			},
		}, 2, 0, -1, 0)
		require.NotEmpty(t, results)
	})

	t.Run("search service path and brute fallback path", func(t *testing.T) {
		service.vecIndex = nil

		svc := search.NewServiceWithDimensions(store, 4)
		nodes, err := store.GetNodesByLabel(QdrantPointLabel)
		require.NoError(t, err)
		for _, n := range nodes {
			require.NoError(t, svc.IndexNode(n))
		}

		results := service.searchCollection(ctx, store, svc, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, nil, 2, 0, -1, 0)
		require.NotEmpty(t, results)

		// Search-service filter/offset paths.
		filtered := service.searchCollection(ctx, store, svc, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, &qpb.Filter{
			Must: []*qpb.Condition{
				{
					ConditionOneOf: &qpb.Condition_Field{
						Field: &qpb.FieldCondition{
							Key:   "group",
							Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "no-match"}},
						},
					},
				},
			},
		}, 2, 1, -1, 0)
		require.Nil(t, filtered)

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()
		require.Nil(t, service.searchCollection(cancelCtx, store, nil, "test_vectors", meta.Distance, "", []float32{1, 0, 0, 0}, nil, 2, 0, -1, 0))
	})

	t.Run("getSearchService branches", func(t *testing.T) {
		service.searchProvider = nil
		require.Nil(t, service.getSearchService("test_vectors", store))

		service.searchProvider = func(database string, st storage.Engine) (*search.Service, error) {
			_ = database
			_ = st
			return nil, errors.New("boom")
		}
		require.Nil(t, service.getSearchService("test_vectors", store))

		expected := search.NewServiceWithDimensions(store, 4)
		service.searchProvider = func(database string, st storage.Engine) (*search.Service, error) {
			_ = database
			_ = st
			return expected, nil
		}
		require.Equal(t, expected, service.getSearchService("test_vectors", store))
	})

	t.Run("extractDenseFromVector and resolvePointIDs", func(t *testing.T) {
		_, err := extractDenseFromVector(context.Background(), nil, nil)
		require.Error(t, err)

		out, err := extractDenseFromVector(context.Background(), nil, &qpb.Vector{
			Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 2, 3, 4}}},
		})
		require.NoError(t, err)
		require.Equal(t, []float32{1, 2, 3, 4}, out)

		out, err = extractDenseFromVector(context.Background(), nil, &qpb.Vector{Data: []float32{4, 3, 2, 1}})
		require.NoError(t, err)
		require.Equal(t, []float32{4, 3, 2, 1}, out)

		_, err = extractDenseFromVector(context.Background(), nil, &qpb.Vector{})
		require.Error(t, err)

		_, err = resolvePointIDs(context.Background(), nil, nil)
		require.Error(t, err)

		_, err = resolvePointIDs(context.Background(), nil, &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{Points: &qpb.PointsIdsList{}},
		})
		require.Error(t, err)

		ids, err := resolvePointIDs(context.Background(), nil, &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{
					Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Num{Num: 11}}},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, ids, 1)

		ids, err = resolvePointIDs(context.Background(), nil, &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Filter{Filter: &qpb.Filter{}},
		})
		require.NoError(t, err)
		require.Nil(t, ids)
	})
}

type denyChecker struct{}

func (denyChecker) AllowDatabaseAccess(ctx context.Context, database string, write bool) error {
	_ = ctx
	_ = database
	_ = write
	return errors.New("denied")
}

func (denyChecker) VisibleDatabases(ctx context.Context, candidates []string) ([]string, error) {
	_ = ctx
	return candidates, nil
}

type fakeCollectionStore struct {
	engine  storage.Engine
	meta    *CollectionMeta
	openErr error
}

func (f *fakeCollectionStore) Create(ctx context.Context, name string, dims int, distance qpb.Distance) error {
	_ = ctx
	_ = name
	_ = dims
	_ = distance
	return nil
}
func (f *fakeCollectionStore) Open(ctx context.Context, name string) (storage.Engine, *CollectionMeta, error) {
	_ = ctx
	_ = name
	if f.openErr != nil {
		return nil, nil, f.openErr
	}
	return f.engine, f.meta, nil
}
func (f *fakeCollectionStore) GetMeta(ctx context.Context, name string) (*CollectionMeta, error) {
	_, meta, err := f.Open(ctx, name)
	return meta, err
}
func (f *fakeCollectionStore) List(ctx context.Context) ([]string, error) {
	_ = ctx
	return []string{"x"}, nil
}
func (f *fakeCollectionStore) Drop(ctx context.Context, name string) error {
	_ = ctx
	_ = name
	return nil
}
func (f *fakeCollectionStore) Exists(name string) bool {
	_ = name
	return true
}
func (f *fakeCollectionStore) PointCount(ctx context.Context, name string) (int64, error) {
	_ = ctx
	_ = name
	return 0, nil
}

type errEngine struct {
	storage.Engine

	batchGetErr        error
	getNodesByLabelErr error
	allNodesErr        error
}

func (e *errEngine) BatchGetNodes(ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	_ = ids
	if e.batchGetErr != nil {
		return nil, e.batchGetErr
	}
	return e.Engine.BatchGetNodes(ids)
}

func (e *errEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e.getNodesByLabelErr != nil {
		return nil, e.getNodesByLabelErr
	}
	return e.Engine.GetNodesByLabel(label)
}

func (e *errEngine) AllNodes() ([]*storage.Node, error) {
	if e.allNodesErr != nil {
		return nil, e.allNodesErr
	}
	return e.Engine.AllNodes()
}

func TestPointsService_ErrorBranches(t *testing.T) {
	ctx := context.Background()
	service, collections, _ := setupPointsService(t)

	t.Run("upsert validation branches", func(t *testing.T) {
		_, err := service.Upsert(ctx, &qpb.UpsertPoints{})
		require.Error(t, err)

		noMut, _, _ := setupPointsService(t)
		noMut.config.AllowVectorMutations = false
		_, err = noMut.Upsert(ctx, &qpb.UpsertPoints{
			CollectionName: "test_vectors",
			Points: []*qpb.PointStruct{
				{
					Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "x"}},
					Vectors: &qpb.Vectors{
						VectorsOptions: &qpb.Vectors_Vector{
							Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
						},
					},
				},
			},
		})
		require.Error(t, err)

		service.config.MaxBatchPoints = 1
		_, err = service.Upsert(ctx, &qpb.UpsertPoints{
			CollectionName: "test_vectors",
			Points: []*qpb.PointStruct{
				{Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "a"}}},
				{Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "b"}}},
			},
		})
		require.Error(t, err)
		service.config.MaxBatchPoints = 1000

		_, err = service.Upsert(ctx, &qpb.UpsertPoints{
			CollectionName: "test_vectors",
			Points:         []*qpb.PointStruct{{}},
		})
		require.Error(t, err)

		_, err = service.Upsert(ctx, &qpb.UpsertPoints{
			CollectionName: "test_vectors",
			Points: []*qpb.PointStruct{
				{Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "missing-v"}}},
			},
		})
		require.Error(t, err)

		_, err = service.Upsert(ctx, &qpb.UpsertPoints{
			CollectionName: "test_vectors",
			Points: []*qpb.PointStruct{
				{
					Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "dim-bad"}},
					Vectors: &qpb.Vectors{
						VectorsOptions: &qpb.Vectors_Vector{
							Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 2}}}},
						},
					},
				},
			},
		})
		require.Error(t, err)
	})

	t.Run("get/delete/count/search/scroll validation branches", func(t *testing.T) {
		_, err := service.Get(ctx, &qpb.GetPoints{})
		require.Error(t, err)
		_, err = service.Get(ctx, &qpb.GetPoints{CollectionName: "test_vectors"})
		require.Error(t, err)

		_, err = service.Delete(ctx, &qpb.DeletePoints{CollectionName: "test_vectors"})
		require.Error(t, err)

		_, err = service.Count(ctx, &qpb.CountPoints{})
		require.Error(t, err)
		_, err = service.Count(ctx, &qpb.CountPoints{CollectionName: "missing"})
		require.Error(t, err)

		_, err = service.Search(ctx, &qpb.SearchPoints{})
		require.Error(t, err)
		_, err = service.Search(ctx, &qpb.SearchPoints{CollectionName: "test_vectors"})
		require.Error(t, err)
		_, err = service.Search(ctx, &qpb.SearchPoints{
			CollectionName: "test_vectors",
			Vector:         []float32{1, 0},
		})
		require.Error(t, err)
		_, err = service.Search(ctx, &qpb.SearchPoints{
			CollectionName: "test_vectors",
			Vector:         []float32{1, 0, 0, 0},
			Limit:          uint64(service.config.MaxTopK + 1),
		})
		require.Error(t, err)
		tooBigOffset := ^uint64(0)
		_, err = service.Search(ctx, &qpb.SearchPoints{
			CollectionName: "test_vectors",
			Vector:         []float32{1, 0, 0, 0},
			Offset:         &tooBigOffset,
		})
		require.Error(t, err)
		tooBigEf := ^uint64(0)
		_, err = service.Search(ctx, &qpb.SearchPoints{
			CollectionName: "test_vectors",
			Vector:         []float32{1, 0, 0, 0},
			Params:         &qpb.SearchParams{HnswEf: &tooBigEf},
		})
		require.Error(t, err)

		_, err = service.Scroll(ctx, &qpb.ScrollPoints{})
		require.Error(t, err)
	})

	t.Run("payload/vector mutation validation branches", func(t *testing.T) {
		_, err := service.DeletePayload(ctx, &qpb.DeletePayloadPoints{})
		require.Error(t, err)
		_, err = service.DeletePayload(ctx, &qpb.DeletePayloadPoints{
			CollectionName: "test_vectors",
			Keys:           nil,
		})
		require.Error(t, err)

		_, err = service.ClearPayload(ctx, &qpb.ClearPayloadPoints{})
		require.Error(t, err)
		_, err = service.ClearPayload(ctx, &qpb.ClearPayloadPoints{
			CollectionName: "test_vectors",
		})
		require.Error(t, err)

		_, err = service.UpdateVectors(ctx, &qpb.UpdatePointVectors{})
		require.Error(t, err)
		noMut, _, _ := setupPointsService(t)
		noMut.config.AllowVectorMutations = false
		_, err = noMut.UpdateVectors(ctx, &qpb.UpdatePointVectors{
			CollectionName: "test_vectors",
			Points:         []*qpb.PointVectors{{}},
		})
		require.Error(t, err)
		_, err = service.UpdateVectors(ctx, &qpb.UpdatePointVectors{
			CollectionName: "test_vectors",
			Points:         []*qpb.PointVectors{{}},
		})
		require.Error(t, err)

		_, err = service.DeleteVectors(ctx, &qpb.DeletePointVectors{})
		require.Error(t, err)
		_, err = noMut.DeleteVectors(ctx, &qpb.DeletePointVectors{
			CollectionName: "test_vectors",
			PointsSelector: &qpb.PointsSelector{},
			Vectors:        &qpb.VectorsSelector{Names: []string{"default"}},
		})
		require.Error(t, err)
		_, err = service.DeleteVectors(ctx, &qpb.DeletePointVectors{
			CollectionName: "test_vectors",
		})
		require.Error(t, err)
		_, err = service.DeleteVectors(ctx, &qpb.DeletePointVectors{
			CollectionName: "test_vectors",
			PointsSelector: &qpb.PointsSelector{},
		})
		require.Error(t, err)
	})

	t.Run("batch/recommend/query/group validation branches", func(t *testing.T) {
		_, err := service.SearchBatch(ctx, &qpb.SearchBatchPoints{})
		require.Error(t, err)
		_, err = service.SearchBatch(ctx, &qpb.SearchBatchPoints{CollectionName: "test_vectors"})
		require.Error(t, err)

		_, err = service.Recommend(ctx, &qpb.RecommendPoints{})
		require.Error(t, err)
		_, err = service.Recommend(ctx, &qpb.RecommendPoints{
			CollectionName: "test_vectors",
			Limit:          uint64(service.config.MaxTopK + 1),
		})
		require.Error(t, err)
		_, err = service.Recommend(ctx, &qpb.RecommendPoints{
			CollectionName: "test_vectors",
			Positive:       nil,
		})
		require.Error(t, err)

		_, err = service.SearchGroups(ctx, &qpb.SearchPointGroups{})
		require.Error(t, err)
		_, err = service.SearchGroups(ctx, &qpb.SearchPointGroups{
			CollectionName: "test_vectors",
		})
		require.Error(t, err)
		_, err = service.SearchGroups(ctx, &qpb.SearchPointGroups{
			CollectionName: "test_vectors",
			Vector:         []float32{1, 0, 0, 0},
		})
		require.Error(t, err)

		_, err = service.Query(ctx, &qpb.QueryPoints{})
		require.Error(t, err)
		_, err = service.Query(ctx, &qpb.QueryPoints{
			CollectionName: "test_vectors",
		})
		require.Error(t, err)
		_, err = service.Query(ctx, &qpb.QueryPoints{
			CollectionName: "test_vectors",
			Query:          &qpb.Query{},
		})
		require.Error(t, err)
	})

	t.Run("allowAccess with checker", func(t *testing.T) {
		blocked := NewPointsService(service.config, collections, nil, service.vecIndex, denyChecker{})
		_, err := blocked.Count(ctx, &qpb.CountPoints{CollectionName: "test_vectors"})
		require.Error(t, err)
	})
}

func TestPointsService_SearchProviderMutationPaths(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	var sharedSearch *search.Service
	service.searchProvider = func(database string, st storage.Engine) (*search.Service, error) {
		_ = database
		if sharedSearch == nil {
			sharedSearch = search.NewServiceWithDimensions(st, 4)
		}
		return sharedSearch, nil
	}

	// Create then update same point to exercise both create/update + searchSvc mutation paths.
	_, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "sp1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	_, err = service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "sp1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{0.9, 0.1, 0, 0}}}},
					},
				},
				Payload: map[string]*qpb.Value{
					"group": {Kind: &qpb.Value_StringValue{StringValue: "g1"}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = service.UpdateVectors(ctx, &qpb.UpdatePointVectors{
		CollectionName: "test_vectors",
		Points: []*qpb.PointVectors{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "sp1"}},
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

	_, err = service.DeleteVectors(ctx, &qpb.DeletePointVectors{
		CollectionName: "test_vectors",
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "sp1"}}}},
			},
		},
		Vectors: &qpb.VectorsSelector{Names: []string{"a"}},
	})
	require.NoError(t, err)

	_, err = service.Delete(ctx, &qpb.DeletePoints{
		CollectionName: "test_vectors",
		Points: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "sp1"}}}},
			},
		},
	})
	require.NoError(t, err)

	// Force search-service path (vecIndex disabled).
	service.vecIndex = nil
	_, err = service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "sp2"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	searchResp, err := service.Search(ctx, &qpb.SearchPoints{
		CollectionName: "test_vectors",
		Vector:         []float32{1, 0, 0, 0},
		Limit:          1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, searchResp.Result)

	// Inject non-collection and chunk-like IDs to exercise searchCollection filtering branches.
	require.NoError(t, sharedSearch.IndexNode(&storage.Node{
		ID:              "other-id",
		NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}},
	}))
	require.NoError(t, sharedSearch.IndexNode(&storage.Node{
		ID:              "qdrant:point:sp-chunk-1",
		NamedEmbeddings: map[string][]float32{"default": {1, 0, 0, 0}},
	}))

	searchResp, err = service.Search(ctx, &qpb.SearchPoints{
		CollectionName: "test_vectors",
		Vector:         []float32{1, 0, 0, 0},
		Limit:          2,
		Offset:         ptrU64(1),
		Filter: &qpb.Filter{
			Must: []*qpb.Condition{
				{
					ConditionOneOf: &qpb.Condition_Field{
						Field: &qpb.FieldCondition{
							Key:   "missing_prop",
							Match: &qpb.Match{MatchValue: &qpb.Match_Keyword{Keyword: "x"}},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

func TestPointsService_MissingCollectionBranches(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)

	_, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "missing_collection",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "m1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
			},
		},
	})
	require.Error(t, err)

	_, err = service.Get(ctx, &qpb.GetPoints{
		CollectionName: "missing_collection",
		Ids:            []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "m1"}}},
	})
	require.Error(t, err)

	_, err = service.Delete(ctx, &qpb.DeletePoints{
		CollectionName: "missing_collection",
		Points: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "m1"}}}},
			},
		},
	})
	require.Error(t, err)

	_, err = service.Count(ctx, &qpb.CountPoints{CollectionName: "missing_collection"})
	require.Error(t, err)

	_, err = service.Search(ctx, &qpb.SearchPoints{
		CollectionName: "missing_collection",
		Vector:         []float32{1, 0, 0, 0},
	})
	require.Error(t, err)

	_, err = service.Scroll(ctx, &qpb.ScrollPoints{
		CollectionName: "missing_collection",
	})
	require.Error(t, err)

	_, err = service.SetPayload(ctx, &qpb.SetPayloadPoints{
		CollectionName: "missing_collection",
		Payload:        map[string]*qpb.Value{"k": {Kind: &qpb.Value_StringValue{StringValue: "v"}}},
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "m1"}}}},
			},
		},
	})
	require.Error(t, err)

	_, err = service.DeletePayload(ctx, &qpb.DeletePayloadPoints{
		CollectionName: "missing_collection",
		Keys:           []string{"k"},
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "m1"}}}},
			},
		},
	})
	require.Error(t, err)

	_, err = service.ClearPayload(ctx, &qpb.ClearPayloadPoints{
		CollectionName: "missing_collection",
		Points: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "m1"}}}},
			},
		},
	})
	require.Error(t, err)

	_, err = service.UpdateVectors(ctx, &qpb.UpdatePointVectors{
		CollectionName: "missing_collection",
		Points: []*qpb.PointVectors{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "m1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
			},
		},
	})
	require.Error(t, err)

	_, err = service.DeleteVectors(ctx, &qpb.DeletePointVectors{
		CollectionName: "missing_collection",
		PointsSelector: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Points{
				Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "m1"}}}},
			},
		},
		Vectors: &qpb.VectorsSelector{Names: []string{"default"}},
	})
	require.Error(t, err)

	_, err = service.SearchBatch(ctx, &qpb.SearchBatchPoints{
		CollectionName: "missing_collection",
		SearchPoints:   []*qpb.SearchPoints{{Vector: []float32{1, 0, 0, 0}, Limit: 1}},
	})
	require.Error(t, err)

	_, err = service.Recommend(ctx, &qpb.RecommendPoints{
		CollectionName: "missing_collection",
		Positive:       []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "m1"}}},
		Limit:          1,
	})
	require.Error(t, err)

	_, err = service.RecommendBatch(ctx, &qpb.RecommendBatchPoints{
		CollectionName: "missing_collection",
		RecommendPoints: []*qpb.RecommendPoints{
			{Positive: []*qpb.PointId{{PointIdOptions: &qpb.PointId_Uuid{Uuid: "m1"}}}, Limit: 1},
		},
	})
	require.Error(t, err)

	_, err = service.SearchGroups(ctx, &qpb.SearchPointGroups{
		CollectionName: "missing_collection",
		Vector:         []float32{1, 0, 0, 0},
		GroupBy:        "g",
	})
	require.Error(t, err)

	_, err = service.CreateFieldIndex(ctx, &qpb.CreateFieldIndexCollection{
		CollectionName: "missing_collection",
		FieldName:      "g",
	})
	require.Error(t, err)

	_, err = service.DeleteFieldIndex(ctx, &qpb.DeleteFieldIndexCollection{
		CollectionName: "missing_collection",
		FieldName:      "g",
	})
	require.Error(t, err)

	_, err = service.Query(ctx, &qpb.QueryPoints{
		CollectionName: "missing_collection",
		Query: &qpb.Query{
			Variant: &qpb.Query_Nearest{
				Nearest: &qpb.VectorInput{
					Variant: &qpb.VectorInput_Dense{
						Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}},
					},
				},
			},
		},
	})
	require.Error(t, err)
}

func TestPointsService_EngineErrorBranches(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	eng := &errEngine{Engine: base}
	store := &fakeCollectionStore{
		engine: eng,
		meta: &CollectionMeta{
			Name:       "err_collection",
			Dimensions: 4,
			Distance:   qpb.Distance_Cosine,
		},
	}
	cfg := DefaultConfig()
	cfg.AllowVectorMutations = true
	svc := NewPointsService(cfg, store, nil, nil, nil)

	eng.batchGetErr = errors.New("batch get failed")
	_, err := svc.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "err_collection",
		Points: []*qpb.PointStruct{
			{
				Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "e1"}},
				Vectors: &qpb.Vectors{
					VectorsOptions: &qpb.Vectors_Vector{
						Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}},
					},
				},
			},
		},
	})
	require.Error(t, err)
	eng.batchGetErr = nil

	eng.getNodesByLabelErr = errors.New("label scan failed")
	_, err = svc.Count(ctx, &qpb.CountPoints{CollectionName: "err_collection"})
	require.Error(t, err)
	_, err = svc.Scroll(ctx, &qpb.ScrollPoints{CollectionName: "err_collection"})
	require.Error(t, err)

	// resolvePointsSelector filter branch error path.
	_, err = svc.Delete(ctx, &qpb.DeletePoints{
		CollectionName: "err_collection",
		Points: &qpb.PointsSelector{
			PointsSelectorOneOf: &qpb.PointsSelector_Filter{
				Filter: &qpb.Filter{},
			},
		},
	})
	require.Error(t, err)
	eng.getNodesByLabelErr = nil

	eng.allNodesErr = errors.New("all nodes failed")
	// Brute fallback path returns empty when AllNodes fails.
	resp, err := svc.Search(ctx, &qpb.SearchPoints{
		CollectionName: "err_collection",
		Vector:         []float32{1, 0, 0, 0},
		Limit:          2,
	})
	require.NoError(t, err)
	require.Empty(t, resp.Result)
}
