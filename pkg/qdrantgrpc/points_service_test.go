package qdrantgrpc

import (
	"context"
	"testing"

	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func setupPointsService(t *testing.T) (*PointsService, CollectionRegistry, storage.Engine) {
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
		return []float32{1, 0, 0, 0}, nil
	}

	err = registry.CreateCollection(ctx, "test_vectors", 4, qpb.Distance_Cosine)
	require.NoError(t, err)

	return NewPointsService(cfg, store, registry, nil, newVectorIndexCache()), registry, store
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

func (r *recordingVectorIndex) dimensions() int         { return r.dim }
func (r *recordingVectorIndex) distance() qpb.Distance  { return r.dist }
func (r *recordingVectorIndex) remove(id string)        { _ = id }
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
			{4.0, 5.0},        // Wrong dimension - should be skipped
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
			{1.0, 2.0, 3.0},   // First vector determines dimension (3)
			{4.0, 5.0},        // Wrong dimension - skipped
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
			{1.0, 2.0, 3.0},   // First vector determines dimension (3)
			{4.0, 5.0},        // Wrong dimension - skipped
			{6.0, 7.0},        // Wrong dimension - skipped
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
			{1.0, 2.0, 3.0},   // First vector determines dimension (3) - included
			{10.0, 20.0},      // Wrong dimension - skipped
			{4.0, 5.0, 6.0},   // Included
		}
		result := averageVectors(vectors)
		require.NotNil(t, result)
		// Should be (1+4)/2, (2+5)/2, (3+6)/2 = [2.5, 3.5, 4.5]
		// NOT (1+4)/3, (2+5)/3, (3+6)/3 = [1.67, 2.33, 3.0] (wrong!)
		assert.Equal(t, []float32{2.5, 3.5, 4.5}, result)
	})
}
