package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

// Every vector of a point reaches the shared search index as an entry of the
// point's node. A client-chosen vector name that contains a legacy entry-key
// separator ("a-named-b", "p-chunk-1") must not turn that entry into another
// point: the search-service path returns only the owning point.
func TestPointsService_SearchServiceReturnsOwningPointForAnyVectorName(t *testing.T) {
	ctx := context.Background()
	service, _, _ := setupPointsService(t)
	var shared *search.Service
	service.searchProvider = func(_ string, st storage.Engine) (*search.Service, error) {
		if shared == nil {
			shared = search.NewServiceWithDimensions(st, 4)
		}
		return shared, nil
	}

	dense := func(values ...float32) *qpb.Vector {
		return &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: values}}}
	}
	_, err := service.Upsert(ctx, &qpb.UpsertPoints{
		CollectionName: "test_vectors",
		Points: []*qpb.PointStruct{{
			Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 7}},
			Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vectors{Vectors: &qpb.NamedVectors{
				Vectors: map[string]*qpb.Vector{
					"default":   dense(0, 0, 0, 1),
					"a-named-b": dense(1, 0, 0, 0),
					"p-chunk-1": dense(0, 1, 0, 0),
				},
			}}},
		}},
	})
	require.NoError(t, err)

	for _, query := range [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 0, 1}} {
		response, err := service.Search(ctx, &qpb.SearchPoints{CollectionName: "test_vectors", Vector: query, Limit: 5})
		require.NoError(t, err)
		require.NotEmpty(t, response.Result, "query %v", query)
		for _, point := range response.Result {
			require.Equal(t, uint64(7), point.GetId().GetNum(), "query %v returned point %v", query, point.GetId())
		}
	}
}
