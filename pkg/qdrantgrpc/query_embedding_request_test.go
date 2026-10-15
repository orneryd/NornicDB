package qdrantgrpc

import (
	"context"
	"testing"

	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

func TestPointsService_NumericAndIDQueriesDoNotBindEmbedding(t *testing.T) {
	service, _, base := setupPointsService(t)
	t.Cleanup(func() { _ = base.Close() })
	ctx := context.Background()
	id := &qpb.PointId{PointIdOptions: &qpb.PointId_Uuid{Uuid: "snapshot-point"}}
	dense := []float32{1, 0, 0, 0}
	_, err := service.Upsert(ctx, &qpb.UpsertPoints{CollectionName: "test_vectors", Points: []*qpb.PointStruct{{
		Id: id, Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: dense}}},
	}}})
	require.NoError(t, err)
	service.config.BindQueryEmbedding = func(context.Context) context.Context {
		t.Fatal("numeric and ID queries must not bind an embedding provider")
		return ctx
	}
	service.config.EmbedQuery = func(context.Context, string) ([]float32, error) {
		t.Fatal("numeric and ID queries must not embed text")
		return nil, nil
	}
	for _, input := range []*qpb.VectorInput{
		{Variant: &qpb.VectorInput_Dense{Dense: &qpb.DenseVector{Data: dense}}},
		{Variant: &qpb.VectorInput_Id{Id: id}},
	} {
		query := &qpb.QueryPoints{CollectionName: "test_vectors", Query: &qpb.Query{Variant: &qpb.Query_Nearest{Nearest: input}}}
		_, err := service.Query(ctx, query)
		require.NoError(t, err)
		_, err = service.QueryBatch(ctx, &qpb.QueryBatchPoints{QueryPoints: []*qpb.QueryPoints{query}})
		require.NoError(t, err)
	}
	for _, recommend := range []*qpb.RecommendPoints{
		{CollectionName: "test_vectors", Positive: []*qpb.PointId{id}},
		{CollectionName: "test_vectors", PositiveVectors: []*qpb.Vector{{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: dense}}}}},
		// Legacy dense data takes precedence over inference fields.
		{CollectionName: "test_vectors", PositiveVectors: []*qpb.Vector{{Data: dense, Vector: &qpb.Vector_Document{Document: &qpb.Document{Text: "ignored"}}}}},
	} {
		_, err := service.Recommend(ctx, recommend)
		require.NoError(t, err)
		_, err = service.RecommendBatch(ctx, &qpb.RecommendBatchPoints{CollectionName: "test_vectors", RecommendPoints: []*qpb.RecommendPoints{nil, recommend}})
		require.NoError(t, err)
	}
}
