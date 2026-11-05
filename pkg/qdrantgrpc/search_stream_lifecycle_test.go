package qdrantgrpc

import (
	"context"
	"testing"

	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

// Standard Qdrant scroll and numeric-offset search do not allocate durable qids.
func TestSearchStreamLifecycleQdrantStatelessPaging(t *testing.T) {
	service, _, base := newPointsServiceFixture(t, pointsFixtureEmptyVectors)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	ctx := context.Background()
	points := make([]*qpb.PointStruct, 3)
	for i := range points {
		points[i] = &qpb.PointStruct{Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: uint64(i + 1)}}, Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: []float32{1, 0, 0, 0}}}}}}}
	}
	_, err := service.Upsert(ctx, &qpb.UpsertPoints{CollectionName: "test_vectors", Points: points})
	require.NoError(t, err)
	for i := 0; i < 70; i++ {
		limit := uint32(1)
		var offset *qpb.PointId
		count := 0
		for {
			page, err := service.Scroll(ctx, &qpb.ScrollPoints{CollectionName: "test_vectors", Limit: &limit, Offset: offset})
			require.NoError(t, err)
			count += len(page.Result)
			offset = page.NextPageOffset
			if offset == nil {
				break
			}
		}
		require.Equal(t, 3, count)
		for offset := uint64(0); offset < 4; offset++ {
			page, err := service.Search(ctx, &qpb.SearchPoints{CollectionName: "test_vectors", Vector: []float32{1, 0, 0, 0}, Limit: 1, Offset: &offset})
			require.NoError(t, err)
			if offset < 3 {
				require.Len(t, page.Result, 1)
			} else {
				require.Empty(t, page.Result)
			}
		}
	}
}
