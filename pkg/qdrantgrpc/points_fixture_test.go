package qdrantgrpc

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"testing"
)

type pointsFixturePurpose uint8

const (
	pointsFixtureEmptyVectors pointsFixturePurpose = iota
	pointsFixtureSeededPayloads
)

// newPointsServiceFixture requires the intended population: the query embedding,
// collection identity and initial records are part of each fixture's contract.
func newPointsServiceFixture(t *testing.T, purpose pointsFixturePurpose) (*PointsService, CollectionStore, storage.Engine) {
	t.Helper()
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	dbm, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbm.Close() })
	collections, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	cfg := DefaultConfig()
	cfg.AllowVectorMutations = true
	var collection string
	var embedding []float32
	switch purpose {
	case pointsFixtureEmptyVectors:
		collection = "test_vectors"
		embedding = []float32{1, 0, 0, 0}
	case pointsFixtureSeededPayloads:
		collection = "test_collection"
		embedding = []float32{0.5, 0.5, 0, 0}
	default:
		t.Fatalf("unknown points fixture purpose %d", purpose)
	}
	cfg.EmbedQuery = func(context.Context, string) ([]float32, error) { return append([]float32(nil), embedding...), nil }
	require.NoError(t, collections.Create(ctx, collection, 4, qpb.Distance_Cosine))
	svc := NewPointsService(cfg, collections, sharedSearchProvider(), nil)
	if purpose == pointsFixtureSeededPayloads {
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

	}
	return svc, collections, base
}
