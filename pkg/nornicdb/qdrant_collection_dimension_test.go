package nornicdb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/qdrantgrpc"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A database with embeddings keeps its search service at the embedding
// dimension. A Qdrant collection of another dimension, written through the
// database's own search-service provider with vector writes allowed, has its
// points rejected before they are stored: no point is left stored but not
// indexed, and the service keeps its dimension.
func TestQdrantCollectionOfOtherDimensionRejectsPointsBeforeStoring(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.Memory.EmbeddingEnabled = true
	cfg.Memory.EmbeddingDimensions = 8
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	defer db.Close()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer base.Close()
	manager, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	collections, err := qdrantgrpc.NewDatabaseCollectionStore(manager, nil)
	require.NoError(t, err)
	_, err = qdrantgrpc.NewCollectionsService(collections, nil).Create(ctx, &qpb.CreateCollection{
		CollectionName: "four",
		VectorsConfig:  &qpb.VectorsConfig{Config: &qpb.VectorsConfig_Params{Params: &qpb.VectorParams{Size: 4, Distance: qpb.Distance_Cosine}}},
	})
	require.NoError(t, err)
	config := qdrantgrpc.DefaultConfig()
	config.AllowVectorMutations = true
	points := qdrantgrpc.NewPointsService(config, collections, db.GetOrCreateSearchService, nil)

	id := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 1}}
	_, err = points.Upsert(ctx, &qpb.UpsertPoints{CollectionName: "four", Points: []*qpb.PointStruct{{
		Id:      id,
		Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: []float32{1, 0, 0, 0}}}},
	}}})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)

	got, err := points.Get(ctx, &qpb.GetPoints{CollectionName: "four", Ids: []*qpb.PointId{id}})
	require.NoError(t, err)
	require.Empty(t, got.GetResult(), "the rejected Upsert must not store the point")
	store, _, err := collections.Open(ctx, "four")
	require.NoError(t, err)
	svc, err := db.GetOrCreateSearchService("four", store)
	require.NoError(t, err)
	require.Equal(t, 8, svc.VectorIndexDimensions())
	require.Zero(t, svc.EmbeddingCount())
}
