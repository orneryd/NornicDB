package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

// A collection's metadata read back from storage carries its numbers as the
// compact integer type that fits them (int8 for 4 dimensions), not the int it
// was written with; it still reads as the same collection.
func TestReadCollectionMeta_CompactIntegerTypes(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "c")
	_, err := engine.CreateNode(&storage.Node{ID: collectionMetaNodeID, Labels: []string{collectionMetaNodeLabel},
		Properties: map[string]any{"dimensions": int8(4), "distance": int8(qpb.Distance_Dot)}})
	require.NoError(t, err)

	meta, err := readCollectionMeta(engine)
	require.NoError(t, err)
	require.Equal(t, 4, meta.Dimensions)
	require.Equal(t, qpb.Distance_Dot, meta.Distance, "not the Cosine default")
}

// A collection stays open past the point where its metadata node leaves the
// storage node cache and is decoded from disk (about 10,000 points).
func TestUpsert_CollectionStaysOpenPastTenThousandPoints(t *testing.T) {
	ctx := context.Background()
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Close() })
	dbm, err := multidb.NewDatabaseManager(be, nil)
	require.NoError(t, err)
	collections, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	cfg := DefaultConfig()
	cfg.AllowVectorMutations = true
	svc := NewPointsService(cfg, collections, sharedSearchProvider(), nil)
	require.NoError(t, collections.Create(ctx, "large", 4, qpb.Distance_Dot))

	for batch := 0; batch < 12; batch++ {
		points := make([]*qpb.PointStruct, 0, 1000)
		for id := batch * 1000; id < (batch+1)*1000; id++ {
			points = append(points, &qpb.PointStruct{Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: uint64(id)}},
				Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: []float32{1, float32(id), 0, 0}}}}})
		}
		_, err := svc.Upsert(ctx, &qpb.UpsertPoints{CollectionName: "large", Points: points})
		require.NoError(t, err, "batch %d", batch)
	}
	meta, err := collections.GetMeta(ctx, "large")
	require.NoError(t, err)
	require.Equal(t, qpb.Distance_Dot, meta.Distance)
}
