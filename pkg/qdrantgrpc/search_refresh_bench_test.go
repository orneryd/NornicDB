package qdrantgrpc

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
)

// An upsert of an existing point writes storage and publishes it to search.
func BenchmarkSearchRefreshQdrantUpsert(b *testing.B) {
	ctx := context.Background()
	base := runtimeFixtureOwner.Track(b, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	b.Cleanup(func() { _ = base.Close() })
	dbm, err := multidb.NewDatabaseManager(base, nil)
	if err != nil {
		b.Fatal(err)
	}
	collections, err := NewDatabaseCollectionStore(dbm, nil)
	if err != nil {
		b.Fatal(err)
	}
	if err := collections.Create(ctx, "test_vectors", 4, qpb.Distance_Cosine); err != nil {
		b.Fatal(err)
	}
	store, _, err := collections.Open(ctx, "test_vectors")
	if err != nil {
		b.Fatal(err)
	}
	svc := search.NewServiceWithDimensions(store, 4)
	b.Cleanup(func() { _ = svc.Close() })
	cfg := DefaultConfig()
	cfg.AllowVectorMutations = true
	points := NewPointsService(cfg, collections, func(string, storage.Engine) (*search.Service, error) { return svc, nil }, nil)
	pid := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 1}}
	vectors := [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}}
	upsert := func(vector []float32) *qpb.UpsertPoints {
		return &qpb.UpsertPoints{CollectionName: "test_vectors", Points: []*qpb.PointStruct{{Id: pid, Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: vector}}}}}}}}
	}
	if _, err := points.Upsert(ctx, upsert(vectors[0])); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := points.Upsert(ctx, upsert(vectors[i%2])); err != nil {
			b.Fatal(err)
		}
	}
}
