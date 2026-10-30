package qdrantgrpc

import (
	"context"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

// consumerBarrierCollections opens each collection behind the shared
// post-success update barrier, so one armed flag covers every engine the
// collection store returns. No goroutine stacks are inspected.
type consumerBarrierCollections struct {
	CollectionStore
	barrier *storagefault.PostUpdateBarrier
}

func (c *consumerBarrierCollections) Open(ctx context.Context, name string) (storage.Engine, *CollectionMeta, error) {
	engine, meta, err := c.CollectionStore.Open(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	return c.barrier.PauseUpdateAfterCommit(engine), meta, nil
}

func consumerUpsert(id *qpb.PointId, vector []float32) *qpb.UpsertPoints {
	return &qpb.UpsertPoints{CollectionName: "test_vectors", Points: []*qpb.PointStruct{{Id: id, Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Vector: &qpb.Vector_Dense{Dense: &qpb.DenseVector{Data: vector}}}}}}}}
}

func setupConsumerPoints(t *testing.T) (*PointsService, *consumerBarrierCollections, *search.Service, storage.Engine) {
	t.Helper()
	ctx := context.Background()
	points, collections, base := newPointsServiceFixture(t, pointsFixtureEmptyVectors)
	t.Cleanup(func() { _ = base.Close() })
	store, _, err := collections.Open(ctx, "test_vectors")
	require.NoError(t, err)
	svc := search.NewService(store, 4, nil)
	t.Cleanup(func() { _ = svc.Close() })
	barrier := &consumerBarrierCollections{CollectionStore: collections, barrier: storagefault.NewPostUpdateBarrier()}
	t.Cleanup(barrier.barrier.ReleaseWrites)
	points.collections = barrier
	points.searchProvider = func(string, storage.Engine) (*search.Service, error) { return svc, nil }
	return points, barrier, svc, store
}

func (c *consumerBarrierCollections) wait(t *testing.T) {
	t.Helper()
	runtimeFixtureOwner.WaitForBarrier(t, c.barrier.Entered, 5*time.Second, c.barrier.ReleaseWrites, "upsert did not reach its barrier")
}

// An upsert's late index call must not resurrect a point deleted after its
// storage write.
func TestQdrantUpsertIndexCannotResurrectDelete(t *testing.T) {
	ctx, points, barrier, svc, store, pid, done := pauseConsumerUpsert(t, 73, func(_ context.Context, svc *search.Service) {
		require.Positive(t, svc.EmbeddingCount())
	})
	_, err := points.Delete(ctx, &qpb.DeletePoints{CollectionName: "test_vectors", Points: &qpb.PointsSelector{PointsSelectorOneOf: &qpb.PointsSelector_Points{Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{pid}}}}})
	require.NoError(t, err)
	barrier.barrier.ReleaseWrites()
	require.NoError(t, <-done)
	_, err = store.GetNode(pointIDToNodeID(pid))
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.Zero(t, svc.EmbeddingCount(), "an upsert's late index call resurrected a deleted point")
}

// An upsert's late index call must not replace a newer upsert's vector.
func TestQdrantUpsertIndexCannotOverwriteNewerUpsert(t *testing.T) {
	ctx, points, barrier, svc, store, pid, done := pauseConsumerUpsert(t, 74, func(ctx context.Context, svc *search.Service) {
		control, err := svc.VectorSearchCandidates(ctx, []float32{1, 0, 0, 0}, search.DefaultSearchOptions())
		require.NoError(t, err)
		require.NotEmpty(t, control, "harness: an upserted vector is searchable")
	})
	_, err := points.Upsert(ctx, consumerUpsert(pid, []float32{0, 0, 1, 0}))
	require.NoError(t, err)
	barrier.barrier.ReleaseWrites()
	require.NoError(t, <-done)
	stored, err := store.GetNode(pointIDToNodeID(pid))
	require.NoError(t, err)
	require.NotNil(t, stored)
	candidates, err := svc.VectorSearchCandidates(ctx, []float32{0, 0, 1, 0}, search.DefaultSearchOptions())
	require.NoError(t, err)
	require.NotEmpty(t, candidates, "the newer stored vector is not in the index: an older upsert replaced it")
	require.InDelta(t, 1.0, candidates[0].Score, 1e-3, "an older upsert's vector replaced the newer stored vector")
}

// pauseConsumerUpsert owns the shared seed, arm, worker and wait schedule.
// The optional control runs before arming so searchability remains a precondition.
func pauseConsumerUpsert(t *testing.T, id uint64, control func(context.Context, *search.Service)) (context.Context, *PointsService, *consumerBarrierCollections, *search.Service, storage.Engine, *qpb.PointId, <-chan error) {
	t.Helper()
	ctx := context.Background()
	points, barrier, svc, store := setupConsumerPoints(t)
	pid := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: id}}
	_, err := points.Upsert(ctx, consumerUpsert(pid, []float32{1, 0, 0, 0}))
	require.NoError(t, err)
	if control != nil {
		control(ctx, svc)
	}
	barrier.barrier.Armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := points.Upsert(ctx, consumerUpsert(pid, []float32{0, 1, 0, 0}))
		done <- err
	}()
	barrier.wait(t)
	return ctx, points, barrier, svc, store, pid, done
}
