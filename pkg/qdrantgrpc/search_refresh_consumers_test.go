package qdrantgrpc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

// consumerPointWriteBarrier pauses the first armed point update after its commit,
// before the upsert indexes its payload. No goroutine stacks are inspected.
type consumerPointWriteBarrier struct {
	storage.Engine
	armed   *atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (b *consumerPointWriteBarrier) UpdateNode(node *storage.Node) error {
	err := b.Engine.UpdateNode(node)
	if err == nil && b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		<-b.release
	}
	return err
}

type consumerBarrierCollections struct {
	CollectionStore
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (c *consumerBarrierCollections) Open(ctx context.Context, name string) (storage.Engine, *CollectionMeta, error) {
	engine, meta, err := c.CollectionStore.Open(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	return &consumerPointWriteBarrier{Engine: engine, armed: &c.armed, entered: c.entered, release: c.release}, meta, nil
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
	barrier := &consumerBarrierCollections{CollectionStore: collections, entered: make(chan struct{}), release: make(chan struct{})}
	points.collections = barrier
	points.searchProvider = func(string, storage.Engine) (*search.Service, error) { return svc, nil }
	return points, barrier, svc, store
}

func (c *consumerBarrierCollections) wait(t *testing.T) {
	t.Helper()
	select {
	case <-c.entered:
	case <-time.After(5 * time.Second):
		close(c.release)
		t.Fatal("upsert did not reach its barrier")
	}
}

// An upsert's late index call must not resurrect a point deleted after its
// storage write.
func TestQdrantUpsertIndexCannotResurrectDelete(t *testing.T) {
	ctx := context.Background()
	points, barrier, svc, store := setupConsumerPoints(t)
	pid := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 73}}
	_, err := points.Upsert(ctx, consumerUpsert(pid, []float32{1, 0, 0, 0}))
	require.NoError(t, err)
	require.Positive(t, svc.EmbeddingCount())
	barrier.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := points.Upsert(ctx, consumerUpsert(pid, []float32{0, 1, 0, 0}))
		done <- err
	}()
	barrier.wait(t)
	_, err = points.Delete(ctx, &qpb.DeletePoints{CollectionName: "test_vectors", Points: &qpb.PointsSelector{PointsSelectorOneOf: &qpb.PointsSelector_Points{Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{pid}}}}})
	require.NoError(t, err)
	close(barrier.release)
	require.NoError(t, <-done)
	_, err = store.GetNode(pointIDToNodeID(pid))
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.Zero(t, svc.EmbeddingCount(), "an upsert's late index call resurrected a deleted point")
}

// An upsert's late index call must not replace a newer upsert's vector.
func TestQdrantUpsertIndexCannotOverwriteNewerUpsert(t *testing.T) {
	ctx := context.Background()
	points, barrier, svc, store := setupConsumerPoints(t)
	pid := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 74}}
	_, err := points.Upsert(ctx, consumerUpsert(pid, []float32{1, 0, 0, 0}))
	require.NoError(t, err)
	control, err := svc.VectorSearchCandidates(ctx, []float32{1, 0, 0, 0}, search.DefaultSearchOptions())
	require.NoError(t, err)
	require.NotEmpty(t, control, "harness: an upserted vector is searchable")
	barrier.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := points.Upsert(ctx, consumerUpsert(pid, []float32{0, 1, 0, 0}))
		done <- err
	}()
	barrier.wait(t)
	_, err = points.Upsert(ctx, consumerUpsert(pid, []float32{0, 0, 1, 0}))
	require.NoError(t, err)
	close(barrier.release)
	require.NoError(t, <-done)
	stored, err := store.GetNode(pointIDToNodeID(pid))
	require.NoError(t, err)
	require.NotNil(t, stored)
	candidates, err := svc.VectorSearchCandidates(ctx, []float32{0, 0, 1, 0}, search.DefaultSearchOptions())
	require.NoError(t, err)
	require.NotEmpty(t, candidates, "the newer stored vector is not in the index: an older upsert replaced it")
	require.InDelta(t, 1.0, candidates[0].Score, 1e-3, "an older upsert's vector replaced the newer stored vector")
}
