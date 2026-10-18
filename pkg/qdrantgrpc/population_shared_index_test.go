package qdrantgrpc

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

type heldPointDeletion struct {
	storage.Engine
	suffix  string
	entered chan struct{}
	resume  chan struct{}
}

func (e *heldPointDeletion) DeleteNode(id storage.NodeID) error {
	if strings.HasSuffix(string(id), e.suffix) {
		close(e.entered)
		<-e.resume
	}
	return e.Engine.DeleteNode(id)
}

type heldPointPopulation struct {
	storage.Engine
	hold     atomic.Bool
	captured chan struct{}
	resume   chan struct{}
}

func (e *heldPointPopulation) IterateNodes(fn func(*storage.Node) bool) error {
	nodes, err := e.Engine.AllNodes()
	if err != nil {
		return err
	}
	if e.hold.Load() {
		close(e.captured)
		<-e.resume
	}
	for _, node := range nodes {
		if !fn(node) {
			break
		}
	}
	return nil
}

// Population may begin while the durable deletion is still in flight. The RPC
// must not acknowledge until the final index removal follows that population.
func TestSharedIndexDeleteOrdersStorageBeforePopulationBoundary(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id := &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 1}}
	memory := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, memory.Close()) })
	base := &heldPointDeletion{Engine: memory, suffix: string(pointIDToNodeID(id)), entered: make(chan struct{}), resume: make(chan struct{})}
	dbm, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	var providerMu sync.Mutex
	var svc *search.Service
	var population *heldPointPopulation
	provider := func(_ string, store storage.Engine) (*search.Service, error) {
		providerMu.Lock()
		defer providerMu.Unlock()
		if svc == nil {
			population = &heldPointPopulation{Engine: store, captured: make(chan struct{}), resume: make(chan struct{})}
			svc = search.NewServiceWithDimensions(population, 4)
			if err := svc.BuildIndexes(ctx); err != nil {
				return nil, err
			}
		}
		return svc, nil
	}
	srv := startSharedIndexServer(t, dbm, base, provider)
	createCosineCollection(t, ctx, srv.collections, "population", 4)
	_, err = srv.points.Upsert(ctx, &qpb.UpsertPoints{CollectionName: "population", Points: []*qpb.PointStruct{
		{Id: id, Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: []float32{1, 0, 0, 0}}}}},
		{Id: &qpb.PointId{PointIdOptions: &qpb.PointId_Num{Num: 2}}, Vectors: &qpb.Vectors{VectorsOptions: &qpb.Vectors_Vector{Vector: &qpb.Vector{Data: []float32{0.8, 0.6, 0, 0}}}}},
	}})
	require.NoError(t, err)
	providerMu.Lock()
	boundService, snapshot := svc, population
	providerMu.Unlock()
	t.Cleanup(func() { require.NoError(t, boundService.Close()) })
	var storageOnce, populationOnce sync.Once
	releaseStorage := func() { storageOnce.Do(func() { close(base.resume) }) }
	releasePopulation := func() { populationOnce.Do(func() { close(snapshot.resume) }) }
	defer releaseStorage()
	defer releasePopulation()
	snapshot.hold.Store(true)
	deleted := make(chan error, 1)
	go func() {
		_, err := srv.points.Delete(ctx, &qpb.DeletePoints{CollectionName: "population", Points: &qpb.PointsSelector{PointsSelectorOneOf: &qpb.PointsSelector_Points{Points: &qpb.PointsIdsList{Ids: []*qpb.PointId{id}}}}})
		deleted <- err
	}()
	select {
	case <-base.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	built := make(chan error, 1)
	go func() { built <- boundService.BuildIndexes(ctx) }()
	select {
	case <-snapshot.captured:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	releaseStorage()
	// The old order can acknowledge here; the corrected order waits on population.
	select {
	case err := <-deleted:
		require.NoError(t, err)
		deleted <- nil
	case <-time.After(50 * time.Millisecond):
	}
	releasePopulation()
	require.NoError(t, <-built)
	require.NoError(t, <-deleted)
	require.Equal(t, 1, boundService.EmbeddingCount(), "an acknowledged delete must not be restored by population")
	result, err := srv.points.Search(ctx, &qpb.SearchPoints{CollectionName: "population", Vector: []float32{1, 0, 0, 0}, Limit: 1})
	require.NoError(t, err)
	require.Len(t, result.Result, 1)
	require.Equal(t, uint64(2), result.Result[0].Id.GetNum())
}
