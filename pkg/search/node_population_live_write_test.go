package search

import (
	"context"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staleBatchEngine reads the population's batch, then holds it until release,
// so the batch is applied after any live write made in between.
type staleBatchEngine struct {
	*storage.MemoryEngine
	entered, release chan struct{}
	once             sync.Once
}

func (e *staleBatchEngine) IterateNodes(fn func(*storage.Node) bool) error {
	nodes, err := e.MemoryEngine.AllNodes()
	if err != nil {
		return err
	}
	snapshot := make([]*storage.Node, 0, len(nodes))
	for _, n := range nodes {
		copied := *n
		copied.Properties = maps.Clone(n.Properties)
		copied.NamedEmbeddings = maps.Clone(n.NamedEmbeddings)
		snapshot = append(snapshot, &copied)
	}
	e.once.Do(func() { close(e.entered) })
	<-e.release
	for _, n := range snapshot {
		if !fn(n) {
			break
		}
	}
	return nil
}

func TestNodePopulationReplaysLiveWritesOverOlderBatches(t *testing.T) {
	engine := &staleBatchEngine{MemoryEngine: storage.NewMemoryEngine(), entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	a := admissionNode("nornic:a", "oldterm", []float32{1, 0, 0}, "named")
	_, err := engine.CreateNode(a)
	require.NoError(t, err)
	gone := admissionNode("nornic:gone", "goneterm", []float32{0, 0, 1}, "named")
	_, err = engine.CreateNode(gone)
	require.NoError(t, err)
	svc := NewServiceWithDimensions(engine, 3)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(engine.release) }) }
	defer release()

	buildDone := make(chan error, 1)
	go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-engine.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("population did not read its batch")
	}
	updated := admissionNode("nornic:a", "newterm", []float32{0, 1, 0}, "named")
	require.NoError(t, engine.UpdateNode(updated))
	require.NoError(t, engine.DeleteNode(gone.ID))
	writes := make(chan error, 1)
	go func() {
		if err := svc.IndexNode(updated); err != nil {
			writes <- err
			return
		}
		writes <- svc.RemoveNode(gone.ID)
	}()
	select {
	case err := <-writes:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("live writes waited for the NODE population")
	}
	release()
	require.NoError(t, <-buildDone)
	require.True(t, svc.IsReady())

	ctx := context.Background()
	for term, want := range map[string]int{"newterm": 1, "oldterm": 0, "goneterm": 0} {
		resp, err := svc.Search(ctx, term, nil, &SearchOptions{Limit: 10})
		require.NoError(t, err)
		assert.Len(t, resp.Results, want, term)
	}
	hits, err := svc.VectorQueryNodes(ctx, []float32{0, 1, 0}, VectorQuerySpec{Label: "Doc", Property: "v", Similarity: "dot", Limit: 10})
	require.NoError(t, err)
	require.Len(t, hits, 1, "the stale batch must not restore the old vector or the deleted node")
	assert.Equal(t, "nornic:a", hits[0].ID)
	assert.InDelta(t, 1, hits[0].Score, 1e-6)
	svc.indexMu.RLock()
	assert.Empty(t, svc.nodePopulationWrites)
	svc.indexMu.RUnlock()
}

func TestNodePopulationCancelledBeforeStartKeepsServingSources(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	a := admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named")
	_, err := engine.CreateNode(a)
	require.NoError(t, err)
	svc := NewServiceWithDimensions(engine, 3)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.True(t, svc.IsReady())

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, svc.BuildIndexes(cancelled), context.Canceled)
	// Nothing was replaced, so the complete indexes keep answering.
	assert.True(t, svc.IsReady())
	svc.indexMu.RLock()
	assert.True(t, svc.nodeSourcesCompleteLocked())
	svc.indexMu.RUnlock()
	resp, err := svc.Search(context.Background(), "aterm", nil, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)

	b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
	_, err = engine.CreateNode(b)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(b))
	svc.indexMu.RLock()
	assert.Empty(t, svc.nodePopulationWrites, "writes to complete sources need no replay")
	svc.indexMu.RUnlock()
}
