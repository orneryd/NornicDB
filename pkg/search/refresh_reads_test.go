package search

import (
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// countingReadEngine counts node reads.
type countingReadEngine struct {
	storage.Engine
	reads atomic.Int32
}

func (e *countingReadEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	e.reads.Add(1)
	return e.Engine.GetNode(id)
}

func newCountingService(t *testing.T) (*Service, *countingReadEngine, storage.Engine) {
	t.Helper()
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	engine := &countingReadEngine{Engine: store}
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	return svc, engine, store
}

// A refresh whose node has no pending reservation (a newer refresh, which
// read storage later, was applied) reads nothing. One superseded by a newer
// reservation still pending reads: discarding the newer one restores it.
func TestRetiredRefreshReadsNothing(t *testing.T) {
	svc, engine, store := newCountingService(t)
	_, err := store.CreateNode(&storage.Node{ID: "a", Properties: map[string]any{"content": "word"}})
	require.NoError(t, err)

	retired := svc.ReserveNodeMutation("a")
	require.NoError(t, svc.RefreshNode("a"))
	before := engine.reads.Load()
	require.NoError(t, svc.RefreshNodeWithMutation(retired))
	require.Equal(t, before, engine.reads.Load(), "a retired refresh read storage")

	superseded := svc.ReserveNodeMutation("a")
	pending := svc.ReserveNodeMutation("a")
	before = engine.reads.Load()
	require.NoError(t, svc.RefreshNodeWithMutation(superseded))
	require.Equal(t, before+1, engine.reads.Load(), "a refresh with a newer pending one must still read")
	require.NoError(t, svc.RefreshNodeWithMutation(pending))
}
