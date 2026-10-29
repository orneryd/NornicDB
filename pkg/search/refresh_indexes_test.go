package search

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// refreshIndexesRecorder is a scope that records what it was asked to prepare
// and answers with a fixed service, deferral or error.
type refreshIndexesRecorder struct {
	mu       sync.Mutex
	requests []RefreshRequest
	svc      *Service
	deferred bool
	err      error
}

func (r *refreshIndexesRecorder) PrepareSearchRefresh(_ context.Context, request RefreshRequest) (*Service, bool, error) {
	r.mu.Lock()
	r.requests = append(r.requests, request)
	r.mu.Unlock()
	return r.svc, r.deferred, r.err
}

func (r *refreshIndexesRecorder) recorded() []RefreshRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RefreshRequest(nil), r.requests...)
}

// refreshIndexesPartialNodeRead fails the node read of "gone" so the operation
// can be observed applying the relationship after a failed node refresh.
type refreshIndexesPartialNodeRead struct {
	storage.Engine
	nodeErr error
}

func (e *refreshIndexesPartialNodeRead) GetNode(id storage.NodeID) (*storage.Node, error) {
	if id == "gone" {
		return nil, e.nodeErr
	}
	return e.Engine.GetNode(id)
}

// refreshIndexesPartialEdgeRead fails the read of relationship "r".
type refreshIndexesPartialEdgeRead struct {
	storage.Engine
	edgeErr error
}

func (e *refreshIndexesPartialEdgeRead) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if id == "r" {
		return nil, e.edgeErr
	}
	return e.Engine.GetEdge(id)
}

func newRefreshIndexesStore(t *testing.T) storage.Engine {
	t.Helper()
	base := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = base.Close() })
	return storage.NewNamespacedEngine(base, "test")
}

func TestRefreshIndexes_EmptyRequestResolvesNoService(t *testing.T) {
	scope := &refreshIndexesRecorder{err: errors.New("scope must not be asked")}
	require.NoError(t, RefreshIndexes(context.Background(), scope, RefreshRequest{}))
	require.NoError(t, RefreshIndexes(context.Background(), scope, RefreshRequest{PostCommit: true}))
	require.Empty(t, scope.recorded(), "an empty request resolved a service")
}

func TestRefreshIndexes_PropagatesPrepareErrorWithoutRefreshing(t *testing.T) {
	ctx := context.Background()
	store := newRefreshIndexesStore(t)
	_, err := store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"content": "refreshword"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "b", EndNode: "b", Properties: map[string]any{"vec": []float64{1, 0}}}))
	svc := NewService(store, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })

	failure := errors.New("resolver unavailable")
	scope := &refreshIndexesRecorder{svc: svc, err: failure}
	require.EqualError(t, RefreshIndexes(ctx, scope, RefreshRequest{Node: "b", Edge: "r"}), failure.Error())
	require.Len(t, scope.recorded(), 1, "the operation retried a failed prepare")
	require.Zero(t, svc.FulltextDocCount(), "a failed prepare still refreshed the node")
	require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "a failed prepare still refreshed the relationship")
}

func TestRefreshIndexes_DeferredRequestRefreshesNothing(t *testing.T) {
	ctx := context.Background()
	store := newRefreshIndexesStore(t)
	_, err := store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"content": "refreshword"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "b", EndNode: "b", Properties: map[string]any{"vec": []float64{1, 0}}}))
	svc := NewService(store, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })

	scope := &refreshIndexesRecorder{svc: svc, deferred: true}
	require.NoError(t, RefreshIndexes(ctx, scope, RefreshRequest{Node: "b", Edge: "r"}))
	require.Zero(t, svc.FulltextDocCount(), "a deferred request refreshed the node")
	require.False(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "a deferred request refreshed the relationship")
	require.Len(t, scope.recorded(), 1)

	// A deferral without a service is the same: the deferring owner refreshes.
	scope = &refreshIndexesRecorder{deferred: true}
	require.NoError(t, RefreshIndexes(ctx, scope, RefreshRequest{Node: "b"}))
	require.Len(t, scope.recorded(), 1)
}

// A node refresh that fails does not skip the relationship refresh, and the
// node's error is the operation's error.
func TestRefreshIndexes_FailedNodeStillRefreshesEdgeAndReturnsNodeError(t *testing.T) {
	ctx := context.Background()
	store := newRefreshIndexesStore(t)
	_, err := store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"content": "refreshword"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "b", EndNode: "b", Properties: map[string]any{"vec": []float64{1, 0}}}))

	nodeErr := errors.New("node read failed")
	svc := NewService(&refreshIndexesPartialNodeRead{Engine: store, nodeErr: nodeErr}, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.EqualError(t, RefreshIndexes(ctx, &refreshIndexesRecorder{svc: svc}, RefreshRequest{Node: "gone", Edge: "r"}), nodeErr.Error())
	require.True(t, svc.HasRelationshipVectorEntries("CITES", "vec"), "a failed node refresh skipped the relationship")
}

// With a successful node refresh, the relationship's failure is reported.
func TestRefreshIndexes_FailedEdgeReportedWhenNodeSucceeds(t *testing.T) {
	ctx := context.Background()
	store := newRefreshIndexesStore(t)
	_, err := store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"content": "refreshword"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "r", Type: "CITES", StartNode: "b", EndNode: "b", Properties: map[string]any{"vec": []float64{1, 0}}}))

	edgeErr := errors.New("edge read failed")
	svc := NewService(&refreshIndexesPartialEdgeRead{Engine: store, edgeErr: edgeErr}, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.EqualError(t, RefreshIndexes(ctx, &refreshIndexesRecorder{svc: svc}, RefreshRequest{Node: "b", Edge: "r"}), edgeErr.Error())
	require.Equal(t, 1, svc.FulltextDocCount(), "the node was not refreshed before the relationship failed")
}

// Both refreshes run: the successful node refresh stays applied after the
// relationship reports its own error.
func TestRefreshIndexes_SuccessfulNodeIsKept(t *testing.T) {
	ctx := context.Background()
	store := newRefreshIndexesStore(t)
	_, err := store.CreateNode(&storage.Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"content": "refreshword"}})
	require.NoError(t, err)

	svc := NewService(store, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, RefreshIndexes(ctx, &refreshIndexesRecorder{svc: svc}, RefreshRequest{Node: "b"}))
	require.Equal(t, 1, svc.FulltextDocCount())
}

func TestRefreshIndexes_PreparesTheWholeRequestOnce(t *testing.T) {
	store := newRefreshIndexesStore(t)
	svc := NewService(store, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	scope := &refreshIndexesRecorder{svc: svc}
	request := RefreshRequest{Node: "a", Edge: "r", PostCommit: true}
	require.NoError(t, RefreshIndexes(context.Background(), scope, request))
	require.Equal(t, []RefreshRequest{request}, scope.recorded())
}

func TestRefreshIndexes_NilServiceStopsAfterPrepare(t *testing.T) {
	scope := &refreshIndexesRecorder{}
	require.NoError(t, RefreshIndexes(context.Background(), scope, RefreshRequest{Node: "a"}))
	require.Len(t, scope.recorded(), 1)
}
