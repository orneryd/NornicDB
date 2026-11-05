package search

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var xhighNestedReadFailure = errors.New("injected overlapping refresh read failure")

type xhighNestedFaultEngine struct {
	storage.Engine
	nodes atomic.Bool
	edges atomic.Bool
}

func (e *xhighNestedFaultEngine) GetInnerEngine() storage.Engine { return e.Engine }
func (e *xhighNestedFaultEngine) GetNode(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	if e.nodes.Load() {
		return nil, xhighNestedReadFailure
	}
	return e.Engine.GetNode(ctx, id)
}
func (e *xhighNestedFaultEngine) GetEdge(ctx context.Context, id storage.EdgeID) (*storage.Edge, error) {
	if e.edges.Load() {
		return nil, xhighNestedReadFailure
	}
	return e.Engine.GetEdge(ctx, id)
}

func xhighNestedService(t *testing.T) (*storage.NamespacedEngine, *Service, *xhighNestedFaultEngine) {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "nesteddiscard")
	fault := &xhighNestedFaultEngine{Engine: engine}
	service := NewService(fault, 2, nil)
	t.Cleanup(func() { _ = service.Close() })
	return engine, service, fault
}

func TestXhighNestedDiscardedRefreshes(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_RESULT_CACHE_ENABLED", "false")
	ctx := context.Background()
	t.Run("reserved_public_write", func(t *testing.T) {
		engine, service, _ := xhighNestedService(t)
		_, err := engine.CreateNode(t.Context(), &storage.Node{ID: "source", ChunkEmbeddings: [][]float32{{1, 0}}})
		require.NoError(t, err)
		older := service.ReserveNodeMutation("source")
		middle := service.ReserveNodeMutation("source")
		latest := service.ReserveNodeMutation("source")
		service.DiscardNodeMutation(middle)
		service.DiscardNodeMutation(latest)
		require.NoError(t, service.RefreshNodeWithMutation(t.Context(), older))
		assert.Equal(t, 1, service.EmbeddingCount(), "two abandoned writes cannot suppress the older valid write")
	})
	t.Run("node_refresh_failures", func(t *testing.T) {
		engine, service, fault := xhighNestedService(t)
		service.SetIndexFlags(true, false)
		_, err := engine.CreateNode(t.Context(), &storage.Node{ID: "source", Properties: map[string]any{"content": "stalenodeword"}})
		require.NoError(t, err)
		require.NoError(t, service.BuildIndexes(ctx))
		opts := DefaultSearchOptions()
		opts.MinRRFScore = 0
		before, err := service.Search(ctx, "stalenodeword", nil, opts)
		require.NoError(t, err)
		require.Len(t, before.Results, 1)
		require.NoError(t, engine.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: &storage.Node{ID: "source", Properties: map[string]any{"content": "survivornestedword"}}}))
		older := service.ReserveNodeMutation("source")
		middle := service.ReserveNodeMutation("source")
		latest := service.ReserveNodeMutation("source")
		fault.nodes.Store(true)
		require.ErrorIs(t, service.RefreshNodeWithMutation(t.Context(), middle), xhighNestedReadFailure)
		require.ErrorIs(t, service.RefreshNodeWithMutation(t.Context(), latest), xhighNestedReadFailure)
		fault.nodes.Store(false)
		require.NoError(t, service.RefreshNodeWithMutation(t.Context(), older))
		stored, err := engine.GetNode(t.Context(), "source")
		require.NoError(t, err)
		require.Equal(t, "survivornestedword", stored.Properties["content"])
		after, err := service.Search(ctx, "survivornestedword", nil, opts)
		require.NoError(t, err)
		t.Logf("older refresh returned nil; committed content=%v; updated search hits=%d", stored.Properties["content"], len(after.Results))
		assert.Len(t, after.Results, 1, "the two failed refreshes never superseded this valid stored update")
	})
	t.Run("relationship_refresh_failures", func(t *testing.T) {
		engine, service, fault := xhighNestedService(t)
		for _, id := range []storage.NodeID{"a", "b"} {
			_, err := engine.CreateNode(t.Context(), &storage.Node{ID: id})
			require.NoError(t, err)
		}
		require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "BEFORE", Properties: map[string]any{"vec": []float32{1, 0}}}))
		require.NoError(t, service.BuildIndexes(ctx))
		require.NoError(t, service.RefreshEdge(t.Context(), "r"))
		require.True(t, service.HasRelationshipVectorEntries("BEFORE", "vec"))
		require.NoError(t, engine.UpdateEdge(t.Context(), storage.EdgeUpdateRequest{After: &storage.Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "AFTER", Properties: map[string]any{"vec": []float32{0, 1}}}}))
		older := service.ReserveEdgeMutation("r")
		middle := service.ReserveEdgeMutation("r")
		latest := service.ReserveEdgeMutation("r")
		fault.edges.Store(true)
		require.ErrorIs(t, service.RefreshEdgeWithMutation(t.Context(), middle), xhighNestedReadFailure)
		require.ErrorIs(t, service.RefreshEdgeWithMutation(t.Context(), latest), xhighNestedReadFailure)
		fault.edges.Store(false)
		require.NoError(t, service.RefreshEdgeWithMutation(t.Context(), older))
		stored, err := engine.GetEdge(t.Context(), "r")
		require.NoError(t, err)
		require.Equal(t, "AFTER", stored.Type)
		t.Logf("older relationship refresh returned nil; committed type=%s; updated vector index=%t", stored.Type, service.HasRelationshipVectorEntries("AFTER", "vec"))
		assert.True(t, service.HasRelationshipVectorEntries("AFTER", "vec"), "the older valid relationship refresh must survive both failed reads")
	})
}
