// Author: Lane #1644 (Codex). Same terminal storage-failure class through public relationship Search.
package search

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type foundationRelationshipReadView struct {
	storage.Engine
	edgeFailure error
	nodeFailure error
	failedNode  storage.NodeID
}

func (v *foundationRelationshipReadView) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if v.edgeFailure != nil {
		return nil, v.edgeFailure
	}
	return v.Engine.GetEdge(id)
}
func (v *foundationRelationshipReadView) GetNode(id storage.NodeID) (*storage.Node, error) {
	if id == v.failedNode {
		return nil, v.nodeFailure
	}
	return v.Engine.GetNode(id)
}
func TestFoundationPublicSearchRelationshipStorageFailure(t *testing.T) {
	t.Setenv("NORNICDB_SEARCH_RELATIONSHIPS_ENABLED", "true")
	for _, policy := range []string{"edge-failure", "start-failure", "end-failure", "missing-edge", "missing-endpoint", "healthy"} {
		t.Run(policy, func(t *testing.T) {
			b := storage.NewMemoryEngine()
			defer b.Close()
			base := storage.NewNamespacedEngine(b, "tenant")
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := base.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}})
				require.NoError(t, err)
			}
			edge := &storage.Edge{ID: "edge", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]any{"content": "alpha relationship"}}
			require.NoError(t, base.CreateEdge(edge))
			cause := errors.New("injected relationship storage read failure")
			view := &foundationRelationshipReadView{Engine: base}
			switch policy {
			case "edge-failure":
				view.edgeFailure = cause
			case "start-failure":
				view.failedNode = "a"
				view.nodeFailure = cause
			case "end-failure":
				view.failedNode = "b"
				view.nodeFailure = cause
			case "missing-edge":
				view.edgeFailure = storage.ErrNotFound
			case "missing-endpoint":
				view.failedNode = "a"
				view.nodeFailure = storage.ErrNotFound
			}
			service := NewService(view, 2, nil)
			defer service.Close()
			service.EnableRelationshipSearch(nil)
			service.StartRelationshipIndex()
			require.NoError(t, service.IndexEdge(edge))
			opts := DefaultSearchOptions()
			opts.Relationships = true
			opts.RelationshipWhere = func(_ *storage.Edge, start, end *storage.Node) bool { return start != nil && end != nil }
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := service.Search(ctx, "alpha", nil, opts)
			if policy == "healthy" {
				require.NoError(t, err)
				require.Len(t, result.Results, 1)
			} else if policy == "missing-edge" || policy == "missing-endpoint" {
				require.NoError(t, err)
				require.Empty(t, result.Results)
			} else {
				require.ErrorIs(t, err, cause)
				require.Nil(t, result)
			}
		})
	}
}
