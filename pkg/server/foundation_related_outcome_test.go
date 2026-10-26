// Author: Lane #1644 (Codex). Public transport request, with a complete
// healthy result and an unrecovered required related-node read afterwards.
package server

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
)

type foundationRelatedFault struct {
	storage.Engine
	active atomic.Bool
	visits atomic.Int64
	cause  error
}

// GetInnerEngine carries native catalog preparation/receipt authority only;
// the issued read view keeps its required-edge fault interception.
func (v *foundationRelatedFault) GetInnerEngine() storage.Engine { return v.Engine }

func (v *foundationRelatedFault) Namespace() string { return storage.DatabaseOf(v.Engine) }
func (v *foundationRelatedFault) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if v.active.Load() {
		v.visits.Add(1)
		return nil, v.cause
	}
	return v.Engine.GetOutgoingEdges(id)
}
func (v *foundationRelatedFault) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if v.active.Load() {
		v.visits.Add(1)
		return nil, v.cause
	}
	return v.Engine.GetIncomingEdges(id)
}
func TestFoundationPublicRelatedOutcome(t *testing.T) {
	db, err := nornicdb.Open("", nil)
	require.NoError(t, err)
	defer db.Close()
	view := &foundationRelatedFault{Engine: db.GetBaseStorageForManager(), cause: errors.New("required related-node read failed")}
	manager, err := multidb.NewDatabaseManager(t.Context(), view, multidb.DefaultConfig())
	require.NoError(t, err)
	engine, err := manager.GetStorage("nornic")
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Memory"}, Properties: map[string]any{"title": "foundationzephyr", "content": "foundationzephyr unique content"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "R"}))
	require.NoError(t, db.BuildSearchIndexes(context.Background()))

	router := newHeimdallDBRouter(db, manager, nil)
	ctx := heimdallAuthDisabled()
	healthy, err := router.Discover(ctx, "nornic", "foundationzephyr", nil, 5, 1)
	require.NoError(t, err)
	require.NotEmpty(t, healthy.Results)
	require.NotEmpty(t, healthy.Results[0].Related)
	view.active.Store(true)
	result, err := router.Discover(ctx, "nornic", "foundationzephyr", nil, 5, 1)
	require.Positive(t, view.visits.Load())
	require.ErrorIs(t, err, view.cause, "public Heimdall discover must not return a successful partial graph: %#v", result)
}
