package multidb

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// rejectingEngine rejects the create of one node or relationship ID, like a
// constraint or a limit would.
type rejectingEngine struct {
	storage.Engine
	rejectNode storage.NodeID
	rejectEdge storage.EdgeID
}

var errRejected = errors.New("rejected")

func (e *rejectingEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if node.ID == e.rejectNode {
		return "", errRejected
	}
	return e.Engine.CreateNode(node)
}

func (e *rejectingEngine) CreateEdge(edge *storage.Edge) error {
	if edge.ID == e.rejectEdge {
		return errRejected
	}
	return e.Engine.CreateEdge(edge)
}

// A bulk create through the size-tracking wrapper is all-or-nothing: when one
// entity of the batch is rejected, the ones created before it are removed.
func TestSizeTrackingEngine_BulkCreateIsAllOrNothing(t *testing.T) {
	manager, dbName := setupTestManager(t)
	id := func(name string) string { return dbName + ":" + name }
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	inner := &rejectingEngine{Engine: base, rejectNode: storage.NodeID(id("c")), rejectEdge: storage.EdgeID(id("e2"))}
	wrapped := newSizeTrackingEngine(inner, manager, dbName)

	err := wrapped.BulkCreateNodes([]*storage.Node{
		{ID: storage.NodeID(id("a")), Labels: []string{"L"}},
		{ID: storage.NodeID(id("b")), Labels: []string{"L"}},
		{ID: storage.NodeID(id("c")), Labels: []string{"L"}},
	})
	require.ErrorIs(t, err, errRejected)
	count, err := base.NodeCount()
	require.NoError(t, err)
	require.Zero(t, count, "no node of the rejected batch stays")

	require.NoError(t, wrapped.BulkCreateNodes([]*storage.Node{
		{ID: storage.NodeID(id("a")), Labels: []string{"L"}},
		{ID: storage.NodeID(id("b")), Labels: []string{"L"}},
	}))
	err = wrapped.BulkCreateEdges([]*storage.Edge{
		{ID: storage.EdgeID(id("e1")), StartNode: storage.NodeID(id("a")), EndNode: storage.NodeID(id("b")), Type: "R"},
		{ID: storage.EdgeID(id("e2")), StartNode: storage.NodeID(id("b")), EndNode: storage.NodeID(id("a")), Type: "R"},
	})
	require.ErrorIs(t, err, errRejected)
	edges, err := base.EdgeCount()
	require.NoError(t, err)
	require.Zero(t, edges, "no relationship of the rejected batch stays")
	nodes, err := base.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 2, nodes)
}
