package cypher

import (
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// adjacentEdgeEngine reads both directions of a node's relationships in one
// call (storage.AdjacentEdgesEngine), counting the calls, and fails them
// when fail is set.
type adjacentEdgeEngine struct {
	storage.Engine
	calls int
	fail  bool
}

func (e *adjacentEdgeEngine) GetAdjacentEdges(id storage.NodeID) ([]*storage.Edge, []*storage.Edge, error) {
	e.calls++
	if e.fail {
		return nil, nil, errEdgeRead
	}
	outgoing, err := e.Engine.GetOutgoingEdges(id)
	if err != nil {
		return nil, nil, err
	}
	incoming, err := e.Engine.GetIncomingEdges(id)
	return outgoing, incoming, err
}

// undirectedIncidentEdges lists each relationship of a node once, a
// self-relationship too, with one read when the store reads both directions
// at once, and a failed read is its error.
func TestUndirectedIncidentEdges(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "incident")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(&storage.Node{ID: id})
		require.NoError(t, err)
	}
	for _, edge := range []*storage.Edge{
		{ID: "ab", StartNode: "a", EndNode: "b", Type: "T"},
		{ID: "aa", StartNode: "a", EndNode: "a", Type: "T"},
		{ID: "ba", StartNode: "b", EndNode: "a", Type: "T"},
	} {
		require.NoError(t, store.CreateEdge(edge))
	}
	ids := func(edges []*storage.Edge) []string {
		out := make([]string, 0, len(edges))
		for _, edge := range edges {
			out = append(out, string(edge.ID))
		}
		sort.Strings(out)
		return out
	}
	adjacent := &adjacentEdgeEngine{Engine: store}
	edges, err := undirectedIncidentEdges(adjacent, "a")
	require.NoError(t, err)
	require.Equal(t, []string{"aa", "ab", "ba"}, ids(edges))
	require.Equal(t, 1, adjacent.calls)
	edges, err = undirectedIncidentEdges(&failingEdgeEngine{Engine: store}, "a")
	require.NoError(t, err)
	require.Equal(t, []string{"aa", "ab", "ba"}, ids(edges))

	adjacent.fail = true
	_, err = undirectedIncidentEdges(adjacent, "a")
	require.ErrorIs(t, err, errEdgeRead)
	for _, failing := range []*failingEdgeEngine{{Engine: store, failOutgoing: true}, {Engine: store, failIncoming: true}} {
		_, err = undirectedIncidentEdges(failing, "a")
		require.ErrorIs(t, err, errEdgeRead)
	}
}
