package cypher

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// undirectedIncidentEdges lists each relationship of a node once, a
// self-relationship too, with one read when the store reads both directions
// at once, and a failed read is its error. The caller's context is the read's
// context, so an already cancelled caller fails the read.
func TestUndirectedIncidentEdges(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "incident")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: id})
		require.NoError(t, err)
	}
	for _, edge := range []*storage.Edge{
		{ID: "ab", StartNode: "a", EndNode: "b", Type: "T"},
		{ID: "aa", StartNode: "a", EndNode: "a", Type: "T"},
		{ID: "ba", StartNode: "b", EndNode: "a", Type: "T"},
	} {
		require.NoError(t, store.CreateEdge(t.Context(), edge))
	}
	ids := func(edges []*storage.Edge) []string {
		out := make([]string, 0, len(edges))
		for _, edge := range edges {
			out = append(out, string(edge.ID))
		}
		sort.Strings(out)
		return out
	}
	adjacent := &adjacencyReadSpy{Engine: store}
	edges, err := undirectedIncidentEdges(context.Background(), adjacent, storage.AdjacentEdgeRead{Node: "a"})
	require.NoError(t, err)
	require.Equal(t, []string{"aa", "ab", "ba"}, ids(edges))
	require.Equal(t, 1, adjacent.calls)
	edges, err = undirectedIncidentEdges(context.Background(), &directionalReadFaultEngine{Engine: store}, storage.AdjacentEdgeRead{Node: "a"})
	require.NoError(t, err)
	require.Equal(t, []string{"aa", "ab", "ba"}, ids(edges))

	adjacent.fail = true
	_, err = undirectedIncidentEdges(context.Background(), adjacent, storage.AdjacentEdgeRead{Node: "a"})
	require.ErrorIs(t, err, errEdgeRead)
	for _, failing := range []*directionalReadFaultEngine{{Engine: store, outgoingErr: errEdgeRead}, {Engine: store, incomingErr: errEdgeRead}} {
		_, err = undirectedIncidentEdges(context.Background(), failing, storage.AdjacentEdgeRead{Node: "a"})
		require.ErrorIs(t, err, errEdgeRead)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = undirectedIncidentEdges(cancelled, &adjacencyReadSpy{Engine: store}, storage.AdjacentEdgeRead{Node: "a"})
	require.ErrorIs(t, err, context.Canceled)
}
