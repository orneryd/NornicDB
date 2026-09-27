package storage

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestAsyncEngineReadsThroughTheWriteCacheMatchTheFlushedEngine: every read
// that overlays the write cache (label lookups and iteration, adjacency,
// relationships between two nodes, type counts, projected streams) answers
// with cached creates, updates and deletes the same as the engine does after
// they are flushed.
func TestAsyncEngineReadsThroughTheWriteCacheMatchTheFlushedEngine(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	ae := NewAsyncEngine(inner, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })

	// Flushed state.
	for _, node := range []*Node{
		{ID: "test:a", Labels: []string{"A"}, Properties: map[string]any{"v": int64(1)}},
		{ID: "test:b", Labels: []string{"B"}, Properties: map[string]any{"v": int64(2)}},
		{ID: "test:c", Labels: []string{"A"}, Properties: map[string]any{"v": int64(3)}},
		{ID: "test:gone", Labels: []string{"A"}, Properties: map[string]any{"v": int64(4)}},
	} {
		_, err := ae.CreateNode(node)
		require.NoError(t, err)
	}
	require.NoError(t, ae.CreateEdge(&Edge{ID: "test:ab", Type: "U", StartNode: "test:a", EndNode: "test:b"}))
	require.NoError(t, ae.CreateEdge(&Edge{ID: "test:cb", Type: "U", StartNode: "test:c", EndNode: "test:b"}))
	require.NoError(t, ae.Flush())

	// Cached, not flushed: a new node, an update, a delete, a new and a
	// deleted relationship.
	_, err := ae.CreateNode(&Node{ID: "test:new", Labels: []string{"A"}, Properties: map[string]any{"v": int64(5)}})
	require.NoError(t, err)
	require.NoError(t, ae.UpdateNode(&Node{ID: "test:c", Labels: []string{"A"}, Properties: map[string]any{"v": int64(30)}}))
	require.NoError(t, ae.DeleteNode("test:gone"))
	require.NoError(t, ae.CreateEdge(&Edge{ID: "test:newb", Type: "U", StartNode: "test:new", EndNode: "test:b"}))
	require.NoError(t, ae.DeleteEdge("test:cb"))
	require.True(t, ae.HasPendingWrites())

	read := func() map[string]string {
		out := map[string]string{}
		nodes, err := ae.GetNodesByLabel("A")
		require.NoError(t, err)
		out["GetNodesByLabel(A)"] = nodeSummary(nodes)

		first, err := ae.GetFirstNodeByLabel("B")
		require.NoError(t, err)
		out["GetFirstNodeByLabel(B)"] = string(first.ID)

		var ids []string
		require.NoError(t, ae.ForEachNodeIDByLabel("A", func(id NodeID) bool { ids = append(ids, string(id)); return true }))
		sort.Strings(ids)
		out["ForEachNodeIDByLabel(A)"] = fmt.Sprint(ids)

		var all []*Node
		require.NoError(t, ae.IterateNodes(func(n *Node) bool { all = append(all, n); return true }))
		out["IterateNodes"] = nodeSummary(all)

		outgoing, err := ae.GetOutgoingEdges("test:new")
		require.NoError(t, err)
		out["GetOutgoingEdges(new)"] = edgeSummary(outgoing)
		incoming, err := ae.GetIncomingEdges("test:b")
		require.NoError(t, err)
		out["GetIncomingEdges(b)"] = edgeSummary(incoming)
		adjOut, adjIn, err := ae.GetAdjacentEdges("test:b")
		require.NoError(t, err)
		out["GetAdjacentEdges(b)"] = edgeSummary(adjOut) + "/" + edgeSummary(adjIn)
		between, err := ae.GetEdgesBetween("test:c", "test:b")
		require.NoError(t, err)
		out["GetEdgesBetween(c,b)"] = edgeSummary(between)
		one := ae.GetEdgeBetween("test:new", "test:b", "U")
		out["GetEdgeBetween(new,b,U)"] = fmt.Sprint(one != nil)
		count, err := ae.EdgeCountByType("U")
		require.NoError(t, err)
		out["EdgeCountByType(U)"] = fmt.Sprint(count)

		var projected []string
		require.NoError(t, ae.StreamNodesByLabelProjected("A", []string{"v"}, func(n *Node) error {
			projected = append(projected, fmt.Sprintf("%s=%v", n.ID, n.Properties["v"]))
			return nil
		}))
		sort.Strings(projected)
		out["StreamNodesByLabelProjected(A)"] = fmt.Sprint(projected)
		return out
	}

	cached := read()
	require.NoError(t, ae.Flush())
	require.False(t, ae.HasPendingWrites())
	require.Equal(t, read(), cached)
	require.Equal(t, "[test:a test:c test:new]", cached["ForEachNodeIDByLabel(A)"])
	require.Equal(t, "[]", cached["GetEdgesBetween(c,b)"])
	require.Equal(t, "2", cached["EdgeCountByType(U)"])
}

func nodeSummary(nodes []*Node) string {
	parts := make([]string, 0, len(nodes))
	for _, n := range nodes {
		parts = append(parts, fmt.Sprintf("%s%v=%v", n.ID, n.Labels, n.Properties["v"]))
	}
	sort.Strings(parts)
	return fmt.Sprint(parts)
}

func edgeSummary(edges []*Edge) string {
	parts := make([]string, 0, len(edges))
	for _, e := range edges {
		parts = append(parts, fmt.Sprintf("%s:%s(%s->%s)", e.ID, e.Type, e.StartNode, e.EndNode))
	}
	sort.Strings(parts)
	return fmt.Sprint(parts)
}
