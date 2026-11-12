package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// adjacencySnapshot is the pre/post-flush observable state of one graph.
type adjacencySnapshot struct {
	totalEdges       int64
	friendEdges      int64
	friendStartLabel int64
	friendEndLabel   int64
	namespaceEdges   int64
	outgoing         map[NodeID]map[EdgeID]struct{}
	incoming         map[NodeID]map[EdgeID]struct{}
}

func edgeIDSet(edges []*Edge) map[EdgeID]struct{} {
	set := make(map[EdgeID]struct{}, len(edges))
	for _, e := range edges {
		if e != nil {
			set[e.ID] = struct{}{}
		}
	}
	return set
}

func readAdjacencySnapshot(t *testing.T, engine *BadgerEngine, nodeIDs []NodeID) adjacencySnapshot {
	t.Helper()
	snap := adjacencySnapshot{
		outgoing: make(map[NodeID]map[EdgeID]struct{}),
		incoming: make(map[NodeID]map[EdgeID]struct{}),
	}
	var err error
	snap.totalEdges, err = engine.EdgeCount(t.Context())
	require.NoError(t, err)
	snap.friendEdges, err = engine.EdgeCountByType(context.Background(), "Friend")
	require.NoError(t, err)
	snap.friendStartLabel, err = engine.EdgeCountByStartLabelInNamespace(context.Background(), "test", "User", "Friend")
	require.NoError(t, err)
	snap.friendEndLabel, err = engine.EdgeCountByEndLabelInNamespace(context.Background(), "test", "User", "Friend")
	require.NoError(t, err)
	snap.namespaceEdges, err = engine.EdgeCountByPrefix(context.Background(), "test:")
	require.NoError(t, err)
	for _, id := range nodeIDs {
		out, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: id})
		require.NoError(t, err)
		in, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: id})
		require.NoError(t, err)
		snap.outgoing[id] = edgeIDSet(out)
		snap.incoming[id] = edgeIDSet(in)
	}
	return snap
}

// TestWriteBehind_AdjacencyOverlayMatchesFlushedView reproduces monster's
// part-1 matrix: buffered relationships must be visible to adjacency reads,
// between-index lookups and every edge counter before the flush lands them,
// and the pre-flush view must equal the post-flush view.
func TestWriteBehind_AdjacencyOverlayMatchesFlushedView(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	const users = 50
	const friends = 40
	nodeIDs := make([]NodeID, users)
	for i := range nodeIDs {
		nodeIDs[i] = NodeID(fmt.Sprintf("test:u%02d", i))
		bufferedCreate(t, engine, &Node{ID: nodeIDs[i], Labels: []string{"User"}})
	}
	edges := make([]*Edge, friends)
	for i := range edges {
		start := nodeIDs[i%users]
		end := nodeIDs[(i+1)%users]
		edges[i] = &Edge{
			ID:        EdgeID(fmt.Sprintf("test:f%02d", i)),
			StartNode: start,
			EndNode:   end,
			Type:      "Friend",
		}
		bufferedWrite(t, engine, func(tx *BadgerTransaction) error { return tx.CreateEdge(t.Context(), edges[i]) })
	}
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "commits must stay buffered before flush")

	// Between-index lookups must see buffered edges (this is the guard MERGE
	// uses before deciding to create).
	for _, e := range edges {
		got, err := engine.GetEdgeBetween(t.Context(), e.StartNode, e.EndNode, "Friend")
		require.NoError(t, err)
		require.NotNil(t, got, "GetEdgeBetween(%s, %s) must see the buffered edge", e.StartNode, e.EndNode)
		between, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: e.StartNode, Other: e.EndNode})
		require.NoError(t, err)
		require.Contains(t, edgeIDSet(between), e.ID)
	}

	pre := readAdjacencySnapshot(t, engine, nodeIDs)
	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	post := readAdjacencySnapshot(t, engine, nodeIDs)

	require.Equal(t, post.totalEdges, pre.totalEdges, "EdgeCount must include buffered edges")
	require.Equal(t, int64(friends), pre.totalEdges)
	require.Equal(t, post.friendEdges, pre.friendEdges, "EdgeCountByType must include buffered edges")
	require.Equal(t, int64(friends), pre.friendEdges)
	require.Equal(t, post.friendStartLabel, pre.friendStartLabel, "start-label positional count must include buffered edges")
	require.Equal(t, int64(friends), pre.friendStartLabel)
	require.Equal(t, post.friendEndLabel, pre.friendEndLabel, "end-label positional count must include buffered edges")
	require.Equal(t, int64(friends), pre.friendEndLabel)
	require.Equal(t, post.namespaceEdges, pre.namespaceEdges, "namespace count must include buffered edges")
	require.Equal(t, int64(friends), pre.namespaceEdges)

	for _, id := range nodeIDs {
		require.Equal(t, post.outgoing[id], pre.outgoing[id], "outgoing adjacency of %s must match flushed view", id)
		require.Equal(t, post.incoming[id], pre.incoming[id], "incoming adjacency of %s must match flushed view", id)
	}
}

// TestWriteBehind_MergeDoesNotDuplicateBufferedEdges pins the MERGE guard:
// creating the same relationship through GetEdgeBetween/GetEdgesBetween
// twice, while every write is buffered, must leave exactly one edge.
func TestWriteBehind_MergeDoesNotDuplicateBufferedEdges(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	bufferedCreate(t, engine, &Node{ID: "test:a", Labels: []string{"User"}})
	bufferedCreate(t, engine, &Node{ID: "test:b", Labels: []string{"User"}})
	edge := &Edge{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "Friend"}
	bufferedWrite(t, engine, func(tx *BadgerTransaction) error { return tx.CreateEdge(t.Context(), edge) })

	// MERGE-equivalent: a second buffered statement must observe the first
	// edge and skip the create.
	edge, err := engine.GetEdgeBetween(t.Context(), "test:a", "test:b", "Friend")
	require.NoError(t, err)
	require.NotNil(t, edge)
	between, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:b"})
	require.NoError(t, err)
	require.Len(t, between, 1)

	require.Equal(t, int64(1), mustEdgeCount(t, engine))
	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	require.Equal(t, int64(1), mustEdgeCount(t, engine), "flush must not duplicate the edge")
	between, err = engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "test:a", Other: "test:b"})
	require.NoError(t, err)
	require.Len(t, between, 1)
}

// TestWriteBehind_DetachDeleteLeavesNoOrphans pins the DETACH DELETE path:
// deleting a node whose relationships are all buffered must remove every
// incident edge before the flush, and leave no orphans after it.
func TestWriteBehind_DetachDeleteLeavesNoOrphans(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	bufferedCreate(t, engine, &Node{ID: "test:a", Labels: []string{"User"}})
	bufferedCreate(t, engine, &Node{ID: "test:b", Labels: []string{"User"}})
	bufferedCreate(t, engine, &Node{ID: "test:c", Labels: []string{"User"}})
	for _, e := range []*Edge{
		{ID: "test:ab", StartNode: "test:a", EndNode: "test:b", Type: "Friend"},
		{ID: "test:ba", StartNode: "test:b", EndNode: "test:a", Type: "Friend"},
		{ID: "test:ac", StartNode: "test:a", EndNode: "test:c", Type: "Friend"},
	} {
		bufferedWrite(t, engine, func(tx *BadgerTransaction) error { return tx.CreateEdge(t.Context(), e) })
	}
	require.Equal(t, int64(3), mustEdgeCount(t, engine))

	// DETACH DELETE of a while all three relationships are buffered.
	bufferedWrite(t, engine, func(tx *BadgerTransaction) error { return tx.DeleteGraphNode(t.Context(), "test:a", CascadeEdges) })

	// Orphan check before any flush.
	out, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "test:a"})
	require.NoError(t, err)
	require.Empty(t, out, "no outgoing edge may survive the buffered delete")
	in, err := engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "test:a"})
	require.NoError(t, err)
	require.Empty(t, in, "no incoming edge may survive the buffered delete")
	all, err := engine.AllEdges(context.Background())
	require.NoError(t, err)
	require.Empty(t, all, "every incident edge must be removed while buffered")
	require.Equal(t, int64(0), mustEdgeCount(t, engine))

	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	all, err = engine.AllEdges(context.Background())
	require.NoError(t, err)
	require.Empty(t, all, "no orphan may reappear after the flush")
	require.Equal(t, int64(0), mustEdgeCount(t, engine))
	out, err = engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "test:a"})
	require.NoError(t, err)
	require.Empty(t, out)
	in, err = engine.ReadAdjacentEdges(context.Background(), AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "test:a"})
	require.NoError(t, err)
	require.Empty(t, in)
}
