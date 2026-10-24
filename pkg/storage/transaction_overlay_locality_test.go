package storage

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestTransactionIncidentOverlayTracksMovesAndCascade(t *testing.T) {
	engine := createTestBadgerEngine(t)
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"P"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:old", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]interface{}{"v": int64(1)}}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:loop", StartNode: "test:a", EndNode: "test:a", Type: "R"}))
	snapshot := beginMergeKeyTx(t, engine)
	_, err := snapshot.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	defer snapshot.Rollback()
	tx := beginMergeKeyTx(t, engine)
	defer tx.Rollback()
	// An initial read with no pending edges need not allocate an index.
	_, err = tx.GetOutgoingEdges("test:c")
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:new", StartNode: "test:a", EndNode: "test:c", Type: "R"}))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:unrelated", StartNode: "test:b", EndNode: "test:c", Type: "R"}))
	out, err := tx.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, out, 3)
	for _, edge := range out {
		if edge.ID == "test:old" {
			edge.Properties["v"] = int64(99)
		}
	}
	old, err := tx.GetEdge("test:old")
	require.NoError(t, err)
	require.Equal(t, int64(1), old.Properties["v"])
	old.StartNode = "test:c"
	require.NoError(t, tx.UpdateEdge(old))
	edge, err := tx.GetEdge("test:new")
	require.NoError(t, err)
	edge.StartNode, edge.EndNode = "test:c", "test:a"
	require.NoError(t, tx.UpdateEdge(edge))
	out, err = tx.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, out, 1)
	require.Equal(t, EdgeID("test:loop"), out[0].ID)
	in, err := tx.GetIncomingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, in, 2)
	between, err := tx.GetEdgesBetween("test:c", "test:a")
	require.NoError(t, err)
	require.Len(t, between, 1)
	require.NoError(t, tx.DeleteEdge("test:new"))
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:new", StartNode: "test:b", EndNode: "test:a", Type: "R", Properties: map[string]interface{}{"v": int64(2)}}))
	require.NoError(t, tx.DeleteNode("test:a"))
	_, err = tx.GetEdge("test:loop")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = tx.GetEdge("test:new")
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, tx.Commit())
	current, err := engine.GetEdge("test:old")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:c"), current.StartNode)
	current, err = snapshot.GetEdge("test:old")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:a"), current.StartNode)
	_, err = snapshot.GetEdge("test:loop")
	require.NoError(t, err)
	_, err = snapshot.GetEdge("test:new")
	require.Error(t, err)
	edges, err := engine.AllEdges()
	require.NoError(t, err)
	require.Len(t, edges, 2)
}

func TestTransactionEqualityOverlayPreservesCandidatesAndSnapshot(t *testing.T) {
	engine := createTestBadgerEngine(t)
	for _, id := range []NodeID{"test:a", "test:b", "other:a"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(1), "group": "shared"}})
		require.NoError(t, err)
	}
	tx := beginMergeKeyTx(t, engine)
	defer tx.Rollback()
	lookup := func(value interface{}) []*Node {
		nodes, complete, err := tx.LookupNodesByEquality("P", map[string]interface{}{"k": value})
		require.NoError(t, err)
		require.True(t, complete)
		return nodes
	}
	nodes := lookup(float64(1))
	require.Len(t, nodes, 2)
	nodes[0].Properties["k"] = int64(99)
	require.Len(t, lookup(int64(1)), 2, "caller edits cannot poison the pinned population")
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:a", Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(2)}}))
	_, err := tx.CreateNode(&Node{ID: "test:new", Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(1)}})
	require.NoError(t, err)
	require.Len(t, lookup(int64(1)), 2)
	require.Equal(t, NodeID("test:a"), lookup(int64(2))[0].ID)
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:b", Labels: []string{"Q"}, Properties: map[string]interface{}{"k": int64(1)}}))
	require.Len(t, lookup(int64(1)), 1)
	require.NoError(t, tx.DeleteNode("test:new"))
	require.Empty(t, lookup(int64(1)))
	// A peer's changes stay outside this snapshot, including the cached path.
	_, err = engine.CreateNode(&Node{ID: "test:peer", Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(1)}})
	require.NoError(t, err)
	require.Empty(t, lookup(int64(1)))
	_, complete, err := tx.LookupNodesByEquality("P", map[string]interface{}{"k": nil})
	require.NoError(t, err)
	require.False(t, complete)
	require.NoError(t, tx.Rollback())
	_, _, err = tx.LookupNodesByEquality("P", map[string]interface{}{"k": int64(1)})
	require.Error(t, err)
}

func TestTransactionEqualityOverlayDeclinesUnstableAndOversizeViews(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	tx := beginMergeKeyTx(t, engine)
	_, complete, err := tx.LookupNodesByEquality("P", map[string]interface{}{"k": int64(1)})
	require.NoError(t, err)
	require.False(t, complete, "read-committed populations cannot be retained as a snapshot")
	require.NoError(t, tx.Rollback())

	stable := createTestBadgerEngine(t)
	_, err = stable.CreateNode(&Node{ID: "test:large", Labels: []string{"P"}, Properties: map[string]interface{}{"k": int64(1), "payload": strings.Repeat("x", maxSnapshotLabelPrefixNodeBytes+1)}})
	require.NoError(t, err)
	reader := beginMergeKeyTx(t, stable)
	defer reader.Rollback()
	nodes, complete, err := reader.LookupNodesByEquality("P", map[string]interface{}{"k": int64(1)})
	require.NoError(t, err)
	require.True(t, complete)
	require.Len(t, nodes, 1, "the first bounded scan still answers completely")
	for _, population := range reader.snapshotEquality {
		require.Nil(t, population.nodes)
		require.Nil(t, population.index)
	}
	_, complete, err = reader.LookupNodesByEquality("P", map[string]interface{}{"k": int64(1)})
	require.NoError(t, err)
	require.False(t, complete, "later probes use the normal scan without retaining oversized bodies")
	nodes, err = reader.GetNodesByLabel("P")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
}
