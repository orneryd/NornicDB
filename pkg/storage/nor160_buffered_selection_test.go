package storage

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// An explicit transaction's own deletes and rewrites supersede committed
// values that are still in the write-behind buffer, on every read the
// transaction answers from the engine's overlaid view: adjacency, between,
// type, all-edges and the type stream, and the label and all-nodes reads.
// The buffer decides with the transaction's selection before it copies, so a
// superseded buffered value is neither returned nor duplicated (NOR-160).
func TestTransactionReadsSupersedeBufferedValues(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		for _, id := range []NodeID{"test:a", "test:b", "test:solo"} {
			if _, err := tx.CreateNode(&Node{ID: id, Labels: []string{"P"}}); err != nil {
				return err
			}
		}
		for _, id := range []EdgeID{"test:gone", "test:kept"} {
			if err := tx.CreateEdge(&Edge{ID: id, StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"v": 1}}); err != nil {
				return err
			}
		}
		return nil
	})
	// Still buffered: the engine reads both relationships from the overlay.
	buffered, err := engine.GetEdgesByType("R")
	require.NoError(t, err)
	require.Len(t, buffered, 2)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteEdge("test:gone"))
	require.NoError(t, tx.UpdateEdge(&Edge{ID: "test:kept", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"v": 2}}))
	require.NoError(t, tx.DeleteGraphNode("test:solo", CascadeEdges))

	requireOnlyRewrite := func(name string, edges []*Edge, err error) {
		t.Helper()
		require.NoError(t, err, name)
		require.Len(t, edges, 1, name)
		require.Equal(t, EdgeID("test:kept"), edges[0].ID, name)
		require.EqualValues(t, 2, edges[0].Properties["v"], name)
	}
	edges, err := tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: "test:a"})
	requireOnlyRewrite("outgoing", edges, err)
	edges, err = tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadIncoming, Node: "test:b"})
	requireOnlyRewrite("incoming", edges, err)
	edges, err = tx.ReadEdges(TransactionEdgeRead{Kind: TransactionReadBetween, Node: "test:a", Other: "test:b"})
	requireOnlyRewrite("between", edges, err)
	edges, err = tx.GetEdgesByType("R")
	requireOnlyRewrite("type", edges, err)
	edges, err = tx.AllEdges()
	requireOnlyRewrite("all edges", edges, err)
	edges = nil
	err = tx.StreamEdgesByType(context.Background(), "R", func(edge *Edge) error {
		edges = append(edges, edge)
		return nil
	})
	requireOnlyRewrite("type stream", edges, err)

	nodeIDs := func(nodes []*Node) []string {
		ids := make([]string, 0, len(nodes))
		for _, node := range nodes {
			ids = append(ids, string(node.ID))
		}
		sort.Strings(ids)
		return ids
	}
	nodes, err := tx.GetNodesByLabel("P")
	require.NoError(t, err)
	require.Equal(t, []string{"test:a", "test:b"}, nodeIDs(nodes))
	nodes, err = tx.AllNodes()
	require.NoError(t, err)
	require.Equal(t, []string{"test:a", "test:b"}, nodeIDs(nodes))
}
