package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The engine's bulk operations commit as one transaction: of any size, all
// or nothing, with the transaction's checks (#703).

func bulkTestNodes(n int, prefix string) []*Node {
	nodes := make([]*Node, n)
	for i := range nodes {
		nodes[i] = &Node{ID: NodeID(fmt.Sprintf("test:%s-%d", prefix, i)), Labels: []string{"Bulk"}, Properties: map[string]any{"i": int64(i), "pad": "0123456789abcdef"}}
	}
	return nodes
}

func bulkTestEdges(n int, prefix string, nodes []*Node) []*Edge {
	edges := make([]*Edge, n)
	for i := range edges {
		edges[i] = &Edge{ID: EdgeID(fmt.Sprintf("test:%s-%d", prefix, i)), StartNode: nodes[i%len(nodes)].ID, EndNode: nodes[(i+1)%len(nodes)].ID, Type: "BULK"}
	}
	return edges
}

func requireBulkCounts(t *testing.T, engine *BadgerEngine, nodes, edges int64) {
	t.Helper()
	nodeCount, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, nodes, nodeCount)
	edgeCount, err := engine.EdgeCount()
	require.NoError(t, err)
	require.Equal(t, edges, edgeCount)
}

func TestBulkOperations_BeyondOneBatchSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	engine := openLargeCommitTestEngine(t, dir)
	batches := countBatches(t)

	const n = 6000
	nodes := bulkTestNodes(n, "bn")
	require.NoError(t, engine.BulkCreateNodes(nodes))
	require.Greater(t, batches.Load(), int64(1), "the node create must have needed several Badger batches")
	edges := bulkTestEdges(n, "be", nodes)
	batches.Store(0)
	require.NoError(t, engine.BulkCreateEdges(edges))
	require.Greater(t, batches.Load(), int64(1), "the edge create must have needed several Badger batches")
	requireBulkCounts(t, engine, n, n)
	requireNoLargeCommitIntent(t, engine)

	require.NoError(t, engine.Close())
	engine = openLargeCommitTestEngine(t, dir)
	defer engine.Close()
	requireBulkCounts(t, engine, n, n)
	byLabel, err := engine.GetNodesByLabel("Bulk")
	require.NoError(t, err)
	require.Len(t, byLabel, n)
	out, err := engine.GetOutgoingEdges(nodes[n/2].ID)
	require.NoError(t, err)
	require.Len(t, out, 1)

	edgeIDs := make([]EdgeID, 0, n/2)
	for _, edge := range edges[:n/2] {
		edgeIDs = append(edgeIDs, edge.ID)
	}
	batches.Store(0)
	require.NoError(t, engine.BulkDeleteEdges(edgeIDs))
	require.Greater(t, batches.Load(), int64(1))
	requireBulkCounts(t, engine, n, n-n/2)

	nodeIDs := make([]NodeID, 0, n)
	for _, node := range nodes {
		nodeIDs = append(nodeIDs, node.ID)
	}
	batches.Store(0)
	require.NoError(t, engine.BulkDeleteNodes(nodeIDs))
	require.Greater(t, batches.Load(), int64(1))
	requireBulkCounts(t, engine, 0, 0)
	requireNoLargeCommitIntent(t, engine)
}

func TestBulkCreate_RejectedItemWritesNothing(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()

	existing := bulkTestNodes(2, "existing")
	require.NoError(t, engine.BulkCreateNodes(existing))
	require.NoError(t, engine.BulkCreateEdges(bulkTestEdges(1, "existing-e", existing)))

	// The rejected item comes last, after enough writes for several batches.
	nodes := append(bulkTestNodes(6000, "rejected"), existing[1])
	require.ErrorIs(t, engine.BulkCreateNodes(nodes), ErrAlreadyExists)
	requireBulkCounts(t, engine, 2, 1)

	edges := append(bulkTestEdges(6000, "rejected-e", existing), &Edge{ID: "test:existing-e-0", StartNode: existing[0].ID, EndNode: existing[1].ID, Type: "BULK"})
	require.ErrorIs(t, engine.BulkCreateEdges(edges), ErrAlreadyExists, "an edge ID that is already committed is rejected")
	requireBulkCounts(t, engine, 2, 1)

	edges[len(edges)-1] = &Edge{ID: "test:dangling", StartNode: existing[0].ID, EndNode: "test:missing", Type: "BULK"}
	err := engine.BulkCreateEdges(edges)
	require.ErrorIs(t, err, ErrNotFound)
	require.ErrorContains(t, err, "end node test:missing does not exist")
	requireBulkCounts(t, engine, 2, 1)
}

func TestBulkDelete_SkipsMissingAndRepeatedIDs(t *testing.T) {
	engine := openLargeCommitTestEngine(t, t.TempDir())
	defer engine.Close()

	nodes := bulkTestNodes(4, "del")
	require.NoError(t, engine.BulkCreateNodes(nodes))
	edges := bulkTestEdges(4, "del-e", nodes)
	require.NoError(t, engine.BulkCreateEdges(edges))

	require.NoError(t, engine.BulkDeleteEdges([]EdgeID{edges[0].ID, edges[0].ID, "", "test:no-such-edge"}))
	requireBulkCounts(t, engine, 4, 3)

	// Deleting a node removes its relationships; a repeated ID counts once.
	require.NoError(t, engine.BulkDeleteNodes([]NodeID{nodes[2].ID, nodes[2].ID, "", "test:no-such-node"}))
	requireBulkCounts(t, engine, 3, 1)
	_, err := engine.GetEdge(edges[3].ID)
	require.NoError(t, err, "the relationship between the remaining nodes stays")
}

func TestBulkCreateNodes_IndexesNodesWaitingForEmbedding(t *testing.T) {
	engine := newTestBadgerEngineForPending(t)
	nodes := []*Node{
		{ID: NodeID(prefixTestID("bulk-pending")), Labels: []string{"Doc"}, Properties: map[string]any{"text": "a"}},
		{ID: NodeID(prefixTestID("bulk-embedded")), Labels: []string{"Doc"}, Properties: map[string]any{"text": "b"}, ChunkEmbeddings: [][]float32{{0.1, 0.2}}},
	}
	require.NoError(t, engine.BulkCreateNodes(nodes))
	require.Equal(t, 1, engine.PendingEmbeddingsCount(), "a bulk-created node is queued for embedding as a created node is")
	found := engine.FindNodeNeedingEmbedding()
	require.NotNil(t, found)
	require.Equal(t, nodes[0].ID, found.ID)
}
