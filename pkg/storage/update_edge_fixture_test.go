package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedUpdateEdgeMutation creates exactly the graph requested by each test.
// Mutations, transactional boundaries and behavioral assertions stay in callers.
func seedUpdateEdgeMutation(t *testing.T, nodeIDs []string, edgeID string) *BadgerEngine {
	t.Helper()
	require.GreaterOrEqual(t, len(nodeIDs), 2)
	engine := createTestBadgerEngine(t)
	for _, id := range nodeIDs {
		_, err := engine.CreateNode(testNode(id))
		require.NoError(t, err, "create node %s", id)
	}
	require.NoError(t, engine.CreateEdge(testEdge(edgeID, nodeIDs[0], nodeIDs[1], "KNOWS")))
	stored, err := engine.GetEdge(EdgeID(prefixTestID(edgeID)))
	require.NoError(t, err)
	assert.Equal(t, NodeID(prefixTestID(nodeIDs[0])), stored.StartNode)
	assert.Equal(t, NodeID(prefixTestID(nodeIDs[1])), stored.EndNode)
	return engine
}

// updateEdgeMutation declares one endpoint/type mutation for an edge seeded by
// seedUpdateEdgeMutation. The mutation is applied either directly to the engine
// or through a transaction that is committed, and both entry points remain
// separate cases so neither path replaces the other.
type updateEdgeMutation struct {
	// after is the replacement edge body handed to UpdateEdge as the
	// mandatory typed request payload.
	after *Edge
	// throughTransaction selects the transaction+Commit entry point instead of
	// the direct engine call.
	throughTransaction bool
	// assert checks the outcome specific to this mutation after it has been
	// applied (and committed, when throughTransaction is set).
	assert func(t *testing.T, engine *BadgerEngine)
}

// runUpdateEdgeMutation seeds the given graph, applies the mutation, and runs
// the per-case assertions. It owns only the repeated seeding/mutation/commit
// mechanics; every case keeps its own assertions.
func runUpdateEdgeMutation(t *testing.T, nodeIDs []string, edgeID string, mutation updateEdgeMutation) {
	t.Helper()
	engine := seedUpdateEdgeMutation(t, nodeIDs, edgeID)
	require.NoError(t, mutation.apply(engine))
	mutation.assert(t, engine)
}

// apply performs the declared mutation against engine.
func (m updateEdgeMutation) apply(engine *BadgerEngine) error {
	if !m.throughTransaction {
		return engine.UpdateEdge(EdgeUpdateRequest{After: m.after})
	}
	tx, err := engine.BeginTransaction()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.UpdateEdge(EdgeUpdateRequest{After: m.after}); err != nil {
		return err
	}
	return tx.Commit()
}

// findEdgeByID returns the edge with the given ID from edges, or nil.
func findEdgeByID(edges []*Edge, id EdgeID) *Edge {
	for _, edge := range edges {
		if edge.ID == id {
			return edge
		}
	}
	return nil
}

// updateEdgeEndpoint asserts the endpoint outcome of an update that moved an
// edge's end node from originalEnd to newEnd: the stored body carries the new
// end node, directed incoming adjacency moved to the new end, and the original
// end no longer has the edge.
func updateEdgeEndpoint(t *testing.T, engine *BadgerEngine, edgeID EdgeID, originalEnd, newEnd NodeID) {
	t.Helper()
	stored, err := engine.GetEdge(edgeID)
	require.NoError(t, err)
	assert.Equal(t, newEnd, stored.EndNode, "stored edge must carry the new end node")

	newIncoming, err := engine.GetIncomingEdges(newEnd)
	require.NoError(t, err)
	require.Len(t, newIncoming, 1, "new end node must have the incoming edge")
	assert.Equal(t, edgeID, newIncoming[0].ID)

	oldIncoming, err := engine.GetIncomingEdges(originalEnd)
	require.NoError(t, err)
	assert.Len(t, oldIncoming, 0, "original end node must lose the incoming edge")
}

// updateEdgeType asserts the stored type and both sides of the type index
// transfer from oldType to newType.
func updateEdgeType(t *testing.T, engine *BadgerEngine, edgeID EdgeID, oldType, newType string) {
	t.Helper()
	stored, err := engine.GetEdge(edgeID)
	require.NoError(t, err)
	assert.Equal(t, newType, stored.Type, "stored edge must carry the new type")

	newEdges, err := engine.GetEdgesByType(newType)
	require.NoError(t, err)
	require.Len(t, newEdges, 1, "new type index must contain the edge")
	assert.Equal(t, edgeID, newEdges[0].ID)

	oldEdges, err := engine.GetEdgesByType(oldType)
	require.NoError(t, err)
	assert.Len(t, oldEdges, 0, "old type index must be emptied")
}
