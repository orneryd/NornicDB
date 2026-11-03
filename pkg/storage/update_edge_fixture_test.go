package storage

import (
	"context"
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
		_, err := engine.CreateNode(t.Context(), testNode(id))
		require.NoError(t, err, "create node %s", id)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), testEdge(edgeID, nodeIDs[0], nodeIDs[1], "KNOWS")))
	stored, err := engine.GetEdge(t.Context(), EdgeID(prefixTestID(edgeID)))
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
		return engine.UpdateEdge(context.Background(), EdgeUpdateRequest{After: m.after})
	}
	tx, err := engine.BeginTransaction(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.UpdateEdge(context.Background(), EdgeUpdateRequest{After: m.after}); err != nil {
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

type updatedEdgeIndex uint8

const (
	updatedEndpointIndex updatedEdgeIndex = iota
	updatedTypeIndex
)

// assertUpdatedEdgeIndex owns the stored-image/new-index/old-index transfer
// assertions for endpoint and type mutations, through both direct and committed
// transaction writes. Key keeps the original NodeID versus string assertion type.
func assertUpdatedEdgeIndex[Key ~string](t *testing.T, engine *BadgerEngine, kind updatedEdgeIndex, edgeID EdgeID, before, after Key) {
	t.Helper()
	stored, err := engine.GetEdge(t.Context(), edgeID)
	require.NoError(t, err)
	value := Key(stored.Type)
	messages := [3]string{"stored edge must carry the new type", "new type index must contain the edge", "old type index must be emptied"}
	read := func(key Key) ([]*Edge, error) { return engine.GetEdgesByType(context.Background(), string(key)) }
	if kind == updatedEndpointIndex {
		value = Key(stored.EndNode)
		messages = [3]string{"stored edge must carry the new end node", "new end node must have the incoming edge", "original end node must lose the incoming edge"}
		read = func(key Key) ([]*Edge, error) { return engine.GetIncomingEdges(context.Background(), NodeID(key)) }
	}
	assert.Equal(t, after, value, messages[0])
	current, err := read(after)
	require.NoError(t, err)
	require.Len(t, current, 1, messages[1])
	assert.Equal(t, edgeID, current[0].ID)
	previous, err := read(before)
	require.NoError(t, err)
	assert.Len(t, previous, 0, messages[2])
}
