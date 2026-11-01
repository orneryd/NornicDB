package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// directionalReadFaultEngine fails only the direction and optional node a case
// arms. The other reads preserve the underlying engine's ordinary behavior.
type directionalReadFaultEngine struct {
	storage.Engine
	outgoingErr, incomingErr error
	only                     storage.NodeID
}

func (e *directionalReadFaultEngine) readEdges(ctx context.Context, id storage.NodeID, incoming bool) ([]*storage.Edge, error) {
	fault := e.outgoingErr
	if incoming {
		fault = e.incomingErr
	}
	if fault != nil && (e.only == "" || e.only == id) {
		return nil, fault
	}
	if incoming {
		return e.Engine.GetIncomingEdges(ctx, id)
	}
	return e.Engine.GetOutgoingEdges(ctx, id)
}
func (e *directionalReadFaultEngine) GetOutgoingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return e.readEdges(ctx, id, false)
}
func (e *directionalReadFaultEngine) GetIncomingEdges(ctx context.Context, id storage.NodeID) ([]*storage.Edge, error) {
	return e.readEdges(ctx, id, true)
}

func TestCallTailHelperParsersAndPredicates(t *testing.T) {
	t.Run("relationship_type_constraint_detection", func(t *testing.T) {
		assert.True(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[:R]->(b) RETURN a"))
		assert.False(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[r]->(b) RETURN a"))
		assert.False(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[':R']->(b) RETURN a"))
		assert.False(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[r:R->(b) RETURN a"))
	})

	t.Run("path_assignment_splitting", func(t *testing.T) {
		left, right, ok := splitPathAssignment("p = (a)-[:R]->(b)")
		require.True(t, ok)
		assert.Equal(t, "p", left)
		assert.Equal(t, "(a)-[:R]->(b)", right)
		_, _, ok = splitPathAssignment("=")
		assert.False(t, ok)
	})
}
