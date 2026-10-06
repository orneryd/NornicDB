package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type callGetNodeErrEngine struct {
	storage.Engine
	err error
}

func (e *callGetNodeErrEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.Engine.GetNode(id)
}

type callOutgoingErrEngine struct {
	storage.Engine
	err error
}

func (e *callOutgoingErrEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.Engine.GetOutgoingEdges(id)
}

type callIncomingErrEngine struct {
	storage.Engine
	err error
}

func (e *callIncomingErrEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.Engine.GetIncomingEdges(id)
}

func TestCallTailHelperParsersAndPredicates(t *testing.T) {
	t.Run("relationship_type_constraint_detection", func(t *testing.T) {
		assert.True(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[:R]->(b) RETURN a"))
		assert.False(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[r]->(b) RETURN a"))
		assert.True(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[':R']->(b) RETURN a"))
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
