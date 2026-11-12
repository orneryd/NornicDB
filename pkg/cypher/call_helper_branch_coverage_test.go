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
		return e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: id})
	}
	return e.Engine.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Node: id})
}
func (e *directionalReadFaultEngine) ReadAdjacentEdges(ctx context.Context, request storage.AdjacentEdgeRead) ([]*storage.Edge, error) {
	if request.Direction == storage.AdjacentIncoming {
		return e.readEdges(ctx, request.Node, true)
	}
	if request.Direction == storage.AdjacentBoth {
		incoming, err := e.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentIncoming, Node: request.Node, Types: request.Types, Headers: request.Headers})
		if err != nil {
			return nil, err
		}
		outgoing, err := e.ReadAdjacentEdges(ctx, storage.AdjacentEdgeRead{Direction: storage.AdjacentOutgoing, Node: request.Node, Types: request.Types, Headers: request.Headers})
		if err != nil {
			return nil, err
		}
		combined := make([]*storage.Edge, 0, len(outgoing)+len(incoming))
		combined = append(combined, outgoing...)
		for _, edge := range incoming {
			if edge.StartNode == edge.EndNode {
				continue
			}
			combined = append(combined, edge)
		}
		return combined, nil
	}
	return e.readEdges(ctx, request.Node, false)
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
