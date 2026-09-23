package cypher

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDeletedEntityProjectionRejectsPropertyAndLabelAccess(t *testing.T) {
	node := &storage.Node{ID: "deleted"}
	rows := []pipelineRow{{"node": node}}
	markPipelineRowsDeletedEntities(rows, []storage.NodeID{node.ID}, nil)

	requireDeletedEntityError(t, validateDeletedEntityProjection(rows, "RETURN node.value"))
	requireDeletedEntityError(t, validateDeletedEntityProjection(rows, "RETURN labels(node)"))
}

func TestDeletedRelationshipProjectionAllowsTypeButRejectsProperties(t *testing.T) {
	edge := &storage.Edge{ID: "deleted", Type: "REL"}
	rows := []pipelineRow{{"relationship": edge}}
	markPipelineRowsDeletedEntities(rows, nil, map[storage.EdgeID]struct{}{edge.ID: {}})

	if err := validateDeletedEntityProjection(rows, "RETURN type(relationship)"); err != nil {
		t.Fatalf("type() must remain available after deletion: %v", err)
	}
	requireDeletedEntityError(t, validateDeletedEntityProjection(rows, "RETURN relationship.value"))
}

func requireDeletedEntityError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", semanticError.Code)
	require.Equal(t, "DeletedEntityAccess", semanticError.Detail)
}
