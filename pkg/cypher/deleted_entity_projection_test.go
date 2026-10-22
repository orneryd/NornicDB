package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDeletedEntityProjectionRejectsPropertyAndLabelAccess(t *testing.T) {
	node := &storage.Node{ID: "deleted"}
	rows := []pipelineRow{{"node": node}}
	deleted := &deletedEntities{}
	deleted.add([]storage.NodeID{node.ID}, nil)

	requireDeletedEntityError(t, validateDeletedEntityReads(context.Background(), rows, "RETURN node.value", deleted))
	requireDeletedEntityError(t, validateDeletedEntityReads(context.Background(), rows, "RETURN labels(node)", deleted))
	require.NoError(t, validateDeletedEntityReads(context.Background(), rows, "RETURN keys(node)", deleted))
	require.NoError(t, validateDeletedEntityReads(context.Background(), rows, "RETURN node.value", nil))
}

func TestDeletedRelationshipProjectionAllowsTypeButRejectsProperties(t *testing.T) {
	edge := &storage.Edge{ID: "deleted", Type: "REL"}
	rows := []pipelineRow{{"relationship": edge}}
	deleted := &deletedEntities{}
	deleted.add(nil, map[storage.EdgeID]struct{}{edge.ID: {}})

	if err := validateDeletedEntityReads(context.Background(), rows, "RETURN type(relationship)", deleted); err != nil {
		t.Fatalf("type() must remain available after deletion: %v", err)
	}
	requireDeletedEntityError(t, validateDeletedEntityReads(context.Background(), rows, "RETURN relationship.value", deleted))
	requireDeletedEntityError(t, validateDeletedEntityReads(context.Background(), rows, "RETURN keys(relationship)", deleted))
}

func requireDeletedEntityError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", semanticError.Code)
	require.Equal(t, "DeletedEntityAccess", semanticError.Detail)
}
