package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/internal/contextslot"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDeletedEntityProjectionRejectsPropertyAndLabelAccess(t *testing.T) {
	node := &storage.Node{ID: "deleted"}
	ctx := deletedEntityReadContext(node, nil)

	requireDeletedEntityError(t, deletedEntityReadFailure(ctx, node, deletedEntityPropertyValue))
	requireDeletedEntityError(t, deletedEntityReadFailure(ctx, node, deletedEntityLabels))
	require.NoError(t, deletedEntityReadFailure(ctx, node, deletedEntityPropertySet))
	// A statement that deleted nothing reads the entity's values.
	require.NoError(t, deletedEntityReadFailure(context.Background(), node, deletedEntityPropertyValue))
}

func TestDeletedRelationshipProjectionAllowsTypeButRejectsProperties(t *testing.T) {
	edge := &storage.Edge{ID: "deleted", Type: "REL"}
	ctx := deletedEntityReadContext(nil, edge)

	requireDeletedEntityError(t, deletedEntityReadFailure(ctx, edge, deletedEntityPropertyValue))
	requireDeletedEntityError(t, deletedEntityReadFailure(ctx, edge, deletedEntityLabels))
	requireDeletedEntityError(t, deletedEntityReadFailure(ctx, edge, deletedEntityPropertySet))
	// type(r) and id(r) read no property or label, so no read below answers
	// for them; the public path (TestDeletedEntityReadsMatchNeo4j) asserts
	// they still answer after the relationship is deleted.
	require.NoError(t, deletedEntityReadFailure(context.Background(), edge, deletedEntityPropertyValue))
}

// Only a null test's own property read is let through: it asks whether the
// property is present, and a deleted entity's is absent (#907). The property's
// value, the entity's labels and the property set are not that read.
func TestDeletedEntityNullTestReadsOnlyPropertyPresence(t *testing.T) {
	node := &storage.Node{ID: "deleted"}
	ctx := deletedEntityReadContext(node, nil)

	require.NoError(t, deletedEntityReadFailure(ctx, node, deletedEntityPropertyPresence))
	requireDeletedEntityError(t, deletedEntityReadFailure(ctx, node, deletedEntityPropertyValue))
	requireDeletedEntityError(t, deletedEntityReadFailure(ctx, node, deletedEntityLabels))
	require.NoError(t, deletedEntityReadFailure(ctx, node, deletedEntityPropertySet))

	// A deleted relationship's property is absent too: r.w IS NULL is true.
	edge := &storage.Edge{ID: "deleted-edge", Type: "REL"}
	edgeCtx := deletedEntityReadContext(nil, edge)
	require.NoError(t, deletedEntityReadFailure(edgeCtx, edge, deletedEntityPropertyPresence))
	requireDeletedEntityError(t, deletedEntityReadFailure(edgeCtx, edge, deletedEntityPropertyValue))
}

// deletedEntityReadContext is a statement context that has deleted the given
// node and relationship: the state deletedEntityReadFailure reads.
func deletedEntityReadContext(node *storage.Node, edge *storage.Edge) context.Context {
	ctx := contextslot.Ensure[deletedEntities](context.Background())
	deleted := contextslot.Get[deletedEntities](ctx)
	if node != nil {
		deleted.add([]storage.NodeID{node.ID}, nil)
	}
	if edge != nil {
		deleted.add(nil, map[storage.EdgeID]struct{}{edge.ID: {}})
	}
	return ctx
}

func requireDeletedEntityError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", semanticError.Code)
	require.Equal(t, "DeletedEntityAccess", semanticError.Detail)
}
