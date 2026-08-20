package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Fixes #301: IN predicates must evaluate list properties on relationships.
func TestRelationshipListPropertyMembership(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "relationship_list_membership")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	principal := &storage.Node{ID: "principal-1", Labels: []string{"Principal"}, Properties: map[string]interface{}{"actions": []string{"s3:GetObject"}}}
	sink := &storage.Node{ID: "sink-1", Labels: []string{"Sink"}, Properties: map[string]interface{}{"id": "sink"}}
	_, err := store.CreateNode(principal)
	require.NoError(t, err)
	_, err = store.CreateNode(sink)
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:         "principal-can-perform-sink",
		Type:       "CAN_PERFORM",
		StartNode:  principal.ID,
		EndNode:    sink.ID,
		Properties: map[string]interface{}{"actions": []string{"s3:GetObject", "s3:ListBucket"}},
	}))

	control, err := exec.Execute(ctx, `MATCH (p:Principal) WHERE 's3:GetObject' IN p.actions RETURN p`, nil)
	require.NoError(t, err)
	require.Len(t, control.Rows, 1)

	stored, err := exec.Execute(ctx, `MATCH ()-[rel:CAN_PERFORM]->() RETURN rel.actions`, nil)
	require.NoError(t, err)
	require.Len(t, stored.Rows, 1)
	require.Equal(t, []string{"s3:GetObject", "s3:ListBucket"}, stored.Rows[0][0])

	matched, err := exec.Execute(ctx, `
		MATCH (p)-[rel:CAN_PERFORM]->(s)
		WHERE 's3:GetObject' IN rel.actions
		RETURN s.id
	`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"s.id"}, matched.Columns)
	require.Len(t, matched.Rows, 1)
	require.Equal(t, "sink", matched.Rows[0][0])

	missing, err := exec.Execute(ctx, `
		MATCH (p)-[rel:CAN_PERFORM]->(s)
		WHERE 'ec2:StartInstances' IN rel.actions
		RETURN s.id
	`, nil)
	require.NoError(t, err)
	require.Empty(t, missing.Rows)
}
