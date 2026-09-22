package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatchRejectsParameterMapInPatternPredicate(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_parameter_semantics"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, "MATCH (n $param) RETURN n", map[string]interface{}{"param": map[string]interface{}{"name": "Alice"}})
	requireMatchSemanticDetail(t, err, "InvalidParameterUse")

	_, err = exec.Execute(ctx, "MATCH (n {name: $name}) RETURN n", map[string]interface{}{"name": "Alice"})
	require.NoError(t, err)
}

func TestMatchRejectsVariablesBoundToDifferentEntityKinds(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_binding_semantics"))
	ctx := context.Background()

	queries := []string{
		"MATCH ()-[r]-() MATCH (r) RETURN r",
		"MATCH r = ()-[]->() MATCH (r) RETURN r",
		"MATCH ()-[r]-(), (r) RETURN r",
		"MATCH r = ()-[]-(), (r) RETURN r",
		"WITH true AS n MATCH (n) RETURN n",
		"WITH [10] AS n MATCH (n) RETURN n",
	}

	for _, query := range queries {
		_, err := exec.Execute(ctx, query, nil)
		requireMatchSemanticDetail(t, err, "VariableTypeConflict")
	}
}

func TestMatchAllowsRepeatedNodeBindings(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_repeated_node"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, "MATCH (n), (n) RETURN n", nil)
	require.NoError(t, err)
}

func TestUndirectedMatchEmitsSelfRelationshipOnce(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_self_relationship")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	nodeID, err := store.CreateNode(&storage.Node{ID: "node"})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:        "relationship",
		Type:      "LOOP",
		StartNode: nodeID,
		EndNode:   nodeID,
	}))

	result, err := exec.Execute(ctx, "MATCH ()-[r]-() RETURN type(r) AS relationshipType", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"relationshipType"}, result.Columns)
	require.Equal(t, [][]interface{}{{"LOOP"}}, result.Rows)
}

func requireMatchSemanticDetail(t *testing.T, err error, detail string) {
	t.Helper()
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
	require.Equal(t, detail, semanticError.Detail)
}
