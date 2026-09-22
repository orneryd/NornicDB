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

func TestMatchAllowsNodeBindingProjectedThroughCoalesce(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_coalesced_node"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, "OPTIONAL MATCH (left) OPTIONAL MATCH (right) WITH coalesce(left, right) AS node MATCH (node)-->(target) RETURN target", nil)
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

func TestMatchRejectsRelationshipReuseWithinOnePattern(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_relationship_uniqueness"))
	ctx := context.Background()

	_, err := exec.Execute(ctx, "MATCH (a)-[r]->()-[r]->(a) RETURN r", nil)
	requireMatchSemanticDetail(t, err, "RelationshipUniquenessViolation")
}

func TestMatchRejectsPathVariableAlreadyBoundByAnotherEntity(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_path_binding_conflict"))
	ctx := context.Background()

	for _, query := range []string{
		"MATCH (path) MATCH path = ()-[]-() RETURN path",
		"MATCH ()-[path]-() MATCH path = ()-[]-() RETURN path",
		"WITH true AS path MATCH path = ()-[]-() RETURN path",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireMatchSemanticDetail(t, err, "VariableAlreadyBound")
	}
}

func TestMatchRejectsEntityBindingAfterPathAsTypeConflict(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_path_then_entity_conflict"))
	ctx := context.Background()

	for _, query := range []string{
		"MATCH path = ()-[]-(), (path) RETURN path",
		"MATCH path = ()-[]-(), ()-[path]-() RETURN path",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireMatchSemanticDetail(t, err, "VariableTypeConflict")
	}
}

func TestUndirectedMatchFiltersUntypedRelationshipProperties(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_relationship_properties")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	leftID, err := store.CreateNode(&storage.Node{ID: "left", Labels: []string{"Left"}})
	require.NoError(t, err)
	rightID, err := store.CreateNode(&storage.Node{ID: "right", Labels: []string{"Right"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{
		ID:         "relationship",
		Type:       "LINK",
		StartNode:  leftID,
		EndNode:    rightID,
		Properties: map[string]interface{}{"name": "selected"},
	}))

	result, err := exec.Execute(ctx, "MATCH (a)-[r {name: 'selected'}]-(b) RETURN a, b", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
}

func TestBidirectionalArrowMatchesRelationshipInEitherDirection(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_bidirectional_arrow")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	leftID, err := store.CreateNode(&storage.Node{ID: "left", Properties: map[string]interface{}{"name": "A"}})
	require.NoError(t, err)
	middleID, err := store.CreateNode(&storage.Node{ID: "middle", Properties: map[string]interface{}{"name": "X"}})
	require.NoError(t, err)
	rightID, err := store.CreateNode(&storage.Node{ID: "right", Properties: map[string]interface{}{"name": "B"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "left-middle", Type: "LINK", StartNode: leftID, EndNode: middleID}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "right-middle", Type: "LINK", StartNode: rightID, EndNode: middleID}))

	result, err := exec.Execute(ctx, "MATCH (a {name: 'A'}), (b {name: 'B'}) MATCH (a)-->(x)<-->(b) RETURN x", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
}

func TestMandatoryMatchDropsNullOptionalBinding(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "match_null_optional_binding"))
	ctx := context.Background()

	result, err := exec.Execute(ctx, "OPTIONAL MATCH (a) WITH a MATCH (a)-->(b) RETURN b", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}

func requireMatchSemanticDetail(t *testing.T, err error, detail string) {
	t.Helper()
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
	require.Equal(t, detail, semanticError.Detail)
}
