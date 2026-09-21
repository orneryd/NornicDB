package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateRejectsRebindingMatchedNode(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	_, err := exec.Execute(context.Background(), "MATCH (person) CREATE (person)", nil)
	requireSemanticDetail(t, err, "VariableAlreadyBound")
}

func TestCreateRejectsChangingBoundNodePattern(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	queries := []string{
		"CREATE (person:First), (person:Second)-[:LINK]->()",
		"CREATE (person {name: 'Ada'}) CREATE (person:Second)-[:LINK]->()",
	}
	for _, query := range queries {
		_, err := exec.Execute(context.Background(), query, nil)
		requireSemanticDetail(t, err, "VariableAlreadyBound")
	}
}

func TestCreateRejectsUndefinedPropertyExpression(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	_, err := exec.Execute(context.Background(), "CREATE (person {name: missing}) RETURN person", nil)
	requireSemanticDetail(t, err, "UndefinedVariable")
}

func TestCreateRejectsInvalidRelationshipShapes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	tests := []struct {
		query  string
		detail string
	}{
		{query: "CREATE ()-->()", detail: "NoSingleRelationshipType"},
		{query: "CREATE ()-[:LINK]-()", detail: "RequiresDirectedRelationship"},
		{query: "CREATE ()<-[:LINK]->()", detail: "RequiresDirectedRelationship"},
		{query: "CREATE ()-[:FIRST|:SECOND]->()", detail: "NoSingleRelationshipType"},
		{query: "CREATE ()-[:LINK*2]->()", detail: "CreatingVarLength"},
	}

	for _, test := range tests {
		_, err := exec.Execute(context.Background(), test.query, nil)
		requireSemanticDetail(t, err, test.detail)
	}
}

func TestSeparateCreateClausesShareBindings(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	query := "CREATE (first) CREATE (second) CREATE (first)-[:LINK]->(second)"

	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	require.Equal(t, 2, result.Stats.NodesCreated)
	require.Equal(t, 1, result.Stats.RelationshipsCreated)
}

func TestCreateChainedPatternReusesIntermediateNodesAtAnyArity(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	const hops = 5

	result, err := exec.Execute(context.Background(), "CREATE ()"+strings.Repeat("-[:LINK]->()", hops), nil)
	require.NoError(t, err)
	require.Equal(t, hops+1, result.Stats.NodesCreated)
	require.Equal(t, hops, result.Stats.RelationshipsCreated)

	nodes, err := exec.Execute(context.Background(), "MATCH (node) RETURN count(node)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(hops+1), nodes.Rows[0][0])
	relationships, err := exec.Execute(context.Background(), "MATCH ()-[relationship:LINK]->() RETURN count(relationship)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(hops), relationships.Rows[0][0])
}

func TestChainedMatchReturnAllExpandsBoundVariables(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.Execute(context.Background(), "CREATE (:First)-[:LINK]->(:Second)-[:LINK]->(:Third)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(context.Background(), "MATCH (first:First)-[left:LINK]->(second:Second)-[right:LINK]->(third:Third) RETURN *", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"first", "left", "right", "second", "third"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], len(result.Columns))
}

func requireSemanticDetail(t *testing.T, err error, detail string) {
	t.Helper()
	require.Error(t, err)
	var semanticError *SemanticError
	require.ErrorAs(t, err, &semanticError)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
	require.Equal(t, detail, semanticError.Detail)
}
