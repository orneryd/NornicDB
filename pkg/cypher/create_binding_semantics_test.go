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

func TestCreateRelationshipBindsPreviouslyUnboundEndpoint(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	result, err := exec.Execute(context.Background(), "CREATE (root) CREATE (root)-[:LINK]->(newcomer)", nil)
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

func TestCreatePipelinePreservesAnonymousRowCardinalityAcrossWildcardProjection(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	_, err := exec.Execute(context.Background(), "CREATE (), ()", nil)
	require.NoError(t, err)

	result, err := exec.Execute(context.Background(), "MATCH () CREATE () WITH * CREATE ()", nil)
	require.NoError(t, err)
	require.Equal(t, 4, result.Stats.NodesCreated)

	count, err := exec.Execute(context.Background(), "MATCH (node) RETURN count(node)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(6), count.Rows[0][0])
}

func TestCreateReturnModifiersDoNotDiscardSideEffects(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	limited, err := exec.Execute(context.Background(), "CREATE (node:Item {num: 1}) RETURN node LIMIT 0", nil)
	require.NoError(t, err)
	require.Empty(t, limited.Rows)

	skipped, err := exec.Execute(context.Background(), "CREATE (node:Item {num: 2}) RETURN node SKIP 1", nil)
	require.NoError(t, err)
	require.Empty(t, skipped.Rows)

	count, err := exec.Execute(context.Background(), "MATCH (node:Item) RETURN count(node)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), count.Rows[0][0])
}

func TestUnwindCreateAppliesProjectionHorizonsAfterEveryMutation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	window, err := exec.Execute(context.Background(), "UNWIND [1, 2, 3, 4, 5] AS value CREATE (node:Item {num: value}) RETURN node.num AS num SKIP 2 LIMIT 2", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}, {int64(4)}}, window.Rows)

	filtered, err := exec.Execute(context.Background(), "UNWIND [1, 2, 3, 4, 5] AS value CREATE (node:Filtered {num: value}) WITH node WHERE node.num % 2 = 0 RETURN node.num AS num", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"num"}, filtered.Columns)
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}}, filtered.Rows)
}

func TestUnwindCreateAggregatesMutatedRows(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	direct, err := exec.Execute(context.Background(), "UNWIND [1, 2, 3, 4, 5] AS value CREATE (node:Direct {num: value}) RETURN sum(node.num) AS total", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"total"}, direct.Columns)
	require.Equal(t, int64(15), direct.Rows[0][0])

	throughWith, err := exec.Execute(context.Background(), "UNWIND [1, 2, 3, 4, 5] AS value CREATE (node:Projected {num: value}) WITH sum(node.num) AS total RETURN total", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"total"}, throughWith.Columns)
	require.Equal(t, int64(15), throughWith.Rows[0][0])
}

func TestUnwindCreateAnonymousRelationshipsPreservesBindings(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	result, err := exec.Execute(context.Background(), "UNWIND [1, 2, 3] AS value CREATE ()-[relationship:LINK {num: value}]->() RETURN sum(relationship.num) AS total", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"total"}, result.Columns)
	require.Equal(t, int64(6), result.Rows[0][0])
	require.Equal(t, 6, result.Stats.NodesCreated)
	require.Equal(t, 3, result.Stats.RelationshipsCreated)
}

func requireSemanticDetail(t *testing.T, err error, detail string) {
	t.Helper()
	require.Error(t, err)
	var semanticError *SemanticError
	require.ErrorAs(t, err, &semanticError)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
	require.Equal(t, detail, semanticError.Detail)
}
