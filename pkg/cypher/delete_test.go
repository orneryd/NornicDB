package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetachDeleteAll(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create some nodes
	_, err := executor.Execute(ctx, `CREATE (a:Person {name: 'Alice', id: 'alice-1'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (b:Person {name: 'Bob', id: 'bob-1'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (c:Person {name: 'Charlie', id: 'charlie-1'})`, nil)
	require.NoError(t, err)

	// Verify nodes exist
	countResult, err := executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(3), count, "Should have 3 nodes")

	// Delete all nodes with DETACH DELETE
	deleteResult, err := executor.Execute(ctx, `MATCH (n) DETACH DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 3, deleteResult.Stats.NodesDeleted, "Should delete 3 nodes")

	// Verify all nodes are gone
	countResult, err = executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Len(t, countResult.Rows, 1)
	count = countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(0), count, "Should have 0 nodes after delete")
}

func TestDetachDeleteWithRelationships(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes with relationships
	_, err := executor.Execute(ctx, `
		CREATE (a:Person {name: 'Alice', id: 'alice-2'})
		CREATE (b:Person {name: 'Bob', id: 'bob-2'})
		CREATE (a)-[:KNOWS]->(b)
	`, nil)
	require.NoError(t, err)

	// Verify nodes and edges exist
	countResult, err := executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(2), count, "Should have 2 nodes")

	// Delete all with DETACH (should delete edges too)
	deleteResult, err := executor.Execute(ctx, `MATCH (n) DETACH DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, deleteResult.Stats.NodesDeleted, "Should delete 2 nodes")
	// Edges should also be deleted
	assert.GreaterOrEqual(t, deleteResult.Stats.RelationshipsDeleted, 1, "Should delete at least 1 relationship")

	// Verify all gone
	countResult, err = executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	count = countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(0), count, "Should have 0 nodes")
}

func TestDeleteWithFilter(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes
	_, err := executor.Execute(ctx, `CREATE (a:Person {name: 'Alice', age: 30})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (b:Person {name: 'Bob', age: 25})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (c:Animal {name: 'Charlie'})`, nil)
	require.NoError(t, err)

	// Delete only Person nodes
	deleteResult, err := executor.Execute(ctx, `MATCH (n:Person) DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, deleteResult.Stats.NodesDeleted, "Should delete 2 Person nodes")

	// Animal should still exist
	countResult, err := executor.Execute(ctx, `MATCH (n:Animal) RETURN count(n)`, nil)
	require.NoError(t, err)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(1), count, "Should have 1 Animal node")
}

func TestDeleteReturnModifiersShapeRowsAfterAllSideEffects(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := executor.Execute(ctx, "CREATE (:Item {num: 1}), (:Item {num: 2}), (:Item {num: 3}), (:Item {num: 4}), (:Item {num: 5})", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, "MATCH (node:Item) DELETE node RETURN 42 AS value SKIP 2 LIMIT 2", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(42)}, {int64(42)}}, result.Rows)
	require.Equal(t, 5, result.Stats.NodesDeleted)
}

func TestDeletePreservesScalarBindingsForFilteringAndAggregation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := executor.Execute(ctx, "CREATE (:Item {num: 1}), (:Item {num: 2}), (:Item {num: 3}), (:Item {num: 4}), (:Item {num: 5})", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, "MATCH (node:Item) WITH node, node.num AS num DELETE node WITH num WHERE num % 2 = 0 RETURN num", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}}, result.Rows)
	require.Equal(t, 5, result.Stats.NodesDeleted)

	_, err = executor.Execute(ctx, "CREATE (:Other {num: 1}), (:Other {num: 2}), (:Other {num: 3}), (:Other {num: 4}), (:Other {num: 5})", nil)
	require.NoError(t, err)
	aggregated, err := executor.Execute(ctx, "MATCH (node:Other) WITH node, node.num AS num DELETE node RETURN sum(num) AS total", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(15)}}, aggregated.Rows)
	require.Equal(t, 5, aggregated.Stats.NodesDeleted)
}

func TestDeleteRejectsInvalidTargetsBeforeMutation(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		detail string
	}{
		{name: "label expression", query: "MATCH (node:Item) DELETE node:Other", detail: "InvalidDelete"},
		{name: "undefined variable", query: "MATCH (node:Item) DELETE missing", detail: "UndefinedVariable"},
		{name: "scalar expression", query: "MATCH (node:Item) DELETE 1 + 1", detail: "InvalidArgumentType"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			_, err := executor.Execute(context.Background(), "CREATE (:Item)", nil)
			require.NoError(t, err)

			_, err = executor.Execute(context.Background(), test.query, nil)
			require.Error(t, err)
			var semantic *SemanticError
			require.ErrorAs(t, err, &semantic)
			assert.Equal(t, "Neo.ClientError.Statement.SyntaxError", semantic.Code)
			assert.Equal(t, test.detail, semantic.Detail)

			result, countErr := executor.Execute(context.Background(), "MATCH (node:Item) RETURN count(node)", nil)
			require.NoError(t, countErr)
			assert.Equal(t, int64(1), result.Rows[0][0])
		})
	}
}

func TestDeletePathTargetsFromNestedCollections(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := executor.Execute(ctx, `
		CREATE (left:User), (right:User)
		CREATE (left)-[:LINK]->(right)
		CREATE (right)-[:LINK]->(left)
	`, nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `
		MATCH path = (:User)-[relationship]->(:User)
		WITH {key: collect(path)} AS paths
		DELETE paths.key[0], paths.key[1]
	`, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Stats.NodesDeleted)
	assert.Equal(t, 2, result.Stats.RelationshipsDeleted)
}

func TestDetachDeleteAnonymousChainedPath(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := executor.Execute(ctx, `
		CREATE (start:Start), (middleOne), (middleTwo), (finish)
		CREATE (start)-[:LINK]->(middleOne)
		CREATE (middleOne)-[:LINK]->(middleTwo)
		CREATE (middleTwo)-[:LINK]->(finish)
	`, nil)
	require.NoError(t, err)
	edges, edgeErr := executor.Execute(ctx, `MATCH ()-[relationship]->() RETURN count(relationship)`, nil)
	require.NoError(t, edgeErr)
	require.Equal(t, int64(3), edges.Rows[0][0])
	parsed := executor.parseTraversalPattern(ctx, `(:Start)-->()-->()-->()`)
	require.NotNil(t, parsed)
	require.Len(t, parsed.Segments, 3)
	require.Len(t, executor.traverseGraph(ctx, parsed), 1)
	matched, matchErr := executor.Execute(ctx, `MATCH path = (:Start)-->()-->()-->() RETURN path`, nil)
	require.NoError(t, matchErr)
	require.Len(t, matched.Rows, 1)

	result, err := executor.Execute(ctx, `MATCH path = (:Start)-->()-->()-->() DETACH DELETE path`, nil)
	require.NoError(t, err)
	assert.Equal(t, 4, result.Stats.NodesDeleted)
	assert.Equal(t, 3, result.Stats.RelationshipsDeleted)
}

func TestUndirectedVariableLengthDeleteCountsRelationshipUniquePaths(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := executor.Execute(ctx, `
		CREATE (first), (second), (third)
		CREATE (first)-[:LINK]->(second)
		CREATE (second)-[:LINK]->(third)
	`, nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx, `MATCH (left)-[*]-(right) DETACH DELETE left, right RETURN count(*) AS count`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(6)}}, result.Rows)
	assert.Equal(t, 3, result.Stats.NodesDeleted)
	assert.Equal(t, 2, result.Stats.RelationshipsDeleted)
}

// TestDetachDeleteParsing tests that DETACH DELETE is parsed correctly
// This fixes the issue where "MATCH (n) DETACH DELETE n" wasn't working
func TestDetachDeleteParsing(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes with relationships
	_, err := executor.Execute(ctx, `
		CREATE (a:Test {id: 'a'})
		CREATE (b:Test {id: 'b'})
		CREATE (a)-[:REL]->(b)
	`, nil)
	require.NoError(t, err)

	// Verify nodes exist
	countResult, err := executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(2), count, "Should have 2 nodes")

	// DETACH DELETE should work (this was failing before the fix)
	deleteResult, err := executor.Execute(ctx, `MATCH (n) DETACH DELETE n`, nil)
	require.NoError(t, err, "DETACH DELETE should work")
	assert.Equal(t, 2, deleteResult.Stats.NodesDeleted, "Should delete 2 nodes")
	assert.GreaterOrEqual(t, deleteResult.Stats.RelationshipsDeleted, 1, "Should delete relationships")

	// Verify all nodes are gone
	countResult, err = executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	count = countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(0), count, "Should have 0 nodes after DETACH DELETE")
}

func TestDeleteWithWhereClause(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes
	_, err := executor.Execute(ctx, `CREATE (a:Person {name: 'Alice', age: 30})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (b:Person {name: 'Bob', age: 25})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (c:Person {name: 'Charlie', age: 35})`, nil)
	require.NoError(t, err)

	// Delete only people over 28
	deleteResult, err := executor.Execute(ctx, `MATCH (n:Person) WHERE n.age > 28 DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, deleteResult.Stats.NodesDeleted, "Should delete Alice and Charlie")

	// Bob should still exist
	countResult, err := executor.Execute(ctx, `MATCH (n:Person) RETURN count(n)`, nil)
	require.NoError(t, err)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(1), count, "Should have 1 Person node (Bob)")
}

func TestDeleteWithParameters(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create nodes
	_, err := executor.Execute(ctx, `CREATE (a:Task {id: 'task-1', status: 'pending'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (b:Task {id: 'task-2', status: 'completed'})`, nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, `CREATE (c:Task {id: 'task-3', status: 'pending'})`, nil)
	require.NoError(t, err)

	// Delete completed tasks using parameters
	deleteResult, err := executor.Execute(ctx, `MATCH (t:Task) WHERE t.status = $status DELETE t`, map[string]interface{}{
		"status": "completed",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, deleteResult.Stats.NodesDeleted, "Should delete 1 completed task")

	// 2 pending tasks should remain
	countResult, err := executor.Execute(ctx, `MATCH (t:Task) RETURN count(t)`, nil)
	require.NoError(t, err)
	count := countResult.Rows[0][0].(int64)
	assert.Equal(t, int64(2), count, "Should have 2 pending tasks")
}

func TestDeleteContentWithCypherKeywords(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)

	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	// Create node with content containing Cypher-like text (regression test)
	_, err := executor.Execute(ctx, `CREATE (n:Memory {id: 'mem-1', content: $content})`, map[string]interface{}{
		"content": "Example: MATCH (n) DELETE n - this is just text, not a command",
	})
	require.NoError(t, err)

	// Verify created
	countResult, err := executor.Execute(ctx, `MATCH (n:Memory) RETURN count(n)`, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), countResult.Rows[0][0].(int64))

	// Delete it
	deleteResult, err := executor.Execute(ctx, `MATCH (n:Memory) DETACH DELETE n`, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, deleteResult.Stats.NodesDeleted)

	// Verify deleted
	countResult, err = executor.Execute(ctx, `MATCH (n:Memory) RETURN count(n)`, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), countResult.Rows[0][0].(int64))
}

func TestDetachDelete_DeduplicatesRepeatedRowsFromOptionalMatch(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	_, err := executor.Execute(ctx, `
		CREATE (o:OriginalText {id: 'o-1'})
		CREATE (t1:TranslatedText {id: 't-1'})
		CREATE (t2:TranslatedText {id: 't-2'})
		CREATE (t3:TranslatedText {id: 't-3'})
		CREATE (o)-[:TRANSLATES_TO]->(t1)
		CREATE (o)-[:TRANSLATES_TO]->(t2)
		CREATE (o)-[:TRANSLATES_TO]->(t3)
	`, nil)
	require.NoError(t, err)

	// OPTIONAL MATCH produces repeated rows for `o`; delete executor must dedupe
	// node and edge deletes so each entity is deleted once.
	deleteResult, err := executor.Execute(ctx, `
		MATCH (o:OriginalText)
		OPTIONAL MATCH (o)-[:TRANSLATES_TO]->(t:TranslatedText)
		DETACH DELETE o, t
	`, nil)
	require.NoError(t, err)
	require.NotNil(t, deleteResult.Stats)
	assert.Equal(t, 4, deleteResult.Stats.NodesDeleted, "expected unique node deletes only")
	assert.GreaterOrEqual(t, deleteResult.Stats.RelationshipsDeleted, 3)

	remaining, err := executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Len(t, remaining.Rows, 1)
	assert.Equal(t, int64(0), remaining.Rows[0][0].(int64))
}

func TestDetachDelete_WhereElementIDWithOptionalMatch(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	_, err := executor.Execute(ctx, `
		CREATE (o:OriginalText {id: 'o-del-1'})
		CREATE (t:TranslatedText {id: 't-del-1'})
		CREATE (o)-[:TRANSLATES_TO]->(t)
	`, nil)
	require.NoError(t, err)

	elemRes, err := executor.Execute(ctx, `
		MATCH (o:OriginalText {id: 'o-del-1'})
		RETURN elementId(o) AS eid
		LIMIT 1
	`, nil)
	require.NoError(t, err)
	require.Len(t, elemRes.Rows, 1)
	eid, ok := elemRes.Rows[0][0].(string)
	require.True(t, ok)
	require.NotEmpty(t, eid)

	deleteResult, err := executor.Execute(ctx, `
		MATCH (o:OriginalText)
		WHERE elementId(o) = $eid
		OPTIONAL MATCH (o)-[:TRANSLATES_TO]->(t:TranslatedText)
		DETACH DELETE o, t
	`, map[string]interface{}{"eid": eid})
	require.NoError(t, err)
	require.NotNil(t, deleteResult.Stats)
	assert.GreaterOrEqual(t, deleteResult.Stats.NodesDeleted, 1)

	remaining, err := executor.Execute(ctx, `MATCH (n) RETURN count(n)`, nil)
	require.NoError(t, err)
	require.Len(t, remaining.Rows, 1)
	assert.Equal(t, int64(0), remaining.Rows[0][0].(int64))
}

func TestDeleteStreamingEligibility(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)

	assert.True(t, executor.isDeleteStreamingEligible("MATCH (n:Person)", "n", true))
	assert.False(t, executor.isDeleteStreamingEligible("MATCH (n:Person) WITH n LIMIT 100", "n", true))
	assert.False(t, executor.isDeleteStreamingEligible("MATCH (n:Person) ORDER BY n.createdAt DESC", "n", true))
	assert.False(t, executor.isDeleteStreamingEligible("MATCH (n:Person)", "n", false))
}

func TestDetachDeleteStreaming_ReturnCountProjection(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	for i := 0; i < deleteStreamingBatchSize+25; i++ {
		_, err := executor.Execute(ctx, fmt.Sprintf("CREATE (:StreamDelete {id: 'sd-%d'})", i), nil)
		require.NoError(t, err)
	}

	deleteResult, err := executor.Execute(ctx, `
MATCH (n:StreamDelete)
DETACH DELETE n
RETURN count(n) AS deleted`, nil)
	require.NoError(t, err)
	require.NotNil(t, deleteResult.Stats)
	require.Len(t, deleteResult.Rows, 1)
	require.Len(t, deleteResult.Columns, 1)
	assert.Equal(t, "deleted", deleteResult.Columns[0])
	assert.Equal(t, int64(deleteStreamingBatchSize+25), deleteResult.Rows[0][0])
	assert.Equal(t, deleteStreamingBatchSize+25, deleteResult.Stats.NodesDeleted)

	remaining, err := executor.Execute(ctx, "MATCH (n:StreamDelete) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, remaining.Rows, 1)
	assert.Equal(t, int64(0), remaining.Rows[0][0])
}

func TestDetachDelete_WithLimitHotPath_ReturnCountProjection(t *testing.T) {
	baseEngine := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(baseEngine, "test")
	executor := NewStorageExecutor(engine)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := executor.Execute(ctx, fmt.Sprintf("CREATE (:TmpDelete {testRun: 'run-1', id: 'd-%d'})", i), nil)
		require.NoError(t, err)
	}

	res, err := executor.Execute(ctx, `
MATCH (n:TmpDelete)
WHERE n.testRun = 'run-1'
WITH n LIMIT 2
DETACH DELETE n
RETURN count(n) AS deleted
`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	assert.Equal(t, int64(2), res.Rows[0][0], "WITH LIMIT delete hot path should delete limited batch")

	remaining, err := executor.Execute(ctx, "MATCH (n:TmpDelete) WHERE n.testRun = 'run-1' RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Len(t, remaining.Rows, 1)
	assert.Equal(t, int64(3), remaining.Rows[0][0])
}
