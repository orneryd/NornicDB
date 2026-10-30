package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func executeBehaviorQuery(t *testing.T, executor *StorageExecutor, query string) *ExecuteResult {
	t.Helper()
	result, err := executor.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	return result
}

func TestReturnFunctionsAfterWithMatchPreserveRows(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})")

	result := executeBehaviorQuery(t, executor, `
		MATCH (a:Person)-[r:KNOWS]->(b:Person)
		WITH a, r, b
		MATCH (b)
		RETURN type(r) AS relationshipType, labels(b) AS nodeLabels, toUpper(b.name) AS upperName
	`)

	require.Equal(t, []string{"relationshipType", "nodeLabels", "upperName"}, result.Columns)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "KNOWS", result.Rows[0][0])
	require.Equal(t, []interface{}{"Person"}, result.Rows[0][1])
	require.Equal(t, "BOB", result.Rows[0][2])
}

func TestWithWhereOperatorsFilterProjectedRows(t *testing.T) {
	executor := setupTestExecutor(t)
	tests := []struct {
		name  string
		where string
	}{
		{name: "membership", where: "v IN ['alpha']"},
		{name: "prefix", where: "v STARTS WITH 'al'"},
		{name: "suffix", where: "v ENDS WITH 'pha'"},
		{name: "substring", where: "v CONTAINS 'ph'"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := executeBehaviorQuery(t, executor,
				"UNWIND ['alpha', 'beta'] AS v WITH v WHERE "+test.where+" RETURN v")
			require.Equal(t, [][]interface{}{{"alpha"}}, result.Rows)
		})
	}
}

func TestUnwindExpressionsEvaluateConcatenation(t *testing.T) {
	executor := setupTestExecutor(t)
	result := executeBehaviorQuery(t, executor,
		"UNWIND ['a', 'b'] AS v RETURN v + 'x' AS value")
	require.Equal(t, [][]interface{}{{"ax"}, {"bx"}}, result.Rows)
}

func TestListSubscriptsOnUnwindBindingsEvaluate(t *testing.T) {
	executor := setupTestExecutor(t)
	result := executeBehaviorQuery(t, executor,
		"UNWIND [[1, 2], [3, 4]] AS row RETURN row[0] AS value")
	require.Equal(t, [][]interface{}{{int64(1)}, {int64(3)}}, result.Rows)
}

func TestListLiteralElementsEvaluateInRowScope(t *testing.T) {
	executor := setupTestExecutor(t)
	result := executeBehaviorQuery(t, executor,
		"UNWIND [{act: 'go'}] AS row RETURN [row.act] AS actions")
	require.Equal(t, [][]interface{}{{[]interface{}{"go"}}}, result.Rows)
}

func TestUnwindExpressionsEvaluateInCreatedProperties(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, `
		UNWIND [{parts: ['a', 'b'], act: 'go'}] AS row
		CREATE (:ExpressionResult {
			concatenated: row.act + '!',
			first: row.parts[0],
			actions: [row.act]
		})
	`)

	result := executeBehaviorQuery(t, executor, `
		MATCH (n:ExpressionResult)
		RETURN n.concatenated, n.first, n.actions
	`)
	require.Equal(t, [][]interface{}{{"go!", "a", []interface{}{"go"}}}, result.Rows)
}

func TestUnwindExpressionsEvaluateInSetProperties(t *testing.T) {
	executor := setupTestExecutor(t)
	executeBehaviorQuery(t, executor, "CREATE (:ExpressionTarget {key: 'target'})")
	executeBehaviorQuery(t, executor, `
		UNWIND [{key: 'target', value: 'set'}] AS row
		MATCH (n:ExpressionTarget {key: row.key})
		SET n.value = row.value + '!', n.values = [row.value]
	`)

	result := executeBehaviorQuery(t, executor,
		"MATCH (n:ExpressionTarget) RETURN n.value, n.values")
	require.Equal(t, [][]interface{}{{"set!", []interface{}{"set"}}}, result.Rows)
}
