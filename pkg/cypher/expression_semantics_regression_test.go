package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCollectedNodesRemainTypedThroughListComprehensionAndMutation(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:Label1 {name: 'original'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MATCH (a:Label1)
		WITH collect(a) AS nodes
		WITH nodes, [x IN nodes | x.name] AS oldNames
		UNWIND nodes AS n
		SET n.name = 'newName'
		RETURN n.name, oldNames
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"newName", []interface{}{"original"}}}, result.Rows)
}

func TestCollectedNodesCanBeFilteredByPropertyInListComprehension(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (:Label1 {name: 'original'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MATCH (a:Label1)
		WITH collect(a) AS nodes
		WITH nodes, [x IN nodes WHERE x.name = 'original'] AS filtered
		UNWIND nodes AS n
		SET n.name = 'newName'
		RETURN n.name, size(filtered)
	`, nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"newName", int64(1)}}, result.Rows)
}

func TestCollectedPathsCanBeProjectedByListComprehension(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "CREATE (a:A)-[:T]->(:B), (a)-[:T]->(:C)", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, `
		MATCH p = (n:A)-->()
		RETURN [x IN collect(p) | head(nodes(x))] AS starts
	`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	starts, ok := result.Rows[0][0].([]interface{})
	require.True(t, ok)
	require.Len(t, starts, 2)
	for _, start := range starts {
		node, ok := start.(*storage.Node)
		require.True(t, ok)
		require.Contains(t, node.Labels, "A")
	}
}

func TestAggregateInsideListComprehensionIsRejected(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	_, err := exec.Execute(ctx, "MATCH (n) RETURN [x IN [1, 2, 3] | count(*)]", nil)
	require.Error(t, err)
	var semanticError *SemanticError
	require.True(t, errors.As(err, &semanticError))
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
	require.Equal(t, "InvalidAggregation", semanticError.Detail)
}

func TestRowPredicateCombinesEqualityWithParenthesizedAlternative(t *testing.T) {
	exec := &StorageExecutor{}
	row := map[string]interface{}{
		"o": &storage.Node{Properties: map[string]interface{}{
			"textKey128": "noise",
			"textKey":    "other",
		}},
	}
	require.False(t, exec.evaluateRowPredicate(context.Background(), `o.textKey128 = "needle"`, row))
	value, ok := rowValue(t, exec, `"unique-key" IS NOT NULL AND o.textKey = "unique-key"`, row)
	require.True(t, ok)
	require.Equal(t, false, value)
	require.False(t, exec.evaluateRowPredicate(context.Background(),
		`("unique-key" IS NOT NULL AND o.textKey = "unique-key")`, row))
	require.False(t, exec.evaluateRowPredicate(context.Background(),
		`o.textKey128 = "needle" OR ("unique-key" IS NOT NULL AND o.textKey = "unique-key")`, row))
}

func TestRowPredicateCombinesNumericBounds(t *testing.T) {
	exec := &StorageExecutor{}
	row := map[string]interface{}{
		"other": &storage.Node{Properties: map[string]interface{}{"age": int64(25)}},
	}
	require.True(t, exec.evaluateRowPredicate(context.Background(),
		"other.age > 24 AND other.age < 26", row))
}

func TestRowPredicateNegatesParenthesizedConjunctionWithoutWhitespace(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	value, ok := rowValue(t, exec, "NOT(n.name = 'apa' AND false)", map[string]interface{}{
		"n": &storage.Node{Properties: map[string]interface{}{"name": "a"}},
	})
	require.True(t, ok)
	require.Equal(t, true, value)
}

func TestReturnRejectsDuplicateProjectedColumnNames(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())
	_, err := exec.Execute(context.Background(), "RETURN 1 AS value, 2 AS value", nil)
	require.Error(t, err)
	var semanticErr *SemanticError
	require.True(t, errors.As(err, &semanticErr))
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticErr.Code)
	require.Equal(t, "ColumnNameConflict", semanticErr.Detail)
}

func TestReturnAllowsCaseDistinctProjectedColumnNames(t *testing.T) {
	exec := NewStorageExecutor(storage.NewMemoryEngine())

	result, err := exec.Execute(context.Background(), "RETURN 1 AS value, 2 AS Value", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"value", "Value"}, result.Columns)
}

func TestListComprehensionFiltersBeforeProjection(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	result, err := exec.Execute(ctx, "RETURN [x IN range(1,5) WHERE x % 2 = 1 | x * 10] AS values", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{int64(10), int64(30), int64(50)}}}, result.Rows)
}

func TestNumericDivisionAndPowerPreserveCypherTypes(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	result, err := exec.Execute(ctx, "RETURN 7 / 2 AS integerDivision, 7.0 / 2 AS floatDivision, 2 ^ 3 AS power", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3), float64(3.5), float64(8)}}, result.Rows)

	result, err = exec.Execute(ctx, "WITH 7 AS a, 2 AS b RETURN a / b AS integerDivision", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)
}

func TestReduceUsesBindingsCarriedThroughWith(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	result, err := exec.Execute(ctx, "WITH [1,2,3] AS values RETURN reduce(total = 0, value IN values | total + value) AS total", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(6)}}, result.Rows)
}

func TestNestedMapAccessUsesTypedValues(t *testing.T) {
	exec, ctx := newConvergenceExecutor(t)
	result, err := exec.Execute(ctx, "WITH {a:1, b:{c:2}} AS m RETURN m.b.c AS nested", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}
