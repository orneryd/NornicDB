package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestApplyOrderByToResult_DottedPropertyBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	result := &ExecuteResult{
		Columns: []string{"t"},
		Rows: [][]interface{}{
			{map[string]interface{}{"createdAt": int64(2)}},
			{map[string]interface{}{"properties": map[string]interface{}{"createdAt": int64(1)}}},
			{map[string]interface{}{"createdAt": int64(3)}},
		},
	}

	ordered := exec.applyOrderByToResult(result, "ORDER BY t.createdAt ASC")
	require.Len(t, ordered.Rows, 3)
	require.EqualValues(t, int64(1), extractPropertyFromValue(ordered.Rows[0][0], "createdAt"))
	require.EqualValues(t, int64(2), extractPropertyFromValue(ordered.Rows[1][0], "createdAt"))
	require.EqualValues(t, int64(3), extractPropertyFromValue(ordered.Rows[2][0], "createdAt"))

	ordered = exec.applyOrderByToResult(result, "ORDER BY t.createdAt DESC LIMIT 2")
	require.Len(t, ordered.Rows, 3)
	require.EqualValues(t, int64(3), extractPropertyFromValue(ordered.Rows[0][0], "createdAt"))
}

func TestApplyOrderByToResult_UnknownAndEmptyBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	base := &ExecuteResult{
		Columns: []string{"name"},
		Rows: [][]interface{}{
			{"b"},
			{"a"},
		},
	}

	unchanged := exec.applyOrderByToResult(base, "ORDER BY missing")
	require.Equal(t, [][]interface{}{{"b"}, {"a"}}, unchanged.Rows)

	unchanged = exec.applyOrderByToResult(base, "ORDER BY")
	require.Equal(t, [][]interface{}{{"b"}, {"a"}}, unchanged.Rows)
}

func TestApplyResultModifiers_KZeroAndUnknownOrderColumn(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	in := &ExecuteResult{
		Columns: []string{"age"},
		Rows: [][]interface{}{
			{int64(3)},
			{int64(1)},
			{int64(2)},
		},
	}

	// k=0 branch: LIMIT 0 with ORDER BY should return no rows.
	out, err := exec.applyResultModifiers(in, "ORDER BY age ASC LIMIT 0")
	require.NoError(t, err)
	require.Empty(t, out.Rows)

	in2 := &ExecuteResult{
		Columns: []string{"name"},
		Rows: [][]interface{}{
			{"c"},
			{"a"},
			{"b"},
		},
	}

	// Unknown ORDER BY column with LIMIT should preserve previous behavior
	// and still apply LIMIT after no-op ORDER BY.
	out2, err := exec.applyResultModifiers(in2, "ORDER BY missing DESC LIMIT 2")
	require.NoError(t, err)
	require.Len(t, out2.Rows, 2)
	require.Equal(t, "c", out2.Rows[0][0])
	require.Equal(t, "a", out2.Rows[1][0])
}

func TestApplyResultModifiersOrdersByEveryTermBeforeMultilineWindow(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	input := &ExecuteResult{
		Columns: []string{"group", "rank", "name"},
		Rows: [][]interface{}{
			{"b", int64(1), "second"},
			{"a", int64(1), "third"},
			{"a", int64(2), "first"},
		},
	}

	result, err := exec.applyResultModifiers(input, "ORDER BY group ASC, rank DESC, name ASC\nSKIP 1\nLIMIT 1")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a", int64(1), "third"}}, result.Rows)
}

func TestApplyResultModifiersSelectsAscendingNumericTopK(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	input := &ExecuteResult{
		Columns: []string{"score"},
		Rows:    [][]interface{}{{0.56}, {0.52}, {0.44}, {0.41}, {0.38}, {0.32}},
	}

	result, err := exec.applyResultModifiers(input, "ORDER BY score ASC\nLIMIT 5")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{0.32}, {0.38}, {0.41}, {0.44}, {0.52}}, result.Rows)
}

func TestPipelineOrderByEvaluatesCompleteBooleanExpressions(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	rows := []pipelineRow{
		{"name": "A", "first": true, "second": true},
		{"name": "B", "first": false, "second": false},
		{"name": "C", "first": false, "second": true},
		{"name": "D", "first": true, "second": true},
		{"name": "E", "first": true, "second": false},
	}

	terms := parseOrderByTerms("ORDER BY NOT (first AND second) DESCENDING LIMIT 3")
	require.Len(t, terms, 1)
	require.Equal(t, "NOT (first AND second)", terms[0].column)
	require.True(t, terms[0].descending)
	require.True(t, exec.orderPipelineRows(rows, terms))
	require.Equal(t, []string{"B", "C", "E", "A", "D"}, []string{
		rows[0]["name"].(string),
		rows[1]["name"].(string),
		rows[2]["name"].(string),
		rows[3]["name"].(string),
		rows[4]["name"].(string),
	})
}

func TestPipelineOrderByUsesIncomingAndProjectedScopeWithoutLeakingBindings(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	rows := []pipelineRow{{"name": "A"}, {"name": "A"}, {"name": "B"}, {"name": "C"}, {"name": "C"}}
	scopes := []pipelineRow{
		{"a": map[string]interface{}{"name": "A"}, "name": "A"},
		{"a": map[string]interface{}{"name": "A"}, "name": "A"},
		{"a": map[string]interface{}{"name": "B"}, "name": "B"},
		{"a": map[string]interface{}{"name": "C"}, "name": "C"},
		{"a": map[string]interface{}{"name": "C"}, "name": "C"},
	}

	require.True(t, exec.orderPipelineRowsWithScopes(rows, scopes, parseOrderByTerms("ORDER BY a.name + 'C' DESC")))
	require.Equal(t, []pipelineRow{{"name": "C"}, {"name": "C"}, {"name": "B"}, {"name": "A"}, {"name": "A"}}, rows)
	for _, row := range rows {
		require.NotContains(t, row, "a")
	}
}

func TestPipelineOrderByUsesGroupingKeyFromIncomingScope(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	rows := []pipelineRow{{"name": "A", "count": int64(2)}, {"name": "B", "count": int64(1)}, {"name": "C", "count": int64(2)}}
	scopes := []pipelineRow{
		{"a": map[string]interface{}{"name": "A"}, "name": "A", "count": int64(2)},
		{"a": map[string]interface{}{"name": "B"}, "name": "B", "count": int64(1)},
		{"a": map[string]interface{}{"name": "C"}, "name": "C", "count": int64(2)},
	}

	require.True(t, exec.orderPipelineRowsWithScopes(rows, scopes, parseOrderByTerms("ORDER BY a.name DESC")))
	require.Equal(t, "C", rows[0]["name"])
	require.NotContains(t, rows[0], "a")
}

func TestRowQuantifiersEvaluateTypedRelationshipPropertiesAndNulls(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	relationships := []interface{}{
		&storage.Edge{Properties: map[string]interface{}{"name": "a"}},
		&storage.Edge{Properties: map[string]interface{}{"name": "b"}},
	}
	values := pipelineRow{"relationships": relationships, "withNull": []interface{}{int64(1), nil}}

	value, ok := exec.evaluateRowExpression("none(x IN relationships WHERE x.name = 'a')", values)
	require.True(t, ok)
	require.Equal(t, false, value)
	value, ok = exec.evaluateRowExpression("any(x IN relationships WHERE x.name = 'a')", values)
	require.True(t, ok)
	require.Equal(t, true, value)
	value, ok = exec.evaluateRowExpression("single(x IN relationships WHERE x.name = 'a')", values)
	require.True(t, ok)
	require.Equal(t, true, value)
	value, ok = exec.evaluateRowExpression("all(x IN relationships WHERE x.name IS NOT NULL)", values)
	require.True(t, ok)
	require.Equal(t, true, value)
	value, ok = exec.evaluateRowExpression("any(x IN withNull WHERE x = 2)", values)
	require.True(t, ok)
	require.Nil(t, value)
}

func TestRowQuantifiersComposeWithNestedQuantifiersAndArithmetic(t *testing.T) {
	exec := &StorageExecutor{}
	values := map[string]interface{}{
		"list": []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8), int64(9)},
	}

	result, ok := exec.evaluateRowExpression(
		"none(x IN list WHERE single(y IN list WHERE abs(x - y) < 3))",
		values,
	)
	require.True(t, ok)
	require.Equal(t, true, result)
}

func TestRowExpressionEvaluatesNestedArithmeticForOrdering(t *testing.T) {
	exec := &StorageExecutor{}
	values := map[string]interface{}{
		"a": &storage.Node{Properties: map[string]interface{}{
			"num":  int64(9),
			"num2": int64(5),
		}},
	}

	for expression, expected := range map[string]interface{}{
		"a.num":                       int64(9),
		"a.num * 2":                   int64(18),
		"a.num2 + (a.num * 2)":        int64(23),
		"(a.num2 + (a.num * 2))":      int64(23),
		"(a.num2 + (a.num * 2)) * -1": int64(-23),
	} {
		result, ok := exec.evaluateRowExpression(expression, values)
		require.True(t, ok, expression)
		require.Equal(t, expected, result, expression)
	}

	result, ok := exec.evaluateRowExpression("(a.num2 + (a.num * 2)) * -1", values)
	require.True(t, ok)
	require.Equal(t, int64(-23), result)
}
