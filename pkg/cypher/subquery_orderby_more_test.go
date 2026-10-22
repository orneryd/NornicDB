package cypher

import (
	"context"
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
			{storage.RemoteEntityValue{"properties": map[string]interface{}{"createdAt": int64(1)}}},
			{map[string]interface{}{"createdAt": int64(3)}},
		},
	}

	fixtureAdmission1884Context, fixtureAdmission1884 := statementTestContext(context.Background(), "ORDER BY t.createdAt ASC")
	_ = fixtureAdmission1884Context
	ordered := exec.applyOrderByToResult(fixtureAdmission1884, result, "ORDER BY t.createdAt ASC")
	require.Len(t, ordered.Rows, 3)
	require.EqualValues(t, int64(1), extractPropertyFromValue(ordered.Rows[0][0], "createdAt"))
	require.EqualValues(t, int64(2), extractPropertyFromValue(ordered.Rows[1][0], "createdAt"))
	require.EqualValues(t, int64(3), extractPropertyFromValue(ordered.Rows[2][0], "createdAt"))

	fixtureAdmission1885Context, fixtureAdmission1885 := statementTestContext(context.Background(), "ORDER BY t.createdAt DESC LIMIT 2")
	_ = fixtureAdmission1885Context
	ordered = exec.applyOrderByToResult(fixtureAdmission1885, result, "ORDER BY t.createdAt DESC LIMIT 2")
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

	fixtureAdmission1886Context, fixtureAdmission1886 := statementTestContext(context.Background(), "ORDER BY missing")
	_ = fixtureAdmission1886Context
	unchanged := exec.applyOrderByToResult(fixtureAdmission1886, base, "ORDER BY missing")
	require.Equal(t, [][]interface{}{{"b"}, {"a"}}, unchanged.Rows)

	fixtureAdmission1887Context, fixtureAdmission1887 := statementTestContext(context.Background(), "ORDER BY")
	_ = fixtureAdmission1887Context
	unchanged = exec.applyOrderByToResult(fixtureAdmission1887, base, "ORDER BY")
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
	fixtureAdmission1789Context, fixtureAdmission1789 := statementTestContext(context.Background(), "ORDER BY age ASC LIMIT 0")
	_ = fixtureAdmission1789Context
	out, err := exec.applyResultModifiers(fixtureAdmission1789, fixtureAdmission1789Context, in, "ORDER BY age ASC LIMIT 0")
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
	fixtureAdmission1790Context, fixtureAdmission1790 := statementTestContext(context.Background(), "ORDER BY missing DESC LIMIT 2")
	_ = fixtureAdmission1790Context
	out2, err := exec.applyResultModifiers(fixtureAdmission1790, fixtureAdmission1790Context, in2, "ORDER BY missing DESC LIMIT 2")
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

	fixtureAdmission1791Context, fixtureAdmission1791 := statementTestContext(context.Background(), "ORDER BY group ASC, rank DESC, name ASC\nSKIP 1\nLIMIT 1")
	_ = fixtureAdmission1791Context
	result, err := exec.applyResultModifiers(fixtureAdmission1791, fixtureAdmission1791Context, input, "ORDER BY group ASC, rank DESC, name ASC\nSKIP 1\nLIMIT 1")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"a", int64(1), "third"}}, result.Rows)
}

func TestApplyResultModifiersSelectsAscendingNumericTopK(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	input := &ExecuteResult{
		Columns: []string{"score"},
		Rows:    [][]interface{}{{0.56}, {0.52}, {0.44}, {0.41}, {0.38}, {0.32}},
	}

	fixtureAdmission1792Context, fixtureAdmission1792 := statementTestContext(context.Background(), "ORDER BY score ASC\nLIMIT 5")
	_ = fixtureAdmission1792Context
	result, err := exec.applyResultModifiers(fixtureAdmission1792, fixtureAdmission1792Context, input, "ORDER BY score ASC\nLIMIT 5")
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
	fixtureAdmission1793Context, fixtureAdmission1793 := statementTestContext(context.Background(), "ORDER BY NOT (first AND second) DESCENDING LIMIT 3")
	_ = fixtureAdmission1793Context
	require.True(t, exec.orderPipelineRows(fixtureAdmission1793, fixtureAdmission1793Context, rows, terms))
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

	fixtureAdmission1794Context, fixtureAdmission1794 := statementTestContext(context.Background(), "ORDER BY a.name + 'C' DESC")
	_ = fixtureAdmission1794Context
	require.True(t, exec.orderPipelineRowsWithScopes(fixtureAdmission1794, fixtureAdmission1794Context, rows, scopes, parseOrderByTerms("ORDER BY a.name + 'C' DESC")))
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

	fixtureAdmission1795Context, fixtureAdmission1795 := statementTestContext(context.Background(), "ORDER BY a.name DESC")
	_ = fixtureAdmission1795Context
	require.True(t, exec.orderPipelineRowsWithScopes(fixtureAdmission1795, fixtureAdmission1795Context, rows, scopes, parseOrderByTerms("ORDER BY a.name DESC")))
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

	value, ok := rowValue(t, exec, "none(x IN relationships WHERE x.name = 'a')", values)
	require.True(t, ok)
	require.Equal(t, false, value)
	value, ok = rowValue(t, exec, "any(x IN relationships WHERE x.name = 'a')", values)
	require.True(t, ok)
	require.Equal(t, true, value)
	value, ok = rowValue(t, exec, "single(x IN relationships WHERE x.name = 'a')", values)
	require.True(t, ok)
	require.Equal(t, true, value)
	value, ok = rowValue(t, exec, "all(x IN relationships WHERE x.name IS NOT NULL)", values)
	require.True(t, ok)
	require.Equal(t, true, value)
	value, ok = rowValue(t, exec, "any(x IN withNull WHERE x = 2)", values)
	require.True(t, ok)
	require.Nil(t, value)
}

func TestRowQuantifiersComposeWithNestedQuantifiersAndArithmetic(t *testing.T) {
	exec := &StorageExecutor{}
	values := map[string]interface{}{
		"list": []interface{}{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8), int64(9)},
	}

	result, ok := rowValue(t, exec,
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
		result, ok := rowValue(t, exec, expression, values)
		require.True(t, ok, expression)
		require.Equal(t, expected, result, expression)
	}

	result, ok := rowValue(t, exec, "(a.num2 + (a.num * 2)) * -1", values)
	require.True(t, ok)
	require.Equal(t, int64(-23), result)
}
