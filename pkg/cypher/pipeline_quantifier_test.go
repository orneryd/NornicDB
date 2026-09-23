package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineQuantifierAcrossRepeatedHorizons(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	expressionRow := pipelineRow{
		"inputList": []interface{}{int64(1), int64(2), int64(3)},
		"list":      []interface{}{int64(1), int64(2)},
		"x":         int64(3),
	}
	_, evaluated := exec.evaluateRowExpression("rand()", expressionRow)
	require.True(t, evaluated)
	_, evaluated = exec.evaluateRowExpression("rand() < 0.5", expressionRow)
	require.True(t, evaluated)
	_, evaluated = exec.evaluateRowExpression("CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END", expressionRow)
	require.True(t, evaluated)
	value, evaluated := exec.evaluateRowExpression("CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x", expressionRow)
	require.True(t, evaluated)
	require.Len(t, value, 3)
	query := `
		WITH [1, 2, 3] AS inputList
		UNWIND inputList AS x
		WITH inputList, x, [y IN inputList WHERE rand() > 0.5 | y] AS list
		WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
		UNWIND inputList AS x
		WITH inputList, x, [y IN inputList WHERE rand() > 0.5 | y] AS list
		WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
		UNWIND inputList AS x
		WITH inputList, x, [y IN inputList WHERE rand() > 0.5 | y] AS list
		WITH inputList, CASE WHEN rand() < 0.5 THEN reverse(list) ELSE list END + x AS list
		WITH list WHERE size(list) > 0
		WITH none(x IN list WHERE false) AS result, count(*) AS cnt
		RETURN result
	`
	clauses, parsed := canExecuteAsPipeline(query)
	require.True(t, parsed)
	rows := []pipelineRow{{}}
	for index, clause := range clauses {
		switch clause.kind {
		case pipelineClauseWith:
			var handled bool
			rows, handled = exec.pipelineApplyWith(ctx, rows, clause.text)
			require.True(t, handled, "WITH clause %d: %s", index, clause.text)
		case pipelineClauseUnwind:
			var handled bool
			rows, handled = exec.pipelineApplyUnwind(ctx, rows, clause.text)
			require.True(t, handled, "UNWIND clause %d: %s", index, clause.text)
		}
	}

	result, handled, err := exec.executePipeline(ctx, query)
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, []string{"result"}, result.Columns)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}
