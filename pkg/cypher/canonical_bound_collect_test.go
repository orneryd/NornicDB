package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

func TestCanonicalBoundScalarAggregateContext(t *testing.T) {
	samples := []struct {
		query   string
		columns []string
		rows    [][]interface{}
	}{
		{"CYPHER 25 UNWIND [1, 2] AS g RETURN g, COLLECT { UNWIND [] AS x RETURN count(*) + g AS c } AS v ORDER BY g", []string{"g", "v"}, [][]interface{}{{int64(1), []interface{}{int64(1)}}, {int64(2), []interface{}{int64(2)}}}},
		{"CYPHER 25 WITH 1 AS g RETURN COLLECT { UNWIND [] AS x RETURN count(*) + g AS c UNION ALL RETURN g AS c } AS v", []string{"v"}, [][]interface{}{{[]interface{}{int64(1), int64(1)}}}},
	}
	for _, sample := range samples {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(mode+sample.query, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				result, err := exec.Execute(ctx, mode+sample.query, nil)
				require.NoError(t, err)
				require.Equal(t, sample.columns, result.Columns)
				if mode == "EXPLAIN " {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, sample.rows, result.Rows)
				}
			})
		}
	}
	// The same source and declared inputs have different roles. A cached scalar
	// continuation may never admit an independent ordinary query's grouping.
	exec, ctx := newUnitExecutor(t)
	source := "UNWIND [] AS x RETURN count(*) + g AS c"
	scalar := withScalarCompilation(ctx, source, "COLLECT")
	incoming := resultmetadata.Known([]string{"g"})
	require.NoError(t, exec.projectedStatementScope(scalar, source, incoming, resultmetadata.Known(nil), true).err)
	require.Error(t, exec.projectedStatementScope(ctx, source, incoming, resultmetadata.Known(nil), true).err)
	other := "UNWIND [] AS x RETURN count(*) + g AS other"
	require.Error(t, exec.projectedStatementScope(scalar, other, incoming, resultmetadata.Known(nil), true).err)
}

// NornicDB deliberately accepts a written COLLECT RETURN * whose expanded
// result has one column. Admission and headers are independent of row count.
func TestCanonicalBoundCollectExtension(t *testing.T) {
	for _, sample := range []struct {
		query  string
		values []interface{}
	}{
		{"RETURN COLLECT { WITH 1 AS x RETURN * } AS xs", []interface{}{int64(1)}},
		{"RETURN COLLECT { UNWIND [] AS x RETURN * } AS xs", []interface{}{}},
		{"RETURN COLLECT { UNWIND [1] AS x RETURN * } AS xs", []interface{}{int64(1)}},
		{"RETURN COLLECT { WITH 1 AS x WITH * RETURN x } AS xs", []interface{}{int64(1)}},
	} {
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(mode+sample.query, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				result, err := exec.Execute(ctx, mode+sample.query, nil)
				require.NoError(t, err)
				require.Equal(t, []string{"xs"}, result.Columns)
				if mode == "EXPLAIN " {
					require.Empty(t, result.Rows)
				} else {
					require.Equal(t, [][]interface{}{{sample.values}}, result.Rows)
				}
			})
		}
	}
	for _, prefix := range []string{"UNWIND [] AS __rule_input ", "UNWIND [0] AS __rule_input "} {
		query := prefix + "RETURN COLLECT { WITH 1 AS x RETURN * } AS xs"
		for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
			t.Run(mode+query, func(t *testing.T) {
				exec, ctx := newUnitExecutor(t)
				_, err := exec.Execute(ctx, mode+query, nil)
				code, detail, message := boundRuleError(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
				require.Equal(t, "InvalidCollectSubquery", detail)
				require.Equal(t, "a COLLECT subquery must return exactly one column (got 2)", message)
				_, err = exec.StatementResultColumnsContext(ctx, query, RawSource)
				require.Error(t, err)
			})
		}
	}
}

func TestCanonicalBoundWildcardProvenance(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	for _, query := range []string{
		"RETURN COLLECT { WITH 1 AS x RETURN * } AS xs",
		"RETURN COLLECT { WITH 1 AS x WITH * RETURN x } AS xs",
	} {
		facts := exec.statementProjectionProgram(ctx, query, resultmetadata.KnownScope(nil), false, nil)
		require.NoError(t, facts.err)
		require.NotNil(t, facts.bound)
		for _, body := range facts.bound.nested {
			require.NotNil(t, body.program)
			for _, branch := range body.branches {
				for _, source := range branch.shape.clauses {
					clause := facts.bound.clauses[source.syntax.ID()]
					if clause.plan == nil {
						continue
					}
					require.False(t, clause.plan.star, "executable plan is expanded")
					require.Equal(t, returnsStar(ctx, source.text), clause.step.Projection.WrittenWildcard)
				}
			}
		}
	}
}
