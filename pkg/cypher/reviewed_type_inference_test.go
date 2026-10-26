package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReviewedTypeInference(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, tc := range []struct {
			name, query, code string
			rows              [][]interface{}
		}{
			{name: "absent_else_static", query: "RETURN [x IN [1] | toUpper(CASE WHEN true THEN x END)] AS v", code: "SyntaxError"},
			{name: "null_list", query: "RETURN [x IN null | labels(x)] AS v", rows: [][]interface{}{{nil}}},
			{name: "empty_list", query: "RETURN [x IN [] | labels(x)] AS v", rows: [][]interface{}{{[]interface{}{}}}},
			{name: "predicate_valid", query: "RETURN [x IN range(0,2) WHERE x>0 | x] AS v", rows: [][]interface{}{{[]interface{}{int64(1), int64(2)}}}},
			{name: "reduce_valid", query: "RETURN reduce(a=0,x IN range(0,2)|a+x) AS v", rows: [][]interface{}{{int64(3)}}},
			{name: "quantifier_valid", query: "RETURN any(x IN [1] WHERE x=1) AS v", rows: [][]interface{}{{true}}},
			{name: "product_difference_static", query: "WITH {k:1} AS n RETURN [10,20][(n.k*1.5)-1] AS v", code: "SyntaxError"},
			{name: "product_product_static", query: "WITH {k:1} AS n RETURN [10,20][(n.k*1.5)*1] AS v", code: "SyntaxError"},
			{name: "product_division_static", query: "WITH {k:1} AS n RETURN [10,20][(n.k*1.5)/1] AS v", code: "SyntaxError"},
			{name: "product_conversion_valid", query: "WITH {k:1} AS n RETURN [10,20][toInteger(n.k*1.5)] AS v", rows: [][]interface{}{{int64(20)}}},
			{name: "empty_product_static", query: "UNWIND [] AS n RETURN [10,20][(n.k*1.5)+1] AS v", code: "SyntaxError"},
			{name: "projected_float_index", query: "WITH {k:1} AS n WITH n.k*1.5 AS w RETURN [10,20][w+1] AS v", code: "SyntaxError"},
			{name: "projected_float_slice", query: "WITH {k:1} AS n WITH n.k*1.5 AS w RETURN [10,20][0..w] AS v", code: "SyntaxError"},
			{name: "projected_float_empty", query: "WITH {k:1} AS n WITH n.k*1.5 AS w UNWIND [] AS outer RETURN [10,20][w+1] AS v", code: "SyntaxError"},
			{name: "projected_float_shadow", query: "WITH {k:1} AS n WITH n.k*1.5 AS w RETURN [w IN [1] | [10,20][w]] AS v", rows: [][]interface{}{{[]interface{}{int64(20)}}}},
			{name: "mixed_case_valid", query: "RETURN toUpper(CASE WHEN false THEN 1 ELSE 'a' END) AS v", rows: [][]interface{}{{"A"}}},
			{name: "mixed_case_runtime", query: "RETURN [x IN [1] | toUpper(CASE WHEN true THEN x ELSE 'a' END)] AS v", code: "TypeError"},
			{name: "homogeneous_case_static", query: "RETURN toUpper(CASE WHEN true THEN 1 ELSE 2 END) AS v", code: "SyntaxError"},
			{name: "nullable_case_runtime", query: "RETURN toUpper(CASE WHEN true THEN 1 ELSE null END) AS v", code: "TypeError"},
			{name: "number_product_integer_row", query: "UNWIND [1,2.5] AS v WITH v WHERE v=1 RETURN [10,20,30][v*1] AS x", rows: [][]interface{}{{int64(20)}}},
			{name: "number_sum_integer_row", query: "UNWIND [1,2.5] AS v WITH v WHERE v=1 RETURN [10,20,30][v+0] AS x", rows: [][]interface{}{{int64(20)}}},
			{name: "number_product_float_row", query: "UNWIND [1,2.5] AS v RETURN [10,20,30][v*1] AS x", code: "TypeError"},
			{name: "number_projection_runtime", query: "UNWIND [1,2.5] AS v WITH v*10 AS w RETURN date('2020-01-01')+w AS x", code: "TypeError"},
			{name: "predicate_integer_static", query: "RETURN [x IN [1] WHERE 1 | x] AS v", code: "SyntaxError"},
			{name: "predicate_empty_outer_static", query: "UNWIND [] AS outer RETURN [x IN [1] WHERE 1 | x] AS v", code: "SyntaxError"},
			{name: "range_iterator_static", query: "RETURN [x IN range(0,2) | toUpper(x)] AS v", code: "SyntaxError"},
			{name: "range_empty_outer_static", query: "UNWIND [] AS outer RETURN [x IN range(0,2) | toUpper(x)] AS v", code: "SyntaxError"},
			{name: "quantifier_iterator_static", query: "RETURN any(x IN [1] WHERE toUpper(x) = 'A') AS v", code: "SyntaxError"},
			{name: "reduce_accumulator_static", query: "RETURN reduce(a = 0, x IN [1] | toUpper(a)) AS v", code: "SyntaxError"},
			{name: "product_sum_index_static", query: "WITH {k:1} AS n RETURN [10,20][(n.k*1.5)+1] AS v", code: "SyntaxError"},
			{name: "product_unary_index_static", query: "WITH {k:1} AS n RETURN [10,20][-(n.k*1.5)] AS v", code: "SyntaxError"},
			{name: "null_product_sum_static", query: "WITH null AS n RETURN [10,20][(n*1.5)+1] AS v", code: "SyntaxError"},
			{name: "product_slice_static", query: "WITH {k:1} AS n RETURN [10,20][0..n.k*1.5] AS v", code: "SyntaxError"},
		} {
			t.Run(fmt.Sprintf("%s/explicit=%t", tc.name, explicit), func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "review_types"))
				if explicit {
					_, err := exec.Execute(context.Background(), "BEGIN", nil)
					require.NoError(t, err)
					t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) })
				}
				result, err := exec.Execute(context.Background(), tc.query, nil)
				if tc.code != "" {
					require.Error(t, err)
					var classified interface{ BoltErrorCode() string }
					require.True(t, errors.As(err, &classified), "%v", err)
					require.Equal(t, "Neo.ClientError.Statement."+tc.code, classified.BoltErrorCode(), "%v", err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.rows, result.Rows)
			})
		}
	}

}

func TestReviewedNullablePathSize(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nullable_path"))
	_, err := exec.Execute(context.Background(), "CREATE (:PhaseA)-[:R]->(:PhaseB)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(context.Background(), "MATCH p=(:PhaseA)-[:R]->() RETURN size(CASE WHEN true THEN p ELSE null END) AS v", nil)
	require.Error(t, err)
	var classified interface{ BoltErrorCode() string }
	require.True(t, errors.As(err, &classified), "%v", err)
	require.Equal(t, "Neo.ClientError.Statement.TypeError", classified.BoltErrorCode())
}

// These malformed nested operands must be rejected by the same expression checker,
// even when callers reach it without the statement's earlier validation passes.
func TestReviewedNestedExpressionErrors(t *testing.T) {
	for _, expression := range []string{
		"[x IN (1+true)|x]", "[x IN [1]|x+true]",
		"reduce(a=0,x IN (1+true)|a)", "reduce(a=0,x IN 1|a)",
		"reduce(a=1+true,x IN [1]|a)", "reduce(a=0,x IN [1]|a+true)",
		"allReduce(a=0,x IN [1]|a+x,1)",
	} {
		t.Run(expression, func(t *testing.T) {
			_, err := (staticOperatorChecker{scope: staticTypeScope{cypher25: true}}).check(originalBraceView(expression))
			require.Error(t, err)
			var classified interface{ BoltErrorCode() string }
			require.True(t, errors.As(err, &classified))
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.BoltErrorCode())
		})
	}
}
