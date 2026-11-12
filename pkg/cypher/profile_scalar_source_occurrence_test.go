package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// profileScalarUnionOperators returns the UnionAll operators of plan, in plan
// order: the scalar subqueries a statement executed as programs of their own.
func profileScalarUnionOperators(plan *ExecutionPlan) []*PlanOperator {
	var unions []*PlanOperator
	var visit func(*PlanOperator)
	visit = func(op *PlanOperator) {
		if op.OperatorType == "UnionAll" {
			unions = append(unions, op)
		}
		for _, child := range op.Children {
			visit(child)
		}
	}
	visit(plan.Root)
	return unions
}

// TestProfileScalarOccurrencesFollowWrittenExpressions is the public regression
// matrix for scalar source occurrences: two equal subquery bodies written at two
// source positions are two operators, one written body stays one however many
// rows it runs for, and the rows themselves are unchanged. Every case runs cold
// and warm, with the compiled row programs both disabled and enabled, because
// the text reader and the compiled reader must report the same occurrences.
func TestProfileScalarOccurrencesFollowWrittenExpressions(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, query string
			want        [][]interface{}
			unions      int
		}{
			{
				name:   "separate projections",
				query:  "RETURN COUNT { RETURN 1 UNION ALL RETURN 1 } AS a, COUNT { RETURN 1 UNION ALL RETURN 1 } AS b",
				want:   [][]interface{}{{int64(2), int64(2)}},
				unions: 2,
			},
			{
				name:   "list items",
				query:  "RETURN [COUNT { RETURN 1 UNION ALL RETURN 1 }, COUNT { RETURN 1 UNION ALL RETURN 1 }] AS counts",
				want:   [][]interface{}{{[]interface{}{int64(2), int64(2)}}},
				unions: 2,
			},
			{
				name:   "list items in arithmetic",
				query:  "RETURN [COUNT { RETURN 1 UNION ALL RETURN 1 } + 1, COUNT { RETURN 1 UNION ALL RETURN 1 } + 1] AS counts",
				want:   [][]interface{}{{[]interface{}{int64(3), int64(3)}}},
				unions: 2,
			},
			{
				name:   "arithmetic operands",
				query:  "RETURN 1 + COUNT { RETURN 1 UNION ALL RETURN 1 } AS a, 1 + COUNT { RETURN 1 UNION ALL RETURN 1 } AS b",
				want:   [][]interface{}{{int64(3), int64(3)}},
				unions: 2,
			},
			{
				name:   "grouped operand",
				query:  "RETURN 1 + (COUNT { RETURN 1 UNION ALL RETURN 1 }) AS a, 1 + (COUNT { RETURN 1 UNION ALL RETURN 1 }) AS b",
				want:   [][]interface{}{{int64(3), int64(3)}},
				unions: 2,
			},
			{
				name:   "function argument",
				query:  "RETURN size([COUNT { RETURN 1 UNION ALL RETURN 1 }]) AS a, size([COUNT { RETURN 1 UNION ALL RETURN 1 }]) AS b",
				want:   [][]interface{}{{int64(1), int64(1)}},
				unions: 2,
			},
			{
				name:   "one occurrence over many rows",
				query:  "UNWIND [1, 2, 3] AS x RETURN COUNT { RETURN x UNION ALL RETURN x } AS c",
				want:   [][]interface{}{{int64(2)}, {int64(2)}, {int64(2)}},
				unions: 1,
			},
		} {
			for _, programs := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/programs=%t", tc.name, programs), func(t *testing.T) {
					previous := rowValueProgramsDisabled.Swap(!programs)
					t.Cleanup(func() { rowValueProgramsDisabled.Store(previous) })
					exec, ctx := newProfileRuntimeTestExecutor(t, "CREATE (:Seed)")
					for _, run := range []string{"cold", "warm"} {
						result, err := exec.Execute(ctx, "PROFILE "+tc.query, nil)
						require.NoError(t, err, run)
						require.Equal(t, tc.want, result.Rows, "%s: %s", run, tc.query)
						plan := executionPlanFromResult(t, result)
						unions := profileScalarUnionOperators(plan)
						require.Len(t, unions, tc.unions, "%s: %s", run, formatPlan(plan))
						for _, union := range unions {
							executions := int64(1)
							if tc.name == "one occurrence over many rows" {
								executions = 3
							}
							require.Equal(t, executions, union.Arguments["executions"], "%s: %s", run, formatPlan(plan))
							require.True(t, union.RowsObserved, run)
							require.Equal(t, 2*executions, union.ActualRows, "%s: %s", run, formatPlan(plan))
						}
					}
				})
			}
		}
	})
}

// TestExpressionOriginsComposeWithoutResetting pins the rule every splitter
// follows: a part starts where its parent starts plus where it starts in it,
// and nothing binds an origin in a row that carries none.
func TestExpressionOriginsComposeWithoutResetting(t *testing.T) {
	values := pipelineRow{}
	require.Equal(t, 7, rowExpressionOccurrence(values, 7))
	if _, bound := advanceRowExpressionOrigin(values, 5, rowExpressionOrigin(values)); bound {
		t.Fatal("a row without a bound origin must not gain one")
	}
	saved, bound := bindRowExpressionOrigin(values, 100)
	require.True(t, bound)
	nested, nestedBound := advanceRowExpressionOrigin(values, 4, rowExpressionOrigin(values))
	require.True(t, nestedBound)
	require.Equal(t, 111, rowExpressionOccurrence(values, 7))
	restoreRowExpressionOrigin(values, nested)
	require.Equal(t, 107, rowExpressionOccurrence(values, 7))
	restoreRowExpressionOrigin(values, saved)
	require.False(t, rowExpressionOrigin(values).had)
}

// TestTopLevelCommaPartsKeepSourceOffsets pins the one comma kernel: the same
// scan reports each part's text and where it starts, and the text appender
// still splits a caller's fixed-size buffer without allocating.
func TestTopLevelCommaPartsKeepSourceOffsets(t *testing.T) {
	input := "COUNT { RETURN 1 },  a + 1 ,[3, 4]"
	var fragmentBuffer [8]expressionFragment
	items := appendExpressionParts(fragmentBuffer[:0], input, false)
	require.Len(t, items, 3)
	for _, item := range items {
		require.Equal(t, item.text, strings.TrimSpace(input[item.start:item.start+len(item.text)]))
	}
	require.Equal(t, "a + 1", items[1].text)
	require.Equal(t, strings.Index(input, "a + 1"), items[1].start)
	require.Equal(t, strings.Index(input, "[3, 4]"), items[2].start)

	var textBuffer [8]string
	texts := appendTopLevelCommaParts(textBuffer[:0], input, false)
	require.Equal(t, []string{"COUNT { RETURN 1 }", "a + 1", "[3, 4]"}, []string(texts))

	allocs := testing.AllocsPerRun(100, func() {
		var buffer [8]string
		_ = appendTopLevelCommaParts(buffer[:0], input, false)
	})
	require.Zero(t, allocs, "splitting into a caller's buffer must not allocate")
}
