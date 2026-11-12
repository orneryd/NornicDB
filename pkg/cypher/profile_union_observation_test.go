package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileUnionKeepsBranchIdentityAndOutputCardinality(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, query, operator string
			want                  [][]interface{}
			branches              []int64
		}{
			{"unequal branches", "UNWIND [1,2,3] AS x RETURN x UNION ALL WITH 9 AS x RETURN x", "UnionAll", [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}, {int64(9)}}, []int64{3, 1}},
			{"identical branches", "WITH 1 AS x RETURN x UNION ALL WITH 1 AS x RETURN x", "UnionAll", [][]interface{}{{int64(1)}, {int64(1)}}, []int64{1, 1}},
			{"distinct", "WITH 1 AS x RETURN x UNION WITH 1 AS x RETURN x", "Union", [][]interface{}{{int64(1)}}, []int64{1, 1}},
			{"correlated CALL", "MATCH (seed:Seed) CALL (seed) { WITH seed RETURN seed.v AS x UNION ALL WITH seed RETURN seed.v+1 AS x } RETURN x", "UnionAll", [][]interface{}{{int64(10)}, {int64(11)}, {int64(20)}, {int64(21)}}, []int64{2, 2}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec, _ := newProfileRuntimeTestExecutor(t, "CREATE (:Seed {v:10}), (:Seed {v:20})")
				result, err := exec.Execute(t.Context(), "PROFILE "+tc.query, nil)
				require.NoError(t, err)
				require.ElementsMatch(t, tc.want, result.Rows)
				plan := executionPlanFromResult(t, result)
				require.Equal(t, int64(len(tc.want)), plan.TotalRows)
				require.Equal(t, int64(len(tc.want)), plan.Root.ActualRows)
				union := planOperatorOfType(plan.Root, tc.operator)
				require.NotNil(t, union, formatPlan(plan))
				require.True(t, union.RowsObserved)
				require.Equal(t, int64(len(tc.want)), union.ActualRows)
				require.Len(t, union.Children, 2, "parallel branches must be siblings")
				require.NotSame(t, union.Children[0], union.Children[1])
				for i, branch := range union.Children {
					require.Equal(t, "UnionBranch", branch.OperatorType)
					require.Equal(t, i, branch.Arguments["branch"])
					require.True(t, branch.RowsObserved)
					require.Equal(t, tc.branches[i], branch.ActualRows)
				}
			})
		}
	})
}

func TestProfileScalarUnionsKeepDistinctSourceOccurrences(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, query string
			want        [][]interface{}
		}{
			{"separate projections", "RETURN COUNT { RETURN 1 UNION ALL RETURN 1 } AS a, COUNT { RETURN 1 UNION ALL RETURN 1 } AS b", [][]interface{}{{int64(2), int64(2)}}},
			{"one list expression", "RETURN [COUNT { RETURN 1 UNION ALL RETURN 1 }, COUNT { RETURN 1 UNION ALL RETURN 1 }] AS counts", [][]interface{}{{[]interface{}{int64(2), int64(2)}}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				exec, _ := newProfileRuntimeTestExecutor(t, "CREATE (:Seed)")
				for range 2 {
					result, err := exec.Execute(t.Context(), "PROFILE "+tc.query, nil)
					require.NoError(t, err)
					require.Equal(t, tc.want, result.Rows)
					plan := executionPlanFromResult(t, result)
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
					require.Len(t, unions, 2, "equal text at different source positions describes different operations: %s", formatPlan(plan))
					for _, union := range unions {
						require.EqualValues(t, 2, union.ActualRows)
						require.EqualValues(t, 1, union.Arguments["executions"])
					}
				}
			})
		}
	})
}
