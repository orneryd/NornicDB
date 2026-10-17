package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestReduceAppendPlanMatchesWholeStep: a reduce whose step is accumulator
// + operands, folded by appending the operands to a list the fold owns
// (reduceAppendPlan), gives what evaluating the whole step each time gives,
// on the row evaluator, its compiled programs and the context evaluator:
// lists, values, null, strings (appended, not concatenated), maps,
// durations, an initial list shared with the row (left unchanged), typed
// lists, the accumulator appended to itself or nested in itself, CASE and
// nested reduce operands, allReduce, and steps that have no plan.
func TestReduceAppendPlanMatchesWholeStep(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:N {id: 1, tags: ['a', 'b']}), (:N {id: 2, tags: ['c']})", nil)
	require.NoError(t, err)
	params := map[string]interface{}{"base": []interface{}{int64(1), int64(2)}, "words": []string{"x", "y"}}
	expressions := []string{
		"reduce(acc = [], x IN range(1, 5) | acc + [x])",
		"reduce(acc = [], x IN range(1, 5) | acc + x)",
		"reduce(acc = [], x IN range(1, 5) | acc + [x] + [x * 2] + x)",
		"reduce(acc = [], x IN [1, null, 3] | acc + x)",
		"reduce(acc = [], x IN [1, 2] | acc + [x] + null)",
		"reduce(acc = [], x IN ['a', 'b'] | acc + x + 'z')",
		"reduce(acc = [], x IN range(1, 3) | acc + {k: x})",
		"reduce(acc = [], x IN range(1, 2) | acc + duration({days: x}))",
		"reduce(acc = $base, x IN [3, 4] | acc + x)",
		"reduce(acc = $words, x IN ['z'] | acc + x)",
		"reduce(acc = [0], x IN range(1, 3) | acc + acc)",
		"reduce(acc = [], x IN range(1, 4) | acc + [acc] + [size(acc)])",
		"reduce(acc = [], x IN range(1, 4) | acc + CASE WHEN x % 2 = 0 THEN [x] ELSE [] END)",
		"reduce(acc = [], x IN range(1, 3) | acc + reduce(inner = [], y IN range(1, x) | inner + y))",
		"reduce(acc = [], x IN range(1, 3) | (acc + [x]) + [x])",
		"reduce(acc = [], x IN range(1, 3) | [x] + acc)",
		"reduce(acc = [], x IN range(1, 3) | acc + [x] = [1])",
		"reduce(acc = 0, x IN range(1, 3) | acc + x)",
		"reduce(acc = '', x IN ['a', 'b'] | acc + x)",
		"reduce(acc = [], x IN [] | acc + x)",
		"reduce(acc = null, x IN [1] | acc + x)",
		"reduce(acc = [], x IN [[1], [], [2, 3]] | acc + x)",
		"allReduce(acc = [], x IN range(1, 6) | acc + x, size(acc) < 4)",
		"allReduce(acc = [], x IN range(1, 3) | acc + [x], size(acc) <= 3)",
		"reduce(acc = [], x IN range(1, 3000) | acc + [x] + x)",
	}
	queries := []string{}
	for _, expression := range expressions {
		queries = append(queries,
			"RETURN "+expression+" AS v",
			"WITH 1 AS one RETURN "+expression+" AS v, $base AS base",
			"MATCH (n:N) WITH n ORDER BY n.id RETURN n.id AS id, "+expression+" AS v")
	}
	// The context evaluator: a reduce over a node's property in a node
	// expression.
	queries = append(queries,
		"MATCH (n:N) WITH n ORDER BY n.id RETURN reduce(acc = [], t IN n.tags | acc + [t] + n.id) AS v",
		"MATCH (n:N) WHERE size(reduce(acc = [], t IN n.tags | acc + t)) > 1 RETURN n.id AS v",
		"MATCH (n:N) SET n.folded = reduce(acc = [], t IN n.tags | acc + t + t) WITH n ORDER BY n.id RETURN n.folded AS v")
	run := func(query string) []interface{} {
		var out []interface{}
		for round := 0; round < 3; round++ { // the third run reads compiled programs
			result, err := exec.Execute(ctx, query, params)
			require.NoError(t, err, query)
			out = append(out, fmt.Sprint(result.Rows))
		}
		return out
	}
	for _, programs := range []bool{true, false} {
		rowValueProgramsDisabled.Store(!programs)
		for _, query := range queries {
			reduceAppendDisabled.Store(true)
			want := run(query)
			reduceAppendDisabled.Store(false)
			require.Equal(t, want, run(query), "programs %v: %s", programs, query)
		}
	}
	rowValueProgramsDisabled.Store(false)

	// A few values pinned outright.
	for query, want := range map[string]interface{}{
		"RETURN reduce(acc = [], x IN ['a', 'b'] | acc + x + 'z') AS v":                []interface{}{"a", "z", "b", "z"},
		"RETURN reduce(acc = [], x IN [1, null, 3] | acc + x) AS v":                    nil,
		"RETURN reduce(acc = [0], x IN range(1, 2) | acc + acc) AS v":                  []interface{}{int64(0), int64(0), int64(0), int64(0)},
		"WITH $base AS base RETURN reduce(acc = base, x IN [3] | acc + x) + base AS v": []interface{}{int64(1), int64(2), int64(3), int64(1), int64(2)},
	} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows[0][0], query)
	}
	require.Equal(t, []interface{}{int64(1), int64(2)}, params["base"])
}

// TestReduceAppendPlanShapes: which steps have a plan, and the operands.
func TestReduceAppendPlanShapes(t *testing.T) {
	for step, want := range map[string][]string{
		"acc + [x]":                  {"[x]"},
		"acc + [x.element] + x.ends": {"[x.element]", "x.ends"},
		"(acc + [x]) + y":            {"[x]", "y"},
		"acc + CASE WHEN a THEN [1] + [2] ELSE [] END": nil,
		"acc + [CASE WHEN a THEN 1 ELSE 2 END]":        nil,
		"[x] + acc":                                    nil,
		"acc - [x]":                                    nil,
		"acc + [x] - [y]":                              nil,
		"acc + [x] = []":                               nil,
		"other + [x]":                                  nil,
		"acc.list + [x]":                               nil,
		"acc":                                          nil,
	} {
		form := reduceForm{accumulator: "acc", variable: "x", step: step}
		plan := form.appendPlan()
		if want == nil {
			require.Nil(t, plan, step)
			continue
		}
		require.NotNil(t, plan, step)
		require.Equal(t, want, plan.terms, step)
	}
	// A step's sum bound as a row name is answered by that binding.
	form := reduceForm{accumulator: "acc", variable: "x", step: "acc + [x]"}
	require.False(t, form.appendPlan().usable(map[string]interface{}{"acc + [x]": 1}))
	require.True(t, form.appendPlan().usable(map[string]interface{}{"acc": 1}))
}
