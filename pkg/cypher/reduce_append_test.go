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
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
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
		"acc + [x] + [acc]":                            nil,
		"acc + [size(acc)]":                            nil,
		"acc + [`acc`]":                                nil,
		"acc + [reduce(b = acc, y IN [] | b)]":         nil,
		"acc + [x.acc]":                                {"[x.acc]"},
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

// TestReduceAppendFoldSharesNothing: the list a fold appends to is shared
// with nothing it hands out (reduceAppendPlan). An operand naming the
// accumulator (a + [x] + [a]) would keep a view of the list the fold goes
// on appending to, so the step has no plan; and the result is a list of its
// own length, so its next owner counts the allocation it holds. On the row
// evaluator, its compiled programs and the context evaluator.
func TestReduceAppendFoldSharesNothing(t *testing.T) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:N {id: 1, tags: ['a', 'b', 'c']})", nil)
	require.NoError(t, err)
	base := make([]interface{}, 2000)
	for i := range base {
		base[i] = int64(i)
	}
	params := map[string]interface{}{"base": base}
	// Every list in a value, nested ones included.
	var lists func(value interface{}, out *[][]interface{})
	lists = func(value interface{}, out *[][]interface{}) {
		if list, ok := value.([]interface{}); ok {
			*out = append(*out, list)
			for _, item := range list {
				lists(item, out)
			}
		}
	}
	for _, query := range []string{
		"RETURN reduce(a = [], x IN [1, 2, 3] | a + [x] + [a]) AS v",
		"RETURN reduce(a = [], x IN [1, 2, 3] | a + [x] + [`a`]) AS v",
		"RETURN reduce(a = [], x IN [1, 2, 3] | a + [x] + [reduce(b = a, y IN [] | b)]) AS v",
		"WITH 1 AS one RETURN reduce(a = [], x IN range(1, 40) | a + [x] + x) AS v",
		"RETURN reduce(a = $base, x IN [1] | a + x) AS v",
		"RETURN [i IN range(1, 5) | reduce(a = $base, x IN [i] | a + x)] AS v",
		"MATCH (n:N) RETURN reduce(a = [], t IN n.tags | a + [t] + n.id) AS v",
		"MATCH (n:N) RETURN reduce(a = [], t IN n.tags | a + [t] + [a]) AS v",
	} {
		for _, programs := range []bool{true, false} {
			rowValueProgramsDisabled.Store(!programs)
			for round := 0; round < 3; round++ { // the third run reads compiled programs
				reduceAppendDisabled.Store(true)
				want, err := exec.Execute(ctx, query, params)
				require.NoError(t, err, query)
				reduceAppendDisabled.Store(false)
				got, err := exec.Execute(ctx, query, params)
				require.NoError(t, err, query)
				require.Equal(t, want.Rows, got.Rows, "programs %v: %s", programs, query)

				var all [][]interface{}
				lists(got.Rows[0][len(got.Rows[0])-1], &all)
				// One list may appear twice (a value is immutable); two lists
				// of different lengths on one backing array are a view of the
				// list the fold appended to.
				backing := map[*interface{}]int{}
				for index, list := range all {
					require.Equal(t, len(list), cap(list), "programs %v: %s: list %d has spare capacity", programs, query, index)
					if len(list) == 0 {
						continue
					}
					if other, shared := backing[&list[0]]; shared && len(all[other]) != len(list) {
						t.Fatalf("programs %v: %s: lists %d and %d share a backing array", programs, query, other, index)
					}
					backing[&list[0]] = index
				}
			}
		}
	}
	rowValueProgramsDisabled.Store(false)
	require.Equal(t, int64(0), base[0])
	require.Len(t, base, 2000)
}

// TestReduceAppendResultHandoff: runReduceForm hands its list over at its
// own length, apart from the list it appended to (which has spare
// capacity, and which a later step's operands saw).
func TestReduceAppendResultHandoff(t *testing.T) {
	base := make([]interface{}, 20000)
	for i := range base {
		base[i] = int64(i)
	}
	form := reduceForm{accumulator: "a", variable: "x", step: "a + x"}
	var folding []interface{}
	result, err := runReduceForm(form, base, []interface{}{int64(1), int64(2)},
		func(a, x interface{}) (interface{}, error) { return nil, nil }, nil,
		&reduceAppender{plan: form.appendPlan(), term: func(_ int, a, x interface{}) (interface{}, error) {
			folding = a.([]interface{})
			return x, nil
		}})
	require.NoError(t, err)
	list := result.([]interface{})
	require.Len(t, list, 20002)
	require.Equal(t, len(list), cap(list))
	// The second step saw the fold's own list (copied from base by the first).
	require.Len(t, folding, 20001)
	require.NotSame(t, &folding[0], &list[0], "the result is the list the fold appended to")
	require.Len(t, base, 20000)
}
