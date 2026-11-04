package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A planned predicate with a part the plan keeps as text evaluates that part
// with the statement context bound in the row it is given, as
// evaluateRowPredicate binds it for a whole predicate. The binding belongs to
// that evaluation only: it must not stay in the row, it must not replace an
// enclosing evaluation's binding, and it must be the one the part's functions
// read.
func TestRowPredicatePlanTextPartBindsStatementContextForOneEvaluation(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plan_context_storage"))
	row := pipelineRow{"n": &storage.Node{ID: "n"}, "x": "a"}
	predicate := func(database string) string {
		return "x = 'a' AND [elementId(n)] = ['4:" + database + ":n']"
	}
	for _, database := range []string{"first_context", "second_context", "first_context"} {
		ctx := withExpressionFailureSlot(context.WithValue(context.Background(), ctxKeyUseDatabase, database))
		for range 3 {
			plan := planRowPredicate(predicate(database))
			require.NotNil(t, plan)
			require.False(t, plan.complete)
			require.True(t, exec.evaluateRowPredicatePlan(ctx, plan, row))
			require.NotContains(t, row, rowStatementContextKey)
			require.NoError(t, getExpressionFailure(ctx))

			// The plan's planned part decides false, so the text part is not
			// reached: the row is left as it was.
			require.False(t, exec.evaluateRowPredicate(ctx, "x = 'b' AND [elementId(n)] = ['4:"+database+":n']", row))
			require.NotContains(t, row, rowStatementContextKey)

			// Through the predicate program, the same plan evaluates the text
			// part with the statement identity its functions read.
			require.True(t, exec.evaluateRowPredicate(ctx, predicate(database), row))
			require.NotContains(t, row, rowStatementContextKey)
			require.NoError(t, getExpressionFailure(ctx))
		}
	}
	// The text part's value is the one its context selects: a different
	// selected database makes the same predicate false.
	require.False(t, exec.evaluateRowPredicate(context.Background(), predicate("second_context"), row))

	// An enclosing evaluation owns its binding; the plan neither replaces nor
	// removes it.
	outer := context.WithValue(context.Background(), ctxKeyUseDatabase, "outer_context")
	row[rowStatementContextKey] = outer
	plan := planRowPredicate(predicate("outer_context"))
	require.NotNil(t, plan)
	require.True(t, exec.evaluateRowPredicatePlan(context.Background(), plan, row))
	require.Contains(t, row, rowStatementContextKey)
	require.Equal(t, outer, row[rowStatementContextKey])
	require.True(t, exec.evaluateRowPredicate(context.Background(), predicate("outer_context"), row))
	require.Contains(t, row, rowStatementContextKey)
	require.Equal(t, outer, row[rowStatementContextKey])
	require.Equal(t, pipelineRow{"n": &storage.Node{ID: "n"}, "x": "a", rowStatementContextKey: outer}, row)
	delete(row, rowStatementContextKey)

	// A part the plan keeps as text and that reads no statement state leaves
	// the row untouched and still evaluates its value.
	plain := pipelineRow{"x": "a"}
	require.False(t, exec.evaluateRowPredicatePlan(context.Background(), planRowPredicate("x = 'a' AND [1, 2] = [1]"), plain))
	require.Equal(t, pipelineRow{"x": "a"}, plain)
	require.Equal(t, pipelineRow{"n": &storage.Node{ID: "n"}, "x": "a"}, row)
}
