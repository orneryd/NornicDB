package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Public-path cancellation of the two expression shapes the v1.5.0 state
// report records: a reduce over a range (report section 19) and a statement
// joining about 720 string literals with + (report section 20). Both kept
// working for seconds to half a minute after the caller's deadline, because
// the work they spend is not in a loop that observes the caller's context:
// range() materialized every value, a reduce folded every element, and
// recognising an arithmetic chain's form scans its text once per operand
// (expressionTextAccepted, staticArithmeticOperandText, the static operator
// checker and the shared arithmetic evaluator took no context).
//
// The tests measure the product result, not a helper: the public Statement
// Execute call with the caller's own deadline must end as that cancellation
// within statementCancellationBudget of the deadline.

// statementCancellationBudget is how long the backend may keep working after
// the caller's cancellation or deadline, the statement workload budget the
// release cancellation contract uses.
const statementCancellationBudget = 100 * time.Millisecond

// expressionChainText renders the reported shape: count string literals
// joined by +. The report's statement is 722 literals (7,957 bytes).
func expressionChainText(count int) string {
	return strings.Repeat("'secret' + ", count) + "'x'"
}

// requireCanceledWithinBudget runs query through the public executor under
// deadline and requires it to end as the caller's own deadline within
// statementCancellationBudget. The time spent before the deadline is the
// statement's own work and is not part of the budget.
func requireCanceledWithinBudget(t *testing.T, exec *StorageExecutor, query string, params map[string]interface{}, deadline time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	start := time.Now()
	_, err := exec.Execute(ctx, query, params)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, context.DeadlineExceeded, "a canceled statement must report the caller's cancellation")
	require.LessOrEqual(t, elapsed-deadline, statementCancellationBudget,
		"the backend kept working for %s after the caller's cancellation", elapsed-deadline)
}

// TestPublicExpressionChainStopsOnCancellation is report section 20: a
// statement whose one expression joins about 720 string literals with + ran
// for 27-34 s after a deadline of 100 ms on the call. Recognising the chain's
// form re-enters the expression-text scan once per operand, so the canceled
// statement must stop at the operand it is recognising instead of after the
// last one. The controls keep both parsers' results for the same shape.
func TestPublicExpressionChainStopsOnCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		// The reported statement's own text: 722 literals, 7,957 bytes.
		const reportedLiterals = 722
		const controlLiterals = 7
		joined := strings.Repeat("secret", controlLiterals) + "x"
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "expression_chain_cancel"))

		// Control: the same shape, small enough to evaluate, returns the
		// joined text.
		result, err := exec.Execute(t.Context(), "RETURN "+expressionChainText(controlLiterals)+" AS v", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{joined}}, result.Rows)

		// A RETURN whose one expression is the reported chain.
		requireCanceledWithinBudget(t, exec, "RETURN "+expressionChainText(reportedLiterals)+" AS v", nil, 20*time.Millisecond)

		// A CREATE whose pattern property value is that chain: the statement
		// is canceled before it stores anything.
		requireCanceledWithinBudget(t, exec,
			"CREATE (:Probe {run: $run, k: "+expressionChainText(reportedLiterals)+"})",
			map[string]interface{}{"run": "canceled"}, 20*time.Millisecond)
		result, err = exec.Execute(t.Context(), "MATCH (n:Probe {run: 'canceled'}) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "a canceled CREATE publishes no node")

		// Control: a CREATE of the same shape that is not canceled stores the
		// joined text.
		_, err = exec.Execute(t.Context(), "CREATE (:Probe {run: 'kept', k: "+expressionChainText(controlLiterals)+"})", nil)
		require.NoError(t, err)
		result, err = exec.Execute(t.Context(), "MATCH (n:Probe {run: 'kept'}) RETURN n.k AS k", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{joined}}, result.Rows)
	})
}

// TestPublicCypherRangeStopsOnCancellation is report section 19's range-only
// statement: range() took no context and appended every value before it
// returned, so a deadline did not end a multi-million value range.
func TestPublicCypherRangeStopsOnCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "range_cancel"))

		// Control: an ordinary range is unchanged.
		result, err := exec.Execute(t.Context(), "RETURN range(1, 3) AS v", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{[]interface{}{int64(1), int64(2), int64(3)}}}, result.Rows)

		requireCanceledWithinBudget(t, exec, "RETURN range(1, 5000000) AS v", nil, 20*time.Millisecond)
	})
}

// TestPublicReduceStopsOnCancellation is report section 19: a reduce over a
// range folded every element without observing the caller, so the deadline
// error arrived seconds after the deadline.
func TestPublicReduceStopsOnCancellation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "reduce_cancel"))

		// Control: an ordinary reduce is unchanged.
		result, err := exec.Execute(t.Context(), "RETURN reduce(s = 0, x IN range(1, 4) | s + x) AS v", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(10)}}, result.Rows)

		// Supply the list as a parameter so this check exercises folding,
		// independently of range materialization. Dynamic UUID work avoids a
		// small constant sum finishing before the requested deadline.
		items := make([]interface{}, 500000)
		for i := range items {
			items[i] = int64(i)
		}
		requireCanceledWithinBudget(t, exec,
			"RETURN reduce(s = 0, x IN $items | s + size(randomUUID())) AS v",
			map[string]interface{}{"items": items}, 20*time.Millisecond)
	})
}

// The component control proves cancellation is observed inside folding,
// rather than only during statement preparation or list construction.
func TestReduceFoldStopsBeforeEvaluatingAnotherItemAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	form, ok := parseReduceForm("reduce", "s = 0, x IN $items | s + x")
	require.True(t, ok)
	calls := 0
	_, _, err := rowReduceLoop(ctx, form, int64(0), []interface{}{int64(1), int64(2), int64(3)}, map[string]interface{}{},
		func() (interface{}, bool, error) { calls++; cancel(); return int64(1), true, nil },
		func() (interface{}, bool, error) { return true, true, nil }, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}
