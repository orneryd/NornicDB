package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A cached rowValueProgram runs list loops itself; each loop checks the
// statement's probe once per element, as the text evaluator does.
func TestCompiledRowProgramsStopAtDeadline(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	small := make([]interface{}, 4)
	large := make([]interface{}, 4000000)
	for index := range large {
		large[index] = int64(index)
	}
	for index := range small {
		small[index] = int64(index)
	}
	for _, expression := range []string{
		"[x IN items | x + 1]",
		"[x IN items WHERE x >= 0]",
		"reduce(s = 0, x IN items | s + x)",
		"all(x IN items WHERE x >= 0)",
		"size([x IN items | [y IN [1, 2] | x + y]])",
	} {
		t.Run(expression, func(t *testing.T) {
			for warm := 0; warm < 3; warm++ {
				_, ok := executor.evaluateRowExpressionWithContext(context.Background(), expression, pipelineRow{"items": small})
				require.True(t, ok)
			}
			require.NotNil(t, rowValueProgramFor(expression), "the program must be compiled before the deadline run")
			ctx, cancel := context.WithTimeout(withExpressionFailureSlot(context.Background()), 20*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			_, ok := executor.evaluateRowExpressionWithContext(ctx, expression, pipelineRow{"items": large})
			require.False(t, ok)
			require.ErrorIs(t, getExpressionFailure(ctx), context.DeadlineExceeded)
			require.Less(t, time.Since(deadline), 250*time.Millisecond, "compiled loop continued past the deadline")
		})
	}
}

// A cold evaluation hands an inner comprehension to the shared evaluator
// (rowValueFallback); that evaluator checks the statement's probe too, so
// the first evaluation of a nested comprehension stops at its deadline.
func TestColdNestedComprehensionStopsAtDeadline(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	items := make([]interface{}, 1000000)
	for index := range items {
		items[index] = int64(index)
	}
	for attempt := 0; attempt < 5; attempt++ {
		clearExpressionCaches()
		ctx, cancel := context.WithTimeout(withExpressionFailureSlot(context.Background()), 20*time.Millisecond)
		deadline, _ := ctx.Deadline()
		_, ok := executor.evaluateRowExpressionWithContext(ctx, "[x IN items | [y IN items | x + y]]", pipelineRow{"items": items})
		overdue := time.Since(deadline)
		cancel()
		require.False(t, ok)
		require.ErrorIs(t, getExpressionFailure(ctx), context.DeadlineExceeded)
		require.Less(t, overdue, 250*time.Millisecond, "cold nested comprehension continued past the deadline")
	}
}
