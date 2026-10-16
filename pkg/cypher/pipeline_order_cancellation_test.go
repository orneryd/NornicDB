package cypher

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type orderComparisonCancelContext struct {
	context.Context
	cancel            context.CancelFunc
	comparisonReached bool
}

func (ctx *orderComparisonCancelContext) Err() error {
	var callers [24]uintptr
	frames := runtime.CallersFrames(callers[:runtime.Callers(1, callers[:])])
	for {
		frame, more := frames.Next()
		if strings.HasSuffix(frame.Function, "compareValuesForSortWithCancellation") {
			ctx.comparisonReached = true
			ctx.cancel()
			break
		}
		if !more {
			break
		}
	}
	return ctx.Context.Err()
}

func TestPipelineOrderTermsStopAtNestedCancellation(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "order_term_stop"))
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &orderComparisonCancelContext{Context: base, cancel: cancel}
	statement := withPipelineCancellation(withExpressionFailureSlot(ctx))
	probe := pipelineCancellationForContext(statement)
	row := pipelineRow{"first": make([]interface{}, 257), "tail": int64(1)}
	terms := make([]orderByTerm, 4096)
	for i := range terms {
		terms[i] = orderByTerm{column: "tail"}
	}
	terms[0] = orderByTerm{column: "first"}
	require.False(t, executor.orderPipelineRowsWithScopes(statement, []pipelineRow{row, row}, []pipelineRow{row, row}, terms))
	require.True(t, ctx.comparisonReached)
	require.ErrorIs(t, probe.err, context.Canceled)
	// The cancelling sample leaves remaining=0. Each subsequent comparison
	// would decrement it below zero even though the error is already latched.
	require.Zero(t, probe.remaining, "ORDER comparator scanned later terms after its child cancelled")
}

func TestPipelineOrderingFallbackCancellation(t *testing.T) {
	value := struct{ Text string }{strings.Repeat("text", 4097)}
	runPreparationBudgetSweep(t, func(_ context.Context, probe *pipelineCancellation) {
		compareValuesForSortWithCancellation(value, value, probe)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	probe := pipelineCancellationForContext(ctx)
	left, right := struct{ Text string }{"a"}, struct{ Text string }{"b"}
	require.Equal(t, compareValuesForSort(left, right), compareValuesForSortWithCancellation(left, right, probe))
	require.NoError(t, probe.err)
}

func TestPipelineOrderPreparationBudgetBoundaries(t *testing.T) {
	for _, clause := range []string{"", "x", "ORDER BY x ASC,y DESC SKIP 1 LIMIT 2", "WITH x ORDER BY 'a b' DESCENDING WHERE x>0", "ORDER BY x ASCENDING, y DESC", "ORDER BY x\u2003DESC", "ORDER BY '" + strings.Repeat("x", 129) + "'", "ORDER BY ,"} {
		want := parseOrderByTerms(clause)
		runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
			actual := parseOrderByTermsWithCancellation(clause, p)
			if p.err == nil {
				require.Equal(t, want, actual)
			}
		})
	}
	for _, text := range []string{"", "a b", "\u2003a\u00a0b\t\r\n", "\xff\xfe text", "a" + strings.Repeat(" ", 129) + "b"} {
		runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
			actual := fieldsWithCancellation(text, p)
			if p.err == nil {
				require.Equal(t, strings.Fields(text), actual)
			}
		})
	}
	expressions := []string{"count(DISTINCT x)", "sum(x)", "x + 1", "'" + strings.Repeat("x ", 65) + "'"}
	terms := []orderByTerm{{column: "COUNT(distinct x)"}, {column: "x+1"}, {column: "sum(x)", descending: true}}
	want := alignOrderTerms(terms, expressions)
	runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
		actual := alignOrderTermsWithCancellation(terms, expressions, p)
		if p.err == nil {
			require.Equal(t, want, actual)
		}
	})
	for _, text := range []string{"", "x", "x ", "x + 1", "'a  b'", "`a b`", "'a\\' b'", "a" + strings.Repeat(" ", 129) + "b"} {
		want := stripExpressionWhitespace(text)
		runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
			actual := stripExpressionWhitespaceWithCancellation(text, p)
			if p.err == nil {
				require.Equal(t, want, actual)
			}
		})
	}
}
