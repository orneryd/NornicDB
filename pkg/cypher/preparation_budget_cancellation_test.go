package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type preparationBudgetSampleKey struct{}

// Move the existing probe's sample across child operations and their parent
// unwinds. No production hook or second cancellation counter is introduced.
func runPreparationBudgetSweep(t *testing.T, run func(context.Context, *pipelineCancellation), phases ...int) {
	t.Helper()
	runPreparationBudgetSamples(t, run, nil, phases...)
}

// The same cancellation assertions serve exhaustive small stages and bounded
// parser samples. A parser's ATN can produce tens of thousands of polls even
// for a short malformed expression; replaying every prefix is quadratic work.
func runPreparationBudgetSamples(t *testing.T, run func(context.Context, *pipelineCancellation), selectStops func(int) []int, phases ...int) {
	t.Helper()
	if len(phases) == 0 {
		phases = []int{1, 31, 64, 127, 128}
	}
	for _, phase := range phases {
		invoke := func(stop int) int {
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			sampled := &queryCacheSampleContext{Context: base, cancel: cancel, stop: stop}
			ctx := withExpressionFailureSlot(sampled)
			probe := &pipelineCancellation{ctx: ctx, remaining: phase}
			ctx = context.WithValue(ctx, pipelineCancellationKey{}, probe)
			ctx = context.WithValue(ctx, preparationBudgetSampleKey{}, sampled)
			run(ctx, probe)
			calls, cancelled := sampled.receipt()
			if stop > 0 && calls >= stop {
				// Execute may reject an already-cancelled caller before entering
				// the shared probe. Nested work must latch through that probe.
				if probe.err != nil {
					require.ErrorIs(t, probe.err, context.Canceled)
					require.ErrorIs(t, getExpressionFailure(ctx), context.Canceled)
				} else {
					require.ErrorIs(t, base.Err(), context.Canceled)
				}
				require.Less(t, time.Since(cancelled), 250*time.Millisecond)
			} else {
				require.NoError(t, probe.err)
			}
			return calls
		}
		calls := invoke(0)
		if selectStops == nil {
			for stop := 1; stop <= calls; stop++ {
				invoke(stop)
			}
			continue
		}
		seen := make(map[int]bool)
		for _, stop := range selectStops(calls) {
			if stop > 0 && stop <= calls && !seen[stop] {
				seen[stop] = true
				invoke(stop)
			}
		}
	}
}

func TestParameterPreparationBudgetBoundaries(t *testing.T) {
	values := make([]interface{}, 257)
	for i := range values {
		values[i] = map[string][]string{"items": {strings.Repeat("'\\", 65), "value"}}
	}
	fields := map[string]interface{}{"values": values, "typed": map[string][]int{"items": {1, 2, 3}}}
	for _, stage := range []struct {
		name string
		run  func(context.Context, *pipelineCancellation)
	}{
		{"normalize", func(_ context.Context, p *pipelineCancellation) { normalizeQueryParametersWithCancellation(fields, p) }},
		{"row", func(_ context.Context, p *pipelineCancellation) { parameterRowValueWithCancellation(fields, p) }},
		{"separate_cypher25", func(_ context.Context, p *pipelineCancellation) {
			separateCypher25ParametersWithCancellation(strings.Repeat("L$props ", 129), p)
		}},
		{"rewrite_get", func(_ context.Context, p *pipelineCancellation) {
			query := "RETURN " + strings.Repeat("x ", 4096)
			markRewriteFree(rewriteFreeFunctionAliases, false, query)
			rewriteFreeWithCancellation(rewriteFreeFunctionAliases, false, query, p)
		}},
		{"rewrite_put", func(_ context.Context, p *pipelineCancellation) {
			markRewriteFreeWithCancellation(rewriteFreeCypher25Expressions, true, "RETURN "+strings.Repeat("x ", 4096), p)
		}},
		{"aliases", func(_ context.Context, p *pipelineCancellation) {
			canonicalizeFunctionAliasesWithCancellation("RETURN property_exists(n,`"+strings.Repeat("a'\\", 257)+"`),"+strings.Repeat("ln(2),", 257)+"ln(2)", p)
		}},
	} {
		t.Run(stage.name, func(t *testing.T) { runPreparationBudgetSweep(t, stage.run) })
	}
}

func TestPreparationCallerBudgetBoundaries(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "admission_budget")
	phases := make([]int, pipelineCancellationInterval)
	for i := range phases {
		phases[i] = i + 1
	}
	for index, query := range []string{
		"RETURN '" + strings.Repeat("x", 129) + "' AS value",
		"RETURN ln(2) AS value",
		"CYPHER 25 RETURN 1 AS value",
		"RETURN 1_000 AS value",
		"RETURN （1） AS value",
	} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			runPreparationBudgetSweep(t, func(ctx context.Context, probe *pipelineCancellation) {
				executor := NewStorageExecutor(engine)
				_, err := executor.Execute(ctx, query, nil)
				sampled := ctx.Value(preparationBudgetSampleKey{}).(*queryCacheSampleContext)
				// Calling Cause/Err on this deliberately instrumented context would
				// itself consume a sample after Execute returned. Read the receipt.
				_, cancelled := sampled.receipt()
				if !cancelled.IsZero() {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
			}, phases...)
		})
	}
}

func TestPreparationInternalCallerBudgetBoundaries(t *testing.T) {
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "internal_admission_budget")
	phases := make([]int, pipelineCancellationInterval)
	for i := range phases {
		phases[i] = i + 1
	}
	for _, query := range []string{"RETURN 1 AS value", "RETURN （1） AS value", "RETURN 1_000 AS value", "RETURN ln(2) AS value", "RETURN '" + strings.Repeat("x", 129) + "' AS value"} {
		t.Run(query[:min(35, len(query))], func(t *testing.T) {
			runPreparationBudgetSweep(t, func(ctx context.Context, _ *pipelineCancellation) {
				_, err := NewStorageExecutor(engine).executeInternal(ctx, query, nil)
				_, cancelled := ctx.Value(preparationBudgetSampleKey{}).(*queryCacheSampleContext).receipt()
				if !cancelled.IsZero() {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
			}, phases...)
		})
	}
}
