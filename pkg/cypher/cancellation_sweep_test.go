package cypher

import (
	"context"
	"testing"
	"time"

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


func replayPrepassWork(t *testing.T, run func(*pipelineCancellation)) {
	t.Helper()
	ctx := &pipelineCountingContext{Context: context.Background()}
	probe := &pipelineCancellation{ctx: ctx}
	run(probe)
	require.NoError(t, probe.err)
	if ctx.calls == 0 {
		return
	}
	work := (ctx.calls-1)*pipelineCancellationInterval + pipelineCancellationInterval + 1 - probe.remaining
	for stop := 1; stop <= work; stop++ {
		ctx := &pipelineCountingContext{Context: context.Background(), stop: 1}
		probe := &pipelineCancellation{ctx: ctx, remaining: stop}
		run(probe)
		require.ErrorIs(t, probe.err, context.Canceled, "stop %d/%d", stop, work)
		require.Equal(t, 1, ctx.calls)
	}
}
