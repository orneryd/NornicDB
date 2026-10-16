package cypher

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

// Vary the shared counter's entry phase, then cancel at each possible poll.
// This covers a one-off outer error branch after a recursive child as well
// as the inner loop, without adding production cancellation hooks.
func runPrivacyReaderSamples(t *testing.T, run func(*pipelineCancellation) interface{}) {
	t.Helper()
	for remaining := 0; remaining <= pipelineCancellationInterval; remaining++ {
		healthy := &pipelineCountingContext{Context: context.Background()}
		probe := pipelineCancellation{ctx: healthy, remaining: remaining}
		_ = run(&probe)
		require.NoError(t, probe.err)
		// Existing site-discovery cases cover every inner site. Entry-phase
		// sweeps need only the first, second and final polls to reach the
		// enclosing one-off propagation branches without quadratic replay.
		seen := map[int]bool{}
		for _, sample := range []int{1, 2, healthy.calls} {
			if sample <= 0 || sample > healthy.calls || seen[sample] {
				continue
			}
			seen[sample] = true
			ctx := &pipelineCountingContext{Context: context.Background(), stop: sample}
			probe := pipelineCancellation{ctx: withExpressionFailureSlot(ctx), remaining: remaining}
			_ = run(&probe)
			if ctx.calls >= sample {
				require.ErrorIs(t, probe.err, context.Canceled)
			} else {
				require.NoError(t, probe.err)
			}
		}
	}
}
