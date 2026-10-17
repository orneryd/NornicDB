package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type reverseSearchCancellationContext struct {
	context.Context
	cancel    context.CancelFunc
	calls     int
	cancelled time.Time
}

func (ctx *reverseSearchCancellationContext) Err() error {
	ctx.calls++
	if ctx.calls == 2 {
		ctx.cancelled = time.Now()
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestPipelineReverseTextSearch(t *testing.T) {
	for _, needle := range []string{"", "/", "/*", "é", strings.Repeat("ab", 40)} {
		for _, offset := range []int{0, 1, 4095, 4096, 4097, 8191} {
			for _, tail := range []string{"", strings.Repeat("z", 4097), needle} {
				text := strings.Repeat("x", offset) + needle + tail
				want := strings.LastIndex(text, needle)
				require.Equal(t, want, lastIndexTextWithCancellation(text, needle, nil))
				ctx, cancel := context.WithCancel(context.Background())
				probe := &pipelineCancellation{ctx: ctx}
				require.Equal(t, want, lastIndexTextWithCancellation(text, needle, probe))
				require.NoError(t, probe.err)
				cancel()
			}
		}
	}
	for _, needle := range []string{"missing", strings.Repeat("ab", 40)} {
		base, cancel := context.WithCancel(context.Background())
		ctx := &reverseSearchCancellationContext{Context: base, cancel: cancel}
		probe := &pipelineCancellation{ctx: ctx}
		require.Equal(t, -1, lastIndexTextWithCancellation(strings.Repeat("x", 4096*130), needle, probe))
		require.ErrorIs(t, probe.err, context.Canceled)
		require.Equal(t, 2, ctx.calls)
		require.Less(t, time.Since(ctx.cancelled), 250*time.Millisecond)
		cancel()
	}
	require.Equal(t, -1, lastIndexTextWithCancellation("x", "longer", &pipelineCancellation{ctx: context.Background()}))
}
