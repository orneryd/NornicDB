package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineGrowthSharesStatementProbe(t *testing.T) {
	for _, write := range []func(*strings.Builder, *pipelineCancellation) bool{
		func(out *strings.Builder, p *pipelineCancellation) bool { return appendPipelineString(out, "!", p) },
		func(out *strings.Builder, p *pipelineCancellation) bool {
			return appendPipelineBytes(out, []byte("!"), p)
		},
		func(out *strings.Builder, p *pipelineCancellation) bool { return appendPipelineByte(out, '!', p) },
		func(out *strings.Builder, p *pipelineCancellation) bool { return appendPipelineRune(out, '世', p) },
		func(out *strings.Builder, p *pipelineCancellation) bool { return reservePipelineString(out, 1, p) },
	} {
		var out strings.Builder
		out.Grow(1 << 20)
		out.WriteString(strings.Repeat("p", out.Cap()))
		ctx := &pipelineCountingContext{Context: context.Background(), stop: 1}
		p := &pipelineCancellation{ctx: withExpressionFailureSlot(ctx), remaining: 3}
		require.False(t, write(&out, p))
		require.ErrorIs(t, p.err, context.Canceled)
		require.ErrorIs(t, getExpressionFailure(p.ctx), context.Canceled)
		require.Equal(t, 4096, out.Len())
	}
	for _, p := range []*pipelineCancellation{nil, {ctx: context.Background()}} {
		require.Equal(t, []int{1, 2, 3}, appendPipelineItems([]int{1}, []int{2, 3}, p))
		require.Equal(t, []int{1, 2}, appendPipelineItem([]int{1}, 2, p))
	}
	ctx := &pipelineCountingContext{Context: context.Background(), stop: 1}
	p := &pipelineCancellation{ctx: ctx, remaining: 3}
	require.Nil(t, appendPipelineValues(make([]interface{}, 1<<18), []interface{}{1}, p))
	require.ErrorIs(t, p.err, context.Canceled)
}
