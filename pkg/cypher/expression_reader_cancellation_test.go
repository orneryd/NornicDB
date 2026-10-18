package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpressionReaderBudgetBoundaries(t *testing.T) {
	for _, text := range []string{"", "x", "a.b", "a..b", "(1)", "((1))", "(1)+2", "['a''b']", "['unterminated]", "CASE WHEN true THEN 'END' ELSE 1 END", "CASE x WHEN 1 THEN CASE y WHEN 2 THEN 3 END END+1", "collect('" + strings.Repeat("\\", 130) + "')", "[x IN [1,2] WHERE x>1 | x+1]"} {
		runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
			want, wok := stripEnclosingExpressionParentheses(text)
			actual, ok := stripEnclosingExpressionParenthesesWithCancellation(text, p)
			if p.err == nil {
				require.Equal(t, want, actual)
				require.Equal(t, wok, ok)
			}
		})
		runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
			want, wok := stripEnclosingRowDelimiter(text, '[', ']')
			actual, ok := stripEnclosingRowDelimiterWithCancellation(text, '[', ']', p)
			if p.err == nil {
				require.Equal(t, want, actual)
				require.Equal(t, wok, ok)
			}
		})
		runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
			name, inner, ok := parseFunctionCallWS(text)
			n, i, o := parseFunctionCallWSWithCancellation(text, p)
			if p.err == nil {
				require.Equal(t, name, n)
				require.Equal(t, inner, i)
				require.Equal(t, ok, o)
			}
		})
		runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
			want := leadingCaseExpressionEnd(text)
			actual := leadingCaseExpressionEndWithCancellation(text, p)
			if p.err == nil {
				require.Equal(t, want, actual)
			}
		})
	}
	for _, text := range []string{"x IN [1,2]", "x IN [1,2] WHERE x>1 | x", "x IN [CASE WHEN true THEN 1 ELSE 2 END] | x", "x IN xs WHERE ", "x IN xs | ", "x IN ", "x", "x /*comment*/ IN '" + strings.Repeat("x", 129) + "' | x"} {
		v, l, w, r, ok := parseListComprehension(text)
		runPreparationBudgetSweep(t, func(_ context.Context, p *pipelineCancellation) {
			a, b, c, d, o := parseListComprehensionWithCancellation(text, p)
			if p.err == nil {
				require.Equal(t, v, a)
				require.Equal(t, l, b)
				require.Equal(t, w, c)
				require.Equal(t, r, d)
				require.Equal(t, ok, o)
			}
		})
	}
}

func TestExpressionReaderContextDeadlines(t *testing.T) {
	large := strings.Repeat("x", 8<<20)
	for _, stage := range []struct {
		name, text string
		run        func(string, *pipelineCancellation)
	}{
		{"function", "collect('" + large + "')", func(s string, p *pipelineCancellation) { parseFunctionCallWSWithCancellation(s, p) }},
		{"parentheses", "('" + large + "')", func(s string, p *pipelineCancellation) { stripEnclosingExpressionParenthesesWithCancellation(s, p) }},
		{"list", "['" + large + "']", func(s string, p *pipelineCancellation) { stripEnclosingRowDelimiterWithCancellation(s, '[', ']', p) }},
		{"comprehension", "'" + large + "' | value", func(s string, p *pipelineCancellation) { findListComprehensionTokenWithCancellation(s, "|", p) }},
		{"case", "CASE WHEN true THEN '" + large + "' END", func(s string, p *pipelineCancellation) { leadingCaseExpressionEndWithCancellation(s, p) }},
		{"property", large + ".value", func(s string, p *pipelineCancellation) { rowPropertyChainShapeWithCancellation(s, p) }},
		{"aggregate", "sum('" + large + "')", func(s string, p *pipelineCancellation) { findAggregateSpansWithCancellation(s, p) }},
	} {
		t.Run(stage.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			p := &pipelineCancellation{ctx: ctx}
			stage.run(stage.text, p)
			returned := time.Now()
			require.ErrorIs(t, p.err, context.DeadlineExceeded)
			require.Less(t, returned.Sub(deadline), 250*time.Millisecond)
			t.Logf("deadline=%s returned=%s overdue=%s", deadline.Format(time.RFC3339Nano), returned.Format(time.RFC3339Nano), returned.Sub(deadline))
		})
	}
}
