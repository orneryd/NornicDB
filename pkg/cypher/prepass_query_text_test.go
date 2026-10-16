package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var prepassCanonicalFixtures = []string{
	"", "RETURN 1", "RETURN\t1", "RETURN  1", "RETURN\u20031", "RETURN  'a  b' AS`a b`", "RETURN [1][0]AS x", "RETURN `a``b`AS x",
	"RETURN 1 // c\r\n/* block\r\n */ AS x", "RETURN '/*text*/', `//name`", "RETURN 'unfinished", "/* unfinished", "RETURN  1 /* tail */",
	":USE db\r\n:param x => 1\n RETURN  $x", ":USE db", "  // c\n:USE db\n /* c */ RETURN 1", ":USE db\rRETURN  1",
	"CREATE PROCEDURE p() { // c\n RETURN  1 }", "CREATE OR REPLACE PROCEDURE p() { /* c\r\n */ RETURN 1 }",
	"CREATE CONSTRAINT c FOR (n:N) REQUIRE {n.x /* c */ IS UNIQUE}", "CREATE CONSTRAINT c FOR (n:N) REQUIRE n.x IS UNIQUE",
	"CREATE DECAY p\n // c\n SET value = 1", "ALTER PROMOTION p\n/* c\n */ SET x = 1", "DROP RETENTION p", "SHOW DECAY", "RETENTION x",
	"MATCH (a) <- [r] - (b) RETURN a", "MERGE (a) - [r] - > (b) RETURN a", "CREATE (a) - > (b)", "RETURN x < - -1, (2) - [3][0]",
	"MATCH (a) WHERE EXISTS { MATCH (b) - > (c) RETURN b } RETURN a", "MATCH (n) ON CREATE SET n.x = 1 RETURN n",
}

func TestPrepassCanonicalQueryParityAndReplay(t *testing.T) {
	for _, query := range append(prepassFixtureQueries, prepassCanonicalFixtures...) {
		want, wantRewrite := canonicalizeQueryText(query)
		p := &pipelineCancellation{ctx: context.Background()}
		got, gotRewrite := canonicalizeQueryTextWithCancellation(query, p)
		require.Equal(t, want, got, query)
		require.Equal(t, wantRewrite, gotRewrite, query)
		require.Equal(t, StripComments(query), StripCommentsWithCancellation(query, p))
		require.Equal(t, classificationText(query), classificationTextWithCancellation(query, p))
		require.Equal(t, hasOwnStatementSyntax(query), hasOwnStatementSyntaxWithCancellation(query, p))
		require.Equal(t, leadingShellCommandsEnd(query), leadingShellCommandsEndWithCancellation(query, p))
		require.Equal(t, queryMayNeedCanonicalRewrite(query), queryMayNeedCanonicalRewriteWithCancellation(query, p))
		require.Equal(t, commentReplacement(query), commentReplacementWithCancellation(query, p))
		require.NoError(t, p.err)
		for _, run := range []func(*pipelineCancellation){
			func(p *pipelineCancellation) {
				text, rewrite := canonicalizeQueryTextWithCancellation(query, p)
				if p.err != nil {
					require.Empty(t, text)
					require.Nil(t, rewrite)
				}
			},
			func(p *pipelineCancellation) {
				text, rewrite := scanCanonicalQueryTextWithCancellation(query, p)
				if p.err != nil {
					require.Empty(t, text)
					require.Nil(t, rewrite)
				}
			},
			func(p *pipelineCancellation) { _ = StripCommentsWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = hasOwnStatementSyntaxWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = leadingShellCommandsEndWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = queryMayNeedCanonicalRewriteWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = commentReplacementWithCancellation(query, p) },
			func(p *pipelineCancellation) { _ = inPatternClauseWithCancellation(query, len(query), p) },
		} {
			replayPrepassWork(t, run)
		}
	}
}

func TestPrepassCanonicalMemoBypass(t *testing.T) {
	query := "RETURN  '" + strings.Repeat("x", 4096) + "' AS x"
	_, memo := canonicalizeQueryText(query)
	require.NotNil(t, memo)
	_, again := canonicalizeQueryText(query)
	require.Same(t, memo, again)
	ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
	p := &pipelineCancellation{ctx: withExpressionFailureSlot(ctx)}
	text, rewrite := canonicalizeQueryTextWithCancellation(query, p)
	require.ErrorIs(t, p.err, context.Canceled)
	require.Empty(t, text)
	require.Nil(t, rewrite)
	require.Same(t, memo, canonicalRewrites.slot(query).Load(), "cancelled scan cannot publish or replace memo")
	p = &pipelineCancellation{ctx: context.Background()}
	_, fresh := canonicalizeQueryTextWithCancellation(query, p)
	require.Equal(t, memo, fresh)
	require.NotSame(t, memo, fresh)
	require.Same(t, memo, canonicalRewrites.slot(query).Load())
}

func TestPrepassCanonicalLongInnerWork(t *testing.T) {
	long := strings.Repeat("x", 1<<20)
	for _, query := range []string{long, "/*" + long + "*/ RETURN 1", ":USE " + long, "CREATE " + long, "RETURN '" + long + "'", "MATCH (" + long + ") - > (b) RETURN b"} {
		ctx := &pipelineCountingContext{Context: context.Background(), stop: 2}
		p := &pipelineCancellation{ctx: ctx}
		text, rewrite := canonicalizeQueryTextWithCancellation(query, p)
		require.ErrorIs(t, p.err, context.Canceled)
		require.Empty(t, text)
		require.Nil(t, rewrite)
	}
}

var prepassCanonicalBenchmarkSink string

// Checked canonicalization deliberately bypasses the legacy memo until the
// caller supplies its cancellable cache owner. Both paths receive identical
// input and the nil path is warmed, exposing the actual cost of that bypass.
func BenchmarkPrepassCanonicalRewrite(b *testing.B) {
	for name, query := range map[string]string{"plain": "MATCH (n) RETURN n", "formatted": "MATCH  (n) /* comment */ RETURN n", "large_comment": "RETURN /*" + strings.Repeat("x", 32<<10) + "*/ 1"} {
		for _, checked := range []bool{false, true} {
			label := "nil_memo"
			if checked {
				label = "checked_bypass"
			}
			b.Run(name+"/"+label, func(b *testing.B) {
				_, _ = canonicalizeQueryText(query)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				b.ReportAllocs()
				b.SetBytes(int64(len(query)))
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					var p *pipelineCancellation
					if checked {
						p = &pipelineCancellation{ctx: ctx}
					}
					prepassCanonicalBenchmarkSink, _ = canonicalizeQueryTextWithCancellation(query, p)
				}
			})
		}
	}
}
