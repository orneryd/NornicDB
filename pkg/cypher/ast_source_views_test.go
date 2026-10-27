package cypher

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestASTRawSourceUnicodeWindows(t *testing.T) {
	query := `\u0052ETURN [\u0027x\u0027, {\u006b: f('\u0062')}] AS result RETURN '\u0061' AS a`
	ast, err := NewASTBuilder().Build(query)
	require.NoError(t, err)
	require.Equal(t, query, ast.RawQuery)
	require.Len(t, ast.Clauses, 2)
	first := ast.Clauses[0]
	require.Equal(t, query[:len(query)-len(` RETURN '\u0061' AS a`)], first.RawText)
	require.Equal(t, 0, first.StartPos)
	require.Equal(t, len(first.RawText), first.EndPos)
	list := first.Return.Items[0].Expression
	require.Equal(t, `[\u0027x\u0027, {\u006b: f('\u0062')}]`, list.RawText)
	require.Len(t, list.List, 2)
	require.Equal(t, `\u0027x\u0027`, list.List[0].RawText)
	require.Equal(t, "x", list.List[0].Literal)
	require.Equal(t, `{\u006b: f('\u0062')}`, list.List[1].RawText)
	require.Equal(t, `f('\u0062')`, list.List[1].Map["k"].RawText)
	second := ast.Clauses[1]
	require.Equal(t, query[second.StartPos:second.EndPos], second.RawText)
	require.Equal(t, `'\u0061'`, second.Return.Items[0].Expression.RawText)
	require.Equal(t, "a", second.Return.Items[0].Expression.Literal)
}

func TestASTRawSourceUnicodeIdentityAndNonrecursive(t *testing.T) {
	for _, query := range []string{`RETURN \u0061 AS x`, "RETURN `\\u0061` AS x", `RETURN \u0060a\u0060 AS x`} {
		ast, err := NewASTBuilder().Build(query)
		require.NoError(t, err)
		require.Len(t, ast.Clauses, 1)
		require.Equal(t, "a", ast.Clauses[0].Return.Items[0].Expression.Variable)
	}
	ast, err := NewASTBuilder().Build(`RETURN '\u005cu0061' AS x`)
	require.NoError(t, err)
	require.Equal(t, `\u0061`, ast.Clauses[0].Return.Items[0].Expression.Literal)
	_, err = NewASTBuilder().Build(`RETURN '\u00zz'`)
	require.Error(t, err)
}

func TestASTSourceUnicodeGeneratedClosureAndParity(t *testing.T) {
	for _, query := range []string{`RETURN 'x\u0027 AS x`, `RETURN \u0027x\u0027 AS \u0078`} {
		ast, err := NewASTBuilder().Build(query)
		require.NoError(t, err)
		require.Len(t, ast.Clauses, 1)
		item := ast.Clauses[0].Return.Items[0]
		require.Equal(t, "x", item.Expression.Literal)
		require.Equal(t, "x", item.Alias)
		require.Equal(t, query, ast.Clauses[0].RawText)
	}
	for _, query := range []string{`RETURN '\\u0061' AS x`, `RETURN '\u005cu0061' AS x`} {
		ast, err := NewASTBuilder().Build(query)
		require.NoError(t, err)
		require.Equal(t, `\u0061`, ast.Clauses[0].Return.Items[0].Expression.Literal)
	}
}

func TestASTSourceViewsBuilderConcurrent(t *testing.T) {
	builder := NewASTBuilder()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 8; j++ {
				ast, err := builder.Build(`RETURN ['\u0061', '\u0061'] AS x`)
				if err != nil || len(ast.Clauses) != 1 {
					t.Errorf("build failed: %v", err)
					return
				}
				list := ast.Clauses[0].Return.Items[0].Expression.List
				if len(list) != 2 || list[0].RawText != `'\u0061'` || list[1].Literal != "a" {
					t.Errorf("lost raw/accepted identity: %#v", list)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestASTSourceNestedCommentsAndMergeActions(t *testing.T) {
	query := `MERGE (\u006e:L {k: ['\u0061', /* middle */ '\u0061']}) ON CREATE SET n.p = {k: '\u0062'} ON MATCH SET n.p = '\u0063' RETURN n`
	ast, err := NewASTBuilder().Build(query)
	require.NoError(t, err)
	require.Len(t, ast.Clauses, 2)
	merge := ast.Clauses[0].Merge
	require.Equal(t, "n", merge.Pattern.Nodes[0].Variable)
	require.Equal(t, `k: ['\u0061', /* middle */ '\u0061']`, merge.Pattern.Nodes[0].RawProps)
	list := merge.Pattern.Nodes[0].Properties["k"]
	require.Equal(t, `['\u0061', /* middle */ '\u0061']`, list.RawText)
	require.Len(t, list.List, 2)
	require.Equal(t, "a", list.List[0].Literal)
	require.Equal(t, "a", list.List[1].Literal)
	require.Equal(t, `'\u0061'`, list.List[0].RawText)
	require.Equal(t, `'\u0061'`, list.List[1].RawText)
	require.Len(t, merge.OnCreate, 1)
	require.Equal(t, `{k: '\u0062'}`, merge.OnCreate[0].RawValue)
	require.Equal(t, `'\u0062'`, merge.OnCreate[0].Value.Map["k"].RawText)
	require.Equal(t, "b", merge.OnCreate[0].Value.Map["k"].Literal)
	require.Len(t, merge.OnMatch, 1)
	require.Equal(t, `'\u0063'`, merge.OnMatch[0].RawValue)
	require.Equal(t, "c", merge.OnMatch[0].Value.Literal)
	// The canonical MERGE owner exposes extents for the same parsed products.
	body := `  (n) ON CREATE SET n.p=1 ON MATCH SET n.p=2 ON CREATE SET n.p=3  `
	var spans mergeActionSpans
	parts := splitMergeClauseActions(body, &spans)
	require.Equal(t, parts.pattern, body[spans.pattern.Start:spans.pattern.End])
	for group, actions := range []mergeActionClauses{parts.onCreate, parts.onMatch} {
		groupSpans := spans.onCreate
		if group == 1 {
			groupSpans = spans.onMatch
		}
		for i := 0; i < actions.len(); i++ {
			span := groupSpans.at(i)
			require.Equal(t, actions.set(i), body[span.Start:span.End])
		}
	}
}

func TestASTCommentProjectionNoCommentAllocations(t *testing.T) {
	query := `RETURN ['//literal', '/*literal*/'] AS x`
	require.Equal(t, query, StripComments(query))
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if StripComments(query) != query {
			panic("changed no-comment text")
		}
	}))
}
