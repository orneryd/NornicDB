package antlr

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatementSyntaxOriginalSpansDecodedNamesAndTrivia(t *testing.T) {
	query := "/* é UNION RETURN wrong */ WITH 1 AS `é` CALL db.labels() YIELD label AS /* UNION */ `a,b` RETURN `a,b`, `é`"
	syntax, err := ParseSyntax(context.Background(), query, StatementEntry, false)
	require.NoError(t, err)
	require.Equal(t, query, syntax.Source())
	var names []string
	var yields, unions int
	syntax.Walk(func(node SyntaxNode) func() {
		start, end := node.Span()
		require.GreaterOrEqual(t, start, 0)
		require.LessOrEqual(t, end, len(query))
		require.Equal(t, query[start:end], node.Text())
		if name, symbol := node.Name(); symbol {
			names = append(names, name)
		}
		if node.Rule() == CypherParserRULE_yieldItem {
			yields++
			require.Equal(t, "label AS   `a,b`", node.Code())
			require.Equal(t, strings.Index(query, "label AS"), start)
		}
		if node.Rule() == CypherParserRULE_unionSt {
			unions++
		}
		return nil
	})
	require.Equal(t, 1, yields)
	require.Zero(t, unions)
	require.Contains(t, names, "a,b")
	require.Contains(t, names, "é")
	var comments int
	last := 0
	for i := 0; i < syntax.TokenCount(); i++ {
		token := syntax.Token(i)
		start, end := token.Span()
		require.Equal(t, last, start)
		require.Equal(t, query[start:end], token.Text())
		last = end
		if token.Kind() == CypherLexerCOMMENT {
			comments++
			require.NotZero(t, token.Channel())
		}
	}
	require.Equal(t, len(query), last)
	require.Equal(t, 2, comments)
}

func TestStatementSyntaxTraversalVisitsEveryChildAndRestoresInReverse(t *testing.T) {
	syntax, err := ParseSyntax(context.Background(), "RETURN coalesce(COUNT { MATCH (n) }, 1) AS value", StatementEntry, true)
	require.NoError(t, err)
	var events []string
	var first, second, depth int
	syntax.Walk(func(node SyntaxNode) func() {
		first++
		depth++
		if node.ID() == 0 {
			events = append(events, "first enter")
		}
		return func() {
			depth--
			if node.ID() == 0 {
				events = append(events, "first leave")
			}
		}
	}, func(node SyntaxNode) func() {
		second++
		require.Positive(t, depth)
		if node.ID() == 0 {
			events = append(events, "second enter")
		}
		return func() {
			require.Positive(t, depth)
			if node.ID() == 0 {
				events = append(events, "second leave")
			}
		}
	})
	require.Equal(t, first, second)
	require.Greater(t, first, 20)
	require.Zero(t, depth)
	require.Equal(t, []string{"first enter", "second enter", "second leave", "first leave"}, events)
}

func TestStatementSyntaxCacheViewsAreImmutableAndCancellationIsObserved(t *testing.T) {
	query := "CALL db.labels() YIELD label AS `a``b` RETURN `a``b`"
	first, err := ParseSyntax(context.Background(), query, StatementEntry, false)
	require.NoError(t, err)
	var workers sync.WaitGroup
	failures := make(chan string, 8)
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			syntax, err := ParseSyntax(context.Background(), query, StatementEntry, false)
			if err != nil {
				failures <- err.Error()
				return
			}
			if first.Source() != syntax.Source() {
				failures <- "cached source changed"
				return
			}
			failure := ""
			syntax.Walk(func(node SyntaxNode) func() {
				if name, symbol := node.Name(); symbol && node.Text() == "`a``b`" {
					if name != "a`b" {
						failure = "cached decoded identity changed"
					}
				}
				return nil
			})
			if failure != "" {
				failures <- failure
			}
		}()
	}
	workers.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ParseSyntax(ctx, query, StatementEntry, false)
	require.ErrorIs(t, err, context.Canceled)
	_, err = ParseSyntax(context.Background(), "1 ROWS", ExpressionEntry, false)
	require.Error(t, err)
}

// Graph selection is scope-neutral structure, including the established
// importing-WITH and later-WITH positions used by composite CALL bodies.
func TestStatementSyntaxScopeSelectionWindows(t *testing.T) {
	for _, query := range []string{
		"CALL { WITH x USE comp.remote RETURN x } RETURN *",
		"CALL { WITH [1] AS rows UNWIND rows AS r WITH collect(r) AS keys USE comp.remote RETURN keys } RETURN *",
		"USE comp.one RETURN 1 AS a UNION ALL USE comp.two RETURN 2 AS a",
		"CALL /* scope */ (x) { WITH 1 AS y USE comp.remote RETURN x + y AS z } RETURN z",
	} {
		t.Run(query, func(t *testing.T) {
			syntax, err := ParseSyntax(context.Background(), query, StatementEntry, true)
			require.NoError(t, err)
			uses := 0
			syntax.Walk(func(node SyntaxNode) func() {
				if node.Rule() == CypherParserRULE_useClause {
					start, end := node.Span()
					require.Equal(t, query[start:end], node.Text())
					require.True(t, strings.HasPrefix(node.Text(), "USE comp."))
					uses++
				}
				return nil
			})
			require.Positive(t, uses)
		})
	}
}
func TestStatementSyntaxWholeSourceIdentityAcrossAdapters(t *testing.T) {
	source := "WITH 1 AS uniqueCanonicalIdentity CALL { WITH uniqueCanonicalIdentity RETURN uniqueCanonicalIdentity AS value } RETURN value"
	fragment, err := ParseSyntax(context.Background(), source, FragmentEntry, false)
	require.NoError(t, err)
	statement, err := ParseSyntax(context.Background(), source, StatementEntry, false)
	require.NoError(t, err)
	require.Equal(t, CypherParserRULE_script, fragment.Root().Rule())
	require.Same(t, fragment.data, statement.data, "a fragment adapter must retain the whole statement's node identities")
	prior, err := ParseSyntax(context.Background(), "RETURN 92741 AS adapterWarmup", ClauseEntry, false)
	require.NoError(t, err)
	require.Equal(t, CypherParserRULE_subqueryBody, prior.Root().Rule())
	full, err := ParseSyntax(context.Background(), "RETURN 92741 AS adapterWarmup", FragmentEntry, false)
	require.NoError(t, err)
	require.Equal(t, CypherParserRULE_script, full.Root().Rule(), "a scope-independent adapter cannot replace a statement with a different entry's tree")
}
