package antlr

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatementSyntaxConsumerWindowsAndNames(t *testing.T) {
	ctx := context.Background()
	source := "/* é prefix */ CYPHER 25 EXPLAIN CALL (`a,b`) { RETURN `a,b` AS value } RETURN value;"
	syntax, err := ParseSyntax(ctx, source, StatementEntry, true)
	require.NoError(t, err)
	start, end := strings.Index(source, "CALL"), strings.LastIndex(source, ";")
	query, found := syntax.Window(start, end, CypherParserRULE_query)
	require.True(t, found, "a statement window may omit its preamble without losing the original source")
	require.Equal(t, CypherParserRULE_query, query.Rule())
	require.Equal(t, source, query.Source())
	_, found = syntax.LeadingCall()
	require.False(t, found, "a CALL after a preamble is not the source-leading token")
	var call SyntaxNode
	syntax.Walk(func(node SyntaxNode) func() {
		if node.Rule() == CypherParserRULE_callSubquery {
			call = node
		}
		return nil
	})
	require.True(t, call.Valid())
	from, to := call.Span()
	window, found := syntax.Window(from, to, CypherParserRULE_callSubquery)
	require.True(t, found)
	require.Equal(t, call.ID(), window.ID())
	first, after := window.TokenRange()
	require.Less(t, first, after)
	require.Equal(t, CypherLexerCALL, syntax.Token(first).Kind())
	parent, found := window.Parent()
	require.True(t, found)
	require.True(t, parent.Valid())
	open, found := call.DirectToken(CypherLexerLBRACE)
	require.True(t, found)
	close, found := call.DirectToken(CypherLexerRBRACE)
	require.True(t, found)
	_, bodyStart := open.Span()
	bodyEnd, _ := close.Span()
	body, found := syntax.Window(bodyStart, bodyEnd, CypherParserRULE_subqueryBody)
	require.True(t, found)
	require.Equal(t, "RETURN `a,b` AS value", body.Text())
	_, found = call.DirectToken(CypherLexerMULT)
	require.False(t, found)
	absent := call.ChildRule(CypherParserRULE_pattern)
	require.False(t, absent.Valid())
	require.False(t, absent.ChildRule(CypherParserRULE_symbol).Valid())
	_, found = syntax.Window(0, strings.Index(source, "*/")+2, CypherParserRULE_query)
	require.False(t, found, "comment text does not supply a query window")
	_, found = syntax.Window(from+1, to, CypherParserRULE_callSubquery)
	require.False(t, found, "a partial token window cannot supply a CALL frame")

	procedure, err := ParseSyntax(ctx, "/* é */ CALL `db`.`la``bels`() YIELD label RETURN label", StatementEntry, false)
	require.NoError(t, err)
	leading, found := procedure.LeadingCall()
	require.True(t, found)
	require.Equal(t, "db.la`bels", leading.ChildRule(CypherParserRULE_invocationName).QualifiedName())
	_, found = leading.DirectToken(CypherLexerLBRACE)
	require.False(t, found)

	empty, err := ParseSyntax(ctx, "CALL () { /* empty body */ }", StatementEntry, false)
	require.NoError(t, err, "structure remains readable before semantic conclusion validation")
	emptyCall, found := empty.LeadingCall()
	require.True(t, found)
	left, found := emptyCall.DirectToken(CypherLexerLBRACE)
	require.True(t, found)
	right, found := emptyCall.DirectToken(CypherLexerRBRACE)
	require.True(t, found)
	_, from = left.Span()
	to, _ = right.Span()
	emptyBody, found := empty.Window(from, to, CypherParserRULE_subqueryBody)
	require.True(t, found)
	from, to = emptyBody.Span()
	require.Equal(t, from, to)
	_, found = (StatementSyntax{}).LeadingCall()
	require.False(t, found)
}

func TestStatementSyntaxEntryViewsAndCacheIdentity(t *testing.T) {
	ctx := context.Background()
	for _, sample := range []struct {
		source string
		entry  SyntaxEntry
		rule   int
	}{
		{"CALL (x) { RETURN x AS value } IN TRANSACTIONS OF 2 ROWS", CallEntry, CypherParserRULE_callSubquery},
		{"WITH $row AS x RETURN x", ClauseEntry, CypherParserRULE_subqueryBody},
		{"coalesce(/* é */ 1, 2)", FragmentEntry, CypherParserRULE_expression},
		{"(n:$any(labels))-[r:T]->(m)", PatternEntry, CypherParserRULE_pattern},
	} {
		syntax, err := ParseSyntax(ctx, sample.source, sample.entry, false)
		require.NoError(t, err, sample.source)
		require.Equal(t, sample.rule, syntax.Root().Rule())
		require.Equal(t, sample.source, syntax.Source())
		if sample.entry == FragmentEntry {
			require.NotContains(t, syntax.Root().Code(), "é")
			_, err = ParseSyntax(ctx, sample.source, StatementEntry, false)
			require.Error(t, err, "an expression cache entry cannot authorize a statement")
		}
	}
	source := "RETURN 537 AS canonicalStatementWarmup"
	statement, err := ParseSyntax(ctx, source, StatementEntry, false)
	require.NoError(t, err)
	fragment, err := ParseSyntax(ctx, source, FragmentEntry, false)
	require.NoError(t, err)
	require.Same(t, statement.data, fragment.data)
}

func TestStatementSyntaxRetainedViewsSurviveCacheCapacity(t *testing.T) {
	ctx := context.Background()
	source := "RETURN 74019 AS retainedViewAcrossCapacity"
	retained, err := ParseSyntax(ctx, source, StatementEntry, false)
	require.NoError(t, err)
	for index := 0; index <= statementSyntaxCacheLimit; index++ {
		_, err = ParseSyntax(ctx, "RETURN "+strconv.Itoa(index)+" AS capacityContract", StatementEntry, false)
		require.NoError(t, err)
	}
	statementSyntaxCache.RLock()
	size := len(statementSyntaxCache.entries)
	statementSyntaxCache.RUnlock()
	require.LessOrEqual(t, size, statementSyntaxCacheLimit)
	require.Equal(t, source, retained.Source())
	require.Equal(t, source, retained.Root().Text())
	require.Equal(t, "RETURN", retained.Token(0).Text())
}
