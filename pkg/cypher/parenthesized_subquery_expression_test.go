package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestParenthesizedSubqueryExpressionsAreNotPatternMaps covers NornicDB
// issue #567: an EXISTS/COUNT/COLLECT subquery expression brace that follows
// an unclosed '(' was misclassified as a node/relationship pattern property
// map by validateStaticMapKeys, raising "pattern property maps require
// key-value entries" for queries Neo4j accepts. Correctness is asserted by
// row counts, not merely the absence of an error.
func TestParenthesizedSubqueryExpressionsAreNotPatternMaps(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornicdb567-parenexpr"))
	ctx := context.Background()

	// One WorkloadInstance with an outgoing edge, one without, plus 30 decoy
	// nodes so a full scan without the predicate would over-count.
	_, err := executor.Execute(ctx, "CREATE (a:WorkloadInstance {id: 'a'})-[:R]->(:Target)", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "CREATE (:WorkloadInstance {id: 'b'})", nil)
	require.NoError(t, err)
	for i := 0; i < 30; i++ {
		_, err = executor.Execute(ctx, "CREATE (:Decoy)", nil)
		require.NoError(t, err)
	}

	tests := []struct {
		name  string
		query string
		want  int64
	}{
		{
			name:  "parenthesized EXISTS",
			query: "MATCH (i:WorkloadInstance) WHERE (EXISTS { MATCH (i)-[:R]->() }) RETURN count(i)",
			want:  1,
		},
		{
			name:  "parenthesized COUNT comparison",
			query: "MATCH (i:WorkloadInstance) WHERE (COUNT { MATCH (i)-[:R]->() } > 0) RETURN count(i)",
			want:  1,
		},
		{
			name:  "nested parens around EXISTS",
			query: "MATCH (i:WorkloadInstance) WHERE ((EXISTS { MATCH (i)-[:R]->() })) RETURN count(i)",
			want:  1,
		},
		{
			name:  "OR combination inside parens",
			query: "MATCH (i:WorkloadInstance) WHERE (i.id = 'b' OR EXISTS { MATCH (i)-[:R]->() }) RETURN count(i)",
			want:  2,
		},
		{
			name:  "AND combination inside parens",
			query: "MATCH (i:WorkloadInstance) WHERE (i.id = 'a' AND EXISTS { MATCH (i)-[:R]->() }) RETURN count(i)",
			want:  1,
		},
		{
			name:  "downstream infraResourceScopePredicate shape",
			query: "MATCH (i:WorkloadInstance) WHERE (i.id IN ['zzz'] OR i.id IN ['zzz'] OR EXISTS { MATCH (i)-[:R]->(t:Target) WHERE (t.id IN ['zzz'] OR t.id IN ['zzz']) }) RETURN count(i)",
			want:  0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := executor.Execute(ctx, test.query, nil)
			require.NoError(t, err, test.query)
			require.Len(t, result.Rows, 1, test.query)
			require.Equal(t, test.want, result.Rows[0][0], test.query)
		})
	}
}

// TestParenthesizedCollectSubqueryIsNotAPatternMap covers the COLLECT {}
// brace of NornicDB issue #567 in its supported position (a RETURN
// projection, per TestCollectSubquery), wrapped in a paren so the '{'
// follows an unclosed '(' the way it would inside a WHERE predicate.
// Correctness is asserted by the collected values, not just no error.
func TestParenthesizedCollectSubqueryIsNotAPatternMap(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornicdb567-collect"))
	ctx := context.Background()

	_, err := executor.Execute(ctx, "CREATE (a:WorkloadInstance {id: 'a'})-[:R]->(:Target {id: 't1'})", nil)
	require.NoError(t, err)

	result, err := executor.Execute(ctx,
		"MATCH (i:WorkloadInstance {id: 'a'}) RETURN (collect { MATCH (i)-[:R]->(t) RETURN t.id }) AS targets", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, []interface{}{"t1"}, result.Rows[0][0])
}

// TestParenthesizedSubqueryExpressionRejectsGenuineBadPatternMap proves the
// fix does not weaken validateStaticMapKeys: a real invalid pattern property
// map must still be rejected, including at nested paren depth, and even when
// the offending variable or label happens to be spelled like one of the
// subquery-expression keywords (EXISTS/COUNT/COLLECT/CALL). A naive keyword
// match on the identifier before '{' would wrongly skip validation for these
// and either let the malformed map through silently or return the wrong
// rows instead of Neo4j's SyntaxError.
func TestParenthesizedSubqueryExpressionRejectsGenuineBadPatternMap(t *testing.T) {
	executor := NewStorageExecutor(storage.NewMemoryEngine())
	tests := []string{
		"MATCH (n {a}) RETURN n",
		"MATCH (n) WHERE (true AND EXISTS((m {a}))) RETURN n",
		// Variable named like a subquery keyword: the pattern element
		// '(' precedes the keyword the same way `(EXISTS { MATCH ... })`
		// does, so only the malformed map body distinguishes them.
		"MATCH (count {bad}) RETURN count",
		"MATCH (exists {bad}) RETURN exists",
		"MATCH (collect {bad}) RETURN collect",
		// Label/type named like a subquery keyword.
		"MATCH (n:Count {bad}) RETURN n",
		"MATCH (n:EXISTS {bad}) RETURN n",
		// Backtick-quoted variable named like a subquery keyword.
		"MATCH (`count` {bad}) RETURN `count`",
	}
	for _, query := range tests {
		_, err := executor.Execute(context.Background(), query, nil)
		require.Error(t, err, query)
		semanticError, ok := err.(*SemanticError)
		require.True(t, ok, "%s: err = %#v", query, err)
		require.Equal(t, "UnexpectedSyntax", semanticError.Detail, query)
	}
}

// TestMapKeyNamedLikeSubqueryKeywordIsNotConfusedWithCall proves a map with
// a property key literally spelled "call" (or the other subquery
// expression keywords) still validates and evaluates as an ordinary map,
// not a CALL subquery: the key name sits after the brace, never in the
// backward scan precedingSubqueryExpressionKeyword performs.
func TestMapKeyNamedLikeSubqueryKeywordIsNotConfusedWithCall(t *testing.T) {
	executor := NewStorageExecutor(storage.NewMemoryEngine())
	result, err := executor.Execute(context.Background(),
		"RETURN {call: 1, exists: 2, count: 3, collect: 4} AS m", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, map[string]interface{}{
		"call": int64(1), "exists": int64(2), "count": int64(3), "collect": int64(4),
	}, result.Rows[0][0])
}

// TestParenthesizedSubqueryExpressionOperatorAndPatternForms covers the
// NornicDB issue #567 shapes the issue explicitly asked for that were still
// missing regression coverage: NOT wrapping a parenthesized EXISTS, EXISTS
// as a parenthesized RETURN value, EXISTS wrapped in parens as one operand
// of an OR (rather than the whole predicate being wrapped), and the bare
// pattern-form EXISTS body (no MATCH keyword) inside parens. Correctness is
// asserted by exact rows, not merely the absence of an error.
func TestParenthesizedSubqueryExpressionOperatorAndPatternForms(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornicdb567-operatorforms"))
	ctx := context.Background()

	// One WorkloadInstance with an outgoing edge (a), one without (b).
	_, err := executor.Execute(ctx, "CREATE (a:WorkloadInstance {id: 'a'})-[:R]->(:Target)", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "CREATE (:WorkloadInstance {id: 'b'})", nil)
	require.NoError(t, err)

	t.Run("NOT wrapping a parenthesized EXISTS", func(t *testing.T) {
		query := "MATCH (i:WorkloadInstance) WHERE NOT (EXISTS { MATCH (i)-[:R]->() }) RETURN count(i)"
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, int64(1), result.Rows[0][0], query) // only b lacks the edge
	})

	t.Run("parenthesized EXISTS as a RETURN value", func(t *testing.T) {
		query := "MATCH (i:WorkloadInstance) WHERE i.id = 'a' RETURN (EXISTS { MATCH (i)-[:R]->() }) AS hasEdge"
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, true, result.Rows[0][0], query)
	})

	t.Run("x OR parenthesized EXISTS", func(t *testing.T) {
		// Only the parenthesized EXISTS is wrapped, not the whole OR
		// predicate; i.id = 'z' matches nothing.
		query := "MATCH (i:WorkloadInstance) WHERE i.id = 'z' OR (EXISTS { MATCH (i)-[:R]->() }) RETURN count(i)"
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, int64(1), result.Rows[0][0], query) // only a
	})

	t.Run("pattern-form EXISTS inside parens", func(t *testing.T) {
		query := "MATCH (i:WorkloadInstance) WHERE (EXISTS { (i)-->() }) RETURN count(i)"
		result, err := executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, int64(1), result.Rows[0][0], query) // only a
	})
}
