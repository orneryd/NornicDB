package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Raw-owner checks accompany the public matrix because Execute may remove
// comments before preflight; both consumers must agree on token boundaries.
func TestMapEntrySyntaxLexicalOwners(t *testing.T) {
	for _, query := range []string{
		"CALL/* comment */(){ CREATE (n {k:1}) RETURN n.k AS k } RETURN k",
		"CALL// comment\n(){ CREATE (n {k:1}) }",
		"RETURN { /* empty */ }",
		"RETURN {`ca``ll`/*key*/:/*value*/COUNT/*body*/{ RETURN 1 }}",
		"RETURN {k: COLLECT { RETURN 1 AS x }}",
		"WITH {} AS m RETURN m { /*dot*/ . /*name*/ `a b`, . /*all*/ * }",
		"CREATE/*ddl*/CONSTRAINT c FOR (n:X) REQUIRE { n.x IS NOT NULL }",
		"CREATE/*ddl*/RANGE INDEX i FOR (n:X) ON (n.x) OPTIONS {indexProvider:'range-1.0'}",
		"MATCH ((a)-[r]->(b)){1,2} RETURN a",
		"RETURN 'CALL /* comment */ () { bad }'",
	} {
		require.NoError(t, validateStaticMapKeys(query), query)
	}
	for _, query := range []string{
		"RETURN {1:2}", "RETURN {a b:2}", "RETURN {a:1,}",
		"MATCH (n /*gap*/ {bad}) RETURN n", "MATCH (n { . /*gap*/ key }) RETURN n",
		"CALL/*comment*/(){ CREATE (n {bad}) RETURN n } RETURN n",
		"RETURN {k:COUNT/*body*/{ CREATE (n {bad}) RETURN n }}",
	} {
		require.Error(t, validateStaticMapKeys(query), query)
	}
	// Import-list history must survive more nesting than the inline stack.
	deep := "CALL (" + strings.Repeat("(", 20) + "x" + strings.Repeat(")", 20) + ") /*gap*/ { RETURN x }"
	brace := strings.IndexByte(deep, '{')
	require.True(t, precedingSubqueryExpressionKeyword(deep, brace))
	for _, query := range []string{"RETURN COUNT", "RETURN COUNT {", "RETURN COUNT {`RETURN`:1}", "RETURN a.COUNT { RETURN 1 }", "MATCH (n:COUNT {x:1})"} {
		if brace := strings.IndexByte(query, '{'); brace >= 0 {
			require.False(t, precedingSubqueryExpressionKeyword(query, brace), query)
		}
	}
	// A valid public statement still runs when the raw classifier sees comments.
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "lexical_owner"))
	result, err := executor.Execute(context.Background(), "CALL /* comment */ () { CREATE (n:MapEntryReview {k:1}) RETURN n.k AS k } RETURN k", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}
