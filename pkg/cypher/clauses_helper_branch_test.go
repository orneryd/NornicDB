package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClauseHelpers_ParseAndRewrite(t *testing.T) {
	require.True(t, isSimplePropertyReference("n.prop"))
	require.True(t, isSimplePropertyReference("a.b.c"))
	require.False(t, isSimplePropertyReference("n..bad"))
	require.False(t, isSimplePropertyReference(""))

	in := "MATCH (a:Person) MATCH (b:Person) WHERE a.id = b.id RETURN a"
	out := rewriteTopLevelMultiMatchToCartesianMatch(in)
	require.Equal(t, "MATCH (a:Person), (b:Person) WHERE a.id = b.id RETURN a", out)
	require.Equal(t, "MATCH (a:Person) RETURN a", rewriteTopLevelMultiMatchToCartesianMatch("MATCH (a:Person) RETURN a"))

	require.True(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row MATCH (n) RETURN count(n) AS c", []interface{}{1, 2, 3}))
	require.False(t, canApplySetBasedUnwindRewrite("", []interface{}{1}))
	require.False(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row MATCH (n) RETURN count(n) AS c", []interface{}{1, 1}))
	require.False(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row MATCH (n) RETURN count(n) AS c", []interface{}{map[string]interface{}{"k": "v"}}))
	require.False(t, canApplySetBasedUnwindRewrite("UNWIND $rows AS row MATCH (n) SET n.x = 1 RETURN count(n) AS c", []interface{}{1}))
}

func TestParseSimpleCountReturnAndRelClause(t *testing.T) {
	alias, ok := parseSimpleCountReturn("RETURN count(n) AS total", "n")
	require.True(t, ok)
	require.Equal(t, "total", alias)

	alias, ok = parseSimpleCountReturn("", "n")
	require.True(t, ok)
	require.Empty(t, alias)

	_, ok = parseSimpleCountReturn("RETURN count(m) AS total", "n")
	require.False(t, ok)
	alias, ok = parseSimpleCountReturn("RETURN count(n)", "n")
	require.True(t, ok)
	require.Equal(t, "count(n)", alias)

	rel, ok := parseUnwindMergeRelationshipClause("MERGE (a)-[r:KNOWS]->(b)")
	require.True(t, ok)
	require.Equal(t, "a", rel.fromVar)
	require.Equal(t, "b", rel.toVar)
	require.Equal(t, "r", rel.relVar)
	require.Equal(t, "KNOWS", rel.relType)

	rel, ok = parseUnwindMergeRelationshipClause("MERGE (a)-[:KNOWS]->(b)")
	require.True(t, ok)
	require.Empty(t, rel.relVar)

	_, ok = parseUnwindMergeRelationshipClause("MERGE (a)<-[:KNOWS]-(b)")
	require.False(t, ok)
	_, ok = parseUnwindMergeRelationshipClause("MATCH (a)-[:KNOWS]->(b)")
	require.False(t, ok)
}
