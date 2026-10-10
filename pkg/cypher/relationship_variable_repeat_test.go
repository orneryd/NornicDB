package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A relationship variable named twice in one MATCH (#907). Without a match
// mode written it is the openCypher TCK's SyntaxError (Match3 [29]). With
// DIFFERENT RELATIONSHIPS written the clause matches nothing, as in Neo4j
// 5.26 and 2026.09: no relationship appears twice in a MATCH. A later MATCH
// joins on it; a relationship and a list under one name is a type mismatch.
func TestRepeatedRelationshipVariable(t *testing.T) {
	exec, ctx := newPathSelectorExecutor(t)
	l := func(values ...interface{}) []interface{} { return values }
	for _, query := range []string{
		"MATCH (a)-[r]->()-[r]->(a) RETURN r",
		"MATCH (a:SP)-[r]->(b), (c)-[r]->(d) RETURN count(*) AS c",
		"OPTIONAL MATCH (a:SP)-[r]->(b), (c)-[r]->(d) RETURN count(*) AS c",
		"MATCH (a:SP {id: 1})-[r]->(b) WHERE EXISTS { MATCH (c)-[r]->(d), (e)-[r]->(f) } RETURN count(*) AS c",
		"MATCH (a:SP)-[r*1..2]->(b), (c)-[r*1..2]->(d) RETURN count(*) AS c",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.ErrorContains(t, err, "relationship variable r is used more than once in the same pattern", query)
		var classified *classifiedCypherError
		require.ErrorAs(t, err, &classified, query)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.code, query)
	}
	for query, want := range map[string][][]interface{}{
		"MATCH DIFFERENT RELATIONSHIPS (a:SP)-[r]->(b)-[r]->(c) RETURN count(*) AS c":                                                  {l(int64(0))},
		"MATCH DIFFERENT RELATIONSHIPS (a:SP)-[r]->(b)<-[r]-(c) RETURN count(*) AS c":                                                  {l(int64(0))},
		"MATCH DIFFERENT RELATIONSHIPS (a:SP)-[r]->(b), (c)-[r]->(d) RETURN count(*) AS c":                                             {l(int64(0))},
		"MATCH DIFFERENT RELATIONSHIPS (a:SP)-[r]->(b), (a)-[r]->(b) RETURN count(*) AS c":                                             {l(int64(0))},
		"MATCH DIFFERENT RELATIONSHIPS (a:SP)-[r]-(b), (b)-[r]-(a) RETURN count(*) AS c":                                               {l(int64(0))},
		"MATCH DIFFERENT RELATIONSHIPS (a:SP)-[r*1..2]->(b), (c)-[r*1..2]->(d) RETURN count(*) AS c":                                   {l(int64(0))},
		"MATCH DIFFERENT RELATIONSHIPS (a:SP)-[r]->+(b), (c)-[r]->+(d) RETURN count(*) AS c":                                           {l(int64(0))},
		"MATCH (a:SP)-[r]->(b) MATCH (c)-[r]->(d) RETURN count(*) AS c":                                                                {l(int64(6))},
		"OPTIONAL MATCH DIFFERENT RELATIONSHIPS (a:SP {id: 1})-[r]->(b), (c)-[r]->(d) RETURN count(*) AS c, count(a) AS ca":            {l(int64(1), int64(0))},
		"MATCH (x:SP {id: 1}) OPTIONAL MATCH DIFFERENT RELATIONSHIPS (x)-[r]->(b), (c)-[r]->(d) RETURN x.id AS x, r IS NULL AS n":      {l(int64(1), true)},
		"MATCH DIFFERENT RELATIONSHIPS p = (a:SP)-[r]->(b), q = (c)-[r]->(d) RETURN count(*) AS c":                                     {l(int64(0))},
		"MATCH DIFFERENT RELATIONSHIPS (a:SP)-[r:T]->(b), ()-[r:T WHERE r.w IS NULL]->() RETURN count(*) AS c":                         {l(int64(0))},
		"MATCH (a:SP {id: 1})-[r]->(b) WHERE EXISTS { MATCH DIFFERENT RELATIONSHIPS (c)-[r]->(d), (e)-[r]->(f) } RETURN count(*) AS c": {l(int64(0))},
		"MATCH DIFFERENT RELATIONSHIPS (a:SP)-[r]->(b), (c)-[r]->(d) WHERE a.id = 99 OR a.id = 1 RETURN count(*) AS c":                 {l(int64(0))},
		"MATCH (a:SP)-[IS T]->(b), (c)-[IS T]->(d) RETURN count(*) AS c":                                                               {l(int64(30))},
		"MATCH (a:SP {id: 1})-[r]->(b), (c:SP {id: 2})-[s]->(d) RETURN count(*) AS c":                                                  {l(int64(2))},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
	// Over a self-loop: no rows under DIFFERENT RELATIONSHIPS, the loop under
	// REPEATABLE ELEMENTS (Neo4j 2026.09).
	_, err := exec.Execute(ctx, "CREATE (a:Loop)-[:R]->(a)", nil)
	require.NoError(t, err)
	for query, want := range map[string]int64{
		"MATCH DIFFERENT RELATIONSHIPS (a:Loop)-[r]->()-[r]->(a) RETURN count(*) AS c": 0,
		"MATCH REPEATABLE ELEMENTS (a:Loop)-[r]->()-[r]->(a) RETURN count(*) AS c":     1,
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	result, err := exec.Execute(ctx, "MATCH DIFFERENT RELATIONSHIPS (a:SP)-[r]->(b), (c)-[r]->(d) RETURN *", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "c", "d", "r"}, result.Columns)
	require.Empty(t, result.Rows)

	_, err = exec.Execute(ctx, "MATCH (a:SP)-[r]->(b), (c)-[r*1..2]->(d) RETURN count(*) AS c", nil)
	require.ErrorContains(t, err, "Type mismatch: r defined with conflicting type Relationship (expected List<Relationship>)")
	_, err = exec.Execute(ctx, "MATCH (a:SP)-[r]->+(b), (c)-[r]->(d) RETURN count(*) AS c", nil)
	require.ErrorContains(t, err, "Type mismatch: r defined with conflicting type List<Relationship> (expected Relationship)")

	rewritten, _, err := desugarLabelExpressions("MATCH DIFFERENT RELATIONSHIPS (a)-[r]->(b), (c)-[r {k: [1]}]->(d) RETURN r")
	require.NoError(t, err)
	require.Equal(t, "MATCH  (a)-[r]->(b), (c)-[__nornic_lx0 {k: [1]}]->(d) WHERE r = __nornic_lx0 RETURN r", rewritten)
	require.True(t, mayRepeatRelationshipVariable("MATCH ()-[r]->(), ()- [r]->() RETURN r"))
	require.False(t, mayRepeatRelationshipVariable("MATCH ()-[r]->(), ()-[s]->() RETURN [r, 'x']"))
	rewritten, _, err = desugarLabelExpressions("MATCH ()-[r]->() RETURN 'x-[r]' AS s")
	require.NoError(t, err)
	require.Equal(t, "MATCH ()-[r]->() RETURN 'x-[r]' AS s", rewritten, "a bracket in a string is a quick-check yes the rewrite reads as text")
	require.False(t, mayRepeatRelationshipVariable("MATCH ()-[:T]->(), ()-[IS T]->() RETURN 1"))
	many := "MATCH "
	for i := 0; i < 17; i++ {
		many += "()-[r" + string(rune('a'+i)) + "]->(), "
	}
	require.True(t, mayRepeatRelationshipVariable(many+"() RETURN 1"), "past its table the check answers true")
	predicates, err := (&labelExpressionRewriter{query: "(a)-[r"}).repeatedRelationshipVariables(0, len("(a)-[r"), false)
	require.NoError(t, err)
	require.Empty(t, predicates, "an unclosed bracket is left to the parser")
}
