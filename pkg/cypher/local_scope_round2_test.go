package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestLocalScopesAndSlicesReadThroughOneOwner: each form a reader used to
// read on its own now reads through the one owner, on every evaluator and
// check (#1227, review round 2). Expected values recorded on Neo4j 5.26.30.
func TestLocalScopesAndSlicesReadThroughOneOwner(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "local_scope_round2"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:N {id:'a', k:1})-[:R {id:'r1'}]->(:N {id:'b', k:2})-[:R {id:'r2'}]->(:N {id:'c', k:3})", nil)
		require.NoError(t, err)
		const group = "MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE "
		const bound = "MATCH (x:N {id:'a'}) MATCH (x)-[r:R]->(y) WHERE "
		for query, want := range map[string]interface{}{
			// Slice bounds (staticSliceBounds): a nested or quoted ".." is
			// the key's.
			"WITH {v:true} AS m, {`a..b`:'v'} AS r RETURN m[r['a..b']] AS v": true,
			"WITH [10,20] AS l RETURN l[head([0,1][0..1])] AS v":             int64(10),
			"WITH {v:true} AS m, ['v'] AS l RETURN m[head(l[0..1])] AS v":    true,
			"WITH {`a..b`:7} AS m RETURN m[['a..b'][0]] AS v":                int64(7),
			// Declarations (bindingDeclaration): x IN[…] and x /* … */ IN.
			group + "all(r IN[[true]] WHERE r[0]))+(t) RETURN count(p) AS c":               int64(2),
			group + "[r IN[[true]] | r[0]][0])+(t) RETURN count(p) AS c":                   int64(2),
			group + "all(r /*comment*/ IN [[true]] WHERE r[0]))+(t) RETURN count(p) AS c":  int64(2),
			bound + "all(x IN[{k:7}] WHERE x.k=7) RETURN count(*) AS c":                    int64(1),
			bound + "all(x /*comment*/ IN [{k:7}] WHERE x.k=7) RETURN count(*) AS c":       int64(1),
			"WITH 7 AS v " + bound + "all(v IN[1] WHERE v=1) AND v=7 RETURN count(*) AS c": int64(1),
			"WITH 7 AS v UNWIND [v IN[1,2] | v*10] AS u RETURN collect(u) AS c":            []interface{}{int64(10), int64(20)},
			"WITH 1 AS x RETURN all(x IN[{k:7}] WHERE x.k=7) AS v":                         true,
			// A named pattern comprehension's path is its own variable.
			"WITH 'outer' AS p " + bound + "size([p = (y)-->(z) | length(p)]) = 1 RETURN count(*) AS c":                                        int64(1),
			"WITH 'outer' AS p " + bound + "[p = (y)-->(z) | length(p)] = [1] RETURN count(*) AS c":                                            int64(1),
			"WITH 7 AS p " + bound + "size([p = (y)-->(z) | length(p)]) = 1 RETURN count(*) AS c":                                              int64(1),
			"WITH 7 AS p " + bound + "size([p = (y)-->(z) | z.id]) = 1 RETURN count(*) AS c":                                                   int64(1),
			"WITH 7 AS p " + bound + "size([p = (y)-->(z) | p]) = 1 RETURN count(*) AS c":                                                      int64(1),
			"WITH 'outer' AS p MATCH p2 = (:N {id:'a'})((x)-[r:R]->(y) WHERE size([p = (y)-->(z) | length(p)]) > 0)+(t) RETURN count(p2) AS c": int64(1),
			// A subquery's pattern sees the local, not the step's variable.
			group + "all(r IN [y] WHERE EXISTS { (r)-->() }))+(t) RETURN count(p) AS c":                               int64(1),
			group + "all(r IN [y] WHERE COUNT { (r)-->() } > 0))+(t) RETURN count(p) AS c":                            int64(1),
			group + "all(r IN [y] WHERE size(COLLECT { MATCH (r)-->(z) RETURN z.id }) > 0))+(t) RETURN count(p) AS c": int64(1),
			// A quoted group variable is the name it binds.
			"WITH {x:true} AS m MATCH p = (:N {id:'a'})((`x x` WHERE m[`x x`.id] IS NULL)-[r:R]->(y))+(t) RETURN count(p) AS c": int64(2),
			"WITH {x:true} AS m MATCH p = (:N {id:'a'})((x)-[`r r`:R WHERE m[`r r`.id] IS NULL]->(y))+(t) RETURN count(p) AS c": int64(2),
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE all(`r r` IN [[true]] WHERE `r r`[0]))+(t) RETURN count(p) AS c":      int64(2),
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		}
		result, err := exec.Execute(ctx, "WITH {v:true} AS m, {`a..b`:'v'} AS r WHERE m[r['a..b']] IS NULL RETURN 1 AS v", nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows)
		for _, query := range []string{
			// The names-only scope readers hid these: the outer x is read
			// where no local binds it.
			"UNWIND [1,2] AS x RETURN x + size(collect([x IN [1] | x])) AS v",
			"RETURN all(z IN [x] WHERE z) OR all(x IN [true] WHERE x) AS v",
			"RETURN CASE WHEN true THEN all(z IN [x] WHERE z) ELSE all(x IN [true] WHERE x) END AS v",
			"RETURN x + size([x IN [1] | x]) AS v",
			// (l) in a pattern comprehension's WHERE binds nothing.
			"MATCH (x:N {id:'a'}) RETURN [(x)-->(z) WHERE z.id IN (l) | z.id] AS v",
		} {
			requireStatementCode(t, exec, query, "Neo.ClientError.Statement.SyntaxError")
		}
	})
}

// The owners the readers go through.
func TestLocalScopeOwners(t *testing.T) {
	for text, want := range map[string][2]interface{}{
		"x IN[1]":        {"x", true},
		"x /* c */ IN l": {"x", true},
		"`x x` IN l":     {"x x", true},
		"x INDEX":        {"", false},
		"x = (a)-->(b)":  {"", false},
		"1 IN l":         {"", false},
		"x":              {"", false},
	} {
		name, _, _, ok := bindingDeclaration(text, 0)
		require.Equal(t, want, [2]interface{}{name, ok}, text)
	}
	binding, ok := localBindingAt("[p = (a)-->(b) | p]", 0)
	require.True(t, ok)
	require.Equal(t, "p", binding.names[0])
	_, ok = localBindingAt("[p =~ 'x']", 0)
	require.False(t, ok)
	_, ok = localBindingAt("reduce(a = 0, x IN l", 6)
	require.False(t, ok)
	require.False(t, localVariableShadowsAt("[x IN l] + x", 11, "x"))
	require.Equal(t, map[string]struct{}{"x": {}, "p": {}, "a": {}, "y": {}}, expressionLocalBindings("[x IN[1] | x] + [p = (n)-->() | p] + reduce(a = 0, y IN l | a) + 'all(z IN l WHERE z)'"))

	pattern := "shortestPath((s)((x)-[r:R WHERE r.id='r1']->(y))+(t))"
	_, _, _, found := nextQuantifiedGroup(pattern, 0)
	require.False(t, found)
	require.Len(t, quantifiedGroups(pattern), 1)
	require.Equal(t, []string{"x x", "y"}, quantifiedGroups("((`x x`)-->(y))+")[0].nodes)

	// The property-access check reads a local where it is bound.
	types := map[string]string{"i": "Integer"}
	require.NoError(t, checkExpressionPropertyAccesses("all(i IN [{p: 1}] WHERE i.p = 1)", types, nil))
	require.Error(t, checkExpressionPropertyAccesses("i.p = 1 AND all(i IN [{p: 1}] WHERE i.p = 1)", types, nil))
	require.NoError(t, checkExpressionPropertyAccesses("[i IN [{p: 1}] | [(n)-->(m) | i.p]]", types, nil))
	require.Error(t, checkExpressionPropertyAccesses("[j IN [1] | [(n)-->(m) | i.p]]", types, nil))
	require.Equal(t, types, newLocalReader("[i IN l | i]").withoutTypes(types, 0))
}
