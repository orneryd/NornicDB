package cypher

import (
	"context"
	"strings"
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
	_, ok = localBindingAt("reduce(a, x IN l | a)", 6)
	require.False(t, ok)
	require.Equal(t, map[string]struct{}{"y": {}}, expressionLocalBindings("/* [x IN l | x] */ [y IN l | y]"))
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

// TestLocalScopeRound3: an aggregate inside reduce stays reduce's list or
// initial value, a local in an enclosing expression gives a subquery
// pattern its own type, and a local inside an EXISTS / COUNT / COLLECT body
// may not redeclare an outer variable (it may in CALL { } and at the top
// level). Recorded on Neo4j 5.26.30 (#1227, review round 3).
func TestLocalScopeRound3(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "local_scope_round3"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:N {id:'a', k:1})-[:R {id:'r1'}]->(:N {id:'b', k:2})-[:R {id:'r2'}]->(:N {id:'c', k:3})", nil)
		require.NoError(t, err)
		for query, want := range map[string]interface{}{
			"UNWIND [1,2] AS x RETURN reduce(acc = 0, y IN collect(x) | acc + y) AS total":                                   int64(3),
			"UNWIND [1,2] AS x RETURN reduce(acc = count(*), y IN [1,2] | acc + y) AS total":                                 int64(5),
			"UNWIND [1,2] AS x WITH reduce(acc = 0, y IN collect(x) | acc + y) AS total RETURN total":                        int64(3),
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE all(r IN [y] WHERE EXISTS { (r)-->() }))+(t) RETURN count(p) AS c": int64(1),
			"WITH 7 AS p RETURN [p = (x:N)-->(z) | length(p)] AS v":                                                          []interface{}{int64(1), int64(1)},
			"WITH 7 AS x CALL (x) { RETURN [x IN [1] | x] AS v } RETURN v":                                                   []interface{}{int64(1)},
			"WITH 7 AS p CALL (p) { MATCH (x:N {id:'a'}) RETURN [p = (x)-->(z) | length(p)] AS v } RETURN v":                 []interface{}{int64(1)},
			"WITH 7 AS x RETURN COLLECT { MATCH (n:N {id:'a'}) RETURN x AS v } AS v":                                         []interface{}{int64(7)},
			// CASE inside a named comprehension sees its path; a backquoted
			// named path is one too.
			"WITH 7 AS p MATCH (x:N {id:'a'}) MATCH (x)-[r:R]->(y) WHERE [p = (y)-->(z) | CASE WHEN length(p)=1 THEN z.id ELSE 'bad' END] = ['c'] RETURN count(*) AS c": int64(1),
			"MATCH (y:N {id:'b'}) RETURN [p = (y)-->(z) | CASE WHEN length(p)=1 THEN z.id ELSE 'bad' END] AS v":                                                         []interface{}{"c"},
			"RETURN [`p p` = (a:N)-->(b:N) | length(`p p`)] AS v":                                                                                                       []interface{}{int64(1), int64(1)},
			"MATCH (y:N {id:'b'}) RETURN [`p p` = (y)-->(z) WHERE length(`p p`) = 1 | z.id] AS v":                                                                       []interface{}{"c"},
			"WITH 7 AS `p p` MATCH (x:N {id:'a'}) MATCH (x)-[r:R]->(y) WHERE size([`p p` = (y)-->(z) | length(`p p`)]) = 1 RETURN count(*) AS c":                        int64(1),
			"RETURN [`x x` IN[1, 2] | `x x` * 10] AS v":                                                                                                                 []interface{}{int64(10), int64(20)},
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		}
		for _, query := range []string{
			"UNWIND [1,2] AS x RETURN x + size(collect([x IN [1] | x])) AS v",
			"MATCH p = (:N {id:'missing'})((x)-[r:R]->(y) WHERE all(r IN [1] WHERE EXISTS { (r)-->() }))+(t) RETURN count(p) AS c",
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE all(r IN [1] WHERE EXISTS { (r)-->() }))+(t) RETURN count(p) AS c",
			"MATCH p = (:N {id:'missing'})((x)-[r:R]->(y) WHERE all(r IN [1] WHERE COUNT { (r)-->() } > 0))+(t) RETURN count(p) AS c",
			"MATCH (x:N {id:'a'})-[r:R]->(y) WHERE all(r IN ['s'] WHERE EXISTS { (r)-->() }) RETURN count(*) AS c",
			"WITH 7 AS p RETURN COLLECT { MATCH (x:N {id:'a'}) RETURN [p = (x)-->(z) | length(p)] AS v } AS v",
			"WITH 7 AS x RETURN COLLECT { MATCH (n:N {id:'a'}) RETURN [x IN [1] | x] AS v } AS v",
			"WITH 7 AS x RETURN COUNT { MATCH (n:N) WHERE all(x IN [1] WHERE x = 1) } AS v",
			"WITH 7 AS p MATCH (y:N {id:'a'}) WHERE EXISTS { MATCH (y) WHERE size([p = (y)-->(z) | p]) > 0 } RETURN count(*) AS c",
			"WITH 7 AS x RETURN COLLECT { MATCH (n:N {id:'a'}) RETURN reduce(x = 0, y IN [1] | x + y) AS v } AS v",
			"MATCH (r:N {id:'a'}) RETURN COUNT { MATCH (n:N) WHERE all(r IN [1] WHERE r = 1) } AS v",
		} {
			requireStatementCode(t, exec, query, "Neo.ClientError.Statement.SyntaxError")
		}
	})
	scope := matchSemanticScope{"r": matchBindingRelationship}
	text := "all(r IN [1] WHERE EXISTS { (r)-->() }) AND all(r IN [y] WHERE true)"
	require.Equal(t, matchBindingValue, withLocalTypes(staticTypeScope{kinds: scope}, text, strings.Index(text, "EXISTS")).kinds["r"])
	require.Equal(t, matchBindingPath, localBindingKind("[p = (a)-->(b) | p]", 17, "p", staticTypeScope{}))
	require.Equal(t, matchBindingUnknown, localBindingKind("reduce(a = b, x IN l | a)", 23, "a", staticTypeScope{}))
	require.Equal(t, matchBindingValue, localBindingKind("reduce(a = 0, x IN l | a)", 23, "a", staticTypeScope{}))
	require.Equal(t, scope, withLocalTypes(staticTypeScope{kinds: scope}, "true", 0).kinds)
	require.Equal(t, matchBindingUnknown, localBindingKind("x", 0, "x", staticTypeScope{}))
	require.Equal(t, matchBindingUnknown, localBindingKind("[x IN| x]", 7, "x", staticTypeScope{}))
	require.Equal(t, matchSemanticScope{"x": matchBindingValue}, withLocalTypes(staticTypeScope{}, "[x IN [1] | x]", 12).kinds)
}

// TestSubqueryBodiesWalkTheirOwnScope: an EXISTS / COUNT / COLLECT body and
// a CALL body are walked by the one clause walk from the variables they
// see: a CALL body from its imports, an expression body from the
// enclosing query's variables and its own MATCH and WITH bindings; a local
// is typed with the locals around it resolved first. Recorded on Neo4j
// 5.26.30 (#1227, review round 4).
func TestSubqueryBodiesWalkTheirOwnScope(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subquery_body_scope"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:N {id:'a', k:1})-[:R {id:'r1'}]->(:N {id:'b', k:2})-[:R {id:'r2'}]->(:N {id:'c', k:3})", nil)
		require.NoError(t, err)
		for query, want := range map[string]interface{}{
			"WITH 7 AS x RETURN COLLECT { CALL (x) { RETURN [x IN [1] | x] AS v } RETURN v AS v } AS v":                              []interface{}{[]interface{}{int64(1)}},
			"WITH 7 AS x RETURN COUNT { CALL (x) { RETURN [x IN [1] | x] AS v } RETURN v } AS v":                                     int64(1),
			"WITH 7 AS x RETURN EXISTS { CALL (x) { RETURN [x IN [1] | x] AS v } RETURN v } AS v":                                    true,
			"WITH 7 AS x RETURN COLLECT { CALL () { RETURN [x IN [1] | x] AS v } RETURN v AS v } AS v":                               []interface{}{[]interface{}{int64(1)}},
			"WITH 7 AS x RETURN COLLECT { CALL { WITH x RETURN [x IN [1] | x] AS v } RETURN v AS v } AS v":                           []interface{}{[]interface{}{int64(1)}},
			"WITH 7 AS x CALL () { RETURN COLLECT { RETURN [x IN [1] | x] AS v } AS v } RETURN v":                                    []interface{}{[]interface{}{int64(1)}},
			"MATCH (a:N {id:'a'})-[x:R]->(y) WHERE all(x IN [y] WHERE all(r IN [x] WHERE EXISTS { (r)-->() })) RETURN count(*) AS v": int64(1),
			"RETURN COUNT { MATCH (x:N) WHERE EXISTS { MATCH (x)-->(n) } } AS v":                                                     int64(2),
			"RETURN COUNT { UNWIND [1, 2] AS x WITH x WHERE EXISTS { WITH x AS y RETURN y } } AS v":                                  int64(2),
			"WITH 1 AS g RETURN COLLECT { UNWIND [1] AS x WITH count(*) AS c RETURN c + g } AS v":                                    []interface{}{int64(2)},
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		}
		for _, query := range []string{
			"WITH 7 AS x CALL (x) { RETURN COLLECT { RETURN [x IN [1] | x] AS v } AS v } RETURN v",
			"MATCH (r:N {id:'missing'}) WHERE all(x IN [1] WHERE all(r IN [x] WHERE EXISTS { (r)-->() })) RETURN count(*) AS v",
			"RETURN COUNT { WITH 7 AS x WHERE EXISTS { WITH [x IN [1] | x] AS v RETURN v } } AS v",
			"RETURN COUNT { MATCH (x:N {id:'missing'}) WHERE EXISTS { MATCH (n) WHERE all(x IN [1] WHERE x = 1) } } AS v",
		} {
			requireStatementCode(t, exec, query, "Neo.ClientError.Statement.SyntaxError")
		}
	})
	require.Equal(t, "COUNT {     } + CALL (x) {        }", maskNestedScopeBodies("COUNT { (n) } + CALL (x) { RETURN }"))
}
