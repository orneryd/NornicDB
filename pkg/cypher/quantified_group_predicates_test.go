package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A quantified group's own WHERE (its elements' inline predicates included)
// is an expression over one iteration: its variables are one node or
// relationship each, a parameter or a function call there is not a pattern
// element, and a {m,n} after the group quantifies it in every selector
// (SHORTEST k, ANY SHORTEST). Recorded on Neo4j 5.26.30 and 2026.09.
func TestQuantifiedGroupPredicates(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quantified_group_predicates"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:G {id:'a'})-[:R {kind:'x'}]->(:G {id:'b'})-[:R {kind:'y'}]->(:G {id:'c'})", nil)
	require.NoError(t, err)
	params := map[string]interface{}{"start": "a", "kinds": []interface{}{"x"}}
	for query, want := range map[string][][]interface{}{
		"MATCH (a:G) WHERE a.id = 'a' MATCH p = SHORTEST 1 (a)(()-[r:R]-()){1,6}(t:G) RETURN t.id, length(p) ORDER BY t.id":                       {{"b", int64(1)}, {"c", int64(2)}},
		"MATCH (a:G {id:'a'}) MATCH p = SHORTEST 2 (a)(()-[r:R]-()){1,6}(t:G) RETURN count(p)":                                                    {{int64(2)}},
		"MATCH p = ANY SHORTEST (a:G {id:'a'})(()-[r:R]-()){1,6}(t:G) RETURN count(p)":                                                            {{int64(2)}},
		"MATCH (s:G {id: $start}) MATCH p = SHORTEST 1 (s)(()-[r:R WHERE size($kinds) = 0 OR r.kind IN $kinds]-())+(t) RETURN t.id ORDER BY t.id": {{"b"}},
		"MATCH (s:G {id:'a'}) WITH s, [] AS kinds MATCH p = SHORTEST 1 (s)(()-[r:R WHERE size(kinds) = 0]-())+(t:G) RETURN t.id ORDER BY t.id":    {{"b"}, {"c"}},
		"MATCH (s:G {id:'a'}) MATCH p = SHORTEST 1 (s)(()-[r:R WHERE type(r) = 'R']-())+(t:G) RETURN t.id ORDER BY t.id":                          {{"b"}, {"c"}},
		"MATCH (s:G {id:'a'}) MATCH p = (s)((x WHERE size($kinds) > 0)-[r:R]-())+(t) RETURN t.id ORDER BY t.id":                                   {{"b"}, {"c"}},
		"MATCH (s:G {id:'a'}) MATCH p = (s)((x)-[r:R]-(y) WHERE x.id <> y.id AND r.kind IS NOT NULL)+(t) RETURN t.id ORDER BY t.id":               {{"b"}, {"c"}},
		"MATCH (s:G {id:'a'}) MATCH p = (s)((x)-[r:R]-(y))+(t) WHERE size(r) >= 1 RETURN t.id ORDER BY t.id":                                      {{"b"}, {"c"}},
		"MATCH (s:G {id:'a'}) MATCH p = (s)(()-[r:R WHERE r.kind + 1]-())+(t) RETURN t.id":                                                        {},
	} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		if len(want) == 0 {
			require.Empty(t, result.Rows, query)
			continue
		}
		require.Equal(t, want, result.Rows, query)
	}
	for _, query := range []string{
		"MATCH (s:G {id:'a'}) MATCH p = (s)(()-[r:R WHERE zz = 1]-())+(t) RETURN t.id",
		"MATCH (s:G {id:'a'}) MATCH p = (s)((x)-[r:R]-() WHERE type(x) = 'A')+(t) RETURN t.id",
		"MATCH (s:G {id:'a'}) MATCH p = (s)((x)-[r:R]-(y) WHERE size(r) = 1)+(t) RETURN t.id",
	} {
		_, err := exec.Execute(ctx, query, params)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
}

// quantifiedGroups finds groups inside another parenthesis, reads their
// elements' inline predicates (kept in place by the ANTLR path) and skips
// quoted text; an unbalanced pattern has no more groups.
func TestQuantifiedGroupsSpans(t *testing.T) {
	pattern := "p = shortestPath((s)((x {name: 'a)'})-[r:R WHERE r.k IN $ks]-(y WHERE size(kinds) = 0))+(t))"
	groups := quantifiedGroups(pattern)
	require.Len(t, groups, 1)
	require.Equal(t, []string{"r.k IN $ks", "size(kinds) = 0"}, groups[0].predicateTexts(pattern))
	require.ElementsMatch(t, []string{"x", "y"}, groups[0].nodes)
	require.Equal(t, []string{"r"}, groups[0].relationships)
	masked := maskQuantifiedGroupPredicates(pattern)
	require.NotContains(t, masked, "$ks")
	require.NotContains(t, masked, "kinds")
	require.Contains(t, masked, "'a)'")

	require.Empty(t, quantifiedGroups("(s)((x)-[r]-(y)"))
	require.Empty(t, elementPredicateSpans("(x)-[r WHERE", 0, len("(x)-[r WHERE")))
	require.Equal(t, "(s)((x)-[r]-(y))+(t)", maskQuantifiedGroupPredicates("(s)((x)-[r]-(y))+(t)"))
	// A parenthesised path without a quantifier is not a group.
	require.Empty(t, quantifiedGroups("((x)-[r WHERE r.k = 1]-(y))"))
	// Quoted text between elements is skipped whole.
	require.Equal(t, [][2]int{{9, 22}}, elementPredicateSpans("'(z)' (y WHERE y.k = 1)", 0, len("'(z)' (y WHERE y.k = 1)")))
}

// TestQuantifiedGroupStepVariablesAreSingletons: inside a quantified group's
// predicates (its own WHERE and its elements' inline WHERE) the group's node
// and relationship variables are one iteration's node and relationship in
// every expression form, a subscript's key included (m[r.id]), in the static
// checks and when the rows run; after the pattern they are the group's lists.
// Graph and expectations recorded on Neo4j 5.26.30 (#1211).
func TestQuantifiedGroupStepVariablesAreSingletons(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quantified_step_singletons"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:N {id:'a'})-[:R {id:'r1'}]->(:N {id:'b'})-[:R {id:'r2'}]->(:N {id:'c'})", nil)
	require.NoError(t, err)
	const head = "WITH {x: true} AS m, ['x'] AS l MATCH (s:N {id: 'a'}) "
	// Each step predicate holds for every relationship: * matches 3 paths
	// from a, + and {1,2} match 2, SHORTEST 1 one per end node (2).
	for _, step := range []string{
		"()-[r:R WHERE NOT startNode(r).id IN l]->()",
		"()-[r:R WHERE m[startNode(r).id] IS NULL]->()",
		"()-[r:R WHERE m[r.id] IS NULL]->()",
		"()-[r:R WHERE m['y'] IS NULL]->()",
		"()-[r:R WHERE l[0] = 'x']->()",
		"()-[r:R WHERE m[toString(r.id)] IS NULL]->()",
		"()-[r:R WHERE r['id'] IS NOT NULL]->()",
		"()-[r:R WHERE properties(r)['id'] IS NOT NULL]->()",
		"()-[r:R WHERE {e: r}.e.id IS NOT NULL]->()",
		"()-[r:R WHERE coalesce(m[r.id], true)]->()",
		"()-[r:R WHERE [m[r.id]][0] IS NULL]->()",
		"()-[r:R WHERE m[r.id][0] IS NULL]->()",
		"()-[r:R WHERE [x IN l WHERE x = r.id] = []]->()",
		"()-[r:R WHERE all(k IN [r.id] WHERE m[k] IS NULL)]->()",
		"()-[r:R WHERE any(k IN keys(r) WHERE m[r[k]] IS NULL)]->()",
		"()-[r:R WHERE reduce(acc = true, k IN [r.id] | acc AND m[k] IS NULL)]->()",
		"()-[r:R WHERE CASE WHEN m[r.id] IS NULL THEN true ELSE false END]->()",
		"()-[r:R WHERE CASE m[r.id] WHEN true THEN false ELSE true END]->()",
		"()-[r:R WHERE type(r) = 'R' AND m[r.id] IS NULL]->()",
		"()-[r:R WHERE EXISTS { MATCH (u)-[r]->(v) }]->()",
		"()-[r:R WHERE COUNT { MATCH (u)-[r]->(v) } = 1]->()",
		"()-[r:R WHERE EXISTS { WITH r MATCH ()-[q]->() WHERE q = r AND m[q.id] IS NULL }]->()",
		"(x WHERE m[x.id] IS NULL)-[r:R]->()",
		"(x WHERE x['id'] IS NOT NULL)-[r:R]->()",
		"(x WHERE none(k IN keys(x) WHERE m[x[k]] IS NOT NULL))-[r:R]->()",
		"(x WHERE CASE WHEN m[x.id] IS NULL THEN true END)-[r:R]->()",
		"(x WHERE COUNT { (x)-->() } >= 1)-[r:R]->()",
		"(x WHERE size([(x)-->(v) | v.id]) >= 1)-[r:R]->()",
		"(x)-[r:R]->(y) WHERE m[r.id] IS NULL",
		"(x)-[r:R]->(y) WHERE m[x.id] IS NULL AND m[y.id] IS NULL",
		"(x)-[r:R]->(y) WHERE x[r.id] IS NULL",
		"(x)-[r:R]->(y) WHERE r[x.id] IS NULL",
	} {
		for query, want := range map[string]int64{
			head + "MATCH p = (s)(" + step + ")*(t) RETURN count(p) AS c":            3,
			head + "MATCH p = (s)(" + step + ")+(t) RETURN count(p) AS c":            2,
			head + "MATCH p = (s)(" + step + "){1,2}(t) RETURN count(p) AS c":        2,
			head + "MATCH p = SHORTEST 1 (s)(" + step + ")+(t) RETURN count(p) AS c": 2,
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		}
	}
	for query, want := range map[string][][]interface{}{
		// The step predicate filters by the step's relationship or node.
		"WITH {r1: true} AS m MATCH p = (:N {id: 'a'})(()-[r:R WHERE m[r.id] IS NULL]->())+(t) RETURN count(p) AS c":                                          {{int64(0)}},
		"WITH {r2: true} AS m MATCH p = (:N {id: 'a'})(()-[r:R WHERE m[r.id] IS NULL]->())*(t) RETURN count(p) AS c":                                          {{int64(2)}},
		"WITH {b: true} AS m MATCH p = (:N {id: 'a'})((x)-[r:R]->(y WHERE m[y.id] IS NULL))+(t) RETURN count(p) AS c":                                         {{int64(0)}},
		"WITH {r1: 1, r2: 2} AS m MATCH p = (:N {id: 'a'})(()-[r:R WHERE m[r.id] > 1]->())+(t) RETURN count(p) AS c":                                          {{int64(0)}},
		"WITH {r1: 1, r2: 2} AS m MATCH p = (:N {id: 'a'})(()-[r:R WHERE m[r.id] >= 1]->()){1,2}(t) RETURN count(p) AS c":                                     {{int64(2)}},
		"WITH {x: true} AS m MATCH p = (:N {id: 'a'})(()-[r:R WHERE r.id IN keys(m)]->())*(t) RETURN count(p) AS c":                                           {{int64(1)}},
		"WITH {x: true} AS m MATCH p = (:N {id: 'a'})(()-[r:R WHERE EXISTS { MATCH (u {id: m[r.id]}) }]->())*(t) RETURN count(p) AS c":                        {{int64(1)}},
		"WITH {x: true} AS m MATCH p = (s:N {id: 'a'})(()-[r:R WHERE m[r.id] IS NULL]->()){1}(()-[q:R WHERE m[q.id] IS NULL]->()){1}(t) RETURN count(p) AS c": {{int64(1)}},
		// After the pattern the variables are the group's lists.
		head + "MATCH p = (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t) RETURN [e IN r | e.id] AS ids ORDER BY size(ids)":              {{[]interface{}{"r1"}}, {[]interface{}{"r1", "r2"}}},
		head + "MATCH p = (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t) WITH r RETURN size(r) AS n ORDER BY n":                         {{int64(1)}, {int64(2)}},
		head + "MATCH p = (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t) WHERE all(e IN r WHERE m[e.id] IS NULL) RETURN count(p) AS c":  {{int64(2)}},
		head + "MATCH p = (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t) WHERE 'r1' IN r RETURN count(p) AS c":                          {{int64(0)}},
		head + "MATCH p = (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t) RETURN m[r[0].id] AS v":                                        {{nil}, {nil}},
		head + "MATCH p = (s)((a)-[r:R]->(b) WHERE m[a.id] IS NULL AND m[r.id] IS NULL)+(t:N {id: 'c'}) RETURN [n IN b | n.id] AS b": {{[]interface{}{"b", "c"}}},
		// Other contexts: OPTIONAL MATCH, subqueries, CALL, selectors.
		head + "OPTIONAL MATCH p = (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t) RETURN count(p) AS c":                                          {{int64(2)}},
		head + "WITH s, m WHERE EXISTS { MATCH (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t) } RETURN count(*) AS c":                            {{int64(1)}},
		head + "RETURN COUNT { MATCH (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t) } AS c":                                                      {{int64(2)}},
		head + "CALL (s, m) { MATCH p = (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t) RETURN count(p) AS c } RETURN c":                          {{int64(2)}},
		head + "CALL { WITH s, m MATCH p = (s)((x)-[r:R]->(y) WHERE m[x.id] IS NULL AND m[r.id] IS NULL)+(t) RETURN count(p) AS c } RETURN c": {{int64(2)}},
		head + "MATCH p = ANY SHORTEST (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t:N {id: 'c'}) RETURN length(p) AS l":                         {{int64(2)}},
		head + "MATCH p = ALL SHORTEST (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t:N {id: 'c'}) RETURN length(p) AS l":                         {{int64(2)}},
		head + "MATCH p = SHORTEST 2 GROUPS (s)(()-[r:R WHERE m[r.id] IS NULL]->())*(t) RETURN length(p) AS l ORDER BY l":                     {{int64(0)}, {int64(1)}, {int64(2)}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
	// A parameter map keyed by the step's relationship.
	for query, want := range map[string]int64{
		"MATCH p = (:N {id: 'a'})(()-[r:R WHERE $m[r.id] IS NULL]->())+(t) RETURN count(p) AS c": 0,
		"MATCH p = (:N {id: 'a'})(()-[r:R WHERE $m[r.id] IS NULL]->())*(t) RETURN count(p) AS c": 1,
	} {
		result, err := exec.Execute(ctx, query, map[string]interface{}{"m": map[string]interface{}{"r1": true}})
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	// Each check sees the step's singletons: a list operation on one is the
	// SyntaxError Neo4j reports, as are list-only uses after the pattern.
	for _, query := range []string{
		"MATCH p = (:N {id: 'a'})(()-[r:R WHERE 'r1' IN r]->())+(t) RETURN count(p) AS c",
		"MATCH p = (:N {id: 'a'})(()-[r:R WHERE r[0] IS NULL]->())+(t) RETURN count(p) AS c",
		"MATCH p = (:N {id: 'a'})((x WHERE x[0] IS NULL)-[r:R]->())+(t) RETURN count(p) AS c",
		"WITH {x: true} AS m MATCH p = (:N {id: 'a'})(()-[r:R WHERE m[r] IS NULL]->())+(t) RETURN count(p) AS c",
		"MATCH p = (:N {id: 'a'})(()-[r:R WHERE size(r) = 1]->())+(t) RETURN count(p) AS c",
		"MATCH p = (:N {id: 'a'})(()-[r:R WHERE r + 1 = 2]->())+(t) RETURN count(p) AS c",
		"MATCH p = (:N {id: 'a'})((x WHERE COUNT { ()-[x]->() } > 0)-[r:R]->())+(t) RETURN count(p) AS c",
		"MATCH p = (:N {id: 'a'})(()-[r:R WHERE EXISTS { (r)-->() }]->())+(t) RETURN count(p) AS c",
		head + "MATCH p = (s)(()-[r:R]->())+(t) RETURN m[r.id] AS v",
		head + "MATCH p = (s)(()-[r:R]->())+(t) WHERE m[r.id] IS NULL RETURN count(p) AS c",
		head + "MATCH p = (s)((x)-[r:R]->())+(t) RETURN x.id AS v",
		head + "MATCH p = (s)(()-[r:R WHERE m[r.id] IS NULL]->())+(t) WITH r, m RETURN m[r.id] AS v",
		// The clause's own WHERE is checked with the clause's types.
		"MATCH p = (s:N {id: 'a'})((x)-[r:R]->())+(t) WHERE type(t) = 'R' RETURN count(p) AS c",
		"MATCH p = (s:N {id: 'a'})((x)-[r:R WHERE type(r) = 'R']->())+(t) WHERE type(r) = 'R' RETURN count(p) AS c",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
	}
	_, err = exec.Execute(ctx, "MATCH p = (:N {id: 'a'})(()-[r:R WHERE r.id[0] = 'r']->())+(t) RETURN count(p) AS c", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.TypeError", code)
}

// forEachParser runs body on the default parser and on ANTLR.
func forEachParser(t *testing.T, body func(t *testing.T)) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			body(t)
		})
	}
}

// requireStatementCode runs query and requires it to fail with code.
func requireStatementCode(t *testing.T, exec *StorageExecutor, query, code string) {
	t.Helper()
	_, err := exec.Execute(context.Background(), query, nil)
	require.Error(t, err, query)
	got, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, code, got, query)
}

// TestQuantifiedGroupDynamicLabels: a dynamic label or type inside a
// quantified group, in its pattern or a predicate, is read per step. Its
// value is a label, not a pattern node (the (l) of the predicate it becomes
// is not one, nor is any parenthesised variable in a group predicate), and a
// value of a type that can't name one is the SyntaxError Neo4j reports when
// it compiles the pattern, as outside a group. Recorded on Neo4j 2026.09
// (#1211).
func TestQuantifiedGroupDynamicLabels(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quantified_dynamic_labels"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:S {k:'a'})-[:R]->(:A {k:'a'})-[:R]->(:A)", nil)
		require.NoError(t, err)
		for _, query := range []string{
			"CYPHER 25 WITH 'A' AS l MATCH p = (:S)(()-->(y WHERE y:$(l)))+(t) RETURN count(p)",
			"CYPHER 25 WITH ['A'] AS l MATCH p = (:S)(()-->(y WHERE y:$any(l)))+(t) RETURN count(p)",
			"CYPHER 25 WITH ['A'] AS l MATCH p = (:S)(()-->(y WHERE y:$all(l)))+(t) RETURN count(p)",
			"CYPHER 25 WITH 'R' AS l MATCH p = (:S)(()-[r WHERE r:$(l)]->())+(t) RETURN count(p)",
			"CYPHER 25 WITH 'A' AS l MATCH p = (:S)(()-->(y:$(l)))+(t) RETURN count(p)",
			"CYPHER 25 WITH 'R' AS l MATCH p = (:S)(()-[r:$(l)]->())+(t) RETURN count(p)",
			"CYPHER 25 WITH 'A' AS l MATCH p = (:S)(()-->(y) WHERE y:$(l))+(t) RETURN count(p)",
			"CYPHER 25 WITH 'A' AS l OPTIONAL MATCH p = (:S)(()-->(y WHERE y:$(l)))+(t) RETURN count(p)",
			"CYPHER 25 WITH 'A' AS l MATCH p = SHORTEST 1 (:S)(()-->(y WHERE y:$(l)))+(t) RETURN count(p)",
			"CYPHER 25 WITH 'A' AS l MATCH (s:S) MATCH p = (s)(()-->(y WHERE y:$(l)))+(t) RETURN count(p)",
			"CYPHER 25 WITH ['a'] AS l MATCH p = (:S)((x WHERE x.k IN (l))-->(y))+(t) RETURN count(p)",
			"CYPHER 25 WITH 'a' AS l MATCH p = (:S)((x)-->(y) WHERE x.k = (l))+(t) RETURN count(p)",
			"CYPHER 25 WITH 'a' AS l OPTIONAL MATCH p = (:S)((x WHERE x.k = (l))-->(y))+(t) RETURN count(p)",
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, query)
		}
		for _, query := range []string{
			"CYPHER 25 WITH 1 AS l MATCH p = (:Missing)((x:$(l))-->(y))+(t) RETURN count(p)",
			"CYPHER 25 WITH 1 AS l MATCH p = (:Missing)(()-[r:$(l)]->())+(t) RETURN count(p)",
			"CYPHER 25 WITH 1 AS l MATCH p = (:S)(()-->(y:$any(l)))+(t) RETURN count(p)",
			"CYPHER 25 WITH [1] AS l MATCH p = (:Missing)((x:$all(l))-->(y))+(t) RETURN count(p)",
			"CYPHER 25 WITH 1 AS l MATCH p = (:S)(()-[r:$any(l)]->())+(t) RETURN count(p)",
		} {
			requireStatementCode(t, exec, query, "Neo.ClientError.Statement.SyntaxError")
		}
		// A group predicate's dynamic label is checked as one outside a
		// group is.
		for group, outside := range map[string]string{
			"CYPHER 25 WITH 1 AS l MATCH p = (:Missing)((x WHERE x:$(l))-->(y))+(t) RETURN count(p)":    "CYPHER 25 WITH 1 AS l MATCH (x:Missing WHERE x:$(l)) RETURN count(x)",
			"CYPHER 25 WITH 1 AS l MATCH p = (:Missing)((x)-->(y) WHERE x:$any(l))+(t) RETURN count(p)": "CYPHER 25 WITH 1 AS l MATCH (x:Missing)-->(y) WHERE x:$any(l) RETURN count(x)",
		} {
			_, groupErr := exec.Execute(ctx, group, nil)
			_, outsideErr := exec.Execute(ctx, outside, nil)
			groupCode, _ := nornicerrors.Neo4jStatus(groupErr)
			outsideCode, _ := nornicerrors.Neo4jStatus(outsideErr)
			require.Equal(t, outsideErr == nil, groupErr == nil, group)
			require.Equal(t, outsideCode, groupCode, group)
		}
		// A local l shadows the statement's: its value is the list's.
		for _, query := range []string{
			"CYPHER 25 WITH 1 AS l MATCH (x:Missing) WHERE all(l IN ['A'] WHERE x:$(l)) RETURN count(x)",
			"CYPHER 25 WITH 1 AS l MATCH p = (:Missing)((x WHERE all(l IN ['A'] WHERE x:$(l)))-->(y))+(t) RETURN count(p)",
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, query)
		}
	})
}

// TestStaticChecksReadLocalScopes: a variable a list comprehension,
// all / any / none / single or reduce binds shadows the clause's of the
// same name where it is bound, in a quantified group's predicates and
// anywhere else, so its subscripts and list operands are checked as the
// local value's (the subscript, list-operand and dynamic-label checks read
// localVariableShadowsAt). The same variable outside that scope is still
// the clause's. Recorded on Neo4j 5.26.30 and 2026.09.
func TestStaticChecksReadLocalScopes(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "static_local_scopes"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:N {id:'a'})-[:R {id:'r1'}]->(:N {id:'b'})-[:R {id:'r2'}]->(:N {id:'c'})", nil)
		require.NoError(t, err)
		for query, want := range map[string]interface{}{
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE all(r IN [[true]] WHERE r[0]))+(t) RETURN count(p) AS c":                       int64(2),
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE [r IN [[true]] | r[0]][0])+(t) RETURN count(p) AS c":                           int64(2),
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE all(x IN [[true]] WHERE x[0]))+(t) RETURN count(p) AS c":                       int64(2),
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE all(r IN [{id:true}] WHERE r['id']))+(t) RETURN count(p) AS c":                 int64(2),
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE all(r IN [[true]] WHERE size(r[0..1]) = 1))+(t) RETURN count(p) AS c":          int64(2),
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE reduce(r = [[true]], v IN [1] | [s IN r | s])[0][0])+(t) RETURN count(p) AS c": int64(2),
			"MATCH (x:N {id:'a'})-[r:R]->(y) WHERE all(r IN [[true]] WHERE r[0]) RETURN count(*) AS c":                                   int64(1),
			"MATCH (x:N {id:'a'})-[r:R]->(y) WHERE [r IN [[true]] | r[0]][0] RETURN count(*) AS c":                                       int64(1),
			"MATCH (x:N {id:'a'})-[r:R]->(y) WHERE all(x IN [[true]] WHERE x[0]) RETURN count(*) AS c":                                   int64(1),
			"MATCH (x:N {id:'a'})-[r:R]->(y) WHERE all(r IN [[true]] WHERE size(r[0..1]) = 1) RETURN count(*) AS c":                      int64(1),
			"MATCH (x:N {id:'a'})-[r:R]->(y) WHERE reduce(r = [[true]], v IN [1] | [s IN r | s])[0][0] RETURN count(*) AS c":             int64(1),
			"MATCH (x:N {id:'a'})-[r:R]->(y) RETURN [r IN [[true]] | r[0]] AS v":                                                         []interface{}{true},
			"MATCH (x:N {id:'a'})-[r:R]->(y) RETURN [r IN [[1]] WHERE 1 IN r] AS v":                                                      []interface{}{[]interface{}{int64(1)}},
			"MATCH (x:N {id:'a'})-[r:R]->(y) RETURN [r IN [[1]] WHERE r[0] = 1] AS v":                                                    []interface{}{[]interface{}{int64(1)}},
			"CYPHER 25 MATCH (x:N {id:'a'})-[r:R]->(y) RETURN allReduce(r = [1], v IN [1] | r + v, 1 IN r) AS v":                         true,
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		}
		for _, query := range []string{
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE r[0] IS NULL AND all(r IN [[true]] WHERE r[0]))+(t) RETURN count(p) AS c",
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE 1 IN r AND [r IN [[1]] | 1 IN r] = [true])+(t) RETURN count(p) AS c",
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE reduce(acc = [], v IN [1] | acc + v) = [1] AND 1 IN r)+(t) RETURN count(p) AS c",
			"MATCH (x:N {id:'a'})-[r:R]->(y) RETURN reduce(acc = r[0], v IN [1] | acc) AS v",
			// A function argument outside the comprehension that binds x
			// reads the clause's node.
			"MATCH (x:Q {id: 1}) RETURN toUpper(x) AS v, [x IN ['a'] | toUpper(x)] AS w",
			// x is local there, r still the step's relationship.
			"MATCH p = (:N {id:'a'})((x)-[r:R]->(y) WHERE all(x IN [1] WHERE r[0] IS NULL))+(t) RETURN count(p) AS c",
			"MATCH (x:N {id:'a'})-[r:R]->(y) WHERE all(x IN [1] WHERE r[0] IS NULL) RETURN count(*) AS c",
		} {
			requireStatementCode(t, exec, query, "Neo.ClientError.Statement.SyntaxError")
		}
	})
}

// localVariableShadowsAt reads each binding form's scope: a quoted or
// commented bracket opens none, an unbalanced reduce binds nothing, and a
// binding's list is outside its scope.
func TestLocalVariableShadowsAt(t *testing.T) {
	for text, shadowed := range map[string]bool{
		"[r IN l WHERE r[0]]":                    true,
		"[r IN l | r[0]]":                        true,
		"[r IN r[0] | 0]":                        false,
		"any(r IN l WHERE r[0])":                 true,
		"single (r IN l WHERE r[0])":             true,
		"reduce(r = [], v IN l | r[0])":          true,
		"reduce(acc = r[0], v IN l | acc)":       false,
		"allReduce(a = 0, r IN l | a, r[0] > 0)": true,
		"reduce(r = [], v IN l | 0) + r[0]":      false,
		"reduce(r = [], v IN l r[0]":             false,
		"'[r IN l | ' + r[0]":                    false,
		"/* [r IN l | */ r[0]":                   false,
		"size(r IN l WHERE r[0])":                false,
		"[x IN l | x] + r[0]":                    false,
	} {
		require.Equal(t, shadowed, localVariableShadowsAt(text, strings.LastIndex(text, "r[0]"), "r"), text)
	}
	scope := staticTypeScope{kinds: matchSemanticScope{"r": matchBindingRelationship}}
	local, shadowed := withoutLocalVariables(scope, "[r IN l | r[0]]", 11)
	require.True(t, shadowed)
	require.False(t, local.bound("r"))
	require.True(t, scope.bound("r"))
	_, shadowed = withoutLocalVariables(scope, "[x IN l | r[0]]", 11)
	require.False(t, shadowed)
}

// TestBoundRowValuesKeepLocalVariables: a MATCH whose WHERE reads variables
// earlier clauses bound has their values written into it, but not over a
// local variable of the same name (all(x IN l WHERE x.id = 7), a
// comprehension's, reduce's): that occurrence reads the local value, in a
// quantified group's step too (each step starts from the previous step's
// node). Recorded on Neo4j 5.26.30.
func TestBoundRowValuesKeepLocalVariables(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "bound_row_locals"))
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:Q {id: 1, s: 'ab'})-[:R {w: 1}]->(:Q {id: 2})-[:R {w: 2}]->(:Q {id: 3})", nil)
		require.NoError(t, err)
		for query, want := range map[string]interface{}{
			"MATCH p = (s:Q {id: 1})((x)-[e:R]->(y) WHERE all(x IN [{id: {k: true}}] WHERE x.id['k']))+(t) RETURN count(p) AS c":                int64(2),
			"MATCH p = (s:Q {id: 1})((x)-[e:R]->(y) WHERE any(x IN [{s: 'z'}] WHERE x.s = 'z'))+(t) RETURN count(p) AS c":                       int64(2),
			"MATCH p = (s:Q {id: 1})((x)-[e:R]->(y) WHERE [x IN [{id: 7}] | x.id][0] = 7)+(t) RETURN count(p) AS c":                             int64(2),
			"MATCH p = (s:Q {id: 1})((x WHERE all(x IN [{id: 7}] WHERE x.id = 7))-[e:R]->(y))+(t) RETURN count(p) AS c":                         int64(2),
			"MATCH (x:Q {id: 1}) MATCH (x)-[e:R]->(y) WHERE all(x IN [{id: 7}] WHERE x.id = 7) RETURN count(*) AS c":                            int64(1),
			"MATCH (x:Q {id: 1}) MATCH (x)-[e:R]->(y) WHERE reduce(x = 0, k IN [7] | x + k) = 7 RETURN count(*) AS c":                           int64(1),
			"MATCH (x:Q {id: 1})-[e:R]->(y) MATCH (y)-[f:R]->(z) WHERE all(y IN [{id: 7}] WHERE y.id = 7) RETURN count(*) AS c":                 int64(1),
			"MATCH (x:Q {id: 1}) OPTIONAL MATCH (x)-[e:R]->(y) WHERE all(x IN [{id: 7}] WHERE x.id = 7) RETURN count(y) AS c":                   int64(1),
			"WITH 7 AS v MATCH (x:Q {id: 1}) MATCH (x)-[e:R]->(y) WHERE all(v IN [1] WHERE v = 1) RETURN count(*) AS c":                         int64(1),
			"WITH {k: 1} AS m MATCH (x:Q {id: 1}) MATCH (x)-[e:R]->(y) WHERE all(m IN [{k: 5}] WHERE m.k = 5) AND m.k = 1 RETURN count(*) AS c": int64(1),
			"WITH 7 AS v UNWIND [v IN [1, 2] | v * 10] AS u RETURN collect(u) AS c":                                                             []interface{}{int64(10), int64(20)},
			// (l) in an element's WHERE is no pattern variable
			// (matchPatternStructure): l stays the list after the pattern.
			"WITH ['ab'] AS l MATCH p = (s:Q {id: 1})((x WHERE x.s IN (l))-[e:R]->(y))+(t) RETURN l AS v": []interface{}{"ab"},
			"WITH ['ab'] AS l MATCH (s:Q WHERE s.s IN (l))-[e:R]->(y) RETURN l AS v":                      []interface{}{"ab"},
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		}
	})
}

// The replacers leave the occurrences localOccurrences keeps: only a
// local's own.
func TestLocalOccurrences(t *testing.T) {
	text := "x.id = 1 AND all(x IN l WHERE x.id = 7)"
	require.True(t, (&localOccurrences{text: text}).keeps(text, "x", strings.LastIndex(text, "x.id")))
	require.False(t, (&localOccurrences{text: text}).keeps(text, "x", 0))
	require.False(t, (&localOccurrences{text: text}).keeps(text, "y", 0))
	require.Equal(t, "1 = 1 AND all(x IN l WHERE x.id = 7)", replaceQualifiedReferenceOutsideQuotesUnless(text, "x.id", "1", &localOccurrences{text: text}, "x"))
	require.Equal(t, "1 = 1 AND all(x IN l WHERE 1 = 7)", replaceQualifiedReferenceOutsideQuotes(text, "x.id", "1"))
	require.Equal(t, "v + [v IN l | v]", replaceIdentifierOutsideQuotesUnless("v + [v IN l | v]", "v", "v", nil))
	for text, want := range map[string]string{
		"v + [v IN l | v]":               "7 + [v IN l | v]",
		"all(v IN v WHERE v > 0)":        "all(v IN 7 WHERE v > 0)",
		"reduce(v = v, k IN l | v + k)":  "reduce(v = 7, k IN l | v + k)",
		"reduce(a = v, v IN v | a + v)":  "reduce(a = 7, v IN 7 | a + v)",
		"reduce(a = 0, k IN l | a + v)":  "reduce(a = 0, k IN l | a + 7)",
		"[v IN l WHERE v > 0 | v] + [v]": "[v IN l WHERE v > 0 | v] + [7]",
	} {
		require.Equal(t, want, replaceIdentifierOutsideQuotesUnless(text, "v", "7", &localOccurrences{text: text}), text)
	}
}

// The readers that go through the one owners: an element's inline WHERE is
// no pattern structure, a reference inside a binding's list is the clause's,
// and a CALL argument keeps its local binding.
func TestLocalScopeAndPatternStructureReaders(t *testing.T) {
	require.Equal(t, []string{"s", "t"}, extractNodeVariables(matchPatternStructure("(s WHERE s.k IN (l))-->(t) WHERE (z)")))
	require.False(t, localVariableShadowsAt("[r  IN l | r]", 2, "r"))
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_locals")
	exec := NewStorageExecutor(engine)
	node := &storage.Node{ID: "n1", Labels: []string{"Outer"}, Properties: map[string]interface{}{"x": int64(5)}}
	_, createErr := engine.CreateNode(node)
	require.NoError(t, createErr)
	require.NoError(t, RegisterUserProcedure(
		ProcedureSpec{Name: "custom.lexical_call_args", Mode: ProcedureModeRead, MinArgs: 2, MaxArgs: 2,
			Params:  []ProcedureParam{{Name: "inside", Type: "ANY"}, {Name: "outside", Type: "ANY"}},
			Returns: []ProcedureColumn{{Name: "inside", Type: "ANY"}, {Name: "outside", Type: "ANY"}}},
		func(_ context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{Columns: []string{"inside", "outside"}, Rows: [][]interface{}{args}}, nil
		}))
	t.Cleanup(ClearUserProcedures)
	result, err := exec.Execute(context.Background(), "MATCH (n:Outer) WITH n, [{x: 7}] AS l CALL custom.lexical_call_args([n IN l | n.x], n.x) YIELD inside, outside RETURN inside, outside", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{int64(7)}, int64(5)}}, result.Rows)
}
