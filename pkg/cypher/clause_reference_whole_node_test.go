package cypher

// N152-264 (report C64): the clause reference analysis skipped whole-node
// variable occurrences in labelled patterns and in keyword-like variables, so
// a projected label scan or traversal endpoint delivered a partial node. Reads
// of that node's properties then answered from an empty or narrowed image, and
// a write through it erased the stored properties the statement had not read.
//
// The reference walker (forEachSemanticExpressionReference) is the single owner
// of "which variables does this clause text read". Its two classification
// rules now keep the whole-node occurrence: a name before a label's ':' inside
// a node pattern, and a keyword-shaped name standing where only an operand can.
// These tests pin the reported read and write answers on the executor, which is
// the shared path behind the report's HTTP and Bolt verifier.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func n152264NewExecutor(t *testing.T, schema ...string) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "n152264"))
	for _, statement := range schema {
		_, err := exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err, statement)
	}
	return exec
}

func n152264Run(t *testing.T, exec *StorageExecutor, statement string) [][]interface{} {
	t.Helper()
	result, err := exec.Execute(context.Background(), statement, nil)
	require.NoError(t, err, statement)
	return result.Rows
}

func n152264Must(t *testing.T, exec *StorageExecutor, statement string) {
	t.Helper()
	_, err := exec.Execute(context.Background(), statement, nil)
	require.NoError(t, err, statement)
}

func n152264DocFixture(t *testing.T, exec *StorageExecutor) {
	t.Helper()
	n152264Must(t, exec, "CREATE (a:Doc {key:'one', path:'guide', status:'current', extra:7, body:'body one'})-[:REL {w:1}]->(b:Doc {key:'two', path:'second', status:'old', extra:8, body:'body two'}), (b)-[:BACK]->(a)")
	n152264Must(t, exec, "CREATE (:File {id:1, restricted:true, owner:'ann', body:'restricted text'}), (:File {id:2, restricted:false, owner:'bob', body:'public text'})")
}

// The report's read claims: a keyword-named endpoint, a scan, and a later
// labelled pattern or EXISTS predicate see the whole stored node.
func TestClauseWholeNodeReadsForKeywordAndLabelledPatternReferences(t *testing.T) {
	one := map[string]interface{}{"body": "body one", "extra": int64(7), "key": "one", "path": "guide", "status": "current"}
	two := map[string]interface{}{"body": "body two", "extra": int64(8), "key": "two", "path": "second", "status": "old"}
	for _, indexed := range []bool{false, true} {
		var schema []string
		if indexed {
			schema = []string{
				"CREATE CONSTRAINT n152264_doc_key FOR (n:Doc) REQUIRE n.key IS UNIQUE",
				"CREATE CONSTRAINT n152264_file_id FOR (n:File) REQUIRE n.id IS UNIQUE",
			}
		}
		exec := n152264NewExecutor(t, schema...)
		n152264DocFixture(t, exec)
		for _, tc := range []struct {
			claim     string
			statement string
			want      [][]interface{}
		}{
			// A traversal endpoint named `end`, read as a value.
			{"endpoint-keyword-properties", "MATCH (start:Doc {key:'one'})-[:REL]->(end) RETURN properties(end) AS p", [][]interface{}{{two}}},
			{"endpoint-keyword-properties-with-property", "MATCH (start:Doc {key:'one'})-[:REL]->(end) RETURN properties(end) AS p, end.extra AS x", [][]interface{}{{two, int64(8)}}},
			{"endpoint-keyword-both-ends", "MATCH (start:Doc)-[:REL]->(end) RETURN properties(end) AS p, properties(start) AS q", [][]interface{}{{two, one}}},
			{"endpoint-keyword-key-count", "MATCH (start:Doc)-[:REL]->(end) RETURN size(keys(end)) AS n", [][]interface{}{{int64(5)}}},
			{"endpoint-keyword-coalesce", "MATCH (start:Doc)-[:REL]->(end) RETURN coalesce(end, null).body AS b", [][]interface{}{{"body two"}}},
			{"endpoint-keyword-case-value", "MATCH (start:Doc)-[:REL]->(end) RETURN properties(CASE WHEN true THEN end ELSE null END) AS p", [][]interface{}{{two}}},
			{"endpoint-control-ordinary-name", "MATCH (a:Doc {key:'one'})-[:REL]->(m) RETURN properties(m) AS p", [][]interface{}{{two}}},
			// A later labelled pattern reads the row's node as a value.
			{"endpoint-labelled-rematch", "MATCH (a:Doc {key:'one'})-[:REL]->(m) MATCH (m:Doc {extra:8}) RETURN m.key", [][]interface{}{{"two"}}},
			{"endpoint-labelled-not-exists", "MATCH (a:Doc {key:'one'})-[:REL]->(m) WHERE NOT EXISTS { (m:Doc {extra:8})-[:BACK]->() } RETURN m.key", [][]interface{}{}},
			{"endpoint-labelled-path-nodes", "MATCH (a)-[:REL]->(m) MATCH p=(m:Doc)-[:BACK]->(x) RETURN [y IN nodes(p) | y.extra] AS l", [][]interface{}{{[]interface{}{int64(8), int64(7)}}}},
			{"endpoint-control-unlabelled-rematch", "MATCH (a:Doc {key:'one'})-[:REL]->(m) MATCH (m {extra:8}) RETURN m.key", [][]interface{}{{"two"}}},
			{"endpoint-control-same-pattern-where", "MATCH (a:Doc {key:'one'})-[:REL]->(m) MATCH (m:Doc) WHERE m.extra = 8 RETURN m.key", [][]interface{}{{"two"}}},
			// A label scan whose variable is keyword-named.
			{"scan-keyword-properties", "MATCH (end:Doc) WHERE end.status = 'old' RETURN properties(end) AS p", [][]interface{}{{two}}},
			{"scan-keyword-key-count", "MATCH (end:Doc) WHERE end.status = 'old' RETURN [end.key, size(keys(end))] AS p", [][]interface{}{{[]interface{}{"two", int64(5)}}}},
			// The scan's variable is read by a later labelled pattern or EXISTS.
			{"scan-labelled-not-exists", "MATCH (f:File) WHERE NOT EXISTS { (f:File {restricted:true}) } RETURN f.body", [][]interface{}{{"public text"}}},
			{"scan-labelled-not-exists-seeded", "MATCH (f:File {id:1}) WHERE NOT EXISTS { (f:File {restricted:true}) } RETURN f.body", [][]interface{}{}},
			{"scan-labelled-exists", "MATCH (f:File {id:1}) WHERE EXISTS { (f:File {owner:'ann'}) } RETURN f.body", [][]interface{}{{"restricted text"}}},
			{"scan-labelled-rematch", "MATCH (n:Doc) WHERE n.status = 'current' MATCH (n:Doc {extra:7}) RETURN n.key", [][]interface{}{{"one"}}},
			{"scan-control-unlabelled-not-exists", "MATCH (f:File) WHERE NOT EXISTS { MATCH (f) WHERE f.restricted = true } RETURN f.body", [][]interface{}{{"public text"}}},
		} {
			got := n152264Run(t, exec, tc.statement)
			require.Equal(t, tc.want, got, "%s (indexed=%t): %s", tc.claim, indexed, tc.statement)
		}
	}
}

// The report's write claims: a node written through a projected endpoint or
// scan keeps every stored property it does not name, and a wrong row driven by
// a partial image deletes the wrong node.
func TestClauseWholeNodeWritesForKeywordAndLabelledPatternReferences(t *testing.T) {
	untouchedSource := []interface{}{"s", map[string]interface{}{"body": "bs", "key": "s", "name": "S"}}
	untouchedTarget := []interface{}{"t", map[string]interface{}{"body": "bt", "key": "t", "name": "T"}}
	flaggedSource := []interface{}{"s", map[string]interface{}{"body": "bs", "flag": int64(1), "key": "s", "name": "S"}}
	flaggedTarget := []interface{}{"t", map[string]interface{}{"body": "bt", "flag": int64(1), "key": "t", "name": "T"}}
	both := [][]interface{}{untouchedSource, untouchedTarget}
	for _, tc := range []struct {
		claim     string
		statement string
		want      [][]interface{}
	}{
		{"scan-keyword", "CALL () { MATCH (end:W {key:'s'}) RETURN coalesce(end, null) AS x } SET x.flag = 1", [][]interface{}{flaggedSource, untouchedTarget}},
		{"scan-labelled-again", "CALL () { MATCH (n:W {key:'s'}) MATCH (n:W)-[:WREL*0..1]->(m) WITH m WHERE m.key = 's' RETURN m AS x } SET x.flag = 1", [][]interface{}{flaggedSource, untouchedTarget}},
		{"endpoint-keyword-start", "CALL () { MATCH (end)-[:WREL]->(m:W {key:'t'}) RETURN coalesce(end, null) AS x } SET x.flag = 1", [][]interface{}{flaggedSource, untouchedTarget}},
		{"endpoint-keyword-start-plain", "CALL () { MATCH (end)-[:WREL]->(m) RETURN coalesce(end, null) AS x } SET x.flag = 1", [][]interface{}{flaggedSource, untouchedTarget}},
		{"endpoint-keyword-end", "CALL () { MATCH (start:W {key:'s'})-[:WREL]->(end) RETURN coalesce(end, null) AS x } SET x.flag = 1", [][]interface{}{untouchedSource, flaggedTarget}},
		{"control-ordinary-name", "CALL () { MATCH (n:W {key:'s'}) RETURN coalesce(n, null) AS x } SET x.flag = 1", [][]interface{}{flaggedSource, untouchedTarget}},
		{"labelled-exists-drives-delete", "CALL () { MATCH (n:W) WHERE NOT EXISTS { (n:W {name:'S'}) } RETURN n.key AS k } MATCH (x:W {key:k}) DETACH DELETE x", [][]interface{}{untouchedSource}},
		{"control-wrong-row", "CALL () { MATCH (n:W) WHERE NOT n.name = 'S' RETURN n.key AS k } MATCH (x:W {key:k}) DETACH DELETE x", [][]interface{}{untouchedSource}},
	} {
		exec := n152264NewExecutor(t)
		n152264Must(t, exec, "CREATE (:W {key:'s', name:'S', body:'bs'})-[:WREL]->(:W {key:'t', name:'T', body:'bt'})")
		n152264Must(t, exec, tc.statement)
		got := n152264Run(t, exec, "MATCH (n:W) RETURN n.key AS k, properties(n) AS p ORDER BY k")
		require.Equal(t, tc.want, got, "%s: %s", tc.claim, tc.statement)
	}
	// The baseline both control tables expect, so a fixture change cannot make
	// them agree vacuously.
	exec := n152264NewExecutor(t)
	n152264Must(t, exec, "CREATE (:W {key:'s', name:'S', body:'bs'})-[:WREL]->(:W {key:'t', name:'T', body:'bt'})")
	require.Equal(t, both, n152264Run(t, exec, "MATCH (n:W) RETURN n.key AS k, properties(n) AS p ORDER BY k"))
}

// Keyword-like variables the statement binds are read as the variable: every
// word the reference analysis treats as an operator or expression keyword also
// works as a variable name here, so a whole-node read of one is that variable.
func TestKeywordNamedVariablesReadTheWholeNode(t *testing.T) {
	for _, name := range []string{
		"end", "start", "when", "then", "else", "case", "in", "not", "and", "or", "is",
		"contains", "starts", "ends", "with", "where", "limit", "skip", "order", "asc", "desc", "xor",
	} {
		t.Run(name, func(t *testing.T) {
			exec := n152264NewExecutor(t)
			n152264Must(t, exec, "CREATE (:K {key:'a', name:'v', other:2})")
			want := [][]interface{}{{map[string]interface{}{"key": "a", "name": "v", "other": int64(2)}}}
			require.Equal(t, want, n152264Run(t, exec, "MATCH ("+name+":K {key:'a'}) RETURN properties("+name+") AS p"))
			// The same variable read by a later labelled pattern.
			require.Equal(t, want, n152264Run(t, exec, "MATCH ("+name+":K {key:'a'}) MATCH ("+name+":K {other:2}) RETURN properties("+name+") AS p"))
		})
	}
}

// Controls: the same words in their keyword positions, and map keys, stay
// keywords and keys, so a narrow property demand is still narrow.
func TestKeywordTokensAreNotVariableReferences(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		claim      string
		variable   string
		where      string
		query      string
		properties []string
	}{
		{"and", "", "p.a = 1 AND p.b = 2", "MATCH (p:P) WHERE p.a = 1 AND p.b = 2 RETURN p.a", []string{"a", "b"}},
		{"or", "", "p.a = 1 OR p.b = 2", "MATCH (p:P) WHERE p.a = 1 OR p.b = 2 RETURN p.a", []string{"a", "b"}},
		{"xor", "", "p.a = 1 XOR p.b = 2", "MATCH (p:P) WHERE p.a = 1 XOR p.b = 2 RETURN p.a", []string{"a", "b"}},
		{"not", "", "NOT p.b = 2", "MATCH (p:P) WHERE NOT p.b = 2 RETURN p.a", []string{"a", "b"}},
		{"is-not-null", "", "p.a IS NOT NULL", "MATCH (p:P) WHERE p.a IS NOT NULL RETURN p.b", []string{"a", "b"}},
		{"in", "", "p.a IN [1, 2]", "MATCH (p:P) WHERE p.a IN [1, 2] RETURN p.a", []string{"a"}},
		{"starts-with", "", "p.a STARTS WITH 'x'", "MATCH (p:P) WHERE p.a STARTS WITH 'x' RETURN p.a", []string{"a"}},
		{"ends-with", "", "p.a ENDS WITH 'x'", "MATCH (p:P) WHERE p.a ENDS WITH 'x' RETURN p.a", []string{"a"}},
		{"contains", "", "p.a CONTAINS 'x'", "MATCH (p:P) WHERE p.a CONTAINS 'x' RETURN p.a", []string{"a"}},
		{"case-when-then-else-end", "", "CASE WHEN p.a = 1 THEN true ELSE false END", "MATCH (p:P) WHERE CASE WHEN p.a = 1 THEN true ELSE false END RETURN p.a", []string{"a"}},
		{"order-by-desc", "", "", "MATCH (p:P) RETURN p.a AS a ORDER BY a DESC", []string{"a"}},
		// A map key spelled like the variable is a key, not a read of it.
		{"map-key-of-variable-name", "", "", "MATCH (p:P) RETURN {p: p.a} AS m", []string{"a"}},
		{"map-key-of-keyword-name", "end", "", "MATCH (end:P) RETURN {end: end.a} AS m", []string{"a"}},
	} {
		variable := tc.variable
		if variable == "" {
			variable = "p"
		}
		projected := nodePatternInfo{variable: variable, labels: []string{"P"}}
		properties, ok := pipelineLabelScanProjection(ctx, projected, tc.where, readTailOf(t, tc.query))
		require.True(t, ok, "%s: %s", tc.claim, tc.query)
		require.Equal(t, tc.properties, properties, "%s: %s", tc.claim, tc.query)
	}

	// The reported whole-node occurrences stay whole: a keyword-named variable
	// returned as a value, and a variable read by a later labelled pattern or
	// an EXISTS predicate.
	for _, tc := range []struct{ claim, variable, where, query string }{
		{"keyword-variable-as-value", "end", "", "MATCH (end:P) RETURN [end] AS l"},
		{"keyword-variable-returned", "end", "", "MATCH (end:P) RETURN end"},
		{"keyword-variable-later-pattern", "end", "", "MATCH (end:P) MATCH (end)-->(q) RETURN q.a"},
		{"later-labelled-pattern", "p", "", "MATCH (p:P) MATCH (p:Q {k: 1}) RETURN p.a"},
		{"exists-labelled-pattern", "p", "EXISTS { (p:Q {k: 1}) }", "MATCH (p:P) WHERE EXISTS { (p:Q {k: 1}) } RETURN p.a"},
	} {
		_, ok := pipelineLabelScanProjection(ctx, nodePatternInfo{variable: tc.variable, labels: []string{"P"}}, tc.where, readTailOf(t, tc.query))
		require.False(t, ok, "%s: %s", tc.claim, tc.query)
	}
}

// The reference walker's two classification rules, at the unit level: a label
// follows a pattern variable and a map key precedes its value; a keyword-shaped
// word is a variable where only an operand can stand and a keyword elsewhere.
func TestSemanticReferencesKeepWholeNodeOccurrences(t *testing.T) {
	type reference struct {
		variable, property string
		hasProperty        bool
	}
	for _, tc := range []struct {
		expression string
		want       []reference
	}{
		{"properties(end)", []reference{{"end", "", false}}},
		{"coalesce(end, null)", []reference{{"end", "", false}}},
		{"(end)", []reference{{"end", "", false}}},
		{"end", []reference{{"end", "", false}}},
		{"MATCH (m:Doc {extra:8})", []reference{{"m", "", false}, {"Doc", "", false}}},
		{"EXISTS { (f:File {restricted:true}) }", []reference{{"EXISTS", "", false}, {"f", "", false}, {"File", "", false}}},
		{"{name: n.name}", []reference{{"n", "name", true}}},
		{"x AND y", []reference{{"x", "", false}, {"y", "", false}}},
		{"x IS NOT NULL", []reference{{"x", "", false}}},
		{"NOT x", []reference{{"x", "", false}}},
		{"CASE WHEN x = 1 THEN y ELSE z END", []reference{{"x", "", false}, {"y", "", false}, {"z", "", false}}},
		{"CASE WHEN true THEN end ELSE null END", []reference{{"end", "", false}}},
		{"collect(DISTINCT x)", []reference{{"x", "", false}}},
	} {
		var got []reference
		require.True(t, forEachSemanticExpressionReference(tc.expression, func(variable, property string, hasProperty bool, _ int) bool {
			got = append(got, reference{variable, property, hasProperty})
			return true
		}), tc.expression)
		require.Equal(t, tc.want, got, tc.expression)
	}
}

// The demand owner reads the same walker: a whole-node occurrence makes the
// variable's stored-property demand complete-but-unbounded (nil), so a scan or
// endpoint reads every body, while a property access, an operand-position name
// and a map key each keep their narrow demand.
func TestPipelineNodePredicatePropertiesKeepsWholeNodeOccurrences(t *testing.T) {
	for _, tc := range []struct{ claim, variable, expression string }{
		{"keyword-operand", "end", "coalesce(end, null)"},
		{"keyword-properties", "end", "properties(end)"},
		{"keyword-pattern", "end", "(end)"},
		{"keyword-value", "end", "end"},
		{"keyword-case-arm", "end", "CASE WHEN true THEN end ELSE null END"},
		{"label-pattern", "m", "MATCH (m:Doc {extra:8})"},
		{"label-pattern-exists", "f", "NOT EXISTS { (f:File {restricted:true}) }"},
		{"label-rematch-clause", "n", "MATCH (n:Doc {extra:7})"},
	} {
		require.Nil(t, pipelineNodePredicateProperties(tc.variable, tc.expression, nil), "%s: %s", tc.claim, tc.expression)
	}
	for _, tc := range []struct {
		claim      string
		variable   string
		expression string
		want       []string
	}{
		{"property-access", "end", "end.extra", []string{"extra"}},
		{"operand-name-property", "start", "start.key", []string{"key"}},
		{"map-key", "p", "{p: p.a}", []string{"a"}},
	} {
		require.Equal(t, tc.want, pipelineNodePredicateProperties(tc.variable, tc.expression, nil), "%s: %s", tc.claim, tc.expression)
	}
}
