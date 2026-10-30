package cypher

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDesugarLabelExpressionsRewrites(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		// Node patterns.
		{"MATCH (n:A|B) RETURN n", "MATCH (n) WHERE n:A|B RETURN n"},
		{"MATCH (n:A&B) RETURN n", "MATCH (n:A:B) RETURN n"},
		{"MATCH (n IS A) RETURN n", "MATCH (n:A) RETURN n"},
		{"MATCH (n IS A|B) RETURN n", "MATCH (n) WHERE n:A|B RETURN n"},
		{"MATCH (:A|B)-[r]->(m) RETURN m", "MATCH (__nornic_lx0)-[r]->(m) WHERE __nornic_lx0:A|B RETURN m"},
		{"MATCH ( :A|B) RETURN 1", "MATCH (__nornic_lx0 ) WHERE __nornic_lx0:A|B RETURN 1"},
		{"MATCH (n:A&(B|C)) WHERE n.x = 1 RETURN n", "MATCH (n:A) WHERE n:A&(B|C) AND n.x = 1 RETURN n"},
		{"MATCH (n:%) WHERE n.x = 1 OR n.y = 2 RETURN n", "MATCH (n) WHERE n:% AND (n.x = 1 OR n.y = 2) RETURN n"},
		{"MATCH (n:!A) WHERE n.x = 1 XOR n.y = 2 RETURN n", "MATCH (n) WHERE n:!A AND (n.x = 1 XOR n.y = 2) RETURN n"},
		{"OPTIONAL MATCH (n:A|B) RETURN n", "OPTIONAL MATCH (n) WHERE n:A|B RETURN n"},
		{"MATCH (n:`Odd Label`|B) RETURN n", "MATCH (n) WHERE n:`Odd Label`|B RETURN n"},
		{"MATCH (n:A|B {x: 1}) RETURN n", "MATCH (n {x: 1}) WHERE n:A|B RETURN n"},
		{"MATCH (n:A|B WHERE n.x > 1) RETURN n", "MATCH (n ) WHERE (n.x > 1) AND n:A|B RETURN n"},
		{"MATCH (n:A|B), (m:C|D) WHERE n.x = m.x RETURN n, m", "MATCH (n), (m) WHERE n:A|B AND m:C|D AND n.x = m.x RETURN n, m"},
		{"MATCH (n:A|B) WITH n MATCH (n)-->(m:!B) RETURN m", "MATCH (n) WHERE n:A|B WITH n MATCH (n)-->(m) WHERE m:!B RETURN m"},
		{"MATCH (n:A|B) RETURN n UNION MATCH (n:%) RETURN n", "MATCH (n) WHERE n:A|B RETURN n UNION MATCH (n) WHERE n:% RETURN n"},
		{"EXPLAIN MATCH (n:A|B) RETURN n", "EXPLAIN MATCH (n) WHERE n:A|B RETURN n"},
		{"MATCH p = shortestPath((a:A)-[*]-(b:B|C)) RETURN p", "MATCH p = shortestPath((a:A)-[*]-(b)) WHERE b:B|C RETURN p"},
		// Relationship patterns.
		{"MATCH (a)-[r:!R]->(b) RETURN r", "MATCH (a)-[r]->(b) WHERE r:!R RETURN r"},
		{"MATCH (a)-[:R&!S]->(b) RETURN b", "MATCH (a)-[__nornic_lx0:R]->(b) WHERE __nornic_lx0:R&!S RETURN b"},
		{"MATCH (a)-[r:R&S]->(b) RETURN b", "MATCH (a)-[r]->(b) WHERE r:R&S RETURN b"},
		{"MATCH (a)-[r IS R|S]->(b) RETURN b", "MATCH (a)-[r:R|S]->(b) RETURN b"},
		{"MATCH (a)-[r IS R]->(b) RETURN b", "MATCH (a)-[r:R]->(b) RETURN b"},
		{"MATCH (a)-[:(R|S)*1..2]->(b) RETURN b", "MATCH (a)-[:R|S*1..2]->(b) RETURN b"},
		{"MATCH (a)-[:R|:S]->(b) RETURN b", "MATCH (a)-[:R|S]->(b) RETURN b"},
		{"MATCH (a)-[r:%]->(b) RETURN b", "MATCH (a)-[r]->(b) WHERE r:% RETURN b"},
		{"MATCH (a)-[:`Odd Type`|(S)]->(b) RETURN b", "MATCH (a)-[:`Odd Type`|S]->(b) RETURN b"},
		// Expressions.
		{"MATCH (n:A) RETURN n IS B, n IS NOT NULL, n IS :: INTEGER", "MATCH (n:A) RETURN n:B, n IS NOT NULL, n IS :: INTEGER"},
		{"MATCH (n) WHERE n IS A|B AND n.x IS NULL RETURN n", "MATCH (n) WHERE n:A|B AND n.x IS NULL RETURN n"},
		{"MATCH (n) WHERE (n)-->(:A|B) RETURN n", "MATCH (n) WHERE EXISTS { MATCH (n)-->(__nornic_lx0) WHERE __nornic_lx0:A|B } RETURN n"},
		{"MATCH (n) WHERE NOT (n)<-[:R]-(m:!A) RETURN n", "MATCH (n) WHERE NOT EXISTS { MATCH (n)<-[:R]-(m) WHERE m:!A } RETURN n"},
		{"MATCH (n) WHERE exists((n)-->(:A|B)) RETURN n", "MATCH (n) WHERE EXISTS { MATCH (n)-->(__nornic_lx0) WHERE __nornic_lx0:A|B } RETURN n"},
		{"MATCH (n) WHERE exists((n)-->(:A&B)) RETURN n", "MATCH (n) WHERE exists((n)-->(:A:B)) RETURN n"},
		{"MATCH (n) WHERE EXISTS { (n)-->(m:A|B) } RETURN n", "MATCH (n) WHERE EXISTS { MATCH (n)-->(m) WHERE m:A|B } RETURN n"},
		{"MATCH (n) WHERE EXISTS { (n)-->(m:A|B) WHERE m.x = 1 OR m.y = 2 } RETURN n", "MATCH (n) WHERE EXISTS { MATCH (n)-->(m) WHERE m:A|B AND (m.x = 1 OR m.y = 2) } RETURN n"},
		{"MATCH (n) WHERE EXISTS { (n)-->(m:A&B) WHERE m.x = 1 } RETURN n", "MATCH (n) WHERE EXISTS { (n)-->(m:A:B) WHERE m.x = 1 } RETURN n"},
		{"MATCH (n) WHERE EXISTS { (n)-->(m) WHERE m IS A } RETURN n", "MATCH (n) WHERE EXISTS { (n)-->(m) WHERE m:A } RETURN n"},
		{"MATCH (n) WHERE EXISTS { p = (n)-->(m:!A) } RETURN n", "MATCH (n) WHERE EXISTS { MATCH p = (n)-->(m) WHERE m:!A } RETURN n"},
		{"MATCH (n) WHERE EXISTS { MATCH (n)-->(m:A|B) } RETURN n", "MATCH (n) WHERE EXISTS { MATCH (n)-->(m) WHERE m:A|B } RETURN n"},
		{"MATCH (n) RETURN COUNT { (n)-->(:%) } AS c", "MATCH (n) RETURN COUNT { MATCH (n)-->(__nornic_lx0) WHERE __nornic_lx0:% } AS c"},
		{"MATCH (n) RETURN COLLECT { MATCH (n)-->(m:!A) RETURN m } AS ms", "MATCH (n) RETURN COLLECT { MATCH (n)-->(m) WHERE m:!A RETURN m } AS ms"},
		{"MATCH (n) RETURN [(n)-->(m:A|B) | m.x] AS xs", "MATCH (n) RETURN [(n)-->(m) WHERE m:A|B | m.x] AS xs"},
		{"MATCH (n) RETURN [p = (n)-->(m:A|B) WHERE m.x > 1 | p] AS ps", "MATCH (n) RETURN [p = (n)-->(m) WHERE m:A|B AND m.x > 1 | p] AS ps"},
		{"MATCH (n) RETURN [(n)-->(m:A&B) WHERE m.x > 1 | m.x] AS xs", "MATCH (n) RETURN [(n)-->(m:A:B) WHERE m.x > 1 | m.x] AS xs"},
		{"MATCH (n) RETURN [(n)-->(m) WHERE m IS A | m.x] AS xs", "MATCH (n) RETURN [(n)-->(m) WHERE m:A | m.x] AS xs"},
		{"MATCH (n) RETURN [x IN [1,2] | x] AS xs, {k: [(n)-->(m:!A) | m]} AS mp", "MATCH (n) RETURN [x IN [1,2] | x] AS xs, {k: [(n)-->(m) WHERE m:!A | m]} AS mp"},
		{"MATCH (n) RETURN [(n)-->(m:!A) WHERE m.x = 1 OR m.y = 2 | m] AS ms", "MATCH (n) RETURN [(n)-->(m) WHERE m:!A AND (m.x = 1 OR m.y = 2) | m] AS ms"},
		{"CALL { MATCH (n:A|B) RETURN n } RETURN n", "CALL { MATCH (n) WHERE n:A|B RETURN n } RETURN n"},
		{"MATCH (x) CALL (x) { MATCH (x)-->(n:!A) RETURN n } RETURN n", "MATCH (x) CALL (x) { MATCH (x)-->(n) WHERE n:!A RETURN n } RETURN n"},
		{"MATCH (n) RETURN CASE WHEN n IS A THEN 1 ELSE 0 END AS c", "MATCH (n) RETURN CASE WHEN n:A THEN 1 ELSE 0 END AS c"},
		// Writes.
		{"UNWIND [1] AS i FOREACH (j IN [i] | CREATE (:A&B)) RETURN i", "UNWIND [1] AS i FOREACH (j IN [i] | CREATE (:A:B)) RETURN i"},
		{"MERGE (n:A&B) ON CREATE SET n.x = 1 ON MATCH SET n.y = 2 RETURN n", "MERGE (n:A:B) ON CREATE SET n.x = 1 ON MATCH SET n.y = 2 RETURN n"},
		{"CREATE (n IS A&B) RETURN n", "CREATE (n:A:B) RETURN n"},
		{"CREATE (a)-[r IS R]->(b) RETURN a", "CREATE (a)-[r:R]->(b) RETURN a"},
		{"CREATE p = (a:A&B)-[:R]->(b) RETURN p", "CREATE p = (a:A:B)-[:R]->(b) RETURN p"},
		{"MERGE (a)-[:(R)]->(b)", "MERGE (a)-[:R]->(b)"},
	} {
		got, rewrite, err := desugarLabelExpressions(tc.in, nil, false)
		require.NoError(t, err, tc.in)
		require.Equal(t, tc.want, got, tc.in)
		require.NotNil(t, rewrite, tc.in)
	}
}

func TestDesugarLabelExpressionsLeavesOtherStatements(t *testing.T) {
	for _, q := range []string{
		"MATCH (a)-[r:R|S]->(b) RETURN b",
		"MATCH (n:A:B) RETURN n",
		"CREATE (a)-[:R]->(b) RETURN a",
		"MATCH (n) RETURN [(n)-->(m) WHERE m:A|B | m.x] AS xs",
		"MATCH (n) WHERE n.name = 'a|b' AND n.s = \"x&y\" AND n.`q|r` = 1 RETURN n",
		"MATCH (n) RETURN n.x % 2 AS m, n.y != 1 AS ne, n.z IS NOT NULL AS z",
		"MATCH (n {limit: 1}) WHERE n:A|B RETURN n.limit",
		"MATCH (n) WHERE n IS NULL OR n IS TYPED INTEGER OR n IS NORMALIZED OR n IS NFC NORMALIZED RETURN n",
		"CREATE FULLTEXT INDEX idx FOR (n:A|B) ON EACH [n.x]",
		"CREATE CONSTRAINT c FOR (n:A) REQUIRE n.x IS UNIQUE",
		"MATCH (n) WHERE n:A|B AND (n)-->() RETURN n",
		"MATCH (n) RETURN [x IN [n] WHERE x:A | x.y] AS ys",
		"MATCH (n) WHERE (n.x) - (n.y) > 1 RETURN n",
		"MATCH (n) RETURN {a: 1} AS m, [1, 2][0] AS f",
		"MATCH (n) WHERE COUNT { (n)-->() } > 1 RETURN n",
		"MATCH (a)-[:R]->(b) RETURN [ (a)-->() | 1 ] AS l",
		"RETURN 1",
		"",
	} {
		got, rewrite, err := desugarLabelExpressions(q, nil, false)
		require.NoError(t, err, q)
		require.Equal(t, q, got, q)
		require.Nil(t, rewrite, q)
	}
}

func TestDesugarLabelExpressionsRejects(t *testing.T) {
	for _, tc := range []struct{ query, message string }{
		{"MATCH (n:A|B:C) RETURN n", "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A|(B&C)."},
		{"MATCH (n:A:B|C) RETURN n", "This expression could be expressed as :(A&B)|C."},
		{"MATCH (n:A:!B) RETURN n", "This expression could be expressed as :A&!B."},
		{"MATCH (n:Code:%) RETURN n", "This expression could be expressed as :Code&%."},
		{"MATCH (n:A:!(B|C)) RETURN n", "This expression could be expressed as :A&!(B|C)."},
		{"CREATE (n:A&B:C)", "This expression could be expressed as :A&B&C."},
		{"MATCH (n IS A:B) RETURN n", "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B."},
		{"MATCH (n) WHERE n:A|B:C RETURN n", "This expression could be expressed as :A|(B&C)."},
		{"MATCH (n) WHERE n:(A|B):C RETURN n", "This expression could be expressed as :(A|B)&C."},
		{"MATCH (n) WHERE n IS A:B RETURN n", "Mixing the IS keyword with colon"},
		{"MATCH (n) WHERE n IS NOT A RETURN n", "Invalid input 'A': expected '::', 'NFC', 'NFD', 'NFKC', 'NFKD', 'NORMALIZED', 'NULL' or 'TYPED'"},
		{"MATCH ()-[r:R|:S]->() RETURN r", "The semantics of using colon in the separation of alternative relationship types in conjunction with\nthe use of variable binding, inlined property predicates, or variable length is no longer supported.\nPlease separate the relationships types using `:R|S` instead."},
		{"MATCH ()-[:R|:S {w:1}]->() RETURN 1", "Please separate the relationships types using `:R|S` instead."},
		{"MATCH ()-[:R|:S*]->() RETURN 1", "Please separate the relationships types using `:R|S` instead."},
		{"MATCH ()-[:R:S]->() RETURN 1", "Relationship types in a relationship type expressions may not be combined using ':'"},
		{"MATCH ()-[:!R*]->() RETURN 1", "Variable length relationships must not use relationship type expressions."},
		{"CREATE (n:A|B)", "Label expressions in patterns are not allowed in a CREATE clause, but only in a MATCH clause and in expressions"},
		{"MERGE (n:%)", "Label expressions in patterns are not allowed in a MERGE clause, but only in a MATCH clause and in expressions"},
		{"MERGE ()-[:!R]->()", "Relationship type expressions in patterns are not allowed in a MERGE clause, but only in a MATCH clause"},
		{"CREATE ()-[:R&S]->()", "Relationship type expressions in patterns are not allowed in a CREATE clause, but only in a MATCH clause"},
		{"MATCH (n) WHERE EXISTS { (n)-->(:A|B:C) } RETURN n", "This expression could be expressed as :A|(B&C)."},
		{"MATCH (n) WHERE EXISTS { MATCH (n:A|B:C) } RETURN n", "This expression could be expressed as :A|(B&C)."},
		{"MATCH (n) WHERE (n)-->(:A|B:C) RETURN n", "This expression could be expressed as :A|(B&C)."},
		{"MATCH (n) RETURN [(n)-->(m:A|B:C) | m] AS ms", "This expression could be expressed as :A|(B&C)."},
		{"MATCH (n) RETURN [(n)-->(m) WHERE m IS A:B | m] AS ms", "Mixing the IS keyword with colon"},
		{"MATCH (n) WHERE EXISTS { (n)-->(m) WHERE m IS NOT A } RETURN n", "Invalid input 'A'"},
		{"FOREACH (x IN [1] | CREATE (:A|B))", "Label expressions in patterns are not allowed in a CREATE clause"},
		{"UNWIND [1] AS x FOREACH (y IN [x] | MERGE ()-[:!R]->())", "Relationship type expressions in patterns are not allowed in a MERGE clause"},
		{"MATCH (n) WHERE exists((n)-[:R:S]->()) RETURN n", "may not be combined using ':'"},
		{"MATCH (n) RETURN [(n)-->(m) WHERE EXISTS { (m)-->(:A|B:C) } | m] AS ms", "This expression could be expressed as :A|(B&C)."},
	} {
		_, _, err := desugarLabelExpressions(tc.query, nil, false)
		require.Error(t, err, tc.query)
		require.Contains(t, err.Error(), tc.message, tc.query)
		var classified interface{ BoltErrorCode() string }
		require.True(t, errors.As(err, &classified), tc.query)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", classified.BoltErrorCode(), tc.query)
	}
}

func TestDesugarLabelExpressionsMapsTextBack(t *testing.T) {
	query := "MATCH (n) RETURN n IS A, [(n)-->(m:A|B) | m.x], n.r AS r"
	got, rewrite, err := desugarLabelExpressions(query, nil, false)
	require.NoError(t, err)
	require.Equal(t, "MATCH (n) RETURN n:A, [(n)-->(m) WHERE m:A|B | m.x], n.r AS r", got)
	require.Equal(t, "n IS A", rewrite.originalText("n:A"))
	require.Equal(t, "[(n)-->(m:A|B) | m.x]", rewrite.originalText("[(n)-->(m) WHERE m:A|B | m.x]"))
	require.Equal(t, "r", rewrite.originalText("r"), "a column the client wrote is kept")

	// A generated variable never collides with one in the statement.
	got, _, err = desugarLabelExpressions("MATCH (__nornic_lx0)-->(:A|B) RETURN __nornic_lx0", nil, false)
	require.NoError(t, err)
	require.Equal(t, "MATCH (__nornic_lx0)-->(__nornic_lx1) WHERE __nornic_lx1:A|B RETURN __nornic_lx0", got)
}

func TestWithoutGeneratedColumns(t *testing.T) {
	require.Nil(t, withoutGeneratedColumns(nil))
	plain := &ExecuteResult{Columns: []string{"a"}, Rows: [][]interface{}{{1}}}
	require.Same(t, plain, withoutGeneratedColumns(plain))
	mixed := &ExecuteResult{Columns: []string{"__nornic_lx0", "m", "__nornic_lx1"}, Rows: [][]interface{}{{1, 2, 3}, {4}}}
	trimmed := withoutGeneratedColumns(mixed)
	require.Equal(t, []string{"m"}, trimmed.Columns)
	require.Equal(t, [][]interface{}{{2}, {}}, trimmed.Rows)
	require.Equal(t, []string{"__nornic_lx0", "m", "__nornic_lx1"}, mixed.Columns, "the result itself is kept")
}

func TestLabelExpressionParts(t *testing.T) {
	parse := func(text string) *labelExpression {
		t.Helper()
		expr, ok := parseLabelExpression(text)
		require.True(t, ok, text)
		return expr
	}
	for _, tc := range []struct {
		expr     string
		labels   []string
		match    bool
		required []string
		rendered string
	}{
		{"A", []string{"A"}, true, []string{"A"}, "A"},
		{"A|B", []string{"B"}, true, nil, "A|B"},
		{"(A|B)&(A|C)", []string{"A"}, true, nil, "(A|B)&(A|C)"},
		{"(A&B)|(A&C)", []string{"A", "C"}, true, []string{"A"}, "(A&B)|(A&C)"},
		{"A&B", []string{"A"}, false, []string{"A", "B"}, "A&B"},
		{"A&(B&C)", []string{"A", "B", "C"}, true, []string{"A", "B", "C"}, "A&B&C"},
		{"!A", []string{"B"}, true, nil, "!A"},
		{"!(A|B)", []string{"B"}, false, nil, "!(A|B)"},
		{"%", nil, false, nil, "%"},
		{"%", []string{"X"}, true, nil, "%"},
		{"A|(B&C)", []string{"C"}, false, nil, "A|(B&C)"},
		{"`a b`|C", []string{"a b"}, true, nil, "`a b`|C"},
		{" A | B & !C ", []string{"B"}, true, nil, "A|(B&!C)"},
	} {
		expr := parse(tc.expr)
		require.Equal(t, tc.match, expr.matches(tc.labels), tc.expr)
		require.Equal(t, tc.required, expr.requiredLabels(), tc.expr)
		require.Equal(t, tc.rendered, expr.String(), tc.expr)
	}
	names, ok := parse("A&B").names()
	require.True(t, ok)
	require.Equal(t, []string{"A", "B"}, names)
	_, ok = parse("A&!B").names()
	require.False(t, ok)
	alternatives, ok := parse("A|(B|C)").alternatives()
	require.True(t, ok)
	require.Equal(t, []string{"A", "B", "C"}, alternatives)
	_, ok = parse("A|(B&C)").alternatives()
	require.False(t, ok)
	require.Equal(t, ":A:`b c`", labelChainText([]string{"A", "b c"}))

	for _, text := range []string{"", "|A", "A|", "!", "!=A", "(A", "(A|B", "()", "A B", "&A"} {
		_, ok := parseLabelExpression(text)
		require.False(t, ok, text)
	}
	chain, ok := scanLabelChain("A :B {x: 1}", false)
	require.True(t, ok)
	require.True(t, chain.colons)
	require.False(t, chain.symbols)
	require.Equal(t, "A :B", "A :B {x: 1}"[:chain.end])
	chain, ok = scanLabelChain("R|:S*2", true)
	require.True(t, ok)
	require.True(t, chain.barColons)
	require.Equal(t, "R|S", chain.expr.String())
	chain, ok = scanLabelChain("A::INT", false)
	require.True(t, ok)
	require.False(t, chain.colons, ":: is not a colon chain")
	chain, ok = scanLabelChain("A: 1", false)
	require.True(t, ok)
	require.Equal(t, 1, chain.end, "a colon without a label after it is left out")
}

func TestLabelExpressionBarAt(t *testing.T) {
	for _, tc := range []struct {
		text string
		bar  bool
	}{
		{"m:A|B", true},
		{"m:`a b`|(B)", true},
		{"m:`a`b`|B", false},
		{"m:a`|B", false},
		{"A|B", false},
		{"m:%|!B", true},
		{"m:A | B", false},
		{"x | x.y", false},
		{"m.a|b", false},
		{":A|B", false},
		{"m:A&B|C", true},
	} {
		at := strings.IndexByte(tc.text, '|')
		require.Equal(t, tc.bar, labelExpressionBarAt(tc.text, 0, at), tc.text)
	}
	require.False(t, labelExpressionBarAt("|A", 0, 0))
	require.False(t, labelExpressionBarAt("A|", 0, 1))
}

func TestMayUseLabelExpressions(t *testing.T) {
	for _, tc := range []struct {
		query string
		may   bool
	}{
		{"MATCH (n:A) RETURN n", false},
		{"MATCH (n:A|B) RETURN n", true},
		{"MATCH (n) WHERE n.x = 'a|b' RETURN n", false},
		{"MATCH (n) WHERE n IS A RETURN n", true},
		{"MATCH (n) WHERE n IS NOT NULL RETURN n", false},
		{"MATCH (n) WHERE n IS NOT A RETURN n", true},
		{"MATCH (n) WHERE n IS :: INTEGER RETURN n", false},
		{"MATCH (n) WHERE n IS", false},
		{"MATCH (n) WHERE this RETURN n", false},
		{"MATCH ()-[:R:S]->() RETURN 1", true},
		{"MATCH ()-[:R]->() RETURN [x IN [1] | x::INT]", true},
		{"MATCH (n:A:B) RETURN n", false},
		{"RETURN [1, 2]", false},
	} {
		require.Equal(t, tc.may, mayUseLabelExpressions(tc.query), tc.query)
	}
}

func TestExtractLabelsFromQueryWithLabelExpressions(t *testing.T) {
	require.Equal(t, []string{"Code", "Document"}, extractLabelsFromQuery("MATCH (n) WHERE n:Code|Document RETURN n"))
	require.Equal(t, []string{"Code"}, extractLabelsFromQuery("MATCH (n:Code) WHERE n:Code&!Document RETURN n"))
	require.Nil(t, extractLabelsFromQuery("MATCH (n) WHERE n:Code|!Document RETURN n"), "the result depends on every write")
	require.Nil(t, extractLabelsFromQuery("MATCH (n) WHERE n:% RETURN n"))
	require.Nil(t, extractLabelsFromQuery("MATCH (n) WHERE n:(A|B) RETURN n"))
}

func TestDesugarLabelExpressionsNestedAndMalformedInput(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"MATCH (n:A|B", "MATCH (n:A|B"},
		{"MATCH (n)-[r:!R RETURN 1", "MATCH (n)-[r:!R RETURN 1"},
		{"MATCH (a)-[:R]->{1,3}(b:A|B) RETURN b", "MATCH (a)-[:R*1..3]->(b) WHERE b:A|B RETURN b"},
		// Relationship quantifiers (#864): the length goes before properties
		// and the element's own WHERE, which applies to each relationship
		// (#878); an abbreviated relationship gets brackets.
		{"MATCH (a)-[r:R WHERE r.w > 1]->{1,2}(b) RETURN b", "MATCH (a)-[r:R*1..2 ]->(b) WHERE all(r IN r WHERE r.w > 1) RETURN b"},
		{"MATCH (a)-[r:R {w: 1}]->{2,}(b) RETURN b", "MATCH (a)-[r:R*2.. {w: 1}]->(b) RETURN b"},
		{"MATCH (a)-[:`R*`]->{,2}(b) RETURN b", "MATCH (a)-[:`R*`*0..2]->(b) RETURN b"},
		{"MATCH (a)<--{2}(b) RETURN b", "MATCH (a)<-[*2..2]-(b) RETURN b"},
		{"MATCH (a)--*(b) RETURN b", "MATCH (a)-[*0..]-(b) RETURN b"},
		{"FOREACH 1 | 2", "FOREACH 1 | 2"},
		{"FOREACH (x IN [1] | CREATE (:A|B)", "FOREACH (x IN [1] | CREATE (:A|B)"},
		{"FOREACH (x IN [n IS A] )", "FOREACH (x IN [n:A] )"},
		{"RETURN {a: [x IN l | x]", "RETURN {a: [x IN l | x]"},
		{"RETURN [x IN l | x", "RETURN [x IN l | x"},
		{"RETURN (n:A|B) AS x", "RETURN (n:A|B) AS x"},
		{"MATCH (n:A|B) WHERE n IS NOT NULL RETURN n", "MATCH (n) WHERE n:A|B AND n IS NOT NULL RETURN n"},
		{"MATCH (n:A|B) RETURN n IS 1", "MATCH (n) WHERE n:A|B RETURN n IS 1"},
		{"MATCH (n:A|B) WHERE EXISTS { } RETURN n", "MATCH (n) WHERE n:A|B AND EXISTS { } RETURN n"},
		{"MATCH (n:A|B) RETURN (1", "MATCH (n) WHERE n:A|B RETURN (1"},
		{"RETURN [(1) | 2] AS l", "RETURN [(1) | 2] AS l"},
		{"MATCH (n) RETURN [(n)-->(m) WHERE m:A|B] AS l", "MATCH (n) RETURN [(n)-->(m) WHERE m:A|B] AS l"},
		{"RETURN [(n | 1] AS l", "RETURN [(n | 1] AS l"},
		{"MATCH (n) WHERE ((n)-->(:A|B)) RETURN n", "MATCH (n) WHERE (EXISTS { MATCH (n)-->(__nornic_lx0) WHERE __nornic_lx0:A|B }) RETURN n"},
		{"MATCH (n:A|B) WHERE (n)-[r]x RETURN n", "MATCH (n) WHERE n:A|B AND (n)-[r]x RETURN n"},
		{"MATCH `p` = (n:A|B) RETURN `p`", "MATCH `p` = (n) WHERE n:A|B RETURN `p`"},
		{"RETURN n:A|B) AS x", "RETURN n:A|B) AS x"},
	} {
		got, _, err := desugarLabelExpressions(tc.in, nil, false)
		require.NoError(t, err, tc.in)
		require.Equal(t, tc.want, got, tc.in)
	}
	for _, q := range []string{
		"MATCH ((n:A|B:C)) RETURN n",
		"FOREACH (x IN [(n)-->(:A|B:C)] | SET x.y = 1)",
		"RETURN {a: (n)-->(:A|B:C)} AS m",
		"RETURN [1, (n)-->(:A|B:C)] AS l",
		"MATCH (n) WHERE EXISTS { (n)-->(m:A|B) WHERE m:C|D:E } RETURN n",
	} {
		_, _, err := desugarLabelExpressions(q, nil, false)
		require.Error(t, err, q)
		require.Contains(t, err.Error(), "Mixing label expression symbols", q)
	}
}

func TestRelationshipChainEnd(t *testing.T) {
	for _, tc := range []struct {
		text string
		end  int
		ok   bool
	}{
		{"(a)-->(b)<-[r]-(c) AND x", len("(a)-->(b)<-[r]-(c)"), true},
		{"(a)--(b)", len("(a)--(b)"), true},
		{"(a)-[r", 0, false},
		{"(a)-[r]x", 0, false},
		{"(a.x) - 1", 0, false},
		{"(a)-->x", 0, false},
		{"(a)-->(b", 0, false},
		{"(a", 0, false},
		{"((a)-->(b))", 0, false},
	} {
		end, ok := relationshipChainEnd(tc.text, 0, len(tc.text))
		require.Equal(t, tc.ok, ok, tc.text)
		if ok {
			require.Equal(t, tc.end, end, tc.text)
		}
	}
}

func TestUnwindLookupReadsRewrittenLabelAlternatives(t *testing.T) {
	variable, labels, _, anyLabel, ok := parseUnwindNodePatternClauseInternal("MATCH (v {uid: row.id}) WHERE v:A|B", "MATCH", true)
	require.True(t, ok)
	require.Equal(t, "v", variable)
	require.Equal(t, []string{"A", "B"}, labels)
	require.True(t, anyLabel)
	for _, tc := range []struct {
		clause       string
		alternatives bool
	}{
		{"MATCH (v {uid: row.id}) RETURN v", true},
		{"MATCH (v {uid: row.id}", true},
		{"MATCH (v {uid: row.id}) WHERE v:A|B", false},
		{"MATCH (v:A {uid: row.id}) WHERE v:B", true},
		{"MATCH (v {uid: row.id}) WHERE w:A|B", true},
		{"MATCH (v {uid: row.id}) WHERE v.x = 1", true},
		{"MATCH (v {uid: row.id}) WHERE v:A&B", true},
		{"MATCH (v {uid: row.id}) WHERE v:A|", true},
	} {
		_, _, _, _, ok := parseUnwindNodePatternClauseInternal(tc.clause, "MATCH", tc.alternatives)
		require.False(t, ok, tc.clause)
	}
}

func TestLabelExpressionsInInternalStatementsAndCase(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.executeInternal(ctx, "MATCH (n:A|B:C) RETURN n", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Mixing label expression symbols")

	rels := map[string]*storage.Edge{"r": {Type: "S"}}
	require.True(t, exec.evaluateCondition(ctx, "r:R|S", nil, rels))
	require.False(t, exec.evaluateCondition(ctx, "r:!S", nil, rels))
	nodes := map[string]*storage.Node{"n": {Labels: []string{"A"}}}
	require.True(t, exec.evaluateCondition(ctx, "n:A&!B", nodes, nil))
}
