package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Literal expectations verified against Neo4j 5.26.30. The supplementary
// adapter corpus also contains malformed text and internal binding contracts;
// those are not a Neo4j semantic oracle.
var rowValueNeo4jCorpus = []struct {
	expression string
	want       interface{}
}{
	{"1", int64(1)},
	{"'a'", "a"},
	{"null", nil},
	{"true", true},
	{"x", int64(1)},
	{"$p", "p"},
	{"$keys", []interface{}{"k1", "k2", "a|k"}},
	{"n.id", "a"},
	{"n.missing", nil},
	{"m.a.b", "c"},
	{"r.kind", "k"},
	{"(n.id)", "a"},
	{"n.id + '|' + r.kind", "a|k"},
	{"startNode(r).id + '|' + endNode(r).id + '|' + r.kind", "a|b|k"},
	{"endNode(r).id", "b"},
	{"1 + 2 * 3", int64(7)},
	{"x - 1", int64(0)},
	{"x % 2", int64(1)},
	{"x * null", nil},
	{"'a' + 1", "a1"},
	{"[1] + 2", []interface{}{int64(1), int64(2)}},
	{"x + y", int64(3)},
	{"x / y", int64(0)},
	{"x = 1", true},
	{"x <> 1", false},
	{"1 < x < 3", false},
	{"x = null", nil},
	{"n.id <> 'zz'", true},
	{"id(n) = id(n)", true},
	{"1 = 1.0", true},
	{"[1, 2] = [1, 2.0]", true},
	{"x IS NULL", false},
	{"n.missing IS NOT NULL", false},
	{"x IN [1, 2]", true},
	{"NOT (x IN [1, null])", false},
	{"'k1' IN $keys", true},
	{"null IN $keys", nil},
	{"x IN null", nil},
	{"n.id STARTS WITH 'a'", true},
	{"x STARTS WITH 'a'", nil},
	{"n.id ENDS WITH null", nil},
	{"n.id CONTAINS 'x'", false},
	{"x = 1 AND y = 2", true},
	{"NOT x = 1", false},
	{"NOT NOT x = 1", true},
	{"x = 1 XOR y = 2", false},
	{"true OR x / 0 = 1", true},
	{"false AND x / 0 = 1", false},
	{"NOT (x = 1 OR y = 2)", false},
	{"any(k IN $keys WHERE k = 'k1')", true},
	{"NOT any(k IN $keys WHERE k = 'zz')", true},
	{"all(k IN $keys WHERE k STARTS WITH 'k')", false},
	{"none(k IN $keys WHERE k = n.id + '|' + r.kind)", false},
	{"single(k IN [1, 2, 1] WHERE k = 1)", false},
	{"any(k IN [1, null] WHERE k = 2)", nil},
	{"any(k IN null WHERE k = 1)", nil},
	{"any(k IN $keys WHERE k = startNode(r).id)", false},
	{"any(k IN [1, 2] WHERE any(j IN [2, 3] WHERE j = k + x))", true},
	{"any(k IN [] WHERE k / 0 = 1)", false},
	{"all(k IN $keys WHERE size(k) = 2)", false},
	{"[k IN $keys WHERE k <> 'k1' | k + '!']", []interface{}{"k2!", "a|k!"}},
	{"[k IN [1, 2, 3] WHERE k > x]", []interface{}{int64(2), int64(3)}},
	{"[k IN [1, 2] | k * y]", []interface{}{int64(2), int64(4)}},
	{"size([k IN $keys WHERE k = n.id + '|' + r.kind | k])", int64(1)},
	{"[1, x, n.id]", []interface{}{int64(1), int64(1), "a"}},
	{"[]", []interface{}{}},
	{"coalesce(n.missing, x)", int64(1)},
	{"coalesce(null, null)", nil},
	{"coalesce(n.id, 1 / 0)", "a"},
	{"size(null)", nil},
	{"size('abc')", int64(3)},
	{"size($keys)", int64(3)},
	{"head([])", nil},
	{"last([1, 2])", int64(2)},
	{"reverse('abc')", "cba"},
	{"reverse(null)", nil},
	{"type(r)", "R"},
	{"type(null)", nil},
	{"toLower(n.id)", "a"},
	{"toLower(startNode(r).id) + 'x'", "ax"},
	{"abs(-1)", int64(1)},
	{"CASE WHEN x = 1 THEN 'one' ELSE 'other' END", "one"},
	{"$keys[0]", "k1"},
	{"m['a']", map[string]interface{}{"b": "c"}},
	{"-x", int64(-1)},
	{"2 ^ 3", float64(8)},
	{"n {.id}", map[string]interface{}{"id": "a"}},
	{"{a: x}", map[string]interface{}{"a": int64(1)}},
	{"`x`", int64(1)},
	{"reduce(acc = 0, k IN [1, 2, 3] | acc + k + x)", int64(9)},
	{"reduce(acc = '', k IN $keys | acc + k)", "k1k2a|k"},
	{"reduce(acc = 0, k IN null | acc + k)", nil},
	{"reduce(acc = 0, k IN [] | 1 / 0)", int64(0)},
	{"reduce(acc = [], k IN $keys | acc + [k + n.id])", []interface{}{"k1a", "k2a", "a|ka"}},
	{"m['a']['b']", "c"},
	{"$keys[size($keys) - 1]", "a|k"},
	{"$keys[0..1][0]", "k1"},
	{"[[1, 2], [3]][x][0]", int64(3)},
	{"COUNT { MATCH (q) }", int64(3)},
	{"[(n)-[:R]->(o) | o.id]", []interface{}{"b"}},
	{"size([(n)-[:R]->(o) | o.id]) > 0", true},
}

var rowPredicateNeo4jCorpus = []struct {
	expression string
	want       bool
}{
	{"true", true},
	{"false", false},
	{"x = 1", true},
	{"x <> 1", false},
	{"x = 1 AND y = 2", true},
	{"x = 1 OR y = 2", true},
	{"(x = 1)", true},
	{"NOT x = 1", false},
	{"x = 1 XOR y = 2", false},
	{"n.id = 'a' AND r.kind <> 'zz'", true},
	{"startNode(r).id <> 'zz'", true},
	{"startNode(r).id + '|' + endNode(r).id + '|' + r.kind <> 'zz'", true},
	{"NOT any(k IN $keys WHERE k = 'zz')", true},
	{"NOT any(k IN $keys WHERE k = startNode(r).id + '|' + r.kind)", false},
	{"any(k IN $keys WHERE k = n.id)", false},
	{"NOT startNode(r).id + '|' + r.kind IN $keys", false},
	{"n.id IN $keys", false},
	{"NOT (x IN [2, 3])", true},
	{"toLower(n.id) IN ['a']", true},
	{"n.id STARTS WITH 'a'", true},
	{"n.id CONTAINS toLower('A')", true},
	{"x IS NULL", false},
	{"n.missing IS NOT NULL", false},
	{"coalesce(n.missing, '') = ''", true},
	{"n:P", true},
	{"n:Q", false},
	{"x + 1 > 1", true},
	{"1 < x + 1 < 3", true},
	{"id(n) = id(n)", true},
	{"x = 1 OR x / 0 = 1", true},
	{"x = 2 AND x / 0 = 1", false},
	{"'a' = 1", false},
	{"true OR (x = 1 AND y = 2)", true},
	{"false AND (x / 0 = 1)", false},
	{"CASE WHEN x = 1 THEN true ELSE false END", true},
	{"CASE WHEN x = 1 THEN y = 2 ELSE false END OR n.id = 'zz'", true},
	{"n.id = 'zz' OR CASE x WHEN 1 THEN true END", true},
	{"x = 1 AND CASE WHEN y = 2 THEN 'yes' END = 'yes'", true},
	{"(CASE WHEN x = 1 THEN 1 ELSE 2 END) + 1 = 2 OR false", true},
	{"size([k IN $keys WHERE k <> 'zz']) > 0", true},
	{"reduce(s = 0, k IN [1, 2] | s + k) = 3", true},
	{"x = 1 AND (y = 2 OR null = 3)", true},
	{"NOT (x = 1 OR y = 2)", false},
	{"NOT NOT x = 1", true},
	{"x = 1 XOR NOT y = 2", true},
	{"COUNT { MATCH (q) } > 0", true},
	{"n.id IS NORMALIZED", true},
	{"m[n['id']] IS NULL", false},
	{"$keys[x] = 'k2'", true},
}

func TestRowValueProgramsMatchNeo4j(t *testing.T) {
	exec, store := perRowTestExecutor(t)
	for _, tc := range rowValueNeo4jCorpus {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.expression, text), func(t *testing.T) {
				fixtureAdmission794Context, fixtureAdmission794 := expressionTestContext(context.Background(), tc.expression)
				_ = fixtureAdmission794
				ctx := withExpressionFailureSlot(fixtureAdmission794Context)
				row := rowValueCorpusRows(t, store)[0]
				row[rowStatementContextKey] = ctx
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				var value interface{}
				var ok bool
				var err error
				if text {
					fixtureAdmission958Context, fixtureAdmission958 := expressionTestContext(context.Background(), tc.expression)
					_ = fixtureAdmission958Context
					value, ok, err = exec.evaluateRowValueText(fixtureAdmission958, tc.expression, row)
				} else {
					root, _ := (&rowValueCompiler{}).compile(tc.expression)
					fixtureAdmission1723Context, fixtureAdmission1723 := statementTestContext(context.Background(), "RETURN "+(tc.expression))
					_ = fixtureAdmission1723Context
					value, ok, err = root.eval(fixtureAdmission1723, exec, row, nil)
				}
				require.NoError(t, err)
				require.NoError(t, getExpressionFailure(ctx))
				require.True(t, ok)
				require.Equal(t, tc.want, value)
			})
		}
	}
}

func TestRowPredicateProgramsMatchNeo4j(t *testing.T) {
	exec, store := perRowTestExecutor(t)
	for _, tc := range rowPredicateNeo4jCorpus {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/text=%v", tc.expression, text), func(t *testing.T) {
				fixtureAdmission795Context, fixtureAdmission795 := expressionTestContext(context.Background(), tc.expression)
				_ = fixtureAdmission795
				ctx := withExpressionFailureSlot(fixtureAdmission795Context)
				row := rowValueCorpusRows(t, store)[0]
				row[rowStatementContextKey] = ctx
				rowValueProgramsDisabled.Store(text)
				defer rowValueProgramsDisabled.Store(false)
				var value bool
				if text {
					fixtureAdmission982Context, fixtureAdmission982 := expressionTestContext(ctx, tc.expression)
					_ = fixtureAdmission982Context
					value = exec.evaluateRowPredicateText(fixtureAdmission982, fixtureAdmission982Context, tc.expression, row)
				} else {
					fixtureAdmission1724Context, fixtureAdmission1724 := statementTestContext(ctx, "RETURN "+(tc.expression))
					_ = fixtureAdmission1724Context
					value = compileRowPredicateProgram(tc.expression).holds(fixtureAdmission1724, exec, fixtureAdmission1724Context, row)
				}
				require.NoError(t, getExpressionFailure(ctx))
				require.Equal(t, tc.want, value)
			})
		}
	}
}
