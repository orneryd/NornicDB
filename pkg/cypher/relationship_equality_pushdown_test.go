package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A single-hop relationship's r.key = <literal> conjuncts join its property
// filter; anything else (another variable, a call, OR, another case of the
// variable, a variable-length or chained pattern) is left to the WHERE.
func TestPushRelationshipEqualities(t *testing.T) {
	exec, _ := boundExpandGraph(t)
	ctx := context.Background()
	for _, test := range []struct {
		pattern, where string
		want           map[string]interface{}
	}{
		{"(a)-[r:T]-(b)", "r.w = 1", map[string]interface{}{"w": int64(1)}},
		{"(a)-[r:T]-(b)", "(r.w = 1)", map[string]interface{}{"w": int64(1)}},
		{"(a)-[r:T]->(b)", "1 = r.w", map[string]interface{}{"w": int64(1)}},
		{"(a)-[r:T]-(b)", "r.w = 1 AND b.k = 2", map[string]interface{}{"w": int64(1)}},
		{"(a)-[r:T]-(b)", "r.w = 'x' AND r.v = true", map[string]interface{}{"w": "x", "v": true}},
		{"(a)-[r:T {w: 2}]-(b)", "r.w = 1", map[string]interface{}{"w": int64(2)}},
		{"(a)-[r:T]-(b)", "R.w = 1", nil},
		{"(a)-[r:T]-(b)", "r.w = 1 OR r.w = 2", nil},
		{"(a)-[r:T]-(b)", "r.w = rand()", nil},
		{"(a)-[r:T]-(b)", "r.w = b.k", nil},
		{"(a)-[r:T]-(b)", "r.w + 0 = 1", nil},
		{"(a)-[r:T]-(b)", "r.w = null", nil},
		{"(a)-[r:T*1..2]-(b)", "all(x IN r WHERE x.w = 1)", nil},
		{"(a)-[r:T]-(b)-[s:T]-(c)", "r.w = 1", nil},
	} {
		fixtureAdmission829Context, fixtureAdmission829 := traversalTestContext(ctx, test.pattern, "", nil)
		_ = fixtureAdmission829
		matches := exec.parseTraversalPattern(fixtureAdmission829, fixtureAdmission829Context, test.pattern)
		require.NotNil(t, matches, test.pattern)
		fixtureAdmission966Context, fixtureAdmission966 := expressionTestContext(ctx, test.where)
		_ = fixtureAdmission966Context
		exec.pushRelationshipEqualities(fixtureAdmission966, fixtureAdmission966Context, matches, test.where)
		if test.want == nil {
			require.Empty(t, matches.Relationship.Properties, "%s WHERE %s", test.pattern, test.where)
			continue
		}
		require.Equal(t, test.want, matches.Relationship.Properties, "%s WHERE %s", test.pattern, test.where)
	}
}

// A pushed equality gives the rows the WHERE alone gives (r.w + 0 = 1 is the
// same test, never pushed), plain and as a quantified group's iteration.
func TestPushedRelationshipEqualityRows(t *testing.T) {
	exec, _ := boundExpandGraph(t)
	ctx := context.Background()
	for _, pair := range [][2]string{
		{"MATCH (a {k: 1})-[r:T]-(b) WHERE r.w = 1 RETURN count(*) AS c",
			"MATCH (a {k: 1})-[r:T]-(b) WHERE r.w + 0 = 1 RETURN count(*) AS c"},
		{"MATCH (a {k: 1})-[r:T]-(b) WHERE r.w = 1.0 AND b:Y RETURN count(*) AS c",
			"MATCH (a {k: 1})-[r:T]-(b) WHERE r.w + 0 = 1.0 AND b:Y RETURN count(*) AS c"},
		{"MATCH (a {k: 1})<-[r]-(b) WHERE r.w = 1 RETURN collect(type(r)) AS c",
			"MATCH (a {k: 1})<-[r]-(b) WHERE r.w + 0 = 1 RETURN collect(type(r)) AS c"},
		{"MATCH (a {k: 1})-[r:T {w: 2}]-(b) WHERE r.w = 1 RETURN count(*) AS c",
			"MATCH (a {k: 1})-[r:T {w: 2}]-(b) WHERE r.w + 0 = 1 RETURN count(*) AS c"},
		{"MATCH (a {k: 1})((x)-[r:T WHERE r.w = 1]-(y))*(m) RETURN count(m) AS c",
			"MATCH (a {k: 1})((x)-[r:T WHERE r.w + 0 = 1]-(y))*(m) RETURN count(m) AS c"},
		{"MATCH (a {k: 1})((x)<-[r:T WHERE r.w = 1]-(y))*(m) RETURN count(m) AS c",
			"MATCH (a {k: 1})((x)<-[r:T WHERE r.w + 0 = 1]-(y))*(m) RETURN count(m) AS c"},
	} {
		pushed, err := exec.Execute(ctx, pair[0], nil)
		require.NoError(t, err, pair[0])
		plain, err := exec.Execute(ctx, pair[1], nil)
		require.NoError(t, err, pair[1])
		require.Equal(t, plain.Rows, pushed.Rows, pair[0])
	}
}

// A pushed parameter, null or list included, an outgoing expansion, an
// unmatched OPTIONAL MATCH and zero repetitions of a quantified group
// between bound endpoints give the rows the unpushed form gives.
func TestPushedRelationshipEqualityParameterRows(t *testing.T) {
	exec, _ := boundExpandGraph(t)
	ctx := context.Background()
	for _, tc := range []struct {
		pushed, plain string
		params        map[string]interface{}
	}{
		{"MATCH (a {k: 1})-[r:T]->(b) WHERE r.w = $w RETURN count(*) AS c",
			"MATCH (a {k: 1})-[r:T]->(b) WHERE r.w + 0 = $w RETURN count(*) AS c", map[string]interface{}{"w": int64(1)}},
		{"MATCH (a {k: 1})-[r:T]-(b) WHERE r.w = $w RETURN count(*) AS c",
			"MATCH (a {k: 1})-[r:T]-(b) WHERE r.w + 0 = $w RETURN count(*) AS c", map[string]interface{}{"w": nil}},
		{"MATCH (a {k: 1})-[r:T]-(b) WHERE r.w = $w RETURN count(*) AS c",
			"MATCH (a {k: 1})-[r:T]-(b) WHERE r.w + 0 = $w RETURN count(*) AS c", map[string]interface{}{"w": []interface{}{int64(1)}}},
		{"MATCH (a {k: 1}) OPTIONAL MATCH (a)-[r:T]->(b) WHERE r.w = 99 RETURN count(a) AS a, count(r) AS r",
			"MATCH (a {k: 1}) OPTIONAL MATCH (a)-[r:T]->(b) WHERE r.w + 0 = 99 RETURN count(a) AS a, count(r) AS r", nil},
		{"MATCH (a {k: 1}), (b {k: 2}) MATCH (a)((x)-[r:T WHERE r.w = 99]-(y))*(b) RETURN count(*) AS c",
			"MATCH (a {k: 1}), (b {k: 2}) MATCH (a)((x)-[r:T WHERE r.w + 0 = 99]-(y))*(b) RETURN count(*) AS c", nil},
		{"MATCH (a {k: 1}) MATCH (a)((x)-[r:T WHERE r.w = 99]-(y))*(a) RETURN count(*) AS c, collect(size(r)) AS sizes",
			"MATCH (a {k: 1}) MATCH (a)((x)-[r:T WHERE r.w + 0 = 99]-(y))*(a) RETURN count(*) AS c, collect(size(r)) AS sizes", nil},
	} {
		pushed, err := exec.Execute(ctx, tc.pushed, tc.params)
		require.NoError(t, err, tc.pushed)
		plain, err := exec.Execute(ctx, tc.plain, tc.params)
		require.NoError(t, err, tc.plain)
		require.Equal(t, plain.Rows, pushed.Rows, "%s %v", tc.pushed, tc.params)
	}
}
