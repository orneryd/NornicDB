package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A single-hop relationship's r.key = <literal> conjuncts join its property
// filter; anything else (another variable, a call, OR, another case of the
// variable, a variable-length or chained pattern) is left to the WHERE.

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
