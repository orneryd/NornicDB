package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// boundNodeLookupGraph: outer O nodes whose ref names S nodes by id (one, two
// with the same id, none, a missing ref, an integer ref stored as a float, a
// list ref), an S that is also T, and a node with a matching id but another
// label.
func boundNodeLookupGraph(t testing.TB) *StorageExecutor {
	exec, store := newTestExecutor(t)
	nodes := []*storage.Node{
		{ID: "s1", Labels: []string{"S"}, Properties: map[string]interface{}{"id": "a", "tags": []interface{}{"x", "y"}, "n": int64(1)}},
		{ID: "s2", Labels: []string{"S"}, Properties: map[string]interface{}{"id": "b", "tags": []interface{}{"x"}, "n": int64(2)}},
		{ID: "s3", Labels: []string{"S", "T"}, Properties: map[string]interface{}{"id": "b", "tags": []interface{}{"z"}, "n": int64(3)}},
		{ID: "s4", Labels: []string{"S"}, Properties: map[string]interface{}{"id": 1.0, "tags": []interface{}{"x"}}},
		{ID: "s5", Labels: []string{"S"}, Properties: map[string]interface{}{"id": []interface{}{"a"}, "tags": []interface{}{"x"}}},
		{ID: "u1", Labels: []string{"U"}, Properties: map[string]interface{}{"id": "a", "tags": []interface{}{"x"}}},
	}
	for i, o := range []map[string]interface{}{
		{"ref": "a", "tag": "x"},
		{"ref": "a", "tag": "q"},
		{"ref": "b", "tag": "x"},
		{"ref": "b", "tag": "z"},
		{"ref": "zz", "tag": "x"},
		{"tag": "x"},
		{"ref": int64(1), "tag": "x"},
		{"ref": []interface{}{"a"}, "tag": "x"},
	} {
		o["name"] = fmt.Sprintf("o%d", i)
		nodes = append(nodes, &storage.Node{ID: storage.NodeID(fmt.Sprintf("o%d", i)), Labels: []string{"O"}, Properties: o})
	}
	require.NoError(t, store.BulkCreateNodes(nodes))
	return exec
}

// The node lookup gives every row the EXISTS, NOT EXISTS and COUNT the
// correlated pipeline gives the same body (with a RETURN, which the lookup
// declines), with and without an index on the looked-up property.
func TestBoundNodeLookupMatchesPipeline(t *testing.T) {
	exec := boundNodeLookupGraph(t)
	ctx := context.Background()
	bodies := []string{
		"MATCH (s:S {id: o.ref})",
		"MATCH (s:S {id: o.ref}) WHERE any(x IN s.tags WHERE x = o.tag)",
		"MATCH (s:S {id: o.ref}) WHERE s.n > 1",
		"MATCH (s:S:T {id: o.ref})",
		"MATCH (s:S {id: o.ref, n: 2})",
		"MATCH (:S {id: o.ref})",
		"MATCH (s:S {id: coalesce(o.ref, 'a')}) WHERE NOT o.tag IN s.tags",
		"MATCH (s:S {`id`: o.ref}) WHERE size(s.tags) = 1 AND s.tags[0] = o.tag",
	}
	check := func(label string) {
		for _, body := range bodies {
			pipeline := body + " RETURN 1 AS one"
			query := fmt.Sprintf(`MATCH (o:O) RETURN o.name AS name,
  EXISTS { %[1]s } AS e, EXISTS { %[2]s } AS pe,
  NOT EXISTS { %[1]s } AS ne, NOT EXISTS { %[2]s } AS pne,
  COUNT { %[1]s } AS c, COUNT { %[2]s } AS pc
ORDER BY name`, body, pipeline)
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Len(t, result.Rows, 8, query)
			for _, row := range result.Rows {
				require.Equal(t, row[2], row[1], "%s %s EXISTS %v", label, body, row)
				require.Equal(t, row[4], row[3], "%s %s NOT EXISTS %v", label, body, row)
				require.Equal(t, row[6], row[5], "%s %s COUNT %v", label, body, row)
			}
		}
	}
	check("scan")
	_, err := exec.Execute(ctx, "CREATE INDEX s_id FOR (n:S) ON (n.id)", nil)
	require.NoError(t, err)
	check("index")
}

// The node lookup takes one labelled node pattern with a map, whose variable
// the row doesn't bind; it declines any other body.
func TestBoundNodeLookupShape(t *testing.T) {
	for _, test := range []struct {
		body string
		ok   bool
	}{
		{"MATCH (s:S {id: o.ref})", true},
		{"MATCH (s:S {id: o.ref}) WHERE s.n > 1", true},
		{"MATCH (:S:T {id: $p, n: 1})", true},
		{"MATCH (s:S {id: o.ref}) RETURN s", false},
		{"MATCH (s {id: o.ref})", false},
		{"MATCH (s:S)", false},
		{"MATCH (s:S|T {id: o.ref})", false},
		{"MATCH (s:S {id: o.ref})-[:R]->(t)", false},
		{"MATCH (s:S {id: o.ref}), (t:S {id: o.ref})", false},
		{"MATCH p = (s:S {id: o.ref})", false},
		{"MATCH (s:S WHERE s.n > 1 {id: o.ref})", false},
		{"MATCH (s:S {id: o.ref, id: 1})", false},
		{"OPTIONAL MATCH (s:S {id: o.ref})", false},
		{"MATCH (s:S {id: o.ref}) WITH s WHERE s.n > 1", false},
	} {
		require.Equal(t, test.ok, parseBoundNodeShape(test.body).ok, test.body)
	}
	exec := boundNodeLookupGraph(t)
	o, err := exec.storage.GetNode("o0")
	require.NoError(t, err)
	s, err := exec.storage.GetNode("s1")
	require.NoError(t, err)
	ctx, admission := expressionTestContext(context.Background(), "COUNT { MATCH (s:S {id: o.ref}) }")
	count, ok := exec.boundNodeLookupCount(admission, ctx, "MATCH (s:S {id: o.ref})", map[string]interface{}{"o": o}, false)
	require.True(t, ok)
	require.Equal(t, int64(1), count)
	_, ok = exec.boundNodeLookupCount(admission, ctx, "MATCH (s:S {id: o.ref})", map[string]interface{}{"o": o, "s": s}, false)
	require.False(t, ok, "a bound variable is a join, the pipeline's")
}

// A null an OPTIONAL MATCH leaves in the row gives the semi-joins (one
// relationship, one node looked up by property) what the pipeline gives the
// same body with a RETURN, which both decline.
func TestSemiJoinsOverOptionalNulls(t *testing.T) {
	exec := boundNodeLookupGraph(t)
	ctx := context.Background()
	for _, body := range []string{
		"MATCH (o)-[r]-(z)",
		"MATCH (o)-[r]-(z) WHERE r.w = 1",
		"MATCH (s:S {id: z.id})",
		"MATCH (s:S {id: o.ref}) WHERE s.n > coalesce(z.n, 0)",
	} {
		pipeline := body + " RETURN 1 AS one"
		query := fmt.Sprintf(`MATCH (o:O) OPTIONAL MATCH (o)-[:NONE]->(z)
RETURN o.name AS name, EXISTS { %[1]s } AS e, EXISTS { %[2]s } AS pe,
  NOT EXISTS { %[1]s } AS ne, NOT EXISTS { %[2]s } AS pne,
  COUNT { %[1]s } AS c, COUNT { %[2]s } AS pc
ORDER BY name`, body, pipeline)
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 8, query)
		for _, row := range result.Rows {
			require.Equal(t, row[2], row[1], "%s EXISTS %v", body, row)
			require.Equal(t, row[4], row[3], "%s NOT EXISTS %v", body, row)
			require.Equal(t, row[6], row[5], "%s COUNT %v", body, row)
		}
	}
}
