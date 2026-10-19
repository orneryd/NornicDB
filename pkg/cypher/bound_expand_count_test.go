package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// boundExpandGraph: a (X, k 1) with parallel, opposite, self and multi-type
// relationships to b (Y, k 2), c (X, k 3) and d (Y), and more relationships
// of its own (to f1..f5) than b has, so a bound b is the cheaper end.
func boundExpandGraph(t testing.TB) (*StorageExecutor, storage.Engine) {
	exec, store := newTestExecutor(t)
	nodes := []*storage.Node{
		{ID: "a", Labels: []string{"X"}, Properties: map[string]interface{}{"k": int64(1)}},
		{ID: "b", Labels: []string{"Y"}, Properties: map[string]interface{}{"k": int64(2)}},
		{ID: "c", Labels: []string{"X"}, Properties: map[string]interface{}{"k": int64(3)}},
		{ID: "d", Labels: []string{"Y"}},
	}
	edges := []*storage.Edge{
		{ID: "e1", Type: "T", StartNode: "a", EndNode: "b", Properties: map[string]interface{}{"w": int64(1)}},
		{ID: "e2", Type: "T", StartNode: "a", EndNode: "b", Properties: map[string]interface{}{"w": int64(2)}},
		{ID: "e3", Type: "U", StartNode: "b", EndNode: "a", Properties: map[string]interface{}{"w": int64(1)}},
		{ID: "e4", Type: "T", StartNode: "a", EndNode: "a"},
		{ID: "e5", Type: "T", StartNode: "c", EndNode: "a", Properties: map[string]interface{}{"w": int64(1)}},
		{ID: "e6", Type: "U", StartNode: "a", EndNode: "c"},
		{ID: "e7", Type: "T", StartNode: "d", EndNode: "b"},
	}
	for i := 1; i <= 5; i++ {
		id := storage.NodeID(fmt.Sprintf("f%d", i))
		nodes = append(nodes, &storage.Node{ID: id, Labels: []string{"F"}})
		edges = append(edges, &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("v%d", i)), Type: "V", StartNode: "a", EndNode: id})
	}
	require.NoError(t, store.BulkCreateNodes(nodes))
	require.NoError(t, store.BulkCreateEdges(edges))
	return exec, store
}

// The expand-into count of a one-relationship EXISTS / COUNT body is the
// traversal's count for every direction, type list, property map, label,
// WHERE, bound or unbound end, bound relationship and bound null.

// A chain longer than one relationship, a variable length and a quantified
// body stay on the traversal.

// The holder-read shapes BenchmarkBoundExpandHolderRead times give their
// answers.

// End to end, [NOT] EXISTS and COUNT over one relationship give, for every
// pair of nodes, what a pattern comprehension over the same pattern gives.
func TestBoundExpandExistsMatchesPatternComprehension(t *testing.T) {
	exec, _ := boundExpandGraph(t)
	ctx := context.Background()
	for _, pattern := range []struct{ pattern, where string }{
		{"(x)-[r:T]->(y)", ""},
		{"(x)-[r]-(y)", "r.w > 1"},
		{"(x)<-[r:T|U]-(y)", ""},
		{"(x)-[r:T {w: 1}]-(y:Y)", ""},
		{"(x)-[r]-(y)", "y.k = 2"},
		{"(x)-[r]-(y)", "startNode(r).k = 1 AND coalesce(endNode(r).k, 0) <> startNode(r).k"},
		{"(x)-[r]-(y)", "endNode(r) = y AND startNode(r) = x"},
	} {
		where := ""
		if pattern.where != "" {
			where = " WHERE " + pattern.where
		}
		query := fmt.Sprintf(`MATCH (x), (y) WHERE NOT x:F AND NOT y:F
RETURN x.k AS xk, y.k AS yk, labels(x) AS xl, labels(y) AS yl,
       EXISTS { MATCH %[1]s%[2]s } AS e, NOT EXISTS { MATCH %[1]s%[2]s } AS ne,
       COUNT { MATCH %[1]s%[2]s } AS c, size([%[1]s%[2]s | r]) AS want
ORDER BY xk, yk, xl, yl`, pattern.pattern, where)
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 16, query)
		for _, row := range result.Rows {
			want := row[7].(int64)
			require.Equal(t, want > 0, row[4], "%s %v", query, row)
			require.Equal(t, want == 0, row[5], "%s %v", query, row)
			require.Equal(t, want, row[6], "%s %v", query, row)
		}
	}
}
