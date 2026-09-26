package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestOrderByReadsOnlyVariablesInScope pins the ORDER BY scope check of WITH
// and RETURN (#713's WITH ORDER BY section): a term's own bindings
// (comprehension, quantifier and reduce variables), keywords, subquery and
// map-projection bodies aren't variables, and a name that is neither in
// scope nor projected is Neo4j's "Variable `y` not defined". Expected rows
// are Neo4j 5.26.30's.
func TestOrderByReadsOnlyVariablesInScope(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "orderscope"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:OL {k: 'a', l: [1, 2, 3]})-[:R]->(:OL {k: 'b', l: [5]}), (:OL {k: 'c', l: []})", nil)
	require.NoError(t, err)
	for query, want := range map[string][][]interface{}{
		"MATCH (n:OL) WITH n ORDER BY size([x IN n.l WHERE x > 1]), n.k RETURN n.k AS k":          {{"c"}, {"b"}, {"a"}},
		"MATCH (n:OL) WITH n ORDER BY CASE WHEN n.k = 'a' THEN 0 ELSE 1 END, n.k RETURN n.k AS k": {{"a"}, {"b"}, {"c"}},
		"MATCH (n:OL) WITH n ORDER BY COUNT { MATCH (n)-->(m) } DESC, n.k RETURN n.k AS k":        {{"a"}, {"b"}, {"c"}},
		"MATCH (n:OL) WITH n ORDER BY reduce(s = 0, v IN n.l | s + v), n.k RETURN n.k AS k":       {{"c"}, {"b"}, {"a"}},
		"MATCH (n:OL) WITH n ORDER BY any(v IN n.l WHERE v > 2) DESC, n.k RETURN n.k AS k":        {{"a"}, {"b"}, {"c"}},
		"MATCH (n:OL) WITH n ORDER BY n.k IS NULL, n.k STARTS WITH 'a', n.k DESC RETURN n.k AS k": {{"c"}, {"b"}, {"a"}},
		"MATCH (n:OL) RETURN n.k AS k ORDER BY reduce(s = 0, v IN n.l | s + v) DESC, k":           {{"a"}, {"b"}, {"c"}},
		"MATCH (n:OL) WITH n ORDER BY n {.k}.k DESC RETURN n.k AS k":                              {{"c"}, {"b"}, {"a"}},
		"MATCH (n:OL) RETURN n.k AS k ORDER BY CASE WHEN n.k = 'c' THEN 0 ELSE 1 END, k":          {{"c"}, {"a"}, {"b"}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
	for _, query := range []string{
		"RETURN 1 AS x ORDER BY y",
		"WITH 0 AS z RETURN z AS x ORDER BY y",
		"UNWIND [1] AS z RETURN z ORDER BY y",
		"MATCH (n:OL) RETURN 1 AS x ORDER BY y",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "variable y is not defined", query)
	}
}
