package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// An EXISTS / COUNT subquery sees the outer row's values: the variable of a
// quantifier (all / any / none / single) or a reduce, nested or inside a larger
// expression, and an outer variable read in a pattern property map
// ((n {id: r.prop})). Recorded on Neo4j 5.26.30.
func TestExistsSeesOuterValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "exists_outer_values"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:G {id:'a'}), (:G {id:'b'}), (:R {targets:['a','b'], prop:'a'})-[:L]->(:G {id:'c'})", nil)
	require.NoError(t, err)
	for query, want := range map[string]interface{}{
		"RETURN all(v IN ['a','b'] WHERE EXISTS { MATCH (n:G) WHERE n.id = v }) AS x":    true,
		"RETURN all(v IN ['a','z'] WHERE EXISTS { MATCH (n:G) WHERE n.id = v }) AS x":    false,
		"RETURN any(v IN ['z','b'] WHERE EXISTS { MATCH (n:G) WHERE n.id = v }) AS x":    true,
		"RETURN none(v IN ['z','y'] WHERE EXISTS { MATCH (n:G) WHERE n.id = v }) AS x":   true,
		"RETURN single(v IN ['a','z'] WHERE EXISTS { MATCH (n:G {id: v}) }) AS x":        true,
		"MATCH (r:R) RETURN EXISTS { MATCH (n:G {id: r.targets[0]}) } AS x":               true,
		"MATCH (r:R) RETURN EXISTS { MATCH (n:G {id: r.prop}) } AS x":                     true,
		"MATCH (r:R) RETURN EXISTS { MATCH (n:G {id: r.targets[1] + 'x'}) } AS x":         false,
		"MATCH (r:R)-[l:L]->() RETURN EXISTS { MATCH (n {id: startNode(l).prop}) } AS x": true,
		"MATCH (r:R) RETURN EXISTS { MATCH (n:G {id: r.prop})-[:L]-() } AS x":             false,
		"RETURN reduce(s = 0, v IN ['a','b','z'] | s + COUNT { MATCH (n:G) WHERE n.id = v }) AS x": int64(2),
		"RETURN [v IN ['a','z'] | any(w IN [v] WHERE EXISTS { MATCH (n:G) WHERE n.id = w })] AS x":  []interface{}{true, false},
		"MATCH (g:G {id:'b'}) RETURN any(v IN ['b'] WHERE EXISTS { MATCH (g) WHERE g.id = v }) AS x": true,
		"RETURN NOT any(v IN ['a'] WHERE EXISTS { MATCH (n:G {id: v}) }) AS x":                   false,
		"RETURN any(v IN ['a'] WHERE v = 'q' OR EXISTS { MATCH (n:G {id: v}) }) AS x":            true,
		"WITH 1 AS one RETURN one + size([v IN ['a','b'] WHERE EXISTS { MATCH (n:G {id: v}) }]) AS x": int64(3),
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
}
