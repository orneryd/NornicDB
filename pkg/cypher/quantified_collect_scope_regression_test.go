package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The HTTP sweep cases cfbe0322775d and b0fec1dd5bcb return one path on
// Neo4j 5.26.30. The local x is the current endpoint y, so the terminal
// endpoint (id 3) has no outgoing neighbour and must reject that step.
func TestQuantifiedCollectSubqueryUsesLocalEndpoint(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "quantified_collect_scope"))
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})-[:R]->(:Q {id: 2})-[:R]->(:Q {id: 3})", nil)
	require.NoError(t, err)
	for _, tc := range []struct{ name, query string }{
		{"group", "WITH {k: 1} AS m MATCH p = (s:Q {id: 1})((x)-[e:R]->(y) WHERE all(x IN [y] WHERE size(COLLECT { MATCH (x)-->(z) RETURN z.id }) > 0))+(t) RETURN count(p) AS c"},
		{"inline", "WITH {k: 1} AS m MATCH p = (s:Q {id: 1})((x)-[e:R WHERE all(x IN [y] WHERE size(COLLECT { MATCH (x)-->(z) RETURN z.id }) > 0)]->(y))+(t) RETURN count(p) AS c"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, tc.query)
		})
	}
	// The body sees the innermost row whichever local binder supplies it;
	// EXISTS/COUNT must agree with COLLECT when their bodies use the pipeline.
	for name, predicate := range map[string]string{
		"any":           "any(x IN [y] WHERE size(COLLECT { MATCH (x)-->(z) RETURN z.id }) > 0)",
		"comprehension": "size([x IN [y] WHERE size(COLLECT { MATCH (x)-->(z) RETURN z.id }) > 0 | x]) > 0",
		"reduce":        "reduce(found = false, x IN [y] | found OR size(COLLECT { MATCH (x)-->(z) RETURN z.id }) > 0)",
		"count":         "all(x IN [y] WHERE COUNT { MATCH (x)-->(z) RETURN z.id } > 0)",
		"exists":        "all(x IN [y] WHERE EXISTS { MATCH (x)-->(z) RETURN z.id })",
		"outer_value":   "all(x IN [y] WHERE size(COLLECT { MATCH (x)-->(z) WHERE z.id > m.k RETURN z.id }) > 0)",
	} {
		for _, group := range []string{
			"((x)-[e:R]->(y) WHERE PREDICATE)",
			"((x)-[e:R WHERE PREDICATE]->(y))",
		} {
			t.Run(name+"/"+group, func(t *testing.T) {
				query := "WITH {k: 1} AS m MATCH p = (s:Q {id: 1})" + strings.ReplaceAll(group, "PREDICATE", predicate) + "+(t) RETURN count(p) AS c"
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, query)
			})
		}
	}
}
