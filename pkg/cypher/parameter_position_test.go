package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A $parameter where an operator or a name must come is Neo4j 5.26.30's
// SyntaxError ("Invalid input '$'"), even when the parameter isn't given
// (#907); a node or relationship pattern's property map parameter and the
// keywords an operand may follow are unchanged.
func TestParameterPosition(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "parameter_position"))
	ctx := context.Background()
	for _, query := range []string{
		"WITH 1 AS a RETURN a $b AS c",
		"UNWIND [1] AS $x RETURN 1",
		"WITH 1 AS a RETURN a AS $b",
	} {
		_, err := exec.Execute(ctx, query, map[string]interface{}{"b": 1, "x": 1})
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
		_, err = exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	params := map[string]interface{}{"props": map[string]interface{}{"k": int64(1)}, "l": []interface{}{int64(1), int64(2)}, "n": int64(1)}
	for query, want := range map[string][][]interface{}{
		"CREATE (n:PP $props) RETURN n.k":                                          {{int64(1)}},
		"CREATE (n $props) RETURN n.k":                                             {{int64(1)}},
		"CREATE (:PP)-[r:R $props]->(:PP) RETURN r.k":                              {{int64(1)}},
		"CREATE (a $props)-[:R $props]->(b:PP $props) RETURN a.k, b.k":             {{int64(1), int64(1)}},
		"CREATE ($props) RETURN 1":                                                 {{int64(1)}},
		"UNWIND $l AS x WITH x WHERE x IN $l RETURN x ORDER BY x SKIP $n LIMIT $n": {{int64(2)}},
		"RETURN CASE WHEN $n > 0 THEN $n ELSE $n END AS v":                         {{int64(1)}},
		"WITH $n AS x RETURN DISTINCT $n AS v":                                     {{int64(1)}},
	} {
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}
