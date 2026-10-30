package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A subquery inside an AND / OR / XOR operand, a nested scope or an ORDER BY
// sees every outer value: a WITH projection, a list, a map member, a
// quantifier variable nested in a comprehension. Recorded on Neo4j 5.26.30;
// the first three are from a user's report.
func TestSubquerySeesOuterRowInEveryOperand(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "subquery_outer_row"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:D {id: 'a'})", nil)
	require.NoError(t, err)
	for query, want := range map[string]interface{}{
		"WITH ['a'] AS ids WHERE EXISTS { MATCH (d:D) WHERE d.id IN ids } RETURN count(*) AS n":                                 int64(1),
		"WITH ['a'] AS ids WHERE false OR EXISTS { MATCH (d:D) WHERE d.id IN ids } RETURN count(*) AS n":                        int64(1),
		"MATCH (x:D) WITH collect(x.id) AS ids WHERE false OR EXISTS { MATCH (d:D) WHERE d.id IN ids } RETURN count(*) AS n":    int64(1),
		"WITH 1 AS i WHERE false OR COUNT { MATCH (d:D) WHERE size(d.id) = i } > 0 RETURN count(*) AS n":                        int64(1),
		"WITH 1 AS i WHERE true AND COUNT { MATCH (d:D) WHERE size(d.id) = i } > 0 RETURN count(*) AS n":                        int64(1),
		"WITH 1 AS i WHERE COUNT { MATCH (d:D) WHERE size(d.id) = i } > 0 XOR false RETURN count(*) AS n":                       int64(1),
		"WITH {a: 'a'} AS m WHERE (m.a = 'z' OR COUNT { MATCH (d:D) WHERE d.id = m.a } > 0) RETURN count(*) AS n":               int64(1),
		"WITH 1 AS i RETURN i AS v ORDER BY [w IN [1, 5] | any(x IN [w] WHERE COUNT { MATCH (d:D) WHERE size(d.id) = x } > 0)]": int64(1),
		"WITH 1 AS i RETURN i AS v ORDER BY [w IN [1, 5] | any(x IN [w] WHERE x > 0)]":                                          int64(1),
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
	}
	_, err = exec.Execute(ctx, "WITH 1 AS i RETURN i AS v ORDER BY [w IN [1, 5] | any(x IN [w] WHERE zz > 0)]", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}
