package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Arithmetic in a function's argument is evaluated by the general expression
// evaluator: operators bind by precedence (+ - loosest, then * / %, then ^)
// and associate to the left, as in Neo4j (openCypher Return6 [16]).
func TestArithmeticPrecedenceInFunctionArguments(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "arithmetic_precedence"))
	ctx := context.Background()
	result, err := exec.Execute(ctx, "WITH 3 AS h, 2 AS g, 10 AS a UNWIND [7] AS x "+
		"RETURN abs(x / h - x / g) AS d, abs(a - 3 - 2) AS s, sign(8 / 2 / 2 - 2) AS q, abs(2 ^ 3 ^ 2) AS p, abs(a - -x) AS u, abs(1e-1 - 3) AS e, abs(a % 4 * 2) AS m", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), int64(5), int64(0), 64.0, int64(17), 2.9, int64(4)}}, result.Rows)

	_, err = exec.Execute(ctx, "CREATE (m {name: 'M'})-[:ATE {times: 6}]->(:F), (m)-[:ATE {times: 9}]->(:F)", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "MATCH (me)-[r1:ATE]->() WITH me, count(r1) AS h MATCH (me)-[r:ATE]->() RETURN sum(abs(r.times / h - r.times / 3)) AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}
