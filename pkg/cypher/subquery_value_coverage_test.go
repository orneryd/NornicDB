package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSubqueryValueRemainingBranches covers the subquery value paths the
// statement tests don't reach: a COLLECT body that fails at runtime, a body
// the pipeline declines that reads a statement parameter, NOT EXISTS over a
// scalar row value, and the aggregation ORDER BY … SKIP / LIMIT of the
// MATCH route (#652).
func TestSubqueryValueRemainingBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "sqvcov"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:SQ {v: 1}), (:SQ {v: 2}), (:SQ {v: 3})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (n:SQ) RETURN COLLECT { MATCH (m:SQ) RETURN m.v / 0 } AS c", nil)
	require.Error(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:SQ {v: 1}) RETURN COUNT { MATCH (m:SQ) CALL (m) { RETURN $p AS q } RETURN q } AS c", map[string]interface{}{"p": int64(7)})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, result.Rows)

	result, err = exec.Execute(ctx, "UNWIND [1, 5] AS x WITH x WHERE NOT EXISTS { MATCH (m:SQ) WHERE m.v = x } RETURN x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5)}}, result.Rows)

	result, err = exec.executeMatch(ctx, "MATCH (n:SQ) RETURN n.v AS v, count(*) AS c ORDER BY v DESC SKIP 0 LIMIT 2")
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3), int64(1)}, {int64(2), int64(1)}}, result.Rows)

	require.False(t, containsIdentifierWord("xn", "n"))
	require.True(t, containsIdentifierWord("x n", "n"))
	require.False(t, patternHasRelationship("(a {s: 'x->y', t: \"<-\"})"))
	require.True(t, patternHasRelationship("(a {s: 'x'})-->(b)"))
}
