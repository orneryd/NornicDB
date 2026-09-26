package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCallTailClausesBetweenWithAndReturn pins a CALL tail whose WITH is
// followed by another clause before RETURN: the MATCH runs for every yielded
// row (#530). The compiled WITH … RETURN projection plan must not read the
// MATCH as part of the WITH's items. Expected rows are Neo4j 5.26.30's.
func TestCallTailClausesBetweenWithAndReturn(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "calltailclauses"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:IdQ {name:'withid', id:'p1'}), (:IdQ {name:'noid'})", nil)
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "CALL db.labels() YIELD label WITH label MATCH (n:IdQ) RETURN n.name AS name, n.id AS id ORDER BY name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"noid", nil}, {"withid", "p1"}}, result.Rows)

	result, err = exec.Execute(ctx, "CALL db.labels() YIELD label WITH label MATCH (n:IdQ) RETURN label, n.name AS name ORDER BY name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"IdQ", "noid"}, {"IdQ", "withid"}}, result.Rows)

	// A WITH … WHERE … RETURN tail keeps its compiled plan: STARTS WITH is an
	// operator, not a clause.
	result, err = exec.Execute(ctx, "CALL db.labels() YIELD label WITH label WHERE label STARTS WITH 'Id' RETURN label", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"IdQ"}}, result.Rows)
}
