package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestProcedureYieldShadowingIsRejected verifies a procedure YIELD that names
// an already-bound variable fails with VariableAlreadyBound whatever clause
// bound it, as in Neo4j (TCK Call1 [15]), and that fresh names and aliases
// still work.
func TestProcedureYieldShadowingIsRejected(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "yieldshadow"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:YS {v: 1})", nil)
	require.NoError(t, err)
	for _, query := range []string{
		"WITH 'Hi' AS label CALL db.labels() YIELD label RETURN *",
		"UNWIND [1] AS label CALL db.labels() YIELD label RETURN label",
		"MATCH (label:YS) CALL db.labels() YIELD label RETURN label",
		"WITH 1 AS x CALL db.labels() YIELD label AS x RETURN x",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "shadows an existing variable", query)
	}
	for _, query := range []string{
		"WITH 'Hi' AS greeting CALL db.labels() YIELD label RETURN greeting, label",
		"WITH 'Hi' AS label CALL db.labels() YIELD label AS l RETURN label, l",
		"CALL db.labels() YIELD *",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
}
