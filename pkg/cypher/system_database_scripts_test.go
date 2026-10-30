package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// On the system database a BEGIN … COMMIT script runs only what its body
// would run on its own, and a procedure call whose RETURN reads the graph is
// refused, as Neo4j 5.26.30 refuses it.
func TestSystemDatabaseChecksScriptBodiesAndProcedureResults(t *testing.T) {
	ctx := context.Background()
	inner := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = inner.Close() })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(inner, "system"))

	for _, refused := range []struct{ statement, message string }{
		{"BEGIN\nRETURN 1 AS one\nCOMMIT", "Not a recognised system command or procedure"},
		{"BEGIN TRANSACTION\nCREATE (:_ProcedurePrivilege {role: 'editor'})\nCOMMIT", "not supported on the system database"},
		{"CALL db.ping() YIELD success RETURN COUNT { (n) } AS c", "not supported on the system database"},
		{"CALL db.ping() YIELD success RETURN EXISTS { (n) } AS e", "not supported on the system database"},
	} {
		_, err := exec.Execute(ctx, refused.statement, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")
		require.ErrorContains(t, err, refused.message, refused.statement)
	}
	nodes, err := inner.AllNodes()
	require.NoError(t, err)
	require.Empty(t, nodes, "a refused script writes nothing")

	result, err := exec.Execute(ctx, "CALL db.ping() YIELD success RETURN success", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}
