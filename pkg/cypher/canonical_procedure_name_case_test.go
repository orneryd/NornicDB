package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Procedure names are case-sensitive, as in Neo4j. Authorization, the
// statement cache and execution resolve the same exact name, so a mis-cased
// CALL is no procedure everywhere: execution reports it as not found and no
// procedure's permission rule applies to it.
func TestCanonicalProcedureNameCaseIsExact(t *testing.T) {
	replaceProcedureRegistryForTest(t, NewProcedureRegistry())
	require.NoError(t, globalProcedureRegistry.RegisterUser(
		ProcedureSpec{Name: "test.Privileged", Mode: ProcedureModeWrite, Admin: true, Returns: []ProcedureColumn{{Name: "ok", Type: "BOOLEAN"}}},
		func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			return &ExecuteResult{Columns: []string{"ok"}, Rows: [][]interface{}{{true}}}, nil
		},
	))
	ctx := context.Background()
	exact := "CALL test.Privileged() YIELD ok RETURN ok"
	misCased := "CALL TEST.privileged() YIELD ok RETURN ok"

	require.Len(t, RegisteredProceduresForCalls(ctx, exact, RawSource), 1)
	exactRequirements := QueryPermissionRequirements(ctx, exact, RawSource)
	require.True(t, exactRequirements.Admin)
	require.True(t, exactRequirements.Write)
	require.True(t, globalProcedureRegistry.callsUserProcedure(exact))

	require.Empty(t, RegisteredProceduresForCalls(ctx, misCased, RawSource))
	misCasedRequirements := QueryPermissionRequirements(ctx, misCased, RawSource)
	require.False(t, misCasedRequirements.Admin)
	require.Empty(t, misCasedRequirements.BoostedProcedures)
	require.False(t, globalProcedureRegistry.callsUserProcedure(misCased))

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
		_, err := exec.Execute(ctx, mode+misCased, nil)
		code, _, _ := boundRuleError(err)
		require.Equal(t, "Neo.ClientError.Procedure.ProcedureNotFound", code, mode+misCased)
	}
}
