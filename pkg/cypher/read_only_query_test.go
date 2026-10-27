package cypher

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReadOnlyQueryUsesLiveProcedurePolicyAndCompleteSource(t *testing.T) {
	registry := NewProcedureRegistry()
	replaceProcedureRegistryForTest(t, registry)
	handler := func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		t.Fatal("read-only admission executed procedure")
		return nil, nil
	}
	const query = "CALL watcher_policy.live()"
	for _, spec := range []ProcedureSpec{
		{Name: "watcher_policy.live", Mode: ProcedureModeRead},
		{Name: "watcher_policy.live", Mode: ProcedureModeWrite},
		{Name: "watcher_policy.live", Mode: ProcedureModeRead, Admin: true},
		{Name: "watcher_policy.live", Mode: ProcedureModeSchema},
		{Name: "watcher_policy.live", Mode: ProcedureModeRead, ExecutesStatements: true},
		{Name: "watcher_policy.live", Mode: ProcedureModeRead},
	} {
		require.NoError(t, registry.RegisterUser(spec, handler))
		err := AuthorizeReadOnlyQuery(t.Context(), query)
		if spec.Mode == ProcedureModeRead && !spec.Admin && !spec.ExecutesStatements {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
	for _, source := range []string{"RETURN 1", "EXPLAIN CREATE (n)", "EXPLAIN CALL unknown.opaque()", "EXPLAIN CREATE (n); RETURN 1"} {
		require.NoError(t, AuthorizeReadOnlyQuery(t.Context(), source), source)
	}
	for _, source := range []string{"", "/* no query */", ";", "EXPLAIN", "PROFILE", "CYPHER 25", "RETURN 1; EXPLAIN", "RETURN (", "CALL unknown.opaque()", "PROFILE CREATE (n)", "EXPLAIN CREATE (n); CREATE (m)", "EXPLAIN RETURN 1; CALL unknown.opaque()", "RETURN 1; CREATE (n)", "CREATE USER ordinary SET PASSWORD 'synthetic'", "SHOW USERS", "SHOW TRANSACTIONS", "EXPLAIN CREATE (n); SHOW USERS"} {
		t.Run("reject/"+source, func(t *testing.T) { require.Error(t, AuthorizeReadOnlyQuery(t.Context(), source)) })
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, AuthorizeReadOnlyQuery(ctx, "RETURN 1"), context.Canceled)
}

func TestReadOnlyQueryProjectsRawUnicodeOnce(t *testing.T) {
	require.NoError(t, AuthorizeReadOnlyQuery(t.Context(), `\u0052ETURN {a:1}`))
	require.Error(t, AuthorizeReadOnlyQuery(t.Context(), `\u0043REATE (n)`))
	require.Error(t, AuthorizeReadOnlyQuery(t.Context(), `RETURN \u00zz`))
}
