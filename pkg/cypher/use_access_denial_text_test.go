package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A USE that selects a graph through an alias or a composite, where the
// principal may not access the alias, its target, the composite or the
// constituent's database, is refused with Neo4j's database-access text, not
// the read-denial text: the statement reads nothing.
func TestUseDeniedAliasAndCompositeUseDatabaseAccessText(t *testing.T) {
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, mgr.CreateDatabase("tenantsecret"))
	require.NoError(t, mgr.CreateAlias("hidden_alias", "tenantsecret"))
	require.NoError(t, mgr.CreateDatabase("pother"))
	require.NoError(t, mgr.CreateCompositeDatabase("pcomp", []multidb.ConstituentRef{
		{Alias: "other", DatabaseName: "pother", Type: "local", AccessMode: "read_write"},
	}))
	session, err := mgr.GetStorage("nornic")
	require.NoError(t, err)
	exec := NewStorageExecutor(session)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})

	const want = "Database access is not allowed for user 'alice' with roles [reader]."
	for _, tc := range []struct{ query, denied string }{
		{"USE hidden_alias RETURN 1", "tenantsecret"},
		{"USE hidden_alias RETURN 1", "hidden_alias"},
		{"USE hidden_alias MATCH (n) RETURN n", "tenantsecret"},
		{"USE pcomp.other RETURN 1", "pother"},
		{"USE pcomp.other RETURN 1", "pcomp"},
		{":USE hidden_alias\nRETURN 1", "tenantsecret"},
	} {
		ctx := WithRequestIdentity(context.Background(), &RequestIdentity{User: &AuthenticatedUser{Name: "alice", Roles: []string{"reader"}}})
		ctx = WithDatabasePermissionResolver(ctx, "nornic", func(database, _ string) bool {
			return database != tc.denied
		})
		result, err := exec.Execute(ctx, tc.query, nil)
		require.Error(t, err, tc.query+" denied "+tc.denied)
		require.Nil(t, result)
		var status interface{ BoltErrorCode() string }
		require.True(t, errors.As(err, &status), tc.query)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", status.BoltErrorCode(), tc.query+" denied "+tc.denied)
		require.Equal(t, want, err.Error(), tc.query+" denied "+tc.denied)
	}
}

// requireDatabaseAccessDenied requires Neo4j's database-access refusal:
// Forbidden, naming the principal (DatabaseAccessDeniedError).
func requireDatabaseAccessDenied(t *testing.T, err error, msgAndArgs ...interface{}) {
	t.Helper()
	var status interface{ BoltErrorCode() string }
	require.True(t, errors.As(err, &status), msgAndArgs...)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", status.BoltErrorCode(), msgAndArgs...)
	require.Regexp(t, `^Database access is not allowed for user '.*' with (no roles|roles \[.*\])\.$`, err.Error(), msgAndArgs...)
}
