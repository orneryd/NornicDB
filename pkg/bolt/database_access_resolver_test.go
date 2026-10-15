package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// A Bolt session's read and write on its database come from the production
// resolver (auth.ResolveDatabaseAccess over the stored privileges and role
// entitlements), through the one decision (cypher.MissingPermission), at
// the RUN check and in the executor alike, and they agree with HTTP: an
// editor whose role lost the write entitlement writes nothing, a role with
// a read privilege but no read entitlement reads nothing, a custom role
// with the read entitlement and no privilege entry reads, and a write-only
// privilege runs a statement that only writes.
func TestBoltReadAndWriteFromTheProductionResolver(t *testing.T) {
	ctx := context.Background()
	authenticator, adapter, server, stores := boltProductionFixture(t)
	for _, role := range []string{"plain", "analyst", "loader"} {
		require.NoError(t, stores.roles.CreateRole(ctx, role))
	}
	require.NoError(t, stores.privileges.SavePrivilege(ctx, "plain", "nornic", true, true))
	require.NoError(t, stores.privileges.SavePrivilege(ctx, "loader", "nornic", false, true))
	require.NoError(t, stores.entitlements.Set(ctx, "analyst", []string{"read"}))
	require.NoError(t, stores.entitlements.Set(ctx, "loader", []string{"read", "write"}))
	for user, role := range map[string]auth.Role{"ed": auth.RoleEditor, "pat": "plain", "ann": "analyst", "lou": "loader"} {
		_, err := authenticator.CreateUser(user, "password123", []auth.Role{role})
		require.NoError(t, err)
	}
	authorize := func(ctx context.Context, query string, _ map[string]any) error {
		return cypher.AuthorizeQuery(ctx, query)
	}
	run := func(user, query string) string {
		t.Helper()
		result, err := adapter.Authenticate("basic", user, "password123")
		require.NoError(t, err)
		executed, code := runOnBoltSession(t, server, result, query, authorize)
		if code == "" {
			require.True(t, executed, "%s: %s", user, query)
		}
		return code
	}
	const forbidden = "Neo.ClientError.Security.Forbidden"

	require.Empty(t, run("ed", "CREATE (:W)"))
	require.NoError(t, stores.entitlements.Set(ctx, "editor", []string{"read"}))
	require.Equal(t, forbidden, run("ed", "CREATE (:W)"), "the editor's write entitlement was removed")
	require.Empty(t, run("ed", "MATCH (n) RETURN n"))

	require.Equal(t, forbidden, run("pat", "MATCH (n) RETURN n"), "a read privilege without the read entitlement")
	require.Equal(t, forbidden, run("pat", "CREATE (:W)"))

	require.Empty(t, run("ann", "MATCH (n) RETURN n"), "the read entitlement without a privilege entry")
	require.Equal(t, forbidden, run("ann", "CREATE (:W)"))

	require.Empty(t, run("lou", "CREATE (:W)"), "a write-only privilege runs a statement that only writes")
	require.Equal(t, forbidden, run("lou", "MATCH (n) RETURN n"))
	require.Equal(t, forbidden, run("lou", "MERGE (:W)"))
}
