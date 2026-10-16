package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// heimdallPrincipal is a request context carrying a principal's permission
// resolver, as the server attaches it to Bifrost requests (withBifrostRBAC).
func heimdallPrincipal(allowed map[auth.Permission]bool) context.Context {
	return auth.WithRequestEntitlementResolver(context.Background(), func(_ string, permission auth.Permission) bool {
		return allowed[permission]
	})
}

// Heimdall's database router checks the request principal's permissions on
// the database it runs against: Discover needs read there, and Query runs
// its statement under the principal's resolver, so the executor refuses a
// read without read and a write without write, as on HTTP and Bolt.
func TestHeimdallRouterChecksDatabasePermissions(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)

	_, err := router.Query(context.Background(), "nornic", "CREATE (:HeimdallSeed {name: 'seed'})", nil)
	require.NoError(t, err)

	noAccess := heimdallPrincipal(nil)
	_, err = router.Discover(noAccess, "nornic", "seed", nil, 5, 0)
	require.ErrorContains(t, err, "nornic")
	_, err = router.Query(noAccess, "nornic", "MATCH (n:HeimdallSeed) RETURN n.name AS name", nil)
	require.ErrorContains(t, err, "nornic")

	writeOnly := heimdallPrincipal(map[auth.Permission]bool{"access": true, auth.PermWrite: true})
	_, err = router.Discover(writeOnly, "nornic", "seed", nil, 5, 0)
	require.ErrorContains(t, err, "nornic")
	_, err = router.Query(writeOnly, "nornic", "MATCH (n:HeimdallSeed) RETURN n.name AS name", nil)
	require.ErrorContains(t, err, "read permission")

	readOnly := heimdallPrincipal(map[auth.Permission]bool{"access": true, auth.PermRead: true})
	_, err = router.Discover(readOnly, "nornic", "seed", nil, 5, 0)
	require.NoError(t, err)
	rows, err := router.Query(readOnly, "nornic", "MATCH (n:HeimdallSeed) RETURN n.name AS name", nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	_, err = router.Query(readOnly, "nornic", "CREATE (:HeimdallDenied)", nil)
	require.ErrorContains(t, err, "write permission")

	rows, err = router.Query(context.Background(), "nornic", "MATCH (n:HeimdallDenied) RETURN count(n) AS n", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, rows[0]["n"])
}
