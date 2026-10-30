package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// PrincipalAccess decides for HTTP and Bolt alike: a graph the principal may
// not use allows nothing; read and write come from the access to the
// database the graph's data is in; the other permissions from the global
// entitlements; with neither resolver every permission is open.
func TestPrincipalAccessAllows(t *testing.T) {
	access := PrincipalAccess{
		GraphAccess: func(graph string) (string, bool) {
			switch graph {
			case "alias":
				return "nornic", true
			case "nornic":
				return "nornic", true
			}
			return "", false
		},
		DatabaseAccess: func(database string) auth.ResolvedAccess {
			return auth.ResolvedAccess{Read: database == "nornic"}
		},
		Entitlement: func(permission string) bool { return permission == "schema" },
	}
	require.True(t, access.Allows("alias", "read"), "read on the alias's database")
	require.False(t, access.Allows("alias", "write"))
	require.True(t, access.Allows("nornic", "schema"))
	require.False(t, access.Allows("nornic", "admin"))
	require.False(t, access.Allows("other", "read"), "a graph the principal may not use")

	entitlementsOnly := PrincipalAccess{Entitlement: func(permission string) bool { return permission == "write" }}
	require.True(t, entitlementsOnly.Allows("any", "write"))
	require.False(t, entitlementsOnly.Allows("any", "read"))
	require.True(t, PrincipalAccess{}.Allows("any", "admin"))
}
