package bolt

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// resolvingAuthenticator resolves impersonation targets from users.
type resolvingAuthenticator struct {
	mockBoltAuthenticator
	users map[string]*BoltAuthResult
}

func (a *resolvingAuthenticator) ImpersonationAuthResult(username string) (*BoltAuthResult, error) {
	if user, ok := a.users[username]; ok {
		return user, nil
	}
	return nil, errors.New("no such user")
}

// imp_user is decided in Neo4j's order: an authenticator that can't resolve
// users can't impersonate (ArgumentError); without a signer, or for a user
// that doesn't exist, the request is refused as one the signer may not make;
// the signer itself is no impersonation.
func TestResolveImpersonationEdges(t *testing.T) {
	session := &Session{version: BoltV4_4, server: &Server{config: &Config{Authenticator: &mockBoltAuthenticator{}}}}
	_, failure := session.resolveImpersonation(map[string]any{impersonatedUserField: "bob"})
	require.NotNil(t, failure)
	require.Equal(t, "Neo.ClientError.Statement.ArgumentError", failure.code)
	require.Equal(t, localization.BoltImpersonationUnsupported(), failure.message)

	resolver := &resolvingAuthenticator{users: map[string]*BoltAuthResult{"bob": {Authenticated: true, Username: "bob"}}}
	session.server.config.Authenticator = resolver
	_, failure = session.resolveImpersonation(map[string]any{impersonatedUserField: "bob"})
	require.NotNil(t, failure, "no signer")
	require.Equal(t, "Neo.ClientError.Security.Forbidden", failure.code)

	session.authResult = &BoltAuthResult{Authenticated: true, Username: "svc", Roles: []string{"admin"}}
	target, failure := session.resolveImpersonation(map[string]any{impersonatedUserField: "svc"})
	require.Nil(t, failure)
	require.Nil(t, target, "the signer itself is no impersonation")
	session.server.SetImpersonationResolver(func([]string, bool, string) bool { return true })
	_, failure = session.resolveImpersonation(map[string]any{impersonatedUserField: "ghost"})
	require.NotNil(t, failure, "an unknown user is refused as a forbidden one")
	require.Equal(t, localization.BoltImpersonationForbidden("ghost"), failure.message)
}

// A ROUTE message's extra dictionary follows its routing context and
// bookmarks; a message that ends early or isn't shaped so has none.
func TestRouteExtra(t *testing.T) {
	context := encodePackStreamMap(map[string]any{"address": "localhost:7687"})
	bookmarks := encodePackStreamList([]any{})
	extra := encodePackStreamMap(map[string]any{"db": "nornic", impersonatedUserField: "bob"})
	require.Nil(t, routeExtra(nil))
	require.Nil(t, routeExtra(encodePackStreamString("not a map")))
	require.Nil(t, routeExtra(context), "no bookmarks")
	require.Nil(t, routeExtra(append(append([]byte{}, context...), encodePackStreamString("x")...)), "bookmarks not a list")
	require.Nil(t, routeExtra(append(append([]byte{}, context...), bookmarks...)), "no extra (Bolt 4.3 sends a name)")
	require.Nil(t, routeExtra(append(append(append([]byte{}, context...), bookmarks...), encodePackStreamString("nornic")...)))
	require.Equal(t, map[string]any{"db": "nornic", impersonatedUserField: "bob"},
		routeExtra(append(append(append([]byte{}, context...), bookmarks...), extra...)))
}

// Without a principal the access refusal names a user with no name and no
// roles; IMPERSONATE needs a server, a signer and its resolver.
func TestImpersonationAllowedForAndRefusalPrincipal(t *testing.T) {
	require.Equal(t, localization.DatabaseAccessDenied("", nil), databaseAccessDenied(nil))
	require.Equal(t, localization.DatabaseAccessDenied("bob", []string{"reader"}),
		databaseAccessDenied(&BoltAuthResult{Username: "bob", Roles: []string{"reader"}}))

	require.False(t, (&Session{}).impersonationAllowedFor("bob"), "no server")
	session := &Session{server: &Server{config: DefaultConfig()}}
	require.False(t, session.impersonationAllowedFor("bob"), "no signer")
	session.authResult = &BoltAuthResult{Authenticated: true, Username: "svc", Roles: []string{"svc"}}
	require.False(t, session.impersonationAllowedFor("bob"), "no resolver")
	var asked []string
	session.server.SetImpersonationResolver(func(roles []string, admin bool, user string) bool {
		asked = append(asked, user)
		return user == "bob" && !admin && roles[0] == "svc"
	})
	require.True(t, session.impersonationAllowedFor("bob"))
	require.False(t, session.impersonationAllowedFor("carol"))
	require.Equal(t, []string{"bob", "carol"}, asked)
}

// A record needs entity attribution only on a composite: a standard database
// (or an alias of one) gets none, a composite a resolver, and a resolver the
// caller already holds is used as given.
func TestRecordEntityDatabaseResolverByDatabaseKind(t *testing.T) {
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	require.NoError(t, mgr.CreateDatabase("tenant"))
	require.NoError(t, mgr.CreateAlias("tenant_alias", "tenant"))
	require.NoError(t, mgr.CreateCompositeDatabase("combined", []multidb.ConstituentRef{{Alias: "t", DatabaseName: "tenant", Type: "local", AccessMode: "read"}}))

	require.Nil(t, (&Session{}).recordEntityDatabaseResolver("tenant"), "no server")
	session := &Session{server: &Server{config: DefaultConfig(), dbManager: mgr}}
	require.Nil(t, session.recordEntityDatabaseResolver("tenant"))
	require.Nil(t, session.recordEntityDatabaseResolver("tenant_alias"))
	require.NotNil(t, session.recordEntityDatabaseResolver("combined"))
	given := boltEntityDatabaseResolver(func(storage.NodeID, storage.EdgeID) string { return "given" })
	require.Equal(t, "given", session.recordEntityDatabaseResolver("combined", given)("", ""))
}
