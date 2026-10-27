package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Admitted catalog aliases are decoded identities. A composite that admits
// both " r " and "r" must select and authorize the registration the decoded
// selection actually names, on every route that resolves a decoded graph name:
// Cypher USE, Cypher graph.byName(), an HTTP request database and an HTTP
// explicit-transaction database.
//
// Both registrations authenticate as their own synthetic principal. A real
// remote server stores a different marker in each backing database. Query
// results and the authenticated registration counters independently prove which
// target was reached; neither is supplied from request parameters. Counters
// exclude transaction-lifecycle and capability requests.

const (
	decodedIdentityCatalogName = "identity_catalog"
	decodedIdentityAllowedName = "allowed"
	decodedIdentityDeniedName  = "denied"
	decodedIdentitySpacedAlias = " r "

	decodedIdentityAllowedPassword = "synthetic-allowed-password"
	decodedIdentityDeniedPassword  = "synthetic-denied-password"

	// The probe reads distinct stored data from the selected backing database.
	decodedIdentityProbe = `MATCH (n:AliasIdentityProbe) RETURN n.marker AS marker`
)

var decodedIdentityPasswords = map[string]string{
	decodedIdentityAllowedName: decodedIdentityAllowedPassword,
	decodedIdentityDeniedName:  decodedIdentityDeniedPassword,
}

// decodedIdentityCatalog is the fixture: a composite database admitting both
// " r " and "r" as distinct remote constituents, and an editor principal that
// may use the catalog and the " r " registration's database only.
type decodedIdentityCatalog struct {
	server  *Server
	token   string
	allowed *aliasIdentityRemote
	denied  *aliasIdentityRemote
}

func newDecodedIdentityCatalog(t *testing.T) *decodedIdentityCatalog {
	t.Helper()
	server, users := setupTestServer(t)
	_, err := users.CreateUser("alias-identity-editor", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, "alias-identity-editor")

	allowed := &aliasIdentityRemote{user: decodedIdentityAllowedName + "-client", marker: decodedIdentityAllowedName}
	denied := &aliasIdentityRemote{user: decodedIdentityDeniedName + "-client", marker: decodedIdentityDeniedName}
	uri := newIdentityRemoteServer(t, map[string]*aliasIdentityRemote{
		allowed.user: allowed,
		denied.user:  denied,
	}, decodedIdentityPasswords, "AliasIdentityProbe")

	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), decodedIdentityCatalogName, []multidb.ConstituentRef{
		{
			Alias: decodedIdentitySpacedAlias, DatabaseName: decodedIdentityAllowedName, Type: "remote",
			AccessMode: "read", URI: uri, AuthMode: "user_password",
			User: allowed.user, Password: decodedIdentityAllowedPassword,
		},
		{
			Alias: "r", DatabaseName: decodedIdentityDeniedName, Type: "remote",
			AccessMode: "read", URI: uri, AuthMode: "user_password",
			User: denied.user, Password: decodedIdentityDeniedPassword,
		},
	}))
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), string(auth.RoleEditor), []string{
		"nornic", decodedIdentityCatalogName, decodedIdentityAllowedName,
	}))
	return &decodedIdentityCatalog{server: server, token: token, allowed: allowed, denied: denied}
}

func (c *decodedIdentityCatalog) resetProbes() {
	c.allowed.mu.Lock()
	c.allowed.probes = 0
	c.allowed.mu.Unlock()
	c.denied.mu.Lock()
	c.denied.probes = 0
	c.denied.mu.Unlock()
}

func (c *decodedIdentityCatalog) commit(t *testing.T, route string, body map[string]any) TransactionResponse {
	t.Helper()
	response := makeRequest(t, c.server, http.MethodPost, route, body, c.token)
	var decoded TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded), response.Body.String())
	return decoded
}

// commitProbe runs one probe statement against database.
func (c *decodedIdentityCatalog) commitProbe(t *testing.T, database, statement string, params map[string]any) TransactionResponse {
	t.Helper()
	if params == nil {
		params = map[string]any{"principal": c.allowed.user}
	}
	return c.commit(t, "/db/"+url.PathEscape(database)+"/tx/commit", map[string]any{
		"statements": []any{map[string]any{"statement": statement, "parameters": params}},
	})
}

// requireMarker asserts exactly one statement result returned marker.
func requireMarker(t *testing.T, response TransactionResponse, marker string) {
	t.Helper()
	require.Empty(t, response.Errors, "the intended selection must succeed: %+v", response.Errors)
	require.Len(t, response.Results, 1)
	require.Len(t, response.Results[0].Data, 1, "the intended registration must answer the probe")
	assert.Equal(t, []any{marker}, response.Results[0].Data[0].Row)
}

// requireDenied asserts the selection is refused and that no probe reached any
// registration.
func requireDenied(t *testing.T, catalog *decodedIdentityCatalog, response TransactionResponse) {
	t.Helper()
	require.NotEmpty(t, response.Errors, "the principal must not select the denied registration")
	assert.Equal(t, "Neo.ClientError.Security.Forbidden", response.Errors[0].Code)
	assert.Zero(t, catalog.denied.observed(), "a denied selection must not dispatch a probe to the denied registration")
	assert.Zero(t, catalog.allowed.observed(), "a denied selection must not dispatch a probe to another registration")
}

// assertDecodedSelection runs one granted selection: it must answer the " r "
// registration's marker and must not reach the "r" registration.
func assertDecodedSelection(t *testing.T, catalog *decodedIdentityCatalog, granted func() TransactionResponse) {
	t.Helper()
	catalog.resetProbes()
	requireMarker(t, granted(), decodedIdentityAllowedName)
	assert.Positive(t, catalog.allowed.observed(), "the intended registration must receive the probe")
	assert.Zero(t, catalog.denied.observed(), "the probe must not reach another registration")
}

// TestDecodedCatalogIdentityStaticUse: a backticked USE keeps the alias's
// surrounding spaces as decoded identity, so it must select " r " and reach
// that registration; the plain alias USE names "r" and is denied.
func TestDecodedCatalogIdentityStaticUse(t *testing.T) {
	catalog := newDecodedIdentityCatalog(t)
	assertDecodedSelection(t, catalog, func() TransactionResponse {
		return catalog.commitProbe(t, "nornic", "USE `"+decodedIdentityCatalogName+"."+decodedIdentitySpacedAlias+"` "+decodedIdentityProbe, nil)
	})
	catalog.resetProbes()
	requireDenied(t, catalog, catalog.commitProbe(t, "nornic", "USE "+decodedIdentityCatalogName+".r "+decodedIdentityProbe, nil))
}

// TestDecodedCatalogIdentityDynamicUse: graph.byName() takes an already-decoded
// parameter value, so "identity_catalog. r " must select " r " and
// "identity_catalog.r" must select "r" (denied).
func TestDecodedCatalogIdentityDynamicUse(t *testing.T) {
	catalog := newDecodedIdentityCatalog(t)
	statement := "USE graph.byName($selected) " + decodedIdentityProbe
	requireDenied(t, catalog, catalog.commitProbe(t, decodedIdentityCatalogName, statement, map[string]any{
		"selected": decodedIdentityCatalogName + ".r",
	}))
	assertDecodedSelection(t, catalog, func() TransactionResponse {
		return catalog.commitProbe(t, decodedIdentityCatalogName, statement, map[string]any{
			"selected": decodedIdentityCatalogName + "." + decodedIdentitySpacedAlias,
		})
	})
	stored, err := catalog.server.dbManager.GetCompositeConstituents(decodedIdentityCatalogName)
	require.NoError(t, err)
	require.Equal(t, []string{decodedIdentitySpacedAlias, "r"}, []string{stored[0].Alias, stored[1].Alias}, "selection must preserve admitted catalog identities")
}

// TestDecodedCatalogIdentityRequestDatabase: an HTTP request database naming a
// decoded constituent is authorized as that registration and routed to it;
// the forbidden registration is denied before dispatch.
func TestDecodedCatalogIdentityRequestDatabase(t *testing.T) {
	catalog := newDecodedIdentityCatalog(t)
	assertDecodedSelection(t, catalog, func() TransactionResponse {
		return catalog.commitProbe(t, decodedIdentityCatalogName+"."+decodedIdentitySpacedAlias, decodedIdentityProbe, nil)
	})
	catalog.resetProbes()
	requireDenied(t, catalog, catalog.commitProbe(t, decodedIdentityCatalogName+".r", decodedIdentityProbe, map[string]any{
		"principal": catalog.denied.user,
	}))
}

// TestDecodedCatalogIdentityExplicitTransaction exercises supported explicit
// composite transactions: each statement keeps its decoded USE selection and
// the coordinator commits through the original composite transaction URL.
func TestDecodedCatalogIdentityExplicitTransaction(t *testing.T) {
	catalog := newDecodedIdentityCatalog(t)
	selection := decodedIdentityCatalogName + "." + decodedIdentitySpacedAlias
	statement := "USE graph.byName($selected) " + decodedIdentityProbe
	body := map[string]any{"statements": []any{map[string]any{"statement": statement, "parameters": map[string]any{"selected": selection}}}}
	catalog.resetProbes()
	opened := catalog.commit(t, "/db/"+decodedIdentityCatalogName+"/tx", body)
	requireMarker(t, opened, decodedIdentityAllowedName)
	require.NotEmpty(t, opened.Commit)
	commitURL, err := url.Parse(opened.Commit)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(commitURL.Path, "/db/"+decodedIdentityCatalogName+"/tx/"))
	executed := catalog.commit(t, strings.TrimSuffix(commitURL.EscapedPath(), "/commit"), body)
	requireMarker(t, executed, decodedIdentityAllowedName)
	committed := catalog.commit(t, commitURL.EscapedPath(), map[string]any{"statements": []any{}})
	require.Empty(t, committed.Errors)
	assert.Positive(t, catalog.allowed.observed())
	assert.Zero(t, catalog.denied.observed())
}

// TestDecodedCatalogIdentityForbiddenTransaction proves the transaction
// lifecycle refuses the forbidden registration before opening it, so no probe
// is dispatched.
func TestDecodedCatalogIdentityForbiddenTransaction(t *testing.T) {
	catalog := newDecodedIdentityCatalog(t)
	catalog.resetProbes()
	opened := catalog.commit(t, "/db/"+url.PathEscape(decodedIdentityCatalogName+".r")+"/tx", map[string]any{"statements": []any{}})
	requireDenied(t, catalog, opened)
}
