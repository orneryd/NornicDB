package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// txErrorCode runs statement as token against database over HTTP and
// returns the first error's code, or "" when it succeeded.
func txErrorCode(t *testing.T, server *Server, database, token, statement string) string {
	t.Helper()
	resp := makeRequest(t, server, "POST", "/db/"+database+"/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{{"statement": statement}},
	}, "Bearer "+token)
	var body struct {
		Results []struct {
			Data []struct {
				Row []interface{} `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	if len(body.Errors) > 0 {
		t.Logf("%s on %s: %s %s", statement, database, body.Errors[0].Code, body.Errors[0].Message)
		return body.Errors[0].Code
	}
	return ""
}

// A non-admin login whose role holds GRANT EXECUTE BOOSTED PROCEDURE db.cdc.*
// may read the change feed, and nothing else that needs administrator rights;
// before the grant and after REVOKE it is refused; SHOW ROLE … PRIVILEGES
// lists the grant; only an administrator may grant.
func TestBoostedProcedureGrantOverHTTP(t *testing.T) {
	ctx := context.Background()
	server, authenticator := setupTestServer(t)
	require.NotNil(t, server.roleStore)
	require.NoError(t, server.roleStore.CreateRole(ctx, "cdc_reader"))
	require.NotNil(t, server.privilegesStore)
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "cdc_reader", "nornic", true, false))
	// The role reads (its global entitlement) and reads the one database.
	require.NotNil(t, server.roleEntitlementsStore)
	require.NoError(t, server.roleEntitlementsStore.Set(ctx, "cdc_reader", []string{string(auth.PermRead)}))
	_, err := authenticator.CreateUser("worker", "password123", []auth.Role{"cdc_reader"})
	require.NoError(t, err)
	admin := getAuthToken(t, authenticator, "admin")
	worker := getAuthToken(t, authenticator, "worker")
	const forbidden = "Neo.ClientError.Security.Forbidden"
	// A change-feed read chained after another procedure call.
	const chained = "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id, [], 5) YIELD id AS change RETURN count(*) AS n"

	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, "CALL db.cdc.query()"))
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, chained))
	require.Equal(t, forbidden, txErrorCode(t, server, "system", worker, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO cdc_reader"))

	require.Empty(t, txErrorCode(t, server, "system", admin, "ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'"))
	require.Empty(t, txErrorCode(t, server, "system", admin, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO cdc_reader"))
	require.Empty(t, txErrorCode(t, server, "nornic", worker, "CALL db.cdc.query()"))
	require.Empty(t, txErrorCode(t, server, "nornic", worker, "CALL db.cdc.current()"))
	require.Empty(t, txErrorCode(t, server, "nornic", worker, chained))
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, "CALL db.txlog.entries()"))
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, "CALL db.cdc.query() YIELD id CALL db.txlog.entries() YIELD txId RETURN count(*)"))
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, "SHOW USERS"))
	require.Equal(t, forbidden, txErrorCode(t, server, "system", worker, "SHOW PRIVILEGES"))

	resp := makeRequest(t, server, "POST", "/db/system/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{{"statement": "SHOW ROLE cdc_reader PRIVILEGES YIELD action, segment, role RETURN action, segment, role"}},
	}, "Bearer "+admin)
	var shown struct {
		Results []struct {
			Data []struct {
				Row []interface{} `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []interface{} `json:"errors"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&shown))
	require.Empty(t, shown.Errors)
	require.Len(t, shown.Results, 1)
	require.Len(t, shown.Results[0].Data, 1)
	require.Equal(t, []interface{}{"execute_boosted", "PROCEDURE(db.cdc.*)", "cdc_reader"}, shown.Results[0].Data[0].Row)

	require.Empty(t, txErrorCode(t, server, "system", admin, "REVOKE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS FROM cdc_reader"))
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, "CALL db.cdc.query()"))
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, chained))
}

// unreadableGrantsEngine fails the label lookup procedure grants are read
// with.
type unreadableGrantsEngine struct {
	storage.Engine
}

var errGrantsUnreadable = errors.New("grants unreadable")

func (unreadableGrantsEngine) GetNodesByLabel(string) ([]*storage.Node, error) {
	return nil, errGrantsUnreadable
}

// grantedWorker sets up role cdc_reader (read entitlement, read access on
// nornic) holding GRANT EXECUTE BOOSTED PROCEDURE db.cdc.*, a user worker
// with that role, and the change feed; it returns admin's and worker's
// tokens.
func grantedWorker(t *testing.T, server *Server, authenticator *auth.Authenticator) (admin, worker string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, server.roleStore.CreateRole(ctx, "cdc_reader"))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "cdc_reader", "nornic", true, false))
	require.NoError(t, server.roleEntitlementsStore.Set(ctx, "cdc_reader", []string{string(auth.PermRead)}))
	_, err := authenticator.CreateUser("worker", "password123", []auth.Role{"cdc_reader"})
	require.NoError(t, err)
	admin = getAuthToken(t, authenticator, "admin")
	worker = getAuthToken(t, authenticator, "worker")
	require.Empty(t, txErrorCode(t, server, "system", admin, "ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'"))
	require.Empty(t, txErrorCode(t, server, "system", admin, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO cdc_reader"))
	return admin, worker
}

// A grant covers procedures called in a CALL { … } subquery as well; a
// subquery or chained call to a procedure the grant doesn't cover is
// refused.
func TestBoostedProcedureGrantSubqueriesOverHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	_, worker := grantedWorker(t, server, authenticator)
	const forbidden = "Neo.ClientError.Security.Forbidden"

	require.Empty(t, txErrorCode(t, server, "nornic", worker, "CALL { CALL db.cdc.query() YIELD id RETURN id } RETURN count(*) AS n"))
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, "CALL { CALL db.txlog.entries() YIELD txId RETURN txId } RETURN count(*) AS n"))
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, "CALL db.cdc.query() YIELD id CALL { CALL db.txlog.entries() YIELD txId RETURN txId } RETURN count(*) AS n"))
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, "CALL db.cdc.earliest() YIELD id CALL db.txlog.entries() YIELD txId RETURN count(*) AS n"))
}

// When the grants can't be read, a granted procedure is refused (the
// failure is logged), and so is it after the grant is revoked while the
// grants can't be read.
func TestBoostedProcedureGrantUnreadableOverHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	admin, worker := grantedWorker(t, server, authenticator)
	const forbidden = "Neo.ClientError.Security.Forbidden"
	require.Empty(t, txErrorCode(t, server, "nornic", worker, "CALL db.cdc.query()"))

	readable := server.procedurePrivileges
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	server.procedurePrivileges = auth.NewProcedurePrivilegeStore(unreadableGrantsEngine{Engine: system})
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, "CALL db.cdc.query()"))
	require.False(t, server.ProcedureGrantChecker([]string{"cdc_reader"})("db.cdc.query"))

	server.procedurePrivileges = readable
	require.Empty(t, txErrorCode(t, server, "system", admin, "REVOKE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS FROM cdc_reader"))
	server.procedurePrivileges = auth.NewProcedurePrivilegeStore(unreadableGrantsEngine{Engine: system})
	require.Equal(t, forbidden, txErrorCode(t, server, "nornic", worker, "CALL db.cdc.query()"))
}

// A grant reaches a user through the roles the user store gives the user,
// and the user's privileges are the union of those roles' (here read
// access from viewer, the boosted procedure from cdc_reader), as in Neo4j
// 5.26.30 Enterprise: a user with a role granted EXECUTE BOOSTED db.cdc.quer?
// and a role granted EXECUTE BOOSTED db.cdc.earliest gets both in SHOW
// PROCEDURES EXECUTABLE BY.
func TestBoostedProcedureGrantThroughUserRoles(t *testing.T) {
	ctx := context.Background()
	server, authenticator := setupTestServer(t)
	admin := getAuthToken(t, authenticator, "admin")
	require.NoError(t, server.roleStore.CreateRole(ctx, "cdc_reader"))
	require.Empty(t, txErrorCode(t, server, "system", admin, "ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'"))
	require.Empty(t, txErrorCode(t, server, "system", admin, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO cdc_reader"))
	_, err := authenticator.CreateUser("member", "password123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "nornic", getAuthToken(t, authenticator, "member"), "CALL db.cdc.query()"))

	require.NoError(t, authenticator.UpdateRoles("member", []auth.Role{auth.RoleViewer, "cdc_reader"}))
	require.Empty(t, txErrorCode(t, server, "nornic", getAuthToken(t, authenticator, "member"), "CALL db.cdc.query()"))
}

// Procedure grants are kept apart from graph data: on the system database
// a statement that writes graph data is refused (Neo4j 5.26.30 refuses
// CREATE on the system database with Neo.ClientError.Statement.SemanticError),
// for an editor and an administrator alike, so a node shaped like a grant
// can't be made there, and the editor stays refused after a later GRANT.
func TestSystemDatabaseGraphWritesOverHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	_, err := authenticator.CreateUser("editor_user", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	admin := getAuthToken(t, authenticator, "admin")
	editor := getAuthToken(t, authenticator, "editor_user")
	require.Empty(t, txErrorCode(t, server, "system", admin, "ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'"))

	for _, token := range []string{editor, admin} {
		require.Equal(t, "Neo.ClientError.Statement.SemanticError",
			txErrorCode(t, server, "system", token, "CREATE (:_ProcedurePrivilege {role: 'editor', procedure: '*', boosted: true})"))
	}
	require.Empty(t, txErrorCode(t, server, "system", admin, "GRANT EXECUTE BOOSTED PROCEDURE db.ping ON DBMS TO viewer"))
	for _, query := range []string{"CALL db.cdc.query()", "CALL db.txlog.entries()"} {
		require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "nornic", editor, query))
	}
	require.Empty(t, txErrorCode(t, server, "nornic", editor, "CREATE (:Note {text: 'user data'})"))
}

// A GRANT in an HTTP explicit transaction takes effect on commit, and a
// rollback undoes it.
func TestProcedureGrantFollowsHTTPTransaction(t *testing.T) {
	server, authenticator := setupTestServer(t)
	admin := getAuthToken(t, authenticator, "admin")
	open := func() string {
		t.Helper()
		resp := makeRequest(t, server, "POST", "/db/system/tx", map[string]interface{}{
			"statements": []map[string]interface{}{{"statement": "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer"}},
		}, "Bearer "+admin)
		var body TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		require.Empty(t, body.Errors)
		location, err := url.Parse(resp.Header().Get("Location"))
		require.NoError(t, err)
		require.NotEmpty(t, location.Path)
		return location.Path
	}
	end := func(method, path string) {
		t.Helper()
		resp := makeRequest(t, server, method, path, map[string]interface{}{"statements": []interface{}{}}, "Bearer "+admin)
		var body TransactionResponse
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
		require.Empty(t, body.Errors)
	}
	granted := func() bool { return server.ProcedureGrantChecker([]string{"viewer"})("db.cdc.query") }

	path := open()
	require.False(t, granted())
	end("DELETE", path)
	require.False(t, granted())

	path = open()
	require.False(t, granted())
	end("POST", path+"/commit")
	require.True(t, granted())
}

// Deleting a role through the API removes its procedure grants: a role
// created again under the name starts without them (Neo4j's DROP ROLE).
func TestDeleteRoleRemovesProcedureGrantsOverHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	admin := getAuthToken(t, authenticator, "admin")
	require.NoError(t, server.roleStore.CreateRole(context.Background(), "ops"))
	require.Empty(t, txErrorCode(t, server, "system", admin, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO ops"))
	resp := makeRequest(t, server, "DELETE", "/auth/roles/ops", nil, "Bearer "+admin)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	require.NoError(t, server.roleStore.CreateRole(context.Background(), "ops"))
	require.False(t, server.ProcedureGrantChecker([]string{"ops"})("db.cdc.query"))
}

// GRANT IMMUTABLE is refused while authentication is enabled
// (Neo.ClientError.Security.Forbidden, as Neo4j 5.26.30 Enterprise), and
// runs on a server whose authentication is disabled.
func TestImmutableProcedureGrantOverHTTP(t *testing.T) {
	server, authenticator := setupTestServer(t)
	admin := getAuthToken(t, authenticator, "admin")
	require.Equal(t, "Neo.ClientError.Security.Forbidden",
		txErrorCode(t, server, "system", admin, "GRANT IMMUTABLE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer"))

	database, err := nornicdb.Open(t.TempDir(), nornicdb.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = database.Close() })
	authStorage := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = authStorage.Close() })
	open, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: false}, authStorage)
	require.NoError(t, err)
	config := DefaultConfig()
	config.EmbeddingEnabled = false
	unauthenticated, err := New(database, open, config)
	require.NoError(t, err)
	t.Cleanup(func() { stopTestServer(t, unauthenticated) })
	resp := makeRequest(t, unauthenticated, "POST", "/db/system/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{{"statement": "GRANT IMMUTABLE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer"}},
	}, "")
	var body TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body), resp.Body.String())
	require.Empty(t, body.Errors)
	grants, err := unauthenticated.procedurePrivileges.List(context.Background())
	require.NoError(t, err)
	require.Equal(t, []auth.ProcedurePrivilege{{Role: "viewer", Procedure: "db.cdc.*", Boosted: true, Immutable: true}}, grants)
}

// Without a server, loaded grants or roles there is no grant check.
func TestProcedureGrantCheckerWithoutGrants(t *testing.T) {
	var nilServer *Server
	if nilServer.ProcedureGrantChecker([]string{"r"}) != nil {
		t.Fatal("a nil server has no grant check")
	}
	if (&Server{}).ProcedureGrantChecker([]string{"r"}) != nil {
		t.Fatal("a server without loaded grants has no grant check")
	}
}
