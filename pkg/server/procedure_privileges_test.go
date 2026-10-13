package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
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
