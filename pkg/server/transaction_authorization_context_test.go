package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestExplicitTransaction_UsesEffectiveGraphForAuthorization(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.dbManager.CreateDatabase("private"))
	require.NotNil(t, server.allowlistStore)
	require.NotNil(t, server.privilegesStore)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{dbName}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", dbName, true, false))

	viewerClaims := &auth.JWTClaims{Username: "reader", Roles: []string{"viewer"}}

	openReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx", strings.NewReader(`{"statements":[]}`))
	openReq = openReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, viewerClaims))
	openRec := httptest.NewRecorder()
	server.handleOpenTransaction(openRec, openReq, dbName)
	require.Equal(t, http.StatusCreated, openRec.Code)

	var openPayload map[string]interface{}
	require.NoError(t, json.NewDecoder(openRec.Body).Decode(&openPayload))
	commitURL, _ := openPayload["commit"].(string)
	require.NotEmpty(t, commitURL)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]
	require.NotEmpty(t, txID)

	// Prove the same principal can read the allowed transaction database:
	// a generic denial on every database must not satisfy the isolation check.
	allowedReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/"+txID, strings.NewReader(`{"statements":[{"statement":"MATCH (n:EffectiveGraphAuthorizationProbe) RETURN count(n) AS n"}]}`))
	allowedReq = allowedReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, viewerClaims))
	allowedRec := httptest.NewRecorder()
	server.handleExecuteInTransaction(allowedRec, allowedReq, dbName, txID)
	require.Equal(t, http.StatusOK, allowedRec.Code)
	var allowedPayload TransactionResponse
	require.NoError(t, json.NewDecoder(allowedRec.Body).Decode(&allowedPayload))
	require.Empty(t, allowedPayload.Errors)
	require.Len(t, allowedPayload.Results, 1)
	require.Len(t, allowedPayload.Results[0].Data, 1)
	require.Equal(t, []interface{}{float64(0)}, allowedPayload.Results[0].Data[0].Row)

	execReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/"+txID, strings.NewReader(`{"statements":[{"statement":"USE private MATCH (n:EffectiveGraphAuthorizationProbe) RETURN count(n) AS n"}]}`))
	execReq = execReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, viewerClaims))
	execRec := httptest.NewRecorder()
	server.handleExecuteInTransaction(execRec, execReq, dbName, txID)
	require.Equal(t, http.StatusOK, execRec.Code)

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(execRec.Body).Decode(&payload))
	errs, ok := payload["errors"].([]interface{})
	require.True(t, ok)
	require.NotEmpty(t, errs)
	require.Empty(t, payload["results"], "denied selection must return no graph data")
	firstErr, ok := errs[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", firstErr["code"])
	require.Equal(t, (&cypher.PermissionDeniedError{Permission: "read"}).StatusMessage(), firstErr["message"])
}

func TestInlineShellUseExecutesStatementAndChecksDatabase(t *testing.T) {
	server, users := setupTestServer(t)
	database := server.dbManager.DefaultDatabaseName()
	token := getAuthToken(t, users, "admin")
	for _, explicit := range []bool{false, true} {
		response, _ := systemProcedureResponse(t, server, token, database, ":USE "+database+" RETURN 'two  spaces' AS value", explicit)
		require.Empty(t, response.Errors)
		require.Len(t, response.Results, 1)
		require.Equal(t, []string{"value"}, response.Results[0].Columns)
		require.Len(t, response.Results[0].Data, 1)
		require.Equal(t, []interface{}{"two  spaces"}, response.Results[0].Data[0].Row)
	}
	require.NoError(t, server.dbManager.CreateDatabase("inline_target"))
	response, _ := systemProcedureResponse(t, server, token, database, ":USE inline_target CREATE (:InlineSelectionProof {value: 7}) RETURN 7 AS value", false)
	require.Empty(t, response.Errors)
	require.Len(t, response.Results, 1)
	require.Equal(t, []interface{}{float64(7)}, response.Results[0].Data[0].Row)
	for target, count := range map[string]int{database: 0, "inline_target": 1} {
		store, err := server.dbManager.GetStorage(target)
		require.NoError(t, err)
		nodes, err := store.GetNodesByLabel("InlineSelectionProof")
		require.NoError(t, err)
		require.Len(t, nodes, count, target)
	}
	for _, query := range []string{":USE missing_inline_graph RETURN 1 AS value", ":USE missing_inline_graph"} {
		response, _ := systemProcedureResponse(t, server, token, database, query, false)
		require.Len(t, response.Errors, 1, query)
		require.Equal(t, "Neo.ClientError.Database.DatabaseNotFound", response.Errors[0].Code, query)
		require.Empty(t, response.Results, query)
	}
}
