package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// TestOneShotCommitIsOneTransaction: a one-shot /tx/commit request of several
// statements is one transaction, as in Neo4j. A failing statement rolls back
// the ones before it, and the response reports no receipt or created IDs for
// the discarded writes (#683).
func TestOneShotCommitIsOneTransaction(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()
	adminCtx := context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{Username: "admin", Roles: []string{"admin"}})
	post := func(db string, statements ...string) TransactionResponse {
		var request TransactionRequest
		for _, statement := range statements {
			request.Statements = append(request.Statements, StatementRequest{Statement: statement})
		}
		body, err := json.Marshal(request)
		require.NoError(t, err)
		req := httptest.NewRequest(http.MethodPost, "/db/"+db+"/tx/commit", bytes.NewReader(body)).WithContext(adminCtx)
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, db)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		return response
	}
	count := func() interface{} {
		response := post(dbName, "MATCH (n:OneShot) RETURN count(n) AS c")
		require.Empty(t, response.Errors)
		return response.Results[0].Data[0].Row[0]
	}

	for name, statements := range map[string][]string{
		"runtime error":            {"CREATE (:OneShot {i: 1})", "RETURN 1 / 0", "CREATE (:OneShot {i: 3})"},
		"syntax error":             {"CREATE (:OneShot {i: 1})", "RETURN RETURN", "CREATE (:OneShot {i: 3})"},
		"error in the last":        {"CREATE (:OneShot {i: 1})", "CREATE (:OneShot {i: 2})", "RETURN 1 / 0"},
		"empty then error":         {"CREATE (:OneShot {i: 1})", "", "RETURN 1 / 0"},
		"empty statement":          {"CREATE (:OneShot {i: 1})", ""},
		"comment only":             {"CREATE (:OneShot {i: 1})", "// nothing here"},
		"error after a MERGE, SET": {"MERGE (n:OneShot {i: 1}) SET n.x = 1", "RETURN 1 / 0"},
	} {
		response := post(dbName, statements...)
		require.Len(t, response.Errors, 1, name)
		require.Nil(t, response.Receipt, name)
		require.Nil(t, response.Optimistic, name)
		require.EqualValues(t, 0, count(), name)
	}
	// An empty statement is Neo4j's SyntaxError.
	response := post(dbName, "   ")
	require.Len(t, response.Errors, 1)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", response.Errors[0].Code)

	response = post(dbName, "CREATE (:OneShot {i: 1})", "CREATE (:OneShot {i: 2})")
	require.Empty(t, response.Errors)
	require.Len(t, response.Results, 2)
	require.EqualValues(t, 2, count())

	// Schema and administration commands still run several to a request.
	response = post(dbName, "CREATE INDEX one_shot_a IF NOT EXISTS FOR (n:OneShot) ON (n.a)", "CREATE INDEX one_shot_b IF NOT EXISTS FOR (n:OneShot) ON (n.b)")
	require.Empty(t, response.Errors)
	response = post("system", "CREATE DATABASE oneshota", "CREATE DATABASE oneshotb")
	require.Empty(t, response.Errors)
	require.Len(t, response.Results, 2)
}
