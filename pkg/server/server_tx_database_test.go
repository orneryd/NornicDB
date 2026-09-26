package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// TestRequestTransactionStaysOnItsDatabase: a transaction cannot span
// databases. A statement of a one-shot request of several statements, or of
// an explicit transaction, that targets another database with :USE or USE
// fails the transaction, and nothing it wrote is kept (#683, #738). A
// single-statement request still runs on the database its :USE names.
func TestRequestTransactionStaysOnItsDatabase(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()
	adminCtx := context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{Username: "admin", Roles: []string{"admin"}})
	request := func(statements ...string) *bytes.Reader {
		var body TransactionRequest
		for _, statement := range statements {
			body.Statements = append(body.Statements, StatementRequest{Statement: statement})
		}
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		return bytes.NewReader(encoded)
	}
	decode := func(rec *httptest.ResponseRecorder) TransactionResponse {
		var response TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), rec.Body.String())
		return response
	}
	post := func(db string, statements ...string) TransactionResponse {
		req := httptest.NewRequest(http.MethodPost, "/db/"+db+"/tx/commit", request(statements...)).WithContext(adminCtx)
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, db)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		return decode(rec)
	}
	count := func(db string) interface{} {
		response := post(db, "MATCH (n:TxDb) RETURN count(n) AS c")
		require.Empty(t, response.Errors)
		return response.Results[0].Data[0].Row[0]
	}
	clean := func() {
		for _, db := range []string{dbName, "txdbother"} {
			require.Empty(t, post(db, "MATCH (n:TxDb) DETACH DELETE n").Errors)
		}
	}
	require.Empty(t, post("system", "CREATE DATABASE txdbother").Errors)
	require.Empty(t, post("system", "CREATE ALIAS txdbhome FOR DATABASE "+dbName).Errors)

	const writeError = "Writing to more than one database per transaction is not allowed. Attempted write to txdbother, currently writing to "
	const readError = "Accessing more than one database per transaction is not allowed. Attempted access to txdbother"
	for name, tc := range map[string]struct {
		statement string
		message   string
	}{
		":USE write": {":USE txdbother\nCREATE (:TxDb {i: 2})", writeError + dbName},
		":USE read":  {":USE txdbother\nMATCH (n) RETURN count(n) AS c", readError},
		"USE write":  {"USE txdbother CREATE (:TxDb {i: 2})", writeError + dbName},
		"USE read":   {"USE txdbother MATCH (n) RETURN count(n) AS c", readError},
	} {
		// One-shot request of several statements.
		response := post(dbName, "CREATE (:TxDb {i: 1})", tc.statement, "CREATE (:TxDb {i: 3})")
		require.Len(t, response.Errors, 1, name)
		require.Equal(t, "Neo.ClientError.Statement.AccessMode", response.Errors[0].Code, name)
		require.Contains(t, response.Errors[0].Message, tc.message, name)
		require.Nil(t, response.Receipt, name)
		require.EqualValues(t, 0, count(dbName), name)
		require.EqualValues(t, 0, count("txdbother"), name)

		// Explicit transaction: the switching statement fails it, and the
		// transaction is gone.
		openReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx", request("CREATE (:TxDb {i: 1})")).WithContext(adminCtx)
		openRec := httptest.NewRecorder()
		server.handleOpenTransaction(openRec, openReq, dbName)
		require.Equal(t, http.StatusCreated, openRec.Code, name)
		commitURL := decode(openRec).Commit
		parts := strings.Split(commitURL, "/")
		txID := parts[len(parts)-2]
		execReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/"+txID, request(tc.statement)).WithContext(adminCtx)
		execRec := httptest.NewRecorder()
		server.handleExecuteInTransaction(execRec, execReq, dbName, txID)
		executed := decode(execRec)
		require.Len(t, executed.Errors, 1, name)
		require.Equal(t, "Neo.ClientError.Statement.AccessMode", executed.Errors[0].Code, name)
		commitReq := httptest.NewRequest(http.MethodPost, commitURL, request()).WithContext(adminCtx)
		commitRec := httptest.NewRecorder()
		server.handleCommitTransaction(commitRec, commitReq, dbName, txID)
		committed := decode(commitRec)
		require.Len(t, committed.Errors, 1, name)
		require.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", committed.Errors[0].Code, name)
		require.EqualValues(t, 0, count(dbName), name)
		require.EqualValues(t, 0, count("txdbother"), name)
	}

	// The transaction's own database, by name or alias, is not a switch.
	response := post(dbName, "CREATE (:TxDb {i: 1})", ":USE "+dbName+"\nCREATE (:TxDb {i: 2})", ":USE txdbhome\nCREATE (:TxDb {i: 3})")
	require.Empty(t, response.Errors)
	require.EqualValues(t, 3, count(dbName))
	clean()

	// A single statement runs, auto-committed, on the database it names.
	response = post(dbName, ":USE txdbother\nCREATE (:TxDb {i: 1})")
	require.Empty(t, response.Errors)
	require.EqualValues(t, 0, count(dbName))
	require.EqualValues(t, 1, count("txdbother"))
	clean()

	// A database that doesn't exist is DatabaseNotFound, as in Neo4j.
	req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", request("CREATE (:TxDb {i: 1})", ":USE txdbnosuch\nMATCH (n) RETURN n")).WithContext(adminCtx)
	rec := httptest.NewRecorder()
	server.handleImplicitTransaction(rec, req, dbName)
	response = decode(rec)
	require.Len(t, response.Errors, 1)
	require.Equal(t, "Neo.ClientError.Database.DatabaseNotFound", response.Errors[0].Code)
	require.EqualValues(t, 0, count(dbName))
}
