package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// TestStatementTargetDatabase_UseClauseStaysOnRequestDatabase: a USE
// clause, well-formed or not, runs on the request's database, whose
// executor routes it and reports its errors (#738).
func TestStatementTargetDatabase_UseClauseStaysOnRequestDatabase(t *testing.T) {
	for _, statement := range []string{"USE graph.byName(", "USE `unterminated", "USE tenant RETURN 1"} {
		db, err := statementTargetDatabase("nornic", statement)
		require.NoError(t, err, statement)
		require.Equal(t, "nornic", db, statement)
	}
}

func TestNormalizeStatementForExecution_ErrorCoverage(t *testing.T) {
	t.Run("empty statement", func(t *testing.T) {
		dbName, query, err := normalizeStatementForExecution(" nornic ", "  ")
		require.NoError(t, err)
		require.Equal(t, "nornic", dbName)
		require.Equal(t, "", query)
	})

	t.Run("colon use missing database in multiline", func(t *testing.T) {
		_, _, err := normalizeStatementForExecution("nornic", ":USE\nMATCH (n) RETURN n")
		require.EqualError(t, err, ":USE requires a database name")
	})

	t.Run("a USE clause stays on the request database", func(t *testing.T) {
		dbName, query, err := normalizeStatementForExecution("nornic", "USE `unterminated")
		require.NoError(t, err)
		require.Equal(t, "nornic", dbName)
		require.Equal(t, "USE `unterminated", query)
	})
}

func TestHandleImplicitTransaction_AdditionalPermissionAndStatusBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	require.NotNil(t, server.auth)

	readBody := `{"statements":[{"statement":"MATCH (n) RETURN n LIMIT 1"}]}`
	writeBody := `{"statements":[{"statement":"CREATE (n:PermCov {id:'p1'})"}]}`

	t.Run("mutation denied when claims missing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(writeBody))
		rec := httptest.NewRecorder()

		server.handleImplicitTransaction(rec, req, "nornic")
		require.Equal(t, http.StatusOK, rec.Code)

		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.NotEmpty(t, resp.Errors)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", resp.Errors[0].Code)
		require.Contains(t, resp.Errors[0].Message, "Access to database 'nornic' is not allowed")
	})

	t.Run("mutation denied when role has no write privilege", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(writeBody))
		req = req.WithContext(context.WithValue(req.Context(), contextKeyClaims, &auth.JWTClaims{
			Username: "reader",
			Sub:      "reader",
			Roles:    []string{"reader"},
		}))
		rec := httptest.NewRecorder()

		server.handleImplicitTransaction(rec, req, "nornic")
		require.Equal(t, http.StatusOK, rec.Code)

		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.NotEmpty(t, resp.Errors)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", resp.Errors[0].Code)
		require.Contains(t, resp.Errors[0].Message, "Write on database 'nornic' is not allowed")
	})

	// An empty statement is Neo4j's SyntaxError (#683).
	t.Run("comment only statement is a syntax error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(`{"statements":[{"statement":"// comment only"}]}`))
		req = req.WithContext(context.WithValue(req.Context(), contextKeyClaims, &auth.JWTClaims{
			Username: "admin",
			Sub:      "admin",
			Roles:    []string{"admin"},
		}))
		rec := httptest.NewRecorder()

		server.handleImplicitTransaction(rec, req, "nornic")
		require.Equal(t, http.StatusOK, rec.Code)

		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Errors, 1)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", resp.Errors[0].Code)
		require.Empty(t, resp.Results)
	})

	t.Run("executor access failure maps to 500 infrastructure status", func(t *testing.T) {
		require.NoError(t, server.dbManager.SetDatabaseStatus("nornic", "offline"))
		t.Cleanup(func() {
			_ = server.dbManager.SetDatabaseStatus("nornic", "online")
		})

		req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(readBody))
		req = req.WithContext(context.WithValue(req.Context(), contextKeyClaims, &auth.JWTClaims{
			Username: "admin",
			Sub:      "admin",
			Roles:    []string{"admin"},
		}))
		rec := httptest.NewRecorder()

		server.handleImplicitTransaction(rec, req, "nornic")
		require.Equal(t, http.StatusInternalServerError, rec.Code)

		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.NotEmpty(t, resp.Errors)
		require.Equal(t, "Neo.ClientError.Database.General", resp.Errors[0].Code)
		require.Contains(t, resp.Errors[0].Message, "Failed to access database 'nornic'")
	})
}
