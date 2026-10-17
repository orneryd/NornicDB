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
		// Without a principal the database itself is refused, before the
		// statement's write permission is considered.
		require.Equal(t, "Database access is not allowed for user '' with no roles.", resp.Errors[0].Message)
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
		require.Equal(t, "Write operations are not allowed for user 'reader' with roles [reader].", resp.Errors[0].Message)
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
		server, _ := setupTestServer(t)
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
