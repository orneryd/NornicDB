package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPStatementFailureReportsRetiredTransaction(t *testing.T) {
	for _, route := range []string{"begin", "execute"} {
		t.Run(route, func(t *testing.T) {
			server, auth := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, auth, "admin")
			path := "/db/" + server.dbManager.DefaultDatabaseName() + "/tx"
			if route == "execute" {
				opened := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": []any{}}, token)
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(opened.Body.Bytes(), &response))
				require.Empty(t, response.Errors)
				require.NotNil(t, response.Transaction)
				u, err := url.Parse(response.Commit)
				require.NoError(t, err)
				path = strings.TrimSuffix(u.Path, "/commit")
			}
			failed := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": []map[string]any{{"statement": "RETURN 1 / 0"}}}, token)
			var response TransactionResponse
			require.NoError(t, json.Unmarshal(failed.Body.Bytes(), &response))
			require.Len(t, response.Errors, 1)
			require.Equal(t, "Neo.ClientError.Statement.ArithmeticError", response.Errors[0].Code)
			require.Nil(t, response.Transaction, "HTTP transaction key must be absent after retirement")
			require.Empty(t, response.Commit, "retired transaction has no usable commit URL")
			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(failed.Body.Bytes(), &raw))
			require.NotContains(t, raw, "transaction")
		})
	}
}
