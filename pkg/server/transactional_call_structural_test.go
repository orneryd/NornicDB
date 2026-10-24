package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/orneryd/nornicdb/internal/transactionalcalltest"
	"github.com/stretchr/testify/require"
)

func TestHTTPTransactionalCallStructuralMatrix(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	query := func(t *testing.T, method, path, statement string, params map[string]any) (TransactionResponse, string) {
		t.Helper()
		statements := []map[string]any{}
		if statement != "" {
			statements = append(statements, map[string]any{"statement": statement, "parameters": params})
		}
		response := makeRequest(t, server, method, path, map[string]any{"statements": statements}, token)
		require.Less(t, response.Code, 400)
		var result TransactionResponse
		if response.Body.Len() != 0 {
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		}
		return result, response.Header().Get("Location")
	}
	transactionalcalltest.Check(t, func(t *testing.T, sample transactionalcalltest.Case) transactionalcalltest.Outcome {
		reset, _ := query(t, http.MethodPost, "/db/nornic/tx/commit", transactionalcalltest.Reset, nil)
		require.Empty(t, reset.Errors)
		target := "/db/nornic/tx/commit"
		if sample.Explicit {
			opened, location := query(t, http.MethodPost, "/db/nornic/tx", "", nil)
			require.Empty(t, opened.Errors)
			require.NotEmpty(t, location)
			parsed, err := url.Parse(location)
			require.NoError(t, err)
			target = parsed.Path
		}
		if sample.Before != "" {
			before, _ := query(t, http.MethodPost, target, sample.Before, nil)
			require.Empty(t, before.Errors)
		}
		result, _ := query(t, http.MethodPost, target, sample.Query, sample.Params)
		got := transactionalcalltest.Outcome{}
		if len(result.Errors) == 0 {
			for _, result := range result.Results {
				for _, row := range result.Data {
					got.Rows = append(got.Rows, row.Row)
				}
			}
		}
		if len(result.Errors) != 0 {
			require.Len(t, result.Errors, 1)
			got.Code = result.Errors[0].Code
		}
		if sample.Explicit {
			_ = makeRequest(t, server, http.MethodDelete, target, nil, token)
		}
		rows, _ := query(t, http.MethodPost, "/db/nornic/tx/commit", transactionalcalltest.Read, nil)
		require.Empty(t, rows.Errors)
		for _, result := range rows.Results {
			for _, row := range result.Data {
				got.Values = append(got.Values, int64(row.Row[0].(float64)))
			}
		}
		return got
	})
}
