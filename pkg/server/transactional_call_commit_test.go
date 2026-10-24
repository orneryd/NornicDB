package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPTransactionalCallKeepsCommittedBatches(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	query := func(statement string) TransactionResponse {
		t.Helper()
		response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, token)
		require.Equal(t, http.StatusOK, response.Code)
		var result TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		return result
	}
	require.NotEmpty(t, query("UNWIND [1,2,0] AS x CALL { WITH x CREATE (:B {v: 10/x}) } IN TRANSACTIONS OF 1 ROW").Errors)
	stored := query("MATCH (n:B) RETURN n.v ORDER BY n.v")
	require.Empty(t, stored.Errors)
	require.Len(t, stored.Results, 1)
	require.Len(t, stored.Results[0].Data, 2)
	require.Equal(t, []any{float64(5)}, stored.Results[0].Data[0].Row)
	require.Equal(t, []any{float64(10)}, stored.Results[0].Data[1].Row)
}

func TestHTTPTransactionalCallBatchSizeExpressionContract(t *testing.T) {
	cases := []struct {
		name, size string
		invalid    bool
		params     map[string]any
		errorCode  string
	}{
		{"parameter_rows", "$n ROWS", false, map[string]any{"n": int64(1)}, ""},
		{"parameter_row", "$n ROW", false, map[string]any{"n": int64(1)}, ""},
		{"addition", "1+0 ROWS", false, nil, ""},
		{"parentheses", "(1) ROWS", false, nil, ""},
		{"function", "toInteger(1) ROWS", false, nil, ""},
		{"zero", "0 ROWS", true, nil, ""},
		{"negative", "-1 ROWS", true, nil, ""},
		{"fraction", "1.5 ROWS", true, nil, ""},
		{"integral_float", "1.0 ROWS", true, nil, ""},
		{"expression_zero", "1-1 ROWS", true, nil, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_zero", "$n ROWS", true, map[string]any{"n": int64(0)}, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_negative", "$n ROWS", true, map[string]any{"n": int64(-1)}, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_float", "$n ROWS", true, map[string]any{"n": 1.5}, "Neo.ClientError.Statement.ArgumentError"},
		{"parameter_null", "$n ROWS", true, map[string]any{"n": nil}, "Neo.ClientError.Statement.TypeError"},
		{"parameter_string", "$n ROWS", true, map[string]any{"n": "1"}, "Neo.ClientError.Statement.SyntaxError"},
	}

	for _, sample := range cases {
		t.Run(sample.name, func(t *testing.T) {
			server, authenticator := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			query := func(statement string, params map[string]any) TransactionResponse {
				t.Helper()
				response := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": statement, "parameters": params}}}, token)
				require.Equal(t, http.StatusOK, response.Code)
				var result TransactionResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				return result
			}
			input := "[1,2,0,4]"
			code := "Neo.ClientError.Statement.ArithmeticError"
			if sample.invalid {
				input = "[1,2]"
				code = "Neo.ClientError.Statement.SyntaxError"
			}
			if sample.errorCode != "" {
				code = sample.errorCode
			}
			statement := "UNWIND " + input + " AS x CALL { WITH x CREATE (:BatchSizeRecord {v:12/x}) } IN TRANSACTIONS OF " + sample.size

			failed := query(statement, sample.params)
			require.Len(t, failed.Errors, 1)
			require.Equal(t, code, failed.Errors[0].Code)
			stored := query("MATCH (n:BatchSizeRecord) RETURN n.v ORDER BY n.v", nil)
			require.Empty(t, stored.Errors)
			require.Len(t, stored.Results, 1)
			if sample.invalid {
				require.Empty(t, stored.Results[0].Data)
			} else {
				require.Len(t, stored.Results[0].Data, 2)
				require.Equal(t, []any{float64(6)}, stored.Results[0].Data[0].Row)
				require.Equal(t, []any{float64(12)}, stored.Results[0].Data[1].Row)
			}
		})
	}
}
