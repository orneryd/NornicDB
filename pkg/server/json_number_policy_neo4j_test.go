package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// HTTP transaction parameters follow Neo4j 5.26's one external number policy
// (the TrueNAS reference answers recorded in the PR): an out-of-range FLOAT is
// +/-Infinity and an INTEGER outside int64 is rejected with Neo4j's error.
func TestHTTPTransactionNumberPolicyMatchesNeo4j(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	host := httptest.NewServer(server.buildRouter())
	t.Cleanup(host.Close)
	post := func(t *testing.T, parameter string) map[string]any {
		t.Helper()
		body := `{"statements":[{"statement":"RETURN toString($v) AS s, valueType($v) AS t","parameters":{"v":` + parameter + `}}]}`
		request, err := http.NewRequest(http.MethodPost, host.URL+"/db/"+server.dbManager.DefaultDatabaseName()+"/tx/commit", strings.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		response, err := host.Client().Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
		raw, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(raw, &decoded), string(raw))
		return decoded
	}
	for _, tc := range []struct{ parameter, text, kind string }{
		{"1e400", "Infinity", "FLOAT NOT NULL"},
		{"-1e400", "-Infinity", "FLOAT NOT NULL"},
		{"1e308", "1.0E308", "FLOAT NOT NULL"},
		{"9007199254740993", "9007199254740993", "INTEGER NOT NULL"},
		{"2.0", "2.0", "FLOAT NOT NULL"},
	} {
		t.Run("value/"+tc.parameter, func(t *testing.T) {
			decoded := post(t, tc.parameter)
			require.Empty(t, decoded["errors"])
			row := decoded["results"].([]any)[0].(map[string]any)["data"].([]any)[0].(map[string]any)["row"]
			require.Equal(t, []any{tc.text, tc.kind}, row)
		})
	}
	for _, integer := range []string{"9223372036854775808", "18446744073709551616", "-9223372036854775809"} {
		t.Run("unsupported/"+integer, func(t *testing.T) {
			decoded := post(t, integer)
			require.Empty(t, decoded["results"])
			errs := decoded["errors"].([]any)
			require.Len(t, errs, 1)
			require.Equal(t, "Neo.DatabaseError.Statement.ExecutionFailed", errs[0].(map[string]any)["code"])
			require.Equal(t, "Unsupported type of Number "+integer, errs[0].(map[string]any)["message"])
		})
	}
}
