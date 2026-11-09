package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// httpRows runs one statement on endpoint and returns its rows; the test
// fails on any error.
func httpRows(t *testing.T, server *Server, token, endpoint, statement string, params map[string]any) [][]any {
	t.Helper()
	response := makeRequest(t, server, http.MethodPost, endpoint, map[string]any{"statements": []any{map[string]any{"statement": statement, "parameters": params}}}, token)
	var body struct {
		Results []struct {
			Data []struct {
				Row []any `json:"row"`
			} `json:"data"`
		} `json:"results"`
		Errors []any `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body), response.Body.String())
	require.Empty(t, body.Errors, statement)
	require.Len(t, body.Results, 1)
	rows := make([][]any, len(body.Results[0].Data))
	for index, data := range body.Results[0].Data {
		rows[index] = data.Row
	}
	return rows
}

// Over HTTP, a change feed read in an explicit transaction lists only the
// changes its snapshot holds: auto-commit requests committed after the
// transaction opened are not listed, and every listed node is found by the
// same statement. A transaction opened afterwards lists and finds them all.
func TestHTTPChangeDataCaptureQueryIsBoundedByTheTransactionSnapshot(t *testing.T) {
	server, authenticator := setupTestServer(t)
	admin := getAuthToken(t, authenticator, "admin")
	token := "Bearer " + admin
	require.Empty(t, txErrorCode(t, server, "system", admin, "ALTER DATABASE nornic SET OPTION txLogEnrichment 'FULL'"))
	const autoCommit = "/db/nornic/tx/commit"
	start := httpRows(t, server, token, autoCommit, "CALL db.cdc.current() YIELD id RETURN id", nil)[0][0].(string)
	httpRows(t, server, token, autoCommit, "UNWIND range(1, 20) AS i CREATE (:Before {i: i})", nil)

	const resolve = "CALL db.cdc.query($from) YIELD event WITH event OPTIONAL MATCH (n) WHERE elementId(n) = event.elementId " +
		"RETURN count(event) AS listed, count(n) AS found"
	commit, errs := openHTTPTransaction(t, server, token)
	require.Empty(t, errs)
	for i := 0; i < 30; i++ {
		httpRows(t, server, token, autoCommit, "CREATE (:After {i: $i})", map[string]any{"i": i})
	}
	rows := httpRows(t, server, token, strings.TrimSuffix(commit, "/commit"), resolve, map[string]any{"from": start})
	require.Equal(t, [][]any{{float64(20), float64(20)}}, rows)
	httpRows(t, server, token, commit, "RETURN 1", nil)

	rows = httpRows(t, server, token, autoCommit, resolve, map[string]any{"from": start})
	require.Equal(t, [][]any{{float64(50), float64(50)}}, rows)
	commit, errs = openHTTPTransaction(t, server, token)
	require.Empty(t, errs)
	rows = httpRows(t, server, token, commit, resolve, map[string]any{"from": start})
	require.Equal(t, [][]any{{float64(50), float64(50)}}, rows)
}
