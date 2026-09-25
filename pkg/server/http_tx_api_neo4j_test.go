package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Parameter numbers keep Neo4j's INTEGER / FLOAT distinction at any depth
// (#570).
func TestDecodeTransactionRequestKeepsIntegerParameters(t *testing.T) {
	body := `{"statements":[{"statement":"RETURN 1","parameters":{"big":9007199254740993,"f":1.5,"e":1e2,"neg":-3,"huge":1e400,"l":[1,2.5,[3]],"m":{"k":7,"f":0.5}}}]}`
	var req TransactionRequest
	require.NoError(t, decodeTransactionRequest(strings.NewReader(body), &req))
	params := req.Statements[0].Parameters
	require.Equal(t, int64(9007199254740993), params["big"])
	require.Equal(t, 1.5, params["f"])
	require.Equal(t, float64(100), params["e"])
	require.Equal(t, int64(-3), params["neg"])
	require.Equal(t, "1e400", params["huge"], "a number outside float64 range is kept as its text")
	require.Equal(t, []interface{}{int64(1), 2.5, []interface{}{int64(3)}}, params["l"])
	require.Equal(t, map[string]interface{}{"k": int64(7), "f": 0.5}, params["m"])
	require.Error(t, decodeTransactionRequest(strings.NewReader(`{"statements":[`), &req))
}

// /tx/commit reports the engine's error code (#575), the executor's counters
// in Neo4j's stats object (#576) and integer parameters (#570); explicit
// transactions report stats too, and the commit URL and Location header
// follow the request host (#578).
func TestHTTPTransactionAPIMatchesNeo4j(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")

	post := func(path string, body map[string]any) (*httptest.ResponseRecorder, TransactionResponse) {
		rec := makeRequest(t, server, http.MethodPost, path, body, token)
		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		return rec, resp
	}

	for _, tc := range []struct{ stmt, code string }{
		{"RETURN 1 / 0 AS x", "Neo.ClientError.Statement.ArithmeticError"},
		{"RETURN toInteger([1]) AS x", "Neo.ClientError.Statement.TypeError"},
	} {
		_, resp := post("/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": tc.stmt}}})
		require.Len(t, resp.Errors, 1, tc.stmt)
		require.Equal(t, tc.code, resp.Errors[0].Code, tc.stmt)
		require.NotContains(t, resp.Errors[0].Message, "Neo.", "the code is not repeated in the message")
		_, resp = post("/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": tc.stmt}, {"statement": "RETURN 1"}}})
		require.Equal(t, tc.code, resp.Errors[0].Code, "multi-statement path")
	}

	_, resp := post("/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{
		{"statement": "UNWIND range(1, 5) AS i RETURN i, $a / 2 AS half SKIP $s LIMIT $l", "parameters": map[string]any{"a": 3, "s": 1, "l": 2}},
	}})
	require.Empty(t, resp.Errors)
	require.Len(t, resp.Results[0].Data, 2)
	require.EqualValues(t, 2, resp.Results[0].Data[0].Row[0])
	require.EqualValues(t, 1, resp.Results[0].Data[0].Row[1], "integer division")

	rec := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{
		{"statement": "CREATE (a:HS {v: 1})-[:R]->(b:HS) RETURN 1 AS ok", "includeStats": true},
		{"statement": "RETURN 1 AS x", "includeStats": true},
		{"statement": "RETURN 1 AS y"},
	}}, token)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	results := raw["results"].([]any)
	writeStats := results[0].(map[string]any)["stats"].(map[string]any)
	require.Len(t, writeStats, 14, "every Neo4j counter is present")
	require.Equal(t, true, writeStats["contains_updates"])
	require.EqualValues(t, 2, writeStats["nodes_created"])
	require.EqualValues(t, 1, writeStats["relationships_created"])
	require.Contains(t, writeStats, "relationship_deleted")
	readStats := results[1].(map[string]any)["stats"].(map[string]any)
	require.Equal(t, false, readStats["contains_updates"])
	require.EqualValues(t, 0, readStats["nodes_created"])
	require.NotContains(t, results[2].(map[string]any), "stats", "no stats without includeStats")

	openReq := httptest.NewRequest(http.MethodPost, "/db/nornic/tx", strings.NewReader(`{"statements":[{"statement":"CREATE (:HS2) RETURN 1 AS ok","includeStats":true}]}`))
	openReq.Host = "db.example:17474"
	openReq.Header.Set("Content-Type", "application/json")
	openReq.Header.Set("Authorization", token)
	openRec := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(openRec, openReq)
	require.Equal(t, http.StatusCreated, openRec.Code, openRec.Body.String())
	var openResp TransactionResponse
	require.NoError(t, json.Unmarshal(openRec.Body.Bytes(), &openResp))
	require.True(t, strings.HasPrefix(openResp.Commit, "http://db.example:17474/db/nornic/tx/"), openResp.Commit)
	require.Equal(t, strings.TrimSuffix(openResp.Commit, "/commit"), openRec.Header().Get("Location"))
	require.NotNil(t, openResp.Results[0].Stats)
	require.Equal(t, 1, openResp.Results[0].Stats.NodesCreated)
	require.True(t, strings.HasSuffix(openResp.Transaction.Expires, " GMT"), openResp.Transaction.Expires)
	rollback := makeRequest(t, server, http.MethodDelete, strings.TrimPrefix(openRec.Header().Get("Location"), "http://db.example:17474"), nil, token)
	require.Equal(t, http.StatusOK, rollback.Code, rollback.Body.String())
}

// A failing statement ends an explicit transaction, as in Neo4j's HTTP API:
// the statements after it in the request are not run, the transaction is
// rolled back (nothing it wrote is kept), and later requests to it get
// Neo.ClientError.Transaction.TransactionNotFound.
func TestHTTPExplicitTransactionEndsOnStatementError(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	post := func(path string, stmts ...string) (int, TransactionResponse) {
		list := make([]map[string]any, 0, len(stmts))
		for _, s := range stmts {
			list = append(list, map[string]any{"statement": s})
		}
		rec := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": list}, token)
		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		return rec.Code, resp
	}
	stored := func() int64 {
		_, resp := post("/db/nornic/tx/commit", "MATCH (n:TxErr) RETURN count(n) AS c")
		require.Empty(t, resp.Errors)
		return int64(resp.Results[0].Data[0].Row[0].(float64))
	}
	for _, failing := range []struct {
		stmt, code string
		results    int // the CREATE before it, and its columns if it compiled (#668)
	}{
		{"RETURN 1 / 0 AS x", "Neo.ClientError.Statement.ArithmeticError", 2},
		{"RETRUN 1", "Neo.ClientError.Statement.SyntaxError", 1},
	} {
		t.Run(failing.stmt, func(t *testing.T) {
			_, _ = post("/db/nornic/tx/commit", "MATCH (n:TxErr) DETACH DELETE n")

			// Error in a request to an open transaction.
			code, resp := post("/db/nornic/tx", "CREATE (:TxErr {v: 'a'})")
			require.Equal(t, http.StatusCreated, code)
			require.Empty(t, resp.Errors)
			txPath := strings.TrimSuffix(resp.Commit, "/commit")
			code, resp = post(txPath, "CREATE (:TxErr {v: 'b'})", failing.stmt, "CREATE (:TxErr {v: 'c'})")
			require.Equal(t, http.StatusOK, code)
			require.Len(t, resp.Errors, 1)
			require.Equal(t, failing.code, resp.Errors[0].Code)
			require.Len(t, resp.Results, failing.results, "the statement after the failing one is not run")
			for _, path := range []string{txPath, txPath + "/commit"} {
				code, resp = post(path, "RETURN 1 AS one")
				require.Equal(t, http.StatusNotFound, code, path)
				require.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", resp.Errors[0].Code, path)
			}
			require.Zero(t, stored(), "nothing the transaction wrote is kept")

			// Error in the request that opens the transaction.
			code, resp = post("/db/nornic/tx", "CREATE (:TxErr {v: 'd'})", failing.stmt)
			require.Equal(t, http.StatusCreated, code)
			require.Equal(t, failing.code, resp.Errors[0].Code)
			code, _ = post(strings.TrimSuffix(resp.Commit, "/commit") + "/commit")
			require.Equal(t, http.StatusNotFound, code)
			require.Zero(t, stored())

			// Error in the commit request.
			_, resp = post("/db/nornic/tx", "CREATE (:TxErr {v: 'e'})")
			txPath = strings.TrimSuffix(resp.Commit, "/commit")
			_, resp = post(txPath+"/commit", failing.stmt, "CREATE (:TxErr {v: 'f'})")
			require.Equal(t, failing.code, resp.Errors[0].Code)
			code, _ = post(txPath + "/commit")
			require.Equal(t, http.StatusNotFound, code)
			require.Zero(t, stored())
		})
	}
}

// A statement that compiled and failed while running reports its columns with
// no rows next to the error; a statement failing at compile time, or one
// without columns, has no result (#668, as Neo4j 5.26).
func TestHTTPFailedStatementReportsItsColumns(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	post := func(path string, statements ...string) (*httptest.ResponseRecorder, TransactionResponse) {
		body := make([]map[string]any, 0, len(statements))
		for _, statement := range statements {
			body = append(body, map[string]any{"statement": statement})
		}
		rec := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": body}, token)
		var resp TransactionResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
		require.Len(t, resp.Errors, 1, rec.Body.String())
		return rec, resp
	}
	columnsOf := func(resp TransactionResponse) [][]string {
		columns := [][]string{}
		for _, result := range resp.Results {
			require.Empty(t, result.Data)
			columns = append(columns, result.Columns)
		}
		return columns
	}

	for statement, want := range map[string][][]string{
		"RETURN 1 AS a, 1 / 0 AS x":                                  {{"a", "x"}},
		"WITH 1 / 0 AS x RETURN x":                                   {{"x"}},
		"UNWIND [1] AS d CALL { WITH d RETURN 1 / 0 AS z } RETURN z": {{"z"}},
		"CREATE (n:F668) SET n.v = 1 / 0":                            {},
		"RETUR 1":                                                    {},
	} {
		_, resp := post("/db/nornic/tx/commit", statement)
		require.Equal(t, want, columnsOf(resp), statement)
	}

	_, resp := post("/db/nornic/tx/commit", "RETURN 1 AS a", "RETURN 1 / 0 AS x", "RETURN 2 AS b")
	require.Len(t, resp.Results, 2)
	require.Equal(t, []string{"a"}, resp.Results[0].Columns)
	require.Len(t, resp.Results[0].Data, 1)
	require.Equal(t, []string{"x"}, resp.Results[1].Columns)
	require.Empty(t, resp.Results[1].Data)

	rec := makeRequest(t, server, http.MethodPost, "/db/nornic/tx", map[string]any{"statements": []map[string]any{}}, token)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var opened TransactionResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &opened))
	_, resp = post(strings.TrimSuffix(opened.Commit, "/commit"), "RETURN 1 / 0 AS x")
	require.Equal(t, [][]string{{"x"}}, columnsOf(resp))
}
