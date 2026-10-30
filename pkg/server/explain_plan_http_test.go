package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestHTTPExplainProfilePlanDelivery(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")

	post := func(stmt string) map[string]any {
		rec := makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": stmt}},
		}, token)
		var raw map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw), rec.Body.String())
		require.Empty(t, raw["errors"], rec.Body.String())
		results := raw["results"].([]any)
		require.Len(t, results, 1)
		return results[0].(map[string]any)
	}

	explain := post("EXPLAIN MATCH (n:Person) RETURN n.name")
	plan, ok := explain["plan"].(map[string]any)
	require.True(t, ok, "EXPLAIN result carries the plan")
	require.Equal(t, "ProduceResults", plan["operatorType"])
	_, hasProfile := explain["profile"]
	require.False(t, hasProfile, "EXPLAIN has no profile key")

	profiled := post("PROFILE MATCH (n:Person) RETURN n.name")
	profile, ok := profiled["plan"].(map[string]any)
	require.True(t, ok, "PROFILE result carries the profiled plan")
	require.Equal(t, "ProduceResults", profile["operatorType"])
	require.Contains(t, profile, "rows")
	require.Contains(t, profile, "dbHits")
	require.NotContains(t, profiled, "profile")

	ordinary := post("MATCH (n:Person) RETURN n.name")
	_, hasPlan := ordinary["plan"]
	require.False(t, hasPlan, "ordinary queries carry no plan")
	_, hasProfile = ordinary["profile"]
	require.False(t, hasProfile, "ordinary queries carry no profile")
}

func TestMonsterHTTPStatementBoundaries(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, explicit := range []bool{false, true} {
				t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
					server, authenticator := setupTestServer(t)
					token := "Bearer " + getAuthToken(t, authenticator, "admin")
					post := func(path, query string) map[string]any {
						t.Helper()
						statements := []map[string]any{}
						if query != "" {
							statements = append(statements, map[string]any{"statement": query})
						}
						recorder := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": statements}, token)
						require.Contains(t, []int{http.StatusOK, http.StatusCreated}, recorder.Code, recorder.Body.String())
						var body map[string]any
						require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body), recorder.Body.String())
						return body
					}
					pathForStatement := func() string {
						if !explicit {
							return "/db/nornic/tx/commit"
						}
						body := post("/db/nornic/tx", "")
						require.Empty(t, body["errors"])
						commit, err := url.Parse(body["commit"].(string))
						require.NoError(t, err)
						return strings.TrimSuffix(commit.Path, "/commit")
					}
					for _, test := range []struct{ query, code string }{
						{"CREATE (:Semi); MATCH (n:Semi) RETURN count(n) AS c", "Neo.ClientError.Statement.SyntaxError"},
						{"EXPLAIN PROFILE RETURN 1", "Neo.ClientError.Statement.ArgumentError"},
						{"PROFILE EXPLAIN RETURN 1", "Neo.ClientError.Statement.ArgumentError"},
						{"RETURN 1 AS x UNION FINISH", "Neo.ClientError.Statement.SyntaxError"},
						{"CREATE (:Semi) RETURN 1 AS x UNION FINISH", "Neo.ClientError.Statement.SyntaxError"},
					} {
						t.Run(test.query, func(t *testing.T) {
							body := post(pathForStatement(), test.query)
							errors := body["errors"].([]any)
							require.Len(t, errors, 1)
							require.Equal(t, test.code, errors[0].(map[string]any)["code"])
							require.Empty(t, body["results"], "compile errors must precede results")
							count := post("/db/nornic/tx/commit", "MATCH (n:Semi) RETURN count(n) AS c")
							require.Empty(t, count["errors"])
							data := count["results"].([]any)[0].(map[string]any)["data"].([]any)
							require.Equal(t, []any{float64(0)}, data[0].(map[string]any)["row"])
						})
					}
					path := pathForStatement()
					body := post(path, "UNWIND [1, 2] AS x CALL (x) { RETURN x * 2 AS y } RETURN y ORDER BY y")
					require.Empty(t, body["errors"])
					data := body["results"].([]any)[0].(map[string]any)["data"].([]any)
					require.Len(t, data, 2)
					require.Equal(t, []any{float64(2)}, data[0].(map[string]any)["row"])
					require.Equal(t, []any{float64(4)}, data[1].(map[string]any)["row"])
					if explicit {
						require.Empty(t, post(path+"/commit", "")["errors"])
					}
				})
			}
		})
	}
}
