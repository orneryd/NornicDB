package server

import (
	"github.com/orneryd/nornicdb/pkg/multidb"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Composed public proof of constructor admission and retained operand rules.
// No external database supplies the contract or expected result.
func TestCanonicalBoundRulePublicConsumers(t *testing.T) {
	type sample struct {
		Name, Query, Code string
		Columns           []string
		Rows              [][]any
	}
	samples := []sample{
		{Name: "arity", Query: "CALL db.labels(1) YIELD label RETURN label", Code: "Neo.ClientError.Statement.SyntaxError"},
		{Name: "arity-empty", Query: "UNWIND [] AS ignored CALL db.labels(1) YIELD label RETURN label", Code: "Neo.ClientError.Statement.SyntaxError"},
		{Name: "arity-one", Query: "UNWIND [0] AS ignored CALL db.labels(1) YIELD label RETURN label", Code: "Neo.ClientError.Statement.SyntaxError"},

		{Name: "yield-renaming", Query: "CALL dbms.components() YIELD name, versions AS name RETURN name", Code: "Neo.ClientError.Statement.SyntaxError"},
		{Name: "yield-all-renaming", Query: "UNWIND [] AS ignored CALL dbms.components() YIELD name AS x, versions AS x RETURN x", Code: "Neo.ClientError.Statement.SyntaxError"},
		{Name: "with-siblings", Query: "UNWIND [] AS ignored WITH 1 AS x, 2 AS x RETURN x", Code: "Neo.ClientError.Statement.SyntaxError"},
		{Name: "node-to-list", Query: "MATCH (n:RuleNode) WITH [n] AS n WHERE n[0] IN n RETURN size(n) AS v", Columns: []string{"v"}, Rows: [][]any{{int64(1)}, {int64(1)}}},
		{Name: "node-to-list-empty", Query: "UNWIND [] AS ignored MATCH (n:RuleNode) WITH [n] AS n WHERE n[0] IN n RETURN size(n) AS v", Columns: []string{"v"}},
		{Name: "relationship-to-list", Query: "MATCH ()-[r:RuleRel]->() WITH [r] AS r WHERE r[0] IN r RETURN size(r) AS v", Columns: []string{"v"}, Rows: [][]any{{int64(1)}}},
		{Name: "path-to-list", Query: "MATCH p=(:RuleNode)-[:RuleRel]->() WITH [p] AS p WHERE p[0] IN p RETURN size(p) AS v", Columns: []string{"v"}, Rows: [][]any{{int64(1)}}},
		{Name: "pattern-kind-empty", Query: "UNWIND [] AS ignored WITH 1 AS e RETURN [(e)-->(w)|w] AS xs", Code: "Neo.ClientError.Statement.SyntaxError"},
		{Name: "pattern-lexical-empty", Query: "UNWIND [] AS ignored RETURN [e IN [1] | [(e)-->(w)|w]] AS xs", Code: "Neo.ClientError.Statement.SyntaxError"},
		{Name: "return-order-empty", Query: "UNWIND [] AS r RETURN r ORDER BY count(*)", Code: "Neo.ClientError.Statement.SyntaxError"},
		{Name: "collect-ordinary", Query: "RETURN COLLECT { WITH 1 AS x RETURN * } AS xs", Columns: []string{"xs"}, Rows: [][]any{{[]any{int64(1)}}}},
		{Name: "collect-empty-body", Query: "RETURN COLLECT { UNWIND [] AS x RETURN * } AS xs", Columns: []string{"xs"}, Rows: [][]any{{[]any{}}}},
		{Name: "collect-one-body", Query: "RETURN COLLECT { UNWIND [1] AS x RETURN * } AS xs", Columns: []string{"xs"}, Rows: [][]any{{[]any{int64(1)}}}},
		{Name: "collect-two-empty-outer", Query: "UNWIND [] AS __rule_input RETURN COLLECT { WITH 1 AS x RETURN * } AS xs", Code: "Neo.ClientError.Statement.SyntaxError"},
		{Name: "collect-two-one-outer", Query: "UNWIND [0] AS __rule_input RETURN COLLECT { WITH 1 AS x RETURN * } AS xs", Code: "Neo.ClientError.Statement.SyntaxError"},
		{Name: "scalar-25-constant", Query: "CYPHER 25 UNWIND [1, 2] AS g RETURN g, COLLECT { UNWIND [] AS x RETURN count(*) + g AS c } AS v ORDER BY g", Columns: []string{"g", "v"}, Rows: [][]any{{int64(1), []any{int64(1)}}, {int64(2), []any{int64(2)}}}},
		{Name: "scalar-25-union", Query: "CYPHER 25 WITH 1 AS g RETURN COLLECT { UNWIND [] AS x RETURN count(*) + g AS c UNION ALL RETURN g AS c } AS v", Columns: []string{"v"}, Rows: [][]any{{[]any{int64(1), int64(1)}}}},
		{Name: "scalar-5-import-empty-outer", Query: "UNWIND [] AS __rule_input UNWIND [1, 2] AS g RETURN g, COLLECT { UNWIND [] AS x RETURN count(*) + g AS c } AS v ORDER BY g", Code: "Neo.ClientError.Statement.SyntaxError"},
	}
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "one"}))
	ctx := context.Background()
	local, err := cypher.NewDatabaseScopedExecutor(server.databaseScopedExecutorOptions("one", token))
	require.NoError(t, err)
	_, err = local.Execute(ctx, "CREATE (:RuleNode {id:1})-[:RuleRel]->(:RuleNode {id:2})", nil)
	require.NoError(t, err)
	driver, err := neo4j.NewDriverWithContext(fabricLifetimeBolt(t, server), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	request := func(t *testing.T, path, query string, codes ...string) (TransactionResponse, string) {
		statements := []map[string]string{}
		if query != "" {
			statements = append(statements, map[string]string{"statement": query})
		}
		response := makeRequest(t, server, http.MethodPost, path, map[string]any{"statements": statements}, token)
		require.Less(t, response.Code, 400)
		var result TransactionResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
		if len(codes) > 0 && codes[0] != "" {
			require.Len(t, result.Errors, 1)
			require.Equal(t, codes[0], result.Errors[0].Code)
		} else {
			require.Empty(t, result.Errors)
		}
		return result, response.Header().Get("Location")
	}
	for _, normalized := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalization=%v", normalized), func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			config.SetCypherQueryNormalizationEnabled(normalized)
			for _, sample := range samples {
				t.Run(sample.Name, func(t *testing.T) {
					for _, route := range []string{"Execute", "Bolt", "HTTP"} {
						for _, explicit := range []bool{false, true} {
							for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
								t.Run(fmt.Sprintf("%s/explicit=%v/mode=%s", route, explicit, prefix), func(t *testing.T) {
									query := prefix + sample.Query
									var columns []string
									var rows [][]any
									switch route {
									case "Execute":
										exec, err := cypher.NewDatabaseScopedExecutor(server.databaseScopedExecutorOptions("one", token))
										require.NoError(t, err)
										if explicit {
											_, err = exec.Execute(ctx, "BEGIN", nil)
											require.NoError(t, err)
											defer exec.Execute(ctx, "ROLLBACK", nil)
										}
										result, err := exec.Execute(ctx, query, nil)
										if sample.Code != "" {
											require.Error(t, err)
											code, _ := nornicerrors.Neo4jStatus(err)
											require.Equal(t, sample.Code, code)
											return
										}
										require.NoError(t, err)
										columns, rows = result.Columns, result.Rows
									case "Bolt":
										session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "one"})
										defer session.Close(ctx)
										var result neo4j.ResultWithContext
										var err error
										if explicit {
											tx, failure := session.BeginTransaction(ctx)
											require.NoError(t, failure)
											defer tx.Rollback(ctx)
											result, err = tx.Run(ctx, query, nil)
										} else {
											result, err = session.Run(ctx, query, nil)
										}
										if sample.Code != "" {
											if err == nil {
												_, err = result.Collect(ctx)
											}
											require.Error(t, err)
											var classified *neo4j.Neo4jError
											require.ErrorAs(t, err, &classified)
											require.Equal(t, sample.Code, classified.Code)
											return
										}
										require.NoError(t, err)
										columns, err = result.Keys()
										require.NoError(t, err)
										records, err := result.Collect(ctx)
										require.NoError(t, err)
										for _, record := range records {
											rows = append(rows, record.Values)
										}
									case "HTTP":
										path := "/db/one/tx/commit"
										if explicit {
											_, location := request(t, "/db/one/tx", "")
											parsed, err := url.Parse(location)
											require.NoError(t, err)
											path = parsed.Path
											defer makeRequest(t, server, http.MethodDelete, path, nil, token)
										}
										result, _ := request(t, path, query, sample.Code)
										if sample.Code != "" {
											return
										}
										require.Len(t, result.Results, 1)
										columns = result.Results[0].Columns
										for _, row := range result.Results[0].Data {
											rows = append(rows, row.Row)
										}
									}
									require.Equal(t, sample.Columns, columns)
									if prefix == "EXPLAIN " {
										require.Empty(t, rows)
									} else {
										require.Len(t, rows, len(sample.Rows))
										expected, err := json.Marshal(append([][]any{}, sample.Rows...))
										require.NoError(t, err)
										actual, err := json.Marshal(append([][]any{}, rows...))
										require.NoError(t, err)
										require.JSONEq(t, string(expected), string(actual))
									}
								})
							}
						}
					}
				})
			}
		})
	}
}
