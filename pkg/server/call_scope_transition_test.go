package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/callscopetest"
	"github.com/orneryd/nornicdb/pkg/bolt"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type callScopeRouteOutcome struct {
	Columns []string
	Rows    [][]any
	Code    string
	Msg     string
}

func callScopeRouteError(err error) callScopeRouteOutcome {
	if err == nil {
		return callScopeRouteOutcome{}
	}
	var ne *neo4j.Neo4jError
	if errors.As(err, &ne) {
		return callScopeRouteOutcome{Code: ne.Code, Msg: ne.Msg}
	}
	code, _ := nornicerrors.Neo4jStatus(err)
	if !nornicerrors.HasNeo4jStatus(err) {
		code = "TRANSPORT: " + err.Error()
	}
	return callScopeRouteOutcome{Code: code, Msg: err.Error()}
}

func callScopeRouteDriver(ctx context.Context, driver neo4j.DriverWithContext, db, q string, params map[string]any, explicit bool) callScopeRouteOutcome {
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: db})
	defer session.Close(ctx)
	var result neo4j.ResultWithContext
	var err error
	var tx neo4j.ExplicitTransaction
	if explicit {
		tx, err = session.BeginTransaction(ctx)
		if err != nil {
			return callScopeRouteError(err)
		}
		defer tx.Close(ctx)
		result, err = tx.Run(ctx, q, params)
	} else {
		result, err = session.Run(ctx, q, params)
	}
	if err != nil {
		return callScopeRouteError(err)
	}
	columns, err := result.Keys()
	if err != nil {
		return callScopeRouteError(err)
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return callScopeRouteError(err)
	}
	if explicit {
		if err = tx.Commit(ctx); err != nil {
			return callScopeRouteError(err)
		}
	}
	out := callScopeRouteOutcome{Columns: columns}
	for _, r := range records {
		out.Rows = append(out.Rows, r.Values)
	}
	return out
}

func (o callScopeRouteOutcome) canonical() string {
	if o.Code != "" {
		return "ERROR " + o.Code
	}
	rows := []string{}
	for _, row := range o.Rows {
		b, _ := json.Marshal(callScopeRouteValue(row))
		rows = append(rows, string(b))
	}
	sort.Strings(rows)
	if o.Columns == nil {
		o.Columns = []string{}
	}
	b, _ := json.Marshal(struct {
		Columns []string
		Rows    []string
	}{o.Columns, rows})
	return string(b)
}

func callScopeRouteValue(value any) any {
	switch value := value.(type) {
	case neo4j.Node:
		return callScopeRouteValue(value.Props)
	case *storage.Node:
		if value == nil {
			return nil
		}
		return callScopeRouteValue(value.Properties)
	case []any:
		out := make([]any, len(value))
		for i, v := range value {
			out[i] = callScopeRouteValue(v)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(value))
		for k, v := range value {
			out[k] = callScopeRouteValue(v)
		}
		return out
	default:
		return value
	}
}
func TestCallScopePublicTransitionConsumers(t *testing.T) {
	ctx := context.Background()
	for _, route := range []string{"Execute", "Bolt-auto", "Bolt-tx", "HTTP-auto", "HTTP-tx"} {
		if selected := os.Getenv("CALL_SCOPE_ROUTE"); selected != "" && selected != route {
			continue
		}
		t.Run(route, func(t *testing.T) {
			srv, authenticator := setupTestServer(t)
			peer := httptest.NewServer(srv.buildRouter())
			defer peer.Close()
			token := getAuthToken(t, authenticator, "admin")
			executor, err := srv.newExecutorForDatabase("nornic")
			require.NoError(t, err)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			bs := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), nil, srv.GetDatabaseManager())
			done := make(chan error, 1)
			go func() { done <- bs.Serve(listener) }()
			defer func() { _ = bs.Close(); <-done }()
			driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
			require.NoError(t, err)
			defer driver.Close(ctx)
			params := map[string]any{"keys": []any{int64(1)}, "p": int64(10), "one": int64(1), "outer": int64(1), "oracleRun": "scope-public-rules"}
			type response struct {
				Commit  string
				Errors  []struct{ Code, Message string }
				Results []struct {
					Columns []string
					Data    []struct{ Row []any }
				}
			}
			post := func(path, q string) response {
				statements := []map[string]any{}
				if q != "" {
					statements = append(statements, map[string]any{"statement": q, "parameters": params})
				}
				body, err := json.Marshal(map[string]any{"statements": statements})
				require.NoError(t, err)
				req, err := http.NewRequest(http.MethodPost, peer.URL+path, bytes.NewReader(body))
				require.NoError(t, err)
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+token)
				res, err := peer.Client().Do(req)
				require.NoError(t, err)
				defer res.Body.Close()
				require.True(t, res.StatusCode == 200 || res.StatusCode == 201, "HTTP %d", res.StatusCode)
				var out response
				require.NoError(t, json.NewDecoder(res.Body).Decode(&out))
				return out
			}
			run := func(q string) callScopeRouteOutcome {
				if route == "Execute" {
					r, err := executor.Execute(ctx, q, params)
					if err != nil {
						return callScopeRouteError(err)
					}
					return callScopeRouteOutcome{Columns: r.Columns, Rows: r.Rows}
				}
				if strings.HasPrefix(route, "Bolt") {
					return callScopeRouteDriver(ctx, driver, "nornic", q, params, route == "Bolt-tx")
				}
				path := "/db/nornic/tx/commit"
				if route == "HTTP-tx" {
					opened := post("/db/nornic/tx", "")
					require.Empty(t, opened.Errors)
					u, err := url.Parse(opened.Commit)
					require.NoError(t, err)
					path = u.Path
				}
				out := post(path, q)
				if len(out.Errors) > 0 {
					return callScopeRouteOutcome{Code: out.Errors[0].Code, Msg: out.Errors[0].Message}
				}
				result := callScopeRouteOutcome{}
				if len(out.Results) > 0 {
					result.Columns = out.Results[0].Columns
					for _, r := range out.Results[0].Data {
						result.Rows = append(result.Rows, r.Row)
					}
				}
				return result
			}
			cases := callscopetest.TransitionCases()
			require.Len(t, cases, 443)
			mismatches := 0
			for _, test := range cases {
				item := callscopetest.Case{Name: test.Name, Reference: test.Query}
				want := callScopeRouteOutcome{Columns: test.Columns, Rows: test.Rows, Code: test.Code}
				original := callScopeRouteOutcome{}
				allowed := ""

				got := run(item.Reference)
				if test.Name == "delete_retained" && got.Code == "" {
					remaining := run("MATCH (n:R73Delete) RETURN count(n) AS remaining")
					wantRemaining := callScopeRouteOutcome{Columns: []string{"remaining"}, Rows: [][]any{{int64(0)}}}
					require.Equal(t, wantRemaining.canonical(), remaining.canonical(), "DELETE effect on %s", route)
				}
				if got.canonical() != want.canonical() {
					mismatches++
					line, _ := json.Marshal(map[string]any{"case": item.Name, "route": route, "parser": os.Getenv("NORNICDB_PARSER"), "query": item.Reference, "compatibility": item.Compatibility, "extension": allowed, "original": original, "expected": want, "actual": got})
					t.Logf("CALL_SCOPE_ROUTE_MISMATCH %s", line)
				}
			}
			summary, _ := json.Marshal(map[string]any{"route": route, "cases": len(cases), "mismatches": mismatches, "reference_queries": 0, "suite": os.Getenv("CALL_SCOPE_SUITE"), "parser": os.Getenv("NORNICDB_PARSER")})
			t.Logf("CALL_SCOPE_ROUTES %s", summary)
			require.Zero(t, mismatches)
		})
	}
}
