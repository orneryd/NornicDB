package cypher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type unionNarrowObservation struct {
	Name    string          `json:"name"`
	Family  string          `json:"family"`
	Query   string          `json:"query"`
	Columns []string        `json:"columns"`
	Rows    [][]interface{} `json:"rows"`
	Error   string          `json:"error,omitempty"`
	Code    string          `json:"code,omitempty"`
	Detail  string          `json:"detail,omitempty"`
}

func unionNarrowObserve(exec *StorageExecutor, name, family, query string) unionNarrowObservation {
	r, err := exec.Execute(context.Background(), query, nil)
	out := unionNarrowObservation{Name: name, Family: family, Query: query}
	if r != nil {
		out.Columns, out.Rows = r.Columns, r.Rows
	}
	if err != nil {
		out.Error = err.Error()
		var semantic *SemanticError
		if errors.As(err, &semantic) {
			out.Code, out.Detail = semantic.Code, semantic.Detail
		}
	}
	return out
}

// Prior findings retain their public contract, including EXPLAIN projection
// columns repaired by the composition parent. This lane owns UNION rows/names.
func TestUnionNarrowPriorFindings(t *testing.T) {
	suffix := "default"
	if os.Getenv("NORNICDB_PARSER") == "antlr" {
		suffix = "antlr"
	}
	blob, err := os.ReadFile("testdata/fabric_union_parent_" + suffix + ".json")
	require.NoError(t, err)
	var baseline []unionNarrowObservation
	require.NoError(t, json.Unmarshal(blob, &baseline))
	// The old parent fixtures remain intact. These out-of-scope controls
	// compare against the exact integration release: nested non-unit terminal
	// CALL is now rejected, while Cypher 25 scope failures remain inherited.
	currentBlob, err := os.ReadFile("testdata/fabric_union_release_f91_" + suffix + ".json")
	require.NoError(t, err)
	var current []unionNarrowObservation
	require.NoError(t, json.Unmarshal(currentBlob, &current))
	require.Len(t, current, 84, "all historical unit/metadata controls remain represented")
	byName := make(map[string]unionNarrowObservation, len(current))
	for _, control := range current {
		byName[control.Name] = control
	}
	exec := newFabricCallRowsExecutor(t, true)
	for _, want := range baseline {
		if want.Family == "original" || want.Family == "r1-columns" {
			continue
		}
		if want.Family == "r4-unit-export" || want.Family == "r4-metadata" {
			control, ok := byName[want.Name]
			require.True(t, ok, want.Name)
			require.Equal(t, want.Query, control.Query, "baseline input population must not change")
			require.Equal(t, want.Family, control.Family)
			want = control
		}
		t.Run(want.Name, func(t *testing.T) {
			got := unionNarrowObserve(exec, want.Name, want.Family, want.Query)
			if want.Family == "r2-empty-chain" {
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", got.Code, want.Query)
				require.Equal(t, "DifferentColumnsInUnion", got.Detail, want.Query)
				return
			}
			if want.Error != "" {
				require.NotEmpty(t, got.Error, want.Query)
				if want.Code != "" {
					require.Equal(t, want.Code, got.Code, want.Query)
					require.Equal(t, want.Detail, got.Detail, want.Query)
				} else {
					require.Equal(t, want.Error, got.Error, want.Query)
				}
				return
			}
			require.Empty(t, got.Error, want.Query)
			require.Equal(t, want.Columns, got.Columns, want.Query)
			rows, err := json.Marshal(got.Rows)
			require.NoError(t, err)
			wants, err := json.Marshal(want.Rows)
			require.NoError(t, err)
			require.JSONEq(t, string(wants), string(rows), want.Query)
		})
	}
}

func TestUnionNarrowRequired(t *testing.T) {
	for _, route := range []string{"composite", "remote"} {
		t.Run(route, func(t *testing.T) {
			exec := newFabricCallRowsExecutor(t, true)
			leftGraph, rightGraph := "comp.one", "comp.two"
			if route == "remote" {
				server := unionNarrowRemote(t)
				mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
				for _, alias := range []string{"r1", "r2"} {
					require.NoError(t, mgr.AddConstituent(t.Context(), "comp", multidb.ConstituentRef{Alias: alias, DatabaseName: "schema", Type: "remote", AccessMode: "read_write", URI: server.URL}))
				}
				leftGraph, rightGraph = "comp.r1", "comp.r2"
			}
			for _, op := range []string{"UNION", "UNION ALL"} {
				body := "USE " + leftGraph + " RETURN 'left' AS value " + op + " USE " + rightGraph + " RETURN 'right' AS value"
				for i, query := range []string{body, "CALL { " + body + " } RETURN value", "CALL { CALL { " + body + " } RETURN value } RETURN value"} {
					t.Run(fmt.Sprintf("%s/rows/%d", op, i), func(t *testing.T) {
						r, err := exec.Execute(context.Background(), query, nil)
						require.NoError(t, err, query)
						require.Equal(t, []string{"value"}, r.Columns)
						require.ElementsMatch(t, [][]interface{}{{"left"}, {"right"}}, r.Rows)
					})
				}
				for _, duplicate := range []bool{false, true} {
					a, b := "c", "d"
					if duplicate {
						a, b = "a", "b"
					}
					body = fmt.Sprintf("USE %s RETURN 'a' AS first, 'b' AS second %s USE %s RETURN '%s' AS second, '%s' AS first", leftGraph, op, rightGraph, b, a)
					for i, query := range []string{body, "CALL { " + body + " } RETURN first, second"} {
						t.Run(fmt.Sprintf("%s/reorder/%t/%d", op, duplicate, i), func(t *testing.T) {
							r, err := exec.Execute(context.Background(), query, nil)
							require.NoError(t, err, query)
							require.Equal(t, []string{"first", "second"}, r.Columns)
							want := [][]interface{}{{"a", "b"}, {a, b}}
							if duplicate && op == "UNION" {
								want = want[:1]
							}
							require.ElementsMatch(t, want, r.Rows)
						})
					}
				}
				for i, rhs := range []string{"RETURN 'right' AS other", "RETURN 'right' AS value, 'extra' AS other", "UNWIND [] AS row RETURN row AS other"} {
					t.Run(fmt.Sprintf("%s/mismatch/%d", op, i), func(t *testing.T) {
						q := "CALL { USE " + leftGraph + " RETURN 'left' AS value " + op + " USE " + rightGraph + " " + rhs + " } RETURN value"
						got := unionNarrowObserve(exec, "", "", q)
						require.Equal(t, "Neo.ClientError.Statement.SyntaxError", got.Code, got.Error)
						require.Equal(t, "DifferentColumnsInUnion", got.Detail, got.Error)
					})
				}
			}

		})
	}
}

func BenchmarkUnionNarrowNoUnion(b *testing.B) {
	for _, item := range []struct{ name, query string }{
		{"return", "RETURN 'row' AS value"},
		{"with", "WITH 'row' AS value RETURN value"},
		{"call", "WITH 'row' AS value CALL (value) { RETURN value AS tag } RETURN value, tag"},
		{"optional", "WITH 'row' AS value OPTIONAL CALL (value) { RETURN value AS tag } RETURN value, tag"},
		{"optional_empty", "WITH 'row' AS value OPTIONAL CALL (value) { UNWIND [] AS hidden RETURN hidden AS tag } RETURN value, tag"},
		{"unit_empty", "UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN *"},
	} {
		b.Run(item.name, func(b *testing.B) {
			store := storage.NewNamespacedEngine(newTestMemoryEngine(b), "bench")
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0, TTL: 0})
			_, err := exec.Execute(context.Background(), item.query, nil)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r, err := exec.Execute(context.Background(), item.query, nil)
				if err != nil || r == nil {
					b.Fatalf("result=%v error=%v", r, err)
				}
			}
		})
	}
}

// The HTTP participant executes emitted Cypher; no result schema is fabricated.
func unionNarrowRemote(t *testing.T) *httptest.Server {
	t.Helper()
	native := NewStorageExecutor(newTestMemoryEngine(t))
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Statements []struct {
				Statement  string
				Parameters map[string]interface{}
			}
		}
		if r.Method == http.MethodPost {
			_ = json.NewDecoder(r.Body).Decode(&request)
		}
		var results []interface{}
		for _, statement := range request.Statements {
			result, err := native.Execute(r.Context(), statement.Statement, statement.Parameters)
			if err != nil {
				t.Errorf("remote statement %s: %v", statement.Statement, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			var data []interface{}
			for _, row := range result.Rows {
				data = append(data, map[string]interface{}{"row": row})
			}
			results = append(results, map[string]interface{}{"columns": result.Columns, "data": data})
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]interface{}{"results": results, "errors": []interface{}{}, "commit": server.URL + "/db/schema/tx/1/commit"}))
	}))
	t.Cleanup(server.Close)
	return server
}
