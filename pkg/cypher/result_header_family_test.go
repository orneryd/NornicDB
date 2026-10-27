package cypher

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type resultHeaderCase struct {
	name, query string
	columns     []string
}

// assertResultHeaderViews owns the public header contract: each query must
// report the same columns through the reference driver (when available), the
// executor's Columns, and StatementColumns, under auto-commit and/or explicit
// transactions, for the plain, PROFILE and EXPLAIN prefixes.
func assertResultHeaderViews(t *testing.T, ctx context.Context, exec *StorageExecutor, reference neo4j.DriverWithContext, tc resultHeaderCase, transactions []bool) {
	t.Helper()
	if reference != nil {
		t.Run("neo4j", func(t *testing.T) {
			for _, prefix := range []string{"", "PROFILE ", "EXPLAIN "} {
				result, err := neo4j.ExecuteQuery(ctx, reference, prefix+tc.query, nil, neo4j.EagerResultTransformer)
				require.NoError(t, err, "%s%s", prefix, tc.query)
				require.Equal(t, tc.columns, result.Keys, "%s%s", prefix, tc.query)
			}
		})
	}

	for _, transaction := range transactions {
		name := "auto_commit"
		if transaction {
			name = "explicit_transaction"
		}
		t.Run(name, func(t *testing.T) {
			if transaction {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() {
					_, err := exec.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}()
			}

			for _, prefix := range []string{"", "PROFILE ", "EXPLAIN "} {
				result, err := exec.Execute(ctx, prefix+tc.query, nil)
				if assert.NoError(t, err, "%s%s", prefix, tc.query) {
					assert.Equal(t, tc.columns, result.Columns, "%s%s", prefix, tc.query)
					assert.Equal(t, tc.columns, exec.StatementColumns(tc.query), "StatementColumns: %s", tc.query)
				}
			}
		})
	}
}

func generatedResultHeaderCases() []resultHeaderCase {
	var cases []resultHeaderCase
	add := func(name, query string, columns []string, union bool) {
		cases = append(cases, resultHeaderCase{name, query, columns})
		if union {
			for _, operator := range []string{"UNION", "UNION ALL"} {
				cases = append(cases, resultHeaderCase{name + "/" + strings.ReplaceAll(operator, " ", "_"), query + " " + operator + " " + query, columns})
			}
		}
	}
	seeds := []string{
		"WITH 1 AS value",
		"UNWIND [1,2] AS value",
		"UNWIND [] AS value",
		"MATCH (value:HeaderMatrix)",
		"MATCH (value:HeaderMatrixMissing)",
		"WITH 1 AS hidden, 2 AS value WITH value",
		"UNWIND [1,2] AS item WITH count(item) AS value",
		"CALL { UNWIND [1] AS value RETURN * }",
		"CALL { UNWIND [] AS value RETURN * }",
		"CALL { RETURN 1 AS value }",
		"CALL { CALL { RETURN 1 AS value } RETURN * }",
	}
	for seed, prefix := range seeds {
		for _, yielded := range []string{"label", "kind"} {
			call := " CALL db.labels() YIELD label"
			if yielded != "label" {
				call += " AS " + yielded
			}
			star := []string{yielded, "value"}
			sort.Strings(star)
			for projection, tail := range []resultHeaderCase{
				{query: "RETURN *", columns: star},
				{query: "RETURN DISTINCT *", columns: star},
				{query: "RETURN *, 2 AS extra", columns: append(append([]string{}, star...), "extra")},
				{query: "WITH value RETURN *", columns: []string{"value"}},
				{query: "WITH count(*) AS total RETURN *", columns: []string{"total"}},
				{query: "RETURN *, count(*) AS total", columns: append(append([]string{}, star...), "total")},
				{query: "RETURN value, " + yielded, columns: []string{"value", yielded}},
			} {
				add(fmt.Sprintf("linear_%d_%s_%d", seed, yielded, projection), prefix+call+" "+tail.query, tail.columns, true)
			}
		}
	}
	for outer, input := range []string{"[]", "[1]"} {
		for inner, seed := range []string{"WITH 1 AS innerValue", "UNWIND [] AS innerValue"} {
			for projection, returned := range []string{"RETURN *", "RETURN innerValue, label", "RETURN *, 2 AS extra"} {
				body := seed + " CALL db.labels() YIELD label " + returned
				for depth := 0; depth < 2; depth++ {
					nested := body
					if depth > 0 {
						nested = "CALL { " + body + " } RETURN *"
					}
					columns := []string{"innerValue", "label", "outerValue"}
					if projection == 2 {
						columns = append([]string{"extra"}, columns...)
					}
					add(fmt.Sprintf("outer_%d_inner_%d_projection_%d_depth_%d", outer, inner, projection, depth), "UNWIND "+input+" AS outerValue CALL { "+nested+" } RETURN *", columns, true)
				}
			}
		}
	}
	for _, procedure := range []struct{ name, output string }{
		{"db.labels", "label"}, {"db.relationshipTypes", "relationshipType"}, {"db.propertyKeys", "propertyKey"},
	} {
		for form, suffix := range []string{"", " YIELD *", " YIELD " + procedure.output, " YIELD " + procedure.output + " AS renamed"} {
			column := procedure.output
			if form == 3 {
				column = "renamed"
			}
			add(fmt.Sprintf("standalone_%s_%d", procedure.name, form), "CALL "+procedure.name+"()"+suffix, []string{column}, false)
		}
	}
	return cases
}

// Generated from scope/projection operations rather than a list of review
// reproductions. Every accepted shape has the same four public header views.
func TestResultHeaderFamilyMatrix(t *testing.T) {
	ctx := context.Background()
	var reference neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reference.Close(ctx)) })
		_, err = neo4j.ExecuteQuery(ctx, reference, "CREATE (:HeaderMatrix {id:1})", nil, neo4j.EagerResultTransformer)
		require.NoError(t, err)
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "header-matrix"))
	_, err := exec.Execute(ctx, "CREATE (:HeaderMatrix {id:1})", nil)
	require.NoError(t, err)
	cases := generatedResultHeaderCases()
	t.Logf("%d generated shapes, EXPLAIN/StatementColumns/execute/PROFILE, auto-commit and explicit transactions", len(cases))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertResultHeaderViews(t, ctx, exec, reference, tc, []bool{false, true})
		})
	}

}

func BenchmarkResultHeaderUnionYield(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "header-union-bench"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:HeaderMatrix {id:1})", nil)
	require.NoError(b, err)
	branch := "WITH 1 AS value CALL db.labels() YIELD label RETURN *"
	query := branch + " UNION ALL " + branch
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, query, map[string]interface{}{"nonce": i}); err != nil {
			b.Fatal(err)
		}
	}
}

func TestResultHeaderUseMatrix(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "header-use"))
	var reference neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reference.Close(ctx)) })
	}
	for _, input := range []string{"[]", "[1]"} {
		for _, body := range []string{
			"USE neo4j UNWIND " + input + " AS before RETURN *",
			"USE neo4j UNWIND " + input + " AS before RETURN before",
			"USE neo4j CALL { UNWIND " + input + " AS before RETURN * } RETURN *",
		} {
			for _, outer := range []string{"", "UNWIND [] AS outerValue ", "UNWIND [1] AS outerValue "} {
				columns := []string{"before"}
				if outer != "" {
					columns = append(columns, "outerValue")
				}
				query := outer + "CALL { " + body + " } RETURN *"
				for _, op := range []string{"", " UNION ", " UNION ALL "} {
					full := query
					if op != "" {
						full += op + query
					}
					t.Run(full, func(t *testing.T) {
						assertResultHeaderViews(t, ctx, exec, reference, resultHeaderCase{name: full, query: full, columns: columns}, []bool{false})
					})
				}
			}
		}
	}
	for _, query := range []string{
		"LOAD CSV FROM 'file:///unused' AS row RETURN *",
		"LOAD CSV FROM 'file:///unused' AS row RETURN * UNION RETURN 1 AS value",
	} {
		t.Run("unknown/"+query, func(t *testing.T) {
			require.Nil(t, exec.StatementColumns(query), "unknown must not be a fabricated empty header or wildcard")
			_, err := exec.Execute(ctx, "EXPLAIN "+query, nil)
			require.Error(t, err, "unresolved metadata must not be successful EXPLAIN")
			exec.StatementFacts(query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(resultmetadata.Statement) {
				t.Fatal("incomplete statement became known")
			}, func() { t.Fatal("owner declined") }, func(reason resultmetadata.Unresolved) {
				require.Error(t, reason.Err)
				require.NotContains(t, reason.Err.Error(), "same return column names", "incomplete facts cannot cause a UNION mismatch")
			})
		})
	}
}

func BenchmarkResultHeaderCallTail(b *testing.B) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(b), "header-tail"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:HeaderTail)", nil)
	require.NoError(b, err)
	query := "CALL db.labels() YIELD label WITH label RETURN label"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := exec.Execute(ctx, query, map[string]interface{}{"nonce": i})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestResultHeaderColdExecution(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "header-cold"))
	var reference neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reference.Close(ctx)) })
	}
	for route, prefix := range []string{"", "PROFILE "} {
		name := fmt.Sprintf("custom.header_cold_zero_%d", route)
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: name, Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 0, Returns: []ProcedureColumn{{Name: "value", Type: "INTEGER"}}},
			func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
				return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{}}, nil
			}))
		t.Cleanup(func() {
			globalProcedureRegistry.mu.Lock()
			delete(globalProcedureRegistry.user, name)
			delete(globalProcedureRegistry.userFold, name)
			globalProcedureRegistry.generation.Add(1)
			globalProcedureRegistry.mu.Unlock()
		})
		for _, tc := range []struct {
			tail      string
			columns   []string
			aggregate bool
		}{
			{" MATCH (n:HeaderColdMissing) RETURN n AS missing", []string{"missing"}, false},
			{" MATCH (n:HeaderColdMissing) RETURN *", []string{"n", "value"}, false},
			{" MATCH (n:HeaderColdMissing) RETURN *,2 AS extra", []string{"n", "value", "extra"}, false},
			{" MATCH (n:HeaderColdMissing) RETURN count(*) AS total", []string{"total"}, true},
			{" WITH value RETURN value AS kept", []string{"kept"}, false},
			{" WITH value RETURN count(*) AS total", []string{"total"}, true},
			{" UNWIND [] AS local RETURN *", []string{"local", "value"}, false},
			{" CALL { RETURN 1 AS innerValue } RETURN *", []string{"innerValue", "value"}, false},
		} {
			t.Run(prefix+tc.tail, func(t *testing.T) {
				query := "CALL " + name + "() YIELD value" + tc.tail
				// Execution is deliberately first: no header API has seen query.
				result, err := exec.Execute(ctx, prefix+query, nil)
				require.NoError(t, err)
				require.Equal(t, tc.columns, result.Columns)
				if tc.aggregate {
					require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
				} else {
					require.Empty(t, result.Rows)
				}
				require.Equal(t, tc.columns, exec.StatementColumns(query))
				if reference != nil {
					expected, err := neo4j.ExecuteQuery(ctx, reference, prefix+"UNWIND [] AS value"+tc.tail, nil, neo4j.EagerResultTransformer)
					require.NoError(t, err)
					require.Equal(t, expected.Keys, result.Columns)
					require.Len(t, result.Rows, len(expected.Records))
				}
			})
		}
	}
}
