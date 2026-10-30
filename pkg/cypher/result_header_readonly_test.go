// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestResultHeaderProcedureFactsStayReadOnly(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "header-readonly"))
	const name = "custom.header_readonly_guard"
	t.Cleanup(func() {
		globalProcedureRegistry.mu.Lock()
		delete(globalProcedureRegistry.user, name)
		delete(globalProcedureRegistry.userFold, name)
		globalProcedureRegistry.generation.Add(1)
		globalProcedureRegistry.mu.Unlock()
	})
	var calls []string
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: name, Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 0,
		Signature: name + "() :: (z :: ANY, a :: STRING)",
	}, func(readerCtx context.Context, _ *StorageExecutor, query string, _ []interface{}) (*ExecuteResult, error) {
		calls = append(calls, query)
		return &ExecuteResult{Columns: []string{"z", "a"}, Rows: [][]interface{}{{int64(1), "value"}}}, nil
	}))
	for _, query := range []string{"CALL " + name + "()", "CALL " + name + "() YIELD *"} {
		clauses, ok := splitPipelineTextClausesAllowingProcedureCalls(t.Context(), query)
		require.True(t, ok)
		original := clauses[0].text
		plan, err := exec.buildExecutionPlan(query)
		require.NoError(t, err)
		before := len(calls)
		for i := 0; i < 2; i++ {
			require.Equal(t, []string{"z", "a"}, exec.StatementColumns(query))
			result, err := exec.Execute(ctx, "EXPLAIN "+query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"z", "a"}, result.Columns)
		}
		require.Len(t, calls, before, "header analysis and EXPLAIN must not call the procedure")
		after, ok := splitPipelineTextClausesAllowingProcedureCalls(t.Context(), query)
		require.True(t, ok)
		require.Equal(t, original, after[0].text, "cached clauses must remain the client's clauses")
		afterPlan, err := exec.buildExecutionPlan(query)
		require.NoError(t, err)
		require.Equal(t, plan, afterPlan, "header facts must not alter planning")
		require.NoError(t, exec.validateSyntax(ctx, query, false, syntaxStatement))
		require.NoError(t, exec.validateSemanticScopes(ctx, query))
		for _, prefix := range []string{"", "PROFILE "} {
			result, err := exec.Execute(ctx, prefix+query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"z", "a"}, result.Columns)
		}
		for _, received := range calls[before:] {
			// A header-only rewrite to explicit YIELD names would leak here.
			require.NotContains(t, received, "YIELD z")
			require.NotContains(t, received, "YIELD a")
		}
	}
	invalid := "WITH 1 AS z CALL " + name + "() YIELD z RETURN z"
	_, before := exec.Execute(ctx, invalid, nil)
	require.Error(t, before)
	exec.StatementColumns(invalid)
	_, after := exec.Execute(ctx, invalid, nil)
	require.Equal(t, statusText(before), statusText(after), "header analysis must not change validation")
}

func TestResultHeaderProfileChecksStreamColumns(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "header-stream-contract"))
	const name = "custom.header_stream_contract"
	t.Cleanup(func() {
		globalProcedureRegistry.mu.Lock()
		delete(globalProcedureRegistry.user, name)
		delete(globalProcedureRegistry.userFold, name)
		globalProcedureRegistry.generation.Add(1)
		globalProcedureRegistry.mu.Unlock()
	})
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: name, Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 0,
		Signature: name + "() :: (declared :: ANY)",
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"unexpected"}, Rows: [][]interface{}{{int64(1)}}}, nil
	}))
	result, err := exec.Execute(context.Background(), "EXPLAIN CALL "+name+"()", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"declared"}, result.Columns)
	result, err = exec.Execute(context.Background(), "PROFILE CALL "+name+"()", nil)
	require.Nil(t, result)
	require.ErrorContains(t, err, "Execution result columns do not match the statement schema")
}

func TestResultHeaderRegistryGenerationAndReturnedSlices(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "header-generation"))
	const name = "custom.header_generation_guard"
	t.Cleanup(func() {
		globalProcedureRegistry.mu.Lock()
		delete(globalProcedureRegistry.user, name)
		delete(globalProcedureRegistry.userFold, name)
		globalProcedureRegistry.generation.Add(1)
		globalProcedureRegistry.mu.Unlock()
	})
	for _, column := range []string{"first", "replacement"} {
		require.NoError(t, RegisterUserProcedure(ProcedureSpec{
			Name: name, Mode: ProcedureModeRead, MinArgs: 0, MaxArgs: 0,
			Returns: []ProcedureColumn{{Name: column, Type: "STRING"}},
		}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
			t.Fatal("metadata must not execute a handler")
			return nil, nil
		}))
		for _, suffix := range []string{"", " YIELD *"} {
			query := "CALL " + name + "()" + suffix
			columns := exec.StatementColumns(query)
			require.Equal(t, []string{column}, columns)
			columns[0] = "mutated"
			require.Equal(t, []string{column}, exec.StatementColumns(query), strings.TrimSpace(query))
		}
	}
}

func TestProjectedScopeAfterClause(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "projected-scope"))
	for _, tc := range []struct {
		clause          string
		incoming, names []string
		complete        bool
	}{
		{"WITH outer AS kept", []string{"hidden", "outer"}, []string{"kept"}, true},
		{"WITH count(*) AS total", []string{"hidden"}, []string{"total"}, true},
		{"UNWIND [] AS local", []string{"outer"}, []string{"local", "outer"}, true},
		{"MATCH (n:HeaderScope)-[r:EDGE]->(m)", []string{"outer"}, []string{"m", "n", "outer", "r"}, true},
		{"OPTIONAL MATCH (n:HeaderScope)", []string{"outer"}, []string{"n", "outer"}, true},
		{"CREATE (created:HeaderScope)", []string{"outer"}, []string{"created", "outer"}, true},
		{"MERGE (merged:HeaderScope)", []string{"outer"}, []string{"merged", "outer"}, true},
		{"LET local = 1", []string{"outer"}, []string{"local", "outer"}, true},
		{"SET outer.value = 1", []string{"outer"}, []string{"outer"}, true},
		{"REMOVE outer.value", []string{"outer"}, []string{"outer"}, true},
		{"DELETE outer", []string{"outer"}, []string{"outer"}, true},
		{"FOREACH (local IN [] | CREATE (:HeaderScope))", []string{"outer"}, []string{"outer"}, true},
		{"UNWIND [] AS", nil, nil, false},
		{"LET", nil, nil, false},
		{"CALL db.labels()", []string{"outer"}, []string{"label", "outer"}, true},
		{"CALL db.labels() YIELD *", []string{"outer"}, []string{"label", "outer"}, true},
		{"CALL db.labels() YIELD label AS kind", []string{"outer"}, []string{"kind", "outer"}, true},
		{"CALL { UNWIND [1] AS before RETURN * }", []string{"outer"}, []string{"before", "outer"}, true},
		{"CALL { UNWIND [] AS before RETURN before AS after }", []string{"outer"}, []string{"after", "outer"}, true},
		{"CALL { WITH outer RETURN outer AS imported }", []string{"outer"}, []string{"imported", "outer"}, true},
		{"CALL { WITH outer USE neo4j UNWIND [] AS before RETURN * }", []string{"outer"}, []string{"before", "outer"}, true},
		{"CALL { WITH outer USE comp.remote UNWIND [] AS before RETURN * UNION WITH outer USE comp.remote RETURN outer, 1 AS before }", []string{"outer"}, []string{"before", "outer"}, true},
		{"WITH 1 AS use RETURN *", nil, []string{"use"}, true},
		{"CALL (outer) { WITH 1 AS inner RETURN * }", []string{"outer"}, []string{"inner", "outer"}, true},
		{"CALL (*) { RETURN outer AS imported }", []string{"outer"}, []string{"imported", "outer"}, true},
		{"CALL () { WITH 1 AS local RETURN * }", []string{"outer"}, []string{"local", "outer"}, true},
		{"CALL (outer) { USE comp.remote WITH 1 AS inner RETURN * }", []string{"outer"}, []string{"inner", "outer"}, true},
		{"CALL { USE neo4j UNWIND [] AS before RETURN * }", []string{"outer"}, []string{"before", "outer"}, true},
		{"CALL { CALL { RETURN 1 AS deep } RETURN * }", []string{"outer"}, []string{"deep", "outer"}, true},
		{"RETURN outer AS shown", []string{"outer"}, []string{"shown"}, true},
		{"CALL custom.unknown_header_procedure()", []string{"outer"}, []string{"outer"}, false},
		{"LOAD CSV FROM 'file:///unused' AS row RETURN row", nil, nil, false},
	} {
		t.Run(tc.clause, func(t *testing.T) {
			original := append([]string(nil), tc.incoming...)
			var names []string
			complete := false
			exec.ProjectedScopeAfterClause(tc.clause, tc.incoming, false).Visit(func(known []string) { names = known; complete = true }, func() {})
			if complete {
				require.Equal(t, tc.names, names)
			} else {
				require.Nil(t, names)
			}
			require.Equal(t, tc.complete, complete)
			require.Equal(t, original, tc.incoming)
			if len(names) > 0 {
				names[0] = "mutated"
				var again []string
				exec.ProjectedScopeAfterClause(tc.clause, tc.incoming, false).Visit(func(known []string) { again = known }, func() { t.Fatal("known scope became unknown") })
				require.Equal(t, tc.names, again)
			}
		})
	}
}

func TestResultHeaderUnionContractsAndBoundInputs(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "header-contracts"))
	for _, operator := range []string{" UNION ", " UNION ALL "} {
		query := "RETURN 1 AS first, 2 AS second" + operator + "RETURN 4 AS second, 3 AS first"
		require.Equal(t, []string{"first", "second"}, exec.StatementColumns(query))
		for _, prefix := range []string{"EXPLAIN ", "", "PROFILE "} {
			result, err := exec.Execute(ctx, prefix+query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"first", "second"}, result.Columns)
		}
	}
	for _, query := range []string{
		"RETURN 1 AS left UNION RETURN 1 AS right",
		"RETURN 1 AS value UNION RETURN 1 AS value UNION ALL RETURN 1 AS value",
		"CALL { RETURN 1 AS left UNION RETURN 1 AS right } RETURN *",
	} {
		_, err := exec.Execute(ctx, "EXPLAIN "+query, nil)
		require.Error(t, err, query)
		require.Empty(t, exec.StatementColumns(query))
	}
	for _, query := range []string{"FINISH", "MATCH (n) FINISH", "CREATE (:HeaderNoRun)", "LOAD CSV FROM 'file:///unused' AS row"} {
		require.Empty(t, exec.StatementColumns(query), query)
	}
	query := "RETURN *"
	require.Empty(t, exec.StatementColumns(query))
	for _, name := range []string{"firstBinding", "differentBinding"} {
		exec.fabricRecordBindings = map[string]interface{}{name: int64(1)}
		require.Equal(t, []string{name}, exec.StatementColumns(query), "bound scopes must not share a text-only cached header")
	}
	exec.fabricRecordBindings = nil
	require.Empty(t, exec.StatementColumns(query))
	for _, query := range []string{
		"CYPHER 25 LET value = 1 CALL db.labels() YIELD label RETURN *",
		"CYPHER 25 UNWIND [] AS outerValue CALL { LET innerValue = 1 CALL db.labels() YIELD label RETURN * } RETURN *",
	} {
		columns := []string{"label", "value"}
		if strings.Contains(query, "outerValue") {
			columns = []string{"innerValue", "label", "outerValue"}
		}
		require.Equal(t, columns, exec.StatementColumns(query))
		for _, prefix := range []string{"EXPLAIN ", ""} {
			result, err := exec.Execute(ctx, prefix+query, nil)
			require.NoError(t, err)
			require.Equal(t, columns, result.Columns)
		}
	}
	complete := false
	exec.ProjectedScopeAfterClause("CALL { WITH outer }", []string{"outer"}, false).Visit(func([]string) { complete = true }, func() {})
	require.False(t, complete, "incomplete import facts cannot become a validation decision")
}

func TestResultHeaderExecutionCacheOnly(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "header-cache-only"))
	ctx := context.Background()
	query := "WITH 3 AS r73CacheOnly CALL db.labels() YIELD label RETURN *"
	key := projectionKey(query, resultmetadata.Known(nil), resultmetadata.Known(nil), false)

	exec.CachedResultColumns(ctx, query, nil).Visit(func([]string) { t.Fatal("a cache miss performed analysis") }, func() {})
	if _, found := projectionFactsCache.get(key); found {
		t.Fatal("lookup compiled a projection program")
	}
	require.Equal(t, []string{"label", "r73CacheOnly"}, exec.StatementColumns(query))
	exec.CachedResultColumns(ctx, query, nil).Visit(func(names []string) { require.Equal(t, []string{"label", "r73CacheOnly"}, names) }, func() { t.Fatal("owner did not populate the statement cache") })
	exec.ResultProjection(context.Background(), "RETURN 1 AS cheapProjection", nil, RawSource)
	present := projectionKey("RETURN 1 AS cheapProjection", resultmetadata.Known(nil), resultmetadata.Known(nil), false)
	if program, found := projectionFactsCache.get(present); !found || !program.complete {
		t.Fatal("execution projection did not use the shared owner program")
	}
	for _, tc := range []struct {
		query string
		names []string
	}{
		{"MATCH (n) RETURN n.name AS cold", []string{"cold"}},
		{"RETURN 1 AS cold UNION RETURN 2 AS cold", []string{"cold"}},
		{"CALL db.labels()", []string{"label"}},
		{"CALL db.labels() YIELD *", []string{"label"}},
		{"WITH 1 AS coldNoWalk RETURN *", []string{"coldNoWalk"}},
		{"CALL { RETURN 1 AS cold } CREATE (:NoOutput)", []string{}},
	} {
		columns, err := exec.StatementResultColumns(tc.query, RawSource)
		require.NoError(t, err)
		require.Equal(t, tc.names, columns)
		key := projectionKey(tc.query, resultmetadata.Known(nil), resultmetadata.Known(nil), false)
		program, found := projectionFactsCache.get(key)
		require.True(t, found)
		require.True(t, program.complete, "header came from a fallback instead of a complete owner program: %s", tc.query)
	}
}

func TestResultHeaderSharedExecutionProjection(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "header-projection-contract"))
	for _, tc := range []struct {
		query                        string
		incoming, expressions, names []string
		standalone, distinct         bool
		modifiers                    string
	}{
		{"MATCH (n)", nil, nil, nil, false, false, ""},
		{"RETURN", nil, nil, nil, false, false, ""},
		{"USE", nil, nil, nil, false, false, ""},
		{"RETURN *", nil, nil, nil, false, false, ""},
		{"RETURN 1 AS unionValue UNION RETURN 2 AS unionValue", nil, nil, nil, false, false, ""},
		{"RETURN value AS kept", nil, nil, nil, false, false, ""},
		{"RETURN value AS kept", []string{"value"}, []string{"value"}, []string{"kept"}, true, false, ""},
		{"USE neo4j RETURN 1 AS value", nil, []string{"1"}, []string{"value"}, true, false, ""},
		{"WITH 1 AS kept RETURN kept, 2 AS extra", nil, []string{"kept", "2"}, []string{"kept", "extra"}, false, false, ""},
		{"RETURN *", []string{"z", "a"}, []string{"a", "z"}, []string{"a", "z"}, true, false, ""},
		{"RETURN *, 2 AS a", []string{"z", "a"}, []string{"z", "2"}, []string{"z", "a"}, true, false, ""},
		{"RETURN DISTINCT value AS kept", nil, nil, nil, false, false, ""},
		{"RETURN DISTINCT value AS kept", []string{"value"}, []string{"value"}, []string{"kept"}, true, true, ""},
		{"RETURN value AS kept SKIP 1 LIMIT 0", nil, nil, nil, false, false, ""},
		{"RETURN value AS kept SKIP 1 LIMIT 0", []string{"value"}, []string{"value"}, []string{"kept"}, true, false, "SKIP 1 LIMIT 0"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			projection := exec.ResultProjection(context.Background(), tc.query, tc.incoming, RawSource)
			require.Equal(t, tc.expressions, projection.Expressions)
			require.Equal(t, tc.standalone, projection.Standalone)
			require.Equal(t, tc.distinct, projection.Distinct)
			require.Equal(t, tc.modifiers, projection.Modifiers)
			projection.Names.Visit(func(names []string) {
				require.NotNil(t, tc.names, "unknown projection exposed names")
				require.Equal(t, tc.names, names)
				if len(names) > 0 {
					names[0] = "mutated"
				}
			}, func() { require.Nil(t, tc.names, "known projection became unknown") })
			if tc.names != nil {
				exec.ResultProjection(context.Background(), tc.query, tc.incoming, RawSource).Names.Visit(func(names []string) {
					require.Equal(t, tc.names, names)
				}, func() { t.Fatal("projection was mutated") })
			}
		})
	}
}

// A cache hint exposes result headers, not the broader set of legal YIELD
// names. Conditional headers remain unresolved for single-header consumers.
func TestCachedHeadersPreserveDeclaredProcedureShapes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "closed_headers"))
	ctx := context.Background()
	for _, sample := range []struct {
		query  string
		header []string
	}{
		{"CALL db.index.fulltext.createNodeIndex('closed_header', ['Memory'], ['content'])", []string{"name", "labels", "properties"}},
		{"CALL db.rretrieve({query:'closed header'})", nil},
		{"SHOW USERS", showUsersColumns},
		{"SHOW CURRENT USER", showUsersColumns},
	} {
		t.Run(sample.query, func(t *testing.T) {
			exec.StatementFacts(sample.query, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(resultmetadata.Statement) {}, func() { t.Fatal("missing procedure facts") }, func(reason resultmetadata.Unresolved) { t.Fatalf("unresolved: %+v", reason) })
			exec.CachedResultColumns(ctx, sample.query, nil).Visit(func(header []string) {
				require.NotNil(t, sample.header, "a conditional procedure cannot supply one fixed header")
				require.Equal(t, sample.header, header)
			}, func() { require.Nil(t, sample.header, "fixed header was lost") })
		})
	}
}
