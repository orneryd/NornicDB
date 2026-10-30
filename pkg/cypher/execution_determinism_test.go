package cypher

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The population is the function catalog crossed with the procedure registry's
// executable-input declarations, rather than a timestamp-specific fixture.
func TestExecutionDeterminismRegisteredInputs(t *testing.T) {
	exec := solR2CacheExecutor(t)
	ensureBuiltInProceduresRegistered()
	seen := map[string]bool{}
	functions, inputs := 0, 0
	for _, function := range cypherFunctionCatalog {
		if function.cacheVolatility == functionStable || seen[function.name] {
			continue
		}
		seen[function.name] = true
		functions++
		invocation := volatileTestInvocation(function)
		t.Run(function.name+"/outer", func(t *testing.T) {
			require.False(t, isCacheableReadQuery("RETURN "+invocation+" AS value"))
		})
		for _, procedure := range globalProcedureRegistry.ListBuiltIns() {
			for index, input := range procedure.ResultCache.CypherInputs {
				if input.StoredText != "" {
					continue
				}
				inputs++
				t.Run(fmt.Sprintf("%s/%s/%d", function.name, procedure.Name, index), func(t *testing.T) {
					args := make([]string, procedure.MinArgs)
					if len(args) <= input.Argument {
						args = make([]string, input.Argument+1)
					}
					params := map[string]any{}
					for i := range args {
						args[i] = "{}"
					}
					text := "RETURN " + invocation + " AS value"
					if input.Field != "" {
						text = invocation + " IS NOT NULL"
						params["request"] = map[string]any{"query": "transcript", input.Field: text}
						args[input.Argument] = "$request"
					} else {
						params["statement"] = text
						args[input.Argument] = "$statement"
					}
					query := "CALL " + procedure.Name + "(" + strings.Join(args, ",") + ")"
					ctx := withQueryParams(context.Background(), params)
					require.False(t, exec.canUseResultCache(ctx, analyzedReadQueryInfo(true), query), query)
				})
			}
		}
	}
	require.GreaterOrEqual(t, functions, 12, "include aliases and UUID, not only the original six spellings")
	require.Greater(t, inputs, functions, "exercise every declared executable argument")
}

func TestExecutionDeterminismPublicPredicates(t *testing.T) {
	exec := solR2CacheExecutor(t)
	require.NoError(t, exec.storage.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "CITES", Properties: map[string]any{"content": "library transcript"}}))
	exec.search().EnableRelationshipSearch(nil)
	exec.search().StartRelationshipIndex()
	seen := map[string]bool{}
	for _, function := range cypherFunctionCatalog {
		if function.cacheVolatility == functionStable || strings.HasPrefix(function.name, "gds.") || strings.HasPrefix(function.name, "graph.") || seen[function.name] {
			continue
		}
		seen[function.name] = true
		t.Run(function.name, func(t *testing.T) {
			invocation := volatileTestInvocation(function)
			queries := []string{"RETURN " + invocation + " AS value"}
			for _, procedure := range globalProcedureRegistry.ListBuiltIns() {
				if procedure.ResultCache.RetrievalRequest {
					queries = append(queries, "CALL "+procedure.Name+"($request)")
				} else if procedure.ResultCache.Deterministic && len(procedure.ResultCache.CypherInputs) != 0 {
					queries = append(queries, "CALL "+procedure.Name+"($statement, {}) YIELD value RETURN value")
				}
			}
			for _, query := range queries {
				hits, _, _, _, _ := exec.cache.Stats()
				params := map[string]any{
					"request":   map[string]any{"query": "transcript", "where": invocation + " IS NOT NULL"},
					"statement": "RETURN " + invocation + " AS value",
				}
				first, err := exec.Execute(context.Background(), query, params)
				require.NoError(t, err, query)
				second, err := exec.Execute(context.Background(), query, params)
				require.NoError(t, err, query)
				require.NotSame(t, first, second, "outer and parameterized predicates must execute: %s", query)
				afterHits, _, _, _, _ := exec.cache.Stats()
				require.Equal(t, hits, afterHits, "non-deterministic execution must not hit the outer cache: %s", query)
			}
		})
	}
}

// Use valid temporal/default and entity inputs, rather than proving rejection
// only with calls whose missing mandatory arguments would already fail.
func volatileTestInvocation(function cypherFunctionSpec) string {
	if function.cacheVolatility == functionTemporalTruncation {
		return function.name + "('day')"
	}
	if function.name == "decay" || function.name == "decayScore" {
		return function.name + "(null)"
	}
	return function.name + "()"
}

func TestExecutionDeterminismResolvedDependencies(t *testing.T) {
	exec := solR2CacheExecutor(t)
	for _, test := range []struct {
		query     string
		params    map[string]any
		cacheable bool
	}{
		{"RETURN 'timestamp() rand()' AS data", nil, true},
		{"RETURN 'LOAD CSV' AS data", nil, true},
		{"load csv from $url AS row RETURN row", nil, false},
		{"RETURN timestamp /* gap */ () AS now", nil, false},
		{"RETURN `randomUUID` ( ) AS id", nil, false},
		{"RETURN date('2020-01-01') AS day", nil, true},
		{"RETURN datetime({timezone:'UTC'}) AS now", nil, false},
		{"RETURN datetime.statement('UTC') AS now", nil, false},
		{"RETURN datetime.truncate('day') AS now", nil, false},
		{"RETURN datetime.truncate('day',datetime('2020-01-01T00:00:00Z')) AS fixed", nil, true},
		{"RETURN unknownFunction() AS value", nil, false},
		{"CALL unknown.procedure()", nil, false},
		{"CALL db.infer({prompt:'x',cache:true})", nil, false},
		{"CALL db.rretrieve({query:'timestamp()'})", nil, true},
		{"CALL db.rretrieve({query:'transcript',where:$predicate})", map[string]any{"predicate": "timestamp() < $deadline", "deadline": int64(1)}, false},
		{"CALL db.rretrieve($request)", map[string]any{"request": map[string]any{"query": "transcript", "where": "node.kind = $kind"}, "kind": "a"}, true},
		{"CALL db.rretrieve($request)", map[string]any{"request": map[string]any{"query": "transcript", "qid": "", "discard": false}}, true},
		{"CALL db.rretrieve($request)", map[string]any{"request": map[string]any{"query": "transcript", "mode": "id", "n": int64(1)}}, false},
		{"CALL apoc.cypher.run($statement,{})", map[string]any{"statement": "RETURN rand() AS value"}, false},
		{"CALL apoc.cypher.run($statement,{})", map[string]any{"statement": "RETURN 1 AS value"}, true},
		{"CALL apoc.cypher.run($statement,{})", map[string]any{"statement": "CREATE (:Synthetic)"}, false},
		{"CALL apoc.cypher.run($statement,{})", map[string]any{"statement": "CALL db.rretrieve($request)", "request": map[string]any{"query": "transcript"}}, true},
		{"CALL apoc.cypher.run($statement,{})", map[string]any{"statement": "CALL db.rretrieve($request)", "request": map[string]any{"query": "transcript", "where": "timestamp() < $deadline"}}, false},
		{"CALL apoc.cypher.run($statement,{request:$request})", map[string]any{"statement": "CALL db.rretrieve($request)", "request": map[string]any{"query": "transcript", "where": "timestamp() < $deadline"}}, false},
		{"CALL apoc.cypher.run($statement,{request:{query:'transcript',where:'timestamp() < $deadline'}})", map[string]any{"statement": "CALL db.rretrieve($request)", "request": map[string]any{"query": "transcript"}}, false},
		{"CALL apoc.cypher.run($statement,{request:{query:'transcript'}})", map[string]any{"statement": "CALL db.rretrieve($request)", "request": map[string]any{"query": "transcript", "where": "timestamp() < $deadline"}}, true},
	} {
		t.Run(test.query+fmt.Sprint(test.params), func(t *testing.T) {
			ctx := withQueryParams(context.Background(), test.params)
			require.Equal(t, test.cacheable, exec.canUseResultCache(ctx, analyzedReadQueryInfo(true), test.query))
		})
	}
}

func TestExecutionDeterminismNestedParameterBindings(t *testing.T) {
	exec := solR2CacheExecutor(t)
	const query = "CALL apoc.cypher.run($statement,{}) YIELD value RETURN value"
	deadline := time.Now().Add(time.Second)
	params := map[string]any{
		"statement": "CALL db.rretrieve($request)",
		"request":   map[string]any{"query": "transcript", "where": "timestamp() < $deadline"},
		"deadline":  deadline.UnixMilli(),
	}
	first, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err)
	require.Len(t, first.Rows, 2)
	time.Sleep(time.Until(deadline.Add(100 * time.Millisecond)))
	second, err := exec.Execute(context.Background(), query, params)
	require.NoError(t, err)
	require.Empty(t, second.Rows, "nested execution retains the caller's actual predicate bindings")
	params["request"] = map[string]any{"query": "transcript"}
	first, err = exec.Execute(context.Background(), query, params)
	require.NoError(t, err)
	requireOwnedResultCacheHit(t, exec, first, func() (*ExecuteResult, error) {
		return exec.Execute(context.Background(), query, params)
	})
}

func TestExecutionDeterminismPureParameterizedCacheHit(t *testing.T) {
	exec := solR2CacheExecutor(t)
	params := map[string]any{"request": map[string]any{"query": "transcript", "where": "node.kind = $kind"}, "kind": "a"}
	first, err := exec.Execute(context.Background(), "CALL db.rretrieve($request)", params)
	require.NoError(t, err)
	second := requireOwnedResultCacheHit(t, exec, first, func() (*ExecuteResult, error) {
		return exec.Execute(context.Background(), "CALL db.rretrieve($request)", params)
	})
	require.Len(t, second.Rows, 1)
}

func TestExecutionDeterminismFabricEmbeddedDeadline(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "determinism_local"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "determinism_composite", []multidb.ConstituentRef{
		{Alias: "local", DatabaseName: "determinism_local", Type: "local", AccessMode: "read_write"},
	}))
	store, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 64, TTL: time.Hour})
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	ctx := WithPermissionChecker(context.Background(), func(string) bool { return true })
	const query = "USE determinism_composite CALL { USE determinism_composite.local CALL apoc.cypher.run($statement,{}) YIELD value RETURN value.fresh AS fresh } RETURN fresh"
	deadline := time.Now().Add(time.Second)
	params := map[string]any{"statement": "RETURN timestamp() < $deadline AS fresh", "deadline": deadline.UnixMilli()}
	first, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Equal(t, [][]any{{true}}, first.Rows)
	time.Sleep(time.Until(deadline.Add(100 * time.Millisecond)))
	second, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Equal(t, [][]any{{false}}, second.Rows, "Fabric must classify the same embedded executable text as local admission")
}
