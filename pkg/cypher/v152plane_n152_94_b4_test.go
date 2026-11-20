package cypher

import (
	"context"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// NOR-54 reproductions. Each test covers a matrix row whose established
// contract is already owned by a named test, and adds only the consumer or
// input axis that owner does not exercise:
//
//   - row 1 aggregation scope on both parser paths (TestAggregatingProjectionScope
//     runs the native parser only);
//   - row 2/3 the declared retrieval header with the *measured* result-cache
//     outcome for repeated and differently-parameterized executions, plain and
//     nested (TestSearchStreamParameterAwareEligibility measures eligibility,
//     not a cache hit);
//   - row 4 UNION headers on both parser paths with quoted names, comments and
//     a header mismatch in RUN as well as EXPLAIN
//     (TestResultHeaderUnionContractsAndBoundInputs is native-parser and
//     EXPLAIN-only for the mismatch);
//   - row 5 an explicit single COLLECT column that returns zero rows
//     (TestCollectSubqueryProjectionContractAcrossExecutionModes covers a
//     non-empty body);
//   - row 6 aggregation, UNION and quoted local bindings across the composite
//     boundary (the existing composite owners cover membership and UNION).
//
// Expected values come from the owning tests above, recorded on Neo4j 5.26.30,
// and from the approved COLLECT and retrieval result schemas.

// nor54RunStatement executes query in the given statement mode and returns the
// result and the Neo4j status code of any error.
func nor54RunStatement(t *testing.T, exec *StorageExecutor, mode, query string, params map[string]any) (*ExecuteResult, string) {
	t.Helper()
	result, err := exec.Execute(context.Background(), mode+query, params)
	if err != nil {
		code, _ := nornicerrors.Neo4jStatus(err)
		return result, code
	}
	return result, ""
}

// nor54CacheOutcome reports the measured result-cache hits and misses.
func nor54CacheOutcome(exec *StorageExecutor) (int64, int64) {
	hits, misses, _, _, _ := exec.cache.Stats()
	return hits, misses
}

// TestNOR54AggregationScopeBothParsers: an aggregating projection's scope is
// the same on both parser paths. A name the projection projects is readable by
// the following WHERE and ORDER BY; a name it does not project is rejected by
// the one scope check. Values are TestAggregatingProjectionScope's Neo4j
// results.
func TestNOR54AggregationScopeBothParsers(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor54_aggregation_scope"))
		for _, tc := range []struct {
			query string
			rows  [][]interface{}
		}{
			{
				query: "UNWIND [1, 2, 2] AS x WITH x AS y, count(*) AS c WHERE x > 1 RETURN y, c",
				rows:  [][]interface{}{{int64(2), int64(2)}},
			},
			{
				query: "UNWIND [1, 2, 2] AS x WITH x AS y, count(*) AS c /* projected */ WHERE x > 1 RETURN y, c",
				rows:  [][]interface{}{{int64(2), int64(2)}},
			},
			{
				query: "UNWIND [1, 2, 2] AS x WITH *, count(*) AS c ORDER BY x RETURN c",
				rows:  [][]interface{}{{int64(1)}, {int64(2)}},
			},
			{
				query: "UNWIND [1, 2, 2] AS x RETURN x AS y, count(*) AS c ORDER BY x",
				rows:  [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(2)}},
			},
		} {
			result, code := nor54RunStatement(t, exec, "", tc.query, nil)
			require.Empty(t, code, tc.query)
			require.Equal(t, tc.rows, result.Rows, tc.query)
		}
		for _, query := range []string{
			"UNWIND [1, 2, 2] AS x RETURN x + 1 AS y, count(*) AS c ORDER BY x",
			"UNWIND [3, 1, 2, 1] AS x WITH x, x * 10 AS y WITH count(*) AS c WHERE x > 1 RETURN *",
		} {
			_, code := nor54RunStatement(t, exec, "", query, nil)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
		}
	})
}

// TestNOR54UnionHeadersAcrossConsumers: a UNION statement's header is the first
// branch's columns, in order; each row is placed under those names, so a branch
// that projects them in another order still fills the first branch's header.
// Every consumer reports that one header, and a branch whose columns differ by
// name or by count is rejected before execution in RUN, EXPLAIN and PROFILE.
// Quoted column names and comments between branches change neither outcome.
func TestNOR54UnionHeadersAcrossConsumers(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor54_union_headers"))
		for _, tc := range []struct {
			query   string
			header  []string
			profile [][]interface{}
		}{
			{
				query:   "RETURN 1 AS first, 2 AS second UNION RETURN 4 AS second, 3 AS first",
				header:  []string{"first", "second"},
				profile: [][]interface{}{{int64(1), int64(2)}, {int64(3), int64(4)}},
			},
			{
				query:   "RETURN 1 AS `a b` /* head */ UNION /* tail */ RETURN 2 AS `a b`",
				header:  []string{"a b"},
				profile: [][]interface{}{{int64(1)}, {int64(2)}},
			},
			{
				query:   "CALL { RETURN 1 AS value /* branch */ UNION RETURN 2 AS value } RETURN *",
				header:  []string{"value"},
				profile: [][]interface{}{{int64(1)}, {int64(2)}},
			},
		} {
			require.Equal(t, tc.header, exec.StatementColumns(tc.query), tc.query)
			result, code := nor54RunStatement(t, exec, "", tc.query, nil)
			require.Empty(t, code, tc.query)
			require.Equal(t, tc.header, result.Columns, tc.query)
			require.ElementsMatch(t, tc.profile, result.Rows, tc.query)
			explained, code := nor54RunStatement(t, exec, "EXPLAIN ", tc.query, nil)
			require.Empty(t, code, tc.query)
			require.Equal(t, tc.header, explained.Columns, tc.query)
			require.Empty(t, explained.Rows, tc.query)
			profiled, code := nor54RunStatement(t, exec, "PROFILE ", tc.query, nil)
			require.Empty(t, code, tc.query)
			require.Equal(t, tc.header, profiled.Columns, tc.query)
			require.ElementsMatch(t, tc.profile, profiled.Rows, tc.query)
		}
		for _, query := range []string{
			"RETURN 1 AS left UNION RETURN 1 AS right",
			"RETURN 1 AS value UNION RETURN 2 AS value, 3 AS extra",
			"CALL { RETURN 1 AS left UNION RETURN 1 AS right } RETURN *",
		} {
			for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
				_, code := nor54RunStatement(t, exec, mode, query, nil)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, mode+query)
			}
			require.Empty(t, exec.StatementColumns(query), query)
		}
	})
}

// TestNOR54CollectExplicitColumnWithZeroRows: a COLLECT subquery's RETURN must
// name exactly one column. An explicit single column is accepted whether it
// produces rows or not and collects the empty list when it produces none;
// wildcard and multi-column RETURNs are rejected in RUN, EXPLAIN and PROFILE
// even when the body returns no rows. The non-empty equivalents are
// TestCollectSubqueryProjectionContractAcrossExecutionModes'.
func TestNOR54CollectExplicitColumnWithZeroRows(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor54_collect_zero_rows"))
		for _, tc := range []struct {
			query string
			rows  [][]interface{}
		}{
			{
				query: "RETURN COLLECT { UNWIND [] AS z RETURN z AS value } AS values",
				rows:  [][]interface{}{{[]interface{}{}}},
			},
			{
				query: "RETURN COLLECT { WITH 1 AS x UNWIND [] AS z RETURN x AS value } AS values",
				rows:  [][]interface{}{{[]interface{}{}}},
			},
		} {
			result, code := nor54RunStatement(t, exec, "", tc.query, nil)
			require.Empty(t, code, tc.query)
			require.Equal(t, []string{"values"}, result.Columns, tc.query)
			require.Equal(t, tc.rows, result.Rows, tc.query)
		}
		for _, body := range []string{
			"UNWIND [] AS z RETURN *",
			"WITH 1 AS x, 2 AS y UNWIND [] AS z RETURN *",
			"UNWIND [] AS z RETURN z AS first, z AS second",
		} {
			for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
				_, code := nor54RunStatement(t, exec, mode, "RETURN COLLECT { "+body+" } AS values", nil)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, mode+body)
			}
		}
	})
}

// TestNOR54CompositeBoundaryScope: scope established inside a constituent is
// retained across the composite boundary. An aggregating projection, a quoted
// named path comprehension and a CALL subquery return the same columns and
// values under USE comp.one as they do natively, and UNION keeps the first
// branch's header.
func TestNOR54CompositeBoundaryScope(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := newFabricCallRowsExecutor(t)
		_, err := exec.Execute(context.Background(), "USE comp.one CREATE (:N {id:'a'})-[:R]->(:N {id:'b'})", nil)
		require.NoError(t, err)
		for _, tc := range []struct {
			query   string
			header  []string
			profile [][]interface{}
		}{
			{
				query:   "USE comp.one UNWIND [1, 2, 2] AS x WITH x AS y, count(*) AS c WHERE x > 1 RETURN y, c",
				header:  []string{"y", "c"},
				profile: [][]interface{}{{int64(2), int64(2)}},
			},
			{
				query:   "USE comp.one UNWIND [1, 2, 2] AS x WITH x AS y, count(*) AS c WHERE x > 1 RETURN y, c UNION RETURN 9 AS y, 9 AS c",
				header:  []string{"y", "c"},
				profile: [][]interface{}{{int64(2), int64(2)}, {int64(9), int64(9)}},
			},
			{
				query:   "USE comp.one MATCH (n:N {id:'a'}) RETURN [`p p` = (n)-[r:R]->(m) | length(`p p`)] AS v",
				header:  []string{"v"},
				profile: [][]interface{}{{[]interface{}{int64(1)}}},
			},
			{
				query:   "USE comp.one WITH 7 AS `p p` MATCH (n:N {id:'a'}) RETURN size([`p p` = (n)-[r:R]->(m) | length(`p p`)]) AS v",
				header:  []string{"v"},
				profile: [][]interface{}{{int64(1)}},
			},
			{
				query:   "CALL { USE comp.one UNWIND [1, 2, 2] AS x WITH x AS y, count(*) AS c WHERE x > 1 RETURN y, c } RETURN y, c",
				header:  []string{"y", "c"},
				profile: [][]interface{}{{int64(2), int64(2)}},
			},
		} {
			result, code := nor54RunStatement(t, exec, "", tc.query, nil)
			require.Empty(t, code, tc.query)
			require.Equal(t, tc.header, result.Columns, tc.query)
			require.Equal(t, tc.profile, result.Rows, tc.query)
			explained, code := nor54RunStatement(t, exec, "EXPLAIN ", tc.query, nil)
			require.Empty(t, code, tc.query)
			require.Equal(t, tc.header, explained.Columns, tc.query)
			require.Empty(t, explained.Rows, tc.query)
			profiled, code := nor54RunStatement(t, exec, "PROFILE ", tc.query, nil)
			require.Empty(t, code, tc.query)
			require.Equal(t, tc.header, profiled.Columns, tc.query)
			require.Equal(t, tc.profile, profiled.Rows, tc.query)
		}
		// The composite route applies the same aggregating-projection scope check.
		_, code := nor54RunStatement(t, exec, "", "USE comp.one UNWIND [1, 2, 2] AS x RETURN x + 1 AS y, count(*) AS c ORDER BY x", nil)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
	})
}

// nor54RetrievalExecutor is the retrieval cache owners' fixture: a search
// service over two embedded documents and a result-cache policy.
func nor54RetrievalExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor54_retrieval")
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(t.Context(), &storage.Node{ID: id, Properties: map[string]any{"content": "library transcript", "kind": string(id)}})
		require.NoError(t, err)
	}
	svc := search.NewService(store, search.DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 64, TTL: time.Hour})
	exec.SetSearchService(svc)
	return exec
}

// TestNOR54RetrievalHeaderAndParameterizedCacheSeparation measures the actual
// result-cache outcome of a supported parameterized db.rretrieve shape instead
// of its eligibility: the repeated execution is a hit, the same statement text
// with different parameters is a miss that returns that request's own rows, and
// a later repetition of the first parameters is served by its own entry. The
// executed header is the registered result schema either way - a hit never
// substitutes a different header. The nested CALL subquery shape, which
// TestSearchStreamParameterAwareEligibility only admits, is measured too.
func TestNOR54RetrievalHeaderAndParameterizedCacheSeparation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := nor54RetrievalExecutor(t)
		declaredRows := retrievalResultSchemas(retrievalColumns)[0]
		requests := []struct {
			name   string
			params map[string]any
			rows   int
		}{
			{
				name:   "kind a",
				params: map[string]any{"request": map[string]any{"query": "transcript", "where": "node.kind = $kind"}, "kind": "a"},
				rows:   1,
			},
			{
				name:   "kind absent",
				params: map[string]any{"request": map[string]any{"query": "transcript", "where": "node.kind = $kind"}, "kind": "absent"},
				rows:   0,
			},
		}
		for _, tc := range []struct {
			name   string
			query  string
			header []string
		}{
			{name: "plain", query: "CALL db.rretrieve($request)", header: declaredRows},
			{name: "nested call subquery", query: "CALL { CALL db.rretrieve($request) YIELD node RETURN node } RETURN node", header: []string{"node"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				first := map[string]*ExecuteResult{}
				for _, request := range requests {
					hits, misses := nor54CacheOutcome(exec)
					result, code := nor54RunStatement(t, exec, "", tc.query, request.params)
					require.Empty(t, code, request.name)
					require.Equal(t, tc.header, result.Columns, request.name)
					require.Len(t, result.Rows, request.rows, request.name)
					if request.rows > 0 {
						node, ok := result.Rows[0][0].(*storage.Node)
						require.True(t, ok, "retrieval must return the requested node, got %T", result.Rows[0][0])
						require.Equal(t, request.params["kind"], node.Properties["kind"], "parameter-specific row identity")
					}
					afterHits, afterMisses := nor54CacheOutcome(exec)
					require.Equal(t, misses+1, afterMisses, "%s: a new parameter set must execute instead of reading another parameter set's entry", request.name)
					require.Equal(t, hits, afterHits, request.name)
					first[request.name] = result

					repeated, code := nor54RunStatement(t, exec, "", tc.query, request.params)
					require.Empty(t, code, request.name)
					hitHits, hitMisses := nor54CacheOutcome(exec)
					require.Equal(t, afterHits+1, hitHits, "%s: the repeated execution must be a measured cache hit", request.name)
					require.Equal(t, afterMisses, hitMisses, request.name)
					require.Equal(t, tc.header, repeated.Columns, "%s: a cache hit must deliver the declared header", request.name)
					require.Equal(t, first[request.name].Rows, repeated.Rows, request.name)
					require.NotSame(t, first[request.name], repeated, "%s: each hit delivers its own result", request.name)
				}
				// The second parameter set must not have replaced the first entry.
				beforeHits, beforeMisses := nor54CacheOutcome(exec)
				again, code := nor54RunStatement(t, exec, "", tc.query, requests[0].params)
				require.Empty(t, code)
				require.Equal(t, tc.header, again.Columns)
				require.Equal(t, first[requests[0].name].Rows, again.Rows)
				hits, misses := nor54CacheOutcome(exec)
				require.Equal(t, beforeHits+1, hits, "both parameter sets keep their own measured cache entry")
				require.Equal(t, beforeMisses, misses)
			})
		}
	})
}
