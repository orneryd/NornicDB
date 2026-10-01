package cypher

// gh728_where_family_test.go — regression tests for #728 (WHERE family
// convergence): multi-MATCH WHERE chains, non-boolean WHERE predicates, and
// WITH … WHERE at the start of a correlated CALL body.
//
// Pinned against neo4j:5.26.30-community (issue tables).

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newGh728Executor(t *testing.T) *StorageExecutor {
	t.Helper()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	store := storage.NewNamespacedEngine(base, "gh728")
	return NewStorageExecutor(store)
}

func TestMonster728ProductAggregateAllocations(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "product"), 0, 0)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "UNWIND range(0, 255) AS i CREATE (:Doc {id: i})", nil)
			require.NoError(t, err)
			if explicit {
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			result, err := exec.Execute(ctx, "MATCH (a:Doc), (b:Doc) RETURN count(*) AS c", nil)
			runtime.ReadMemStats(&after)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(65536)}}, result.Rows)
			t.Logf("total_alloc_bytes=%d", after.TotalAlloc-before.TotalAlloc)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8*1024*1024), "product bindings must be consumed incrementally")
		})
	}
}

func TestMonster728ProductAggregateSemantics(t *testing.T) {
	exec := newGh728Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Doc {id: 1}), (:Doc {id: 2})", nil)
	require.NoError(t, err)
	for _, testCase := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"MATCH (a:Doc), (b:Doc) RETURN sum(a.id) AS s, avg(b.id) AS a, min(a.id) AS lo, max(b.id) AS hi", [][]interface{}{{int64(6), float64(1.5), int64(1), int64(2)}}},
		{"MATCH (a:Doc), (b:Doc) RETURN a.id AS id, count(*) AS c, sum(b.id) AS s ORDER BY id", [][]interface{}{{int64(1), int64(2), int64(3)}, {int64(2), int64(2), int64(3)}}},
		{"MATCH (a:Doc), (b:Doc) WITH a.id AS id, sum(b.id) AS s RETURN id, s ORDER BY id", [][]interface{}{{int64(1), int64(3)}, {int64(2), int64(3)}}},
		{"MATCH (a:Doc), (b:Missing) RETURN count(*) AS c, sum(a.id) AS s", [][]interface{}{{int64(0), int64(0)}}},
		{"MATCH (a:Doc), (b:Doc), (c:Doc) RETURN count(*) AS c, sum(c.id) AS s", [][]interface{}{{int64(8), int64(12)}}},
		{"MATCH (a:Doc), (b:Doc) RETURN count(DISTINCT b.id) AS c, count(b.absent) AS missing, sum(a.id * 1.0) AS s", [][]interface{}{{int64(2), int64(0), float64(6)}}},
		{"MATCH (a:Doc), (b:Doc) RETURN a.id AS a, b.id AS b ORDER BY a, b", [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(2)}, {int64(2), int64(1)}, {int64(2), int64(2)}}},
		{"MATCH (a:Doc {id: 1}) MATCH (a:Doc), (b:Doc) RETURN a.id AS a, b.id AS b ORDER BY b", [][]interface{}{{int64(1), int64(1)}, {int64(1), int64(2)}}},
		{"MATCH (a:Doc), (a:Doc) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
}

func TestMonster728ProductSourceContracts(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "source"))
	ctx := withExpressionFailureSlot(context.Background())
	_, err := exec.Execute(ctx, "CREATE (:Doc {id: 1}), (:Doc {id: 2})", nil)
	require.NoError(t, err)
	for _, clause := range []string{
		"MATCH (a:Doc)",
		"MATCH (a:Doc)-[:R]->(b:Doc), (c:Doc)",
		"MATCH p = (a:Doc), (b:Doc)",
		"MATCH (a:Doc), (b:Doc) WHERE a.id = b.id",
		"MATCH (a:Doc), (b:Doc {id: a.id})",
	} {
		_, supported, err := exec.pipelineNodeProductSource(ctx, []pipelineRow{{}}, clause)
		require.NoError(t, err)
		require.False(t, supported, clause)
	}
	source, supported, err := exec.pipelineNodeProductSource(ctx, []pipelineRow{{"$id": int64(1)}, {"$id": int64(2)}, {"$id": int64(1)}}, "MATCH (a:Doc {id: $id}), (b:Doc)")
	require.NoError(t, err)
	require.True(t, supported)
	retained, completed := materializePipelineSource(source)
	require.True(t, completed)
	require.Len(t, retained, 6)
	for _, row := range retained {
		node, typed := row["a"].(*storage.Node)
		require.True(t, typed)
		require.Equal(t, row["$id"], node.Properties["id"])
	}
	visited := 0
	require.True(t, source(func(row pipelineRow) bool {
		visited++
		require.NotNil(t, row["a"])
		return false
	}))
	require.Equal(t, 1, visited)
	nullSource, supported, err := exec.pipelineNodeProductSource(ctx, []pipelineRow{{"a": nil}}, "MATCH (a:Doc), (b:Doc)")
	require.NoError(t, err)
	require.True(t, supported)
	empty, completed := materializePipelineSource(nullSource)
	require.True(t, completed)
	require.Empty(t, empty)
	canceled, cancel := context.WithCancel(withExpressionFailureSlot(context.Background()))
	cancel()
	canceledSource, supported, err := exec.pipelineNodeProductSource(canceled, []pipelineRow{{}}, "MATCH (a:Doc), (b:Doc)")
	require.NoError(t, err)
	require.True(t, supported)
	require.False(t, canceledSource(func(pipelineRow) bool { t.Error("canceled source yielded a row"); return true }))
	require.ErrorIs(t, getExpressionFailure(canceled), context.Canceled)
}

func TestComprehensionStrictPredicateAcrossExecutionRoutes(t *testing.T) {
	exec := newGh728Executor(t)
	for _, expression := range []string{"[x IN [1,2] WHERE x + 1]", "[x IN [1,2] WHERE x + 1 | x]"} {
		t.Run(expression, func(t *testing.T) {
			ctx := withExpressionFailureSlot(context.Background())
			exec.evaluateExpressionWithContextFull(ctx, expression, nil, nil, nil, nil, nil, 0)
			require.Error(t, getExpressionFailure(ctx))
			for _, prefix := range []string{"RETURN ", "CREATE (n:StrictComprehension) RETURN "} {
				_, err := exec.Execute(context.Background(), prefix+expression, nil)
				require.Error(t, err)
			}
		})
	}
}

func TestMapKeyOrderSnapshotsAndCachedResults(t *testing.T) {
	require.Nil(t, snapshotMapKeyOrders(context.Background()))
	failure := &expressionFailure{}
	ctx := context.WithValue(context.Background(), expressionFailureKey{}, failure)
	require.Nil(t, snapshotMapKeyOrders(ctx))
	keys := []string{"node", "k"}
	failure.recordMapKeyOrder(map[string]interface{}{"node": 1, "k": 2}, keys)
	snapshot := snapshotMapKeyOrders(ctx)
	keys[0] = "changed"
	failure.recordMapKeyOrder(map[string]interface{}{"other": 1}, []string{"other"})
	require.Len(t, snapshot, 1)
	for _, order := range snapshot {
		require.Equal(t, []string{"node", "k"}, order)
	}
	exec := newGh728Executor(t)
	for iteration := 0; iteration < 3; iteration++ {
		result, err := exec.Execute(context.Background(), "RETURN {node:1,k:2} AS m", nil)
		require.NoError(t, err)
		require.NotEmpty(t, result.MapKeyOrders)
		for _, order := range result.MapKeyOrders {
			require.Equal(t, []string{"node", "k"}, order)
		}
	}
}

func TestGh728_MultiMatchWhereChains(t *testing.T) {
	exec := newGh728Executor(t)
	ctx := context.Background()
	// Graph from the original report: a,b,c → x1/x2 KNOWS edges.
	_, err := exec.Execute(ctx, `
		CREATE (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'}),
		       (x1 {name: 'x1'}), (x2 {name: 'x2'}),
		       (a)-[:KNOWS]->(x1), (a)-[:KNOWS]->(x2),
		       (b)-[:KNOWS]->(x1), (b)-[:KNOWS]->(x2),
		       (c)-[:KNOWS]->(x1)
	`, nil)
	require.NoError(t, err)

	strRows := func(t *testing.T, result *ExecuteResult) []string {
		t.Helper()
		out := make([]string, 0, len(result.Rows))
		for _, row := range result.Rows {
			require.Len(t, row, 1)
			out = append(out, row[0].(string))
		}
		return out
	}

	t.Run("original_1_two_where_then_join", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a) WHERE a.name = 'A'
			MATCH (b) WHERE b.name = 'B'
			MATCH (a)-->(x), (b)-->(x)
			RETURN x.name AS x ORDER BY x
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"x1", "x2"}, strRows(t, result))
	})

	t.Run("original_2_three_where_then_join", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a) WHERE a.name = 'A'
			MATCH (b) WHERE b.name = 'B'
			MATCH (c) WHERE c.name = 'C'
			MATCH (a)-->(x), (b)-->(x), (c)-->(x)
			RETURN x.name AS x
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"x1"}, strRows(t, result))
	})

	t.Run("original_3_two_where_one_side", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a) WHERE a.name = 'A'
			MATCH (b) WHERE b.name = 'B'
			MATCH (a)-->(x)
			RETURN x.name AS x ORDER BY x
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"x1", "x2"}, strRows(t, result))
	})

	t.Run("original_4_prop_patterns_join", func(t *testing.T) {
		result, err := exec.Execute(ctx, `
			MATCH (a {name: 'A'}), (b {name: 'B'}), (c {name: 'C'})
			MATCH (a)-->(x), (b)-->(x), (c)-->(x)
			RETURN x.name AS x
		`, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"x1"}, strRows(t, result))
	})
}

func TestReportedPredicateContextVariants(t *testing.T) {
	for _, testCase := range []struct {
		name, query, code string
		rows              [][]interface{}
	}{
		{"and true", "MATCH (n:W1) WHERE n.id + 'z' AND true RETURN n.id", "", [][]interface{}{}},
		{"or comparison", "MATCH (n:W1) WHERE n.id + 'z' OR n.id = 'a' RETURN n.id", "", [][]interface{}{{"a"}}},
		{"numeric multiplication", "MATCH (n:W2) WHERE n.id * 0 RETURN n.id", "SyntaxError", nil},
		{"comprehension arithmetic", "MATCH (n:W2) RETURN [x IN [1, 2] WHERE x + n.id] AS l", "TypeError", nil},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			_, err := exec.Execute(context.Background(), "CREATE (:W1 {id:'a'}), (:W2 {id:1})", nil)
			require.NoError(t, err)
			result, err := exec.Execute(context.Background(), testCase.query, nil)
			if testCase.code != "" {
				require.ErrorContains(t, err, testCase.code)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.rows, result.Rows)
		})
	}
}

func TestGh728_NonBooleanWhere(t *testing.T) {
	exec := newGh728Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:W728 {id: 'a', f: true})", nil)
	require.NoError(t, err)

	t.Run("literal_integer_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "MATCH (n:W728) WHERE 1 RETURN n.id", nil)
		require.ErrorContains(t, err, "SyntaxError")
	})
	t.Run("literal_list_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "MATCH (n:W728) WHERE [1] RETURN n.id", nil)
		require.ErrorContains(t, err, "SyntaxError")
	})
	t.Run("with_literal_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "WITH 1 AS x WHERE x RETURN x", nil)
		require.ErrorContains(t, err, "SyntaxError")
	})
	t.Run("unwind_with_string_arithmetic_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "UNWIND ['a'] AS x WITH x WHERE x + 1 RETURN x", nil)
		require.ErrorContains(t, err, "SyntaxError")
	})
	t.Run("call_yield_where_nonboolean_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CALL db.labels() YIELD label WHERE label + 1 RETURN label", nil)
		require.ErrorContains(t, err, "SyntaxError")
	})
	t.Run("runtime_string_property_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "MATCH (n:W728) WHERE n.id RETURN n.id", nil)
		require.Error(t, err)
	})
	t.Run("runtime_string_expression_filters_row", func(t *testing.T) {
		for _, query := range []string{
			"MATCH (n:W728) WHERE n.id + 'z' RETURN n.id",
			"MATCH (n:W728) WITH n WHERE n.id + 'z' RETURN n.id",
		} {
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Empty(t, result.Rows, query)
		}
	})
	t.Run("runtime_bool_property", func(t *testing.T) {
		for _, predicate := range []string{"NOT (n.id + 'z')", "(n.id + 'z') AND n.f"} {
			_, err := exec.Execute(ctx, "MATCH (n:W728) WHERE "+predicate+" RETURN n.id", nil)
			require.ErrorContains(t, err, "TypeError", predicate)
		}
		result, err := exec.Execute(ctx, "MATCH (n:W728) WHERE n.f RETURN n.id", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
	})
	t.Run("missing_property_no_rows", func(t *testing.T) {
		result, err := exec.Execute(ctx, "MATCH (n:W728) WHERE n.missing RETURN n.id", nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows)
	})
}

func TestGh728_CallBodyWithWhere(t *testing.T) {
	exec := newGh728Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, `
		CREATE (a:W728 {id: 'a', f: true})-[:USES]->(b:W728 {id: 'b', f: false}),
		       (a)-[:USES]->(c:W728 {id: 'c', f: true})
	`, nil)
	require.NoError(t, err)

	strRows := func(t *testing.T, result *ExecuteResult) []string {
		t.Helper()
		out := make([]string, 0, len(result.Rows))
		for _, row := range result.Rows {
			require.Len(t, row, 1)
			if row[0] == nil {
				out = append(out, "<null>")
				continue
			}
			out = append(out, row[0].(string))
		}
		return out
	}

	t.Run("with_where_neq", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (i:W728) CALL (i) { WITH i WHERE i.id <> 'a' RETURN i.id AS id } RETURN id ORDER BY id", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"b", "c"}, strRows(t, result))
	})
	t.Run("with_where_bool_prop", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (i:W728) CALL (i) { WITH i WHERE i.f RETURN i.id AS id } RETURN id ORDER BY id", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "c"}, strRows(t, result))
	})
	t.Run("with_where_count_star", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (i:W728) CALL (i) { WITH i WHERE i.id = 'zz' RETURN i.id AS id } RETURN count(*) AS c", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(0), result.Rows[0][0])
	})
	t.Run("with_where_outer_var_projected", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (i:W728) CALL (i) { WITH i WHERE i.id <> 'a' RETURN i.id AS id } RETURN i.id AS i, id ORDER BY i", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2)
		require.Equal(t, "b", result.Rows[0][0])
		require.Equal(t, "b", result.Rows[0][1])
		require.Equal(t, "c", result.Rows[1][0])
		require.Equal(t, "c", result.Rows[1][1])
	})
	t.Run("with_alias_where", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (i:W728) CALL (i) { WITH i AS j WHERE j.id <> 'a' RETURN j.id AS id } RETURN id ORDER BY id", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"b", "c"}, strRows(t, result))
	})
	t.Run("with_extra_item_where", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (i:W728) CALL (i) { WITH i, 1 AS one WHERE i.id <> 'a' RETURN i.id AS id } RETURN id ORDER BY id", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"b", "c"}, strRows(t, result))
	})
	t.Run("uncorrelated_importing_with_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx,
			"MATCH (i:W728) CALL { WITH i WHERE i.id <> 'a' RETURN i.id AS id } RETURN id ORDER BY id", nil)
		require.Error(t, err)
	})
	t.Run("match_where_form", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (i:W728) CALL (i) { MATCH (i) WHERE i.id <> 'a' RETURN i.id AS id } RETURN id ORDER BY id", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"b", "c"}, strRows(t, result))
	})
	t.Run("unwind_import_where", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"UNWIND [1, 2, 3] AS x CALL (x) { WITH x WHERE x > 1 RETURN x * 10 AS y } RETURN y ORDER BY y", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 2)
		require.Equal(t, int64(20), result.Rows[0][0])
		require.Equal(t, int64(30), result.Rows[1][0])
	})
}

func TestGh728_CommaMatchWhereCorrectness(t *testing.T) {
	exec := newGh728Executor(t)
	ctx := context.Background()
	// Small-scale correctness pins for the #692 comma-form shapes (memory
	// behavior is covered by the pushdown code path, not by timing).
	_, err := exec.Execute(ctx, "UNWIND range(0, 99) AS i CREATE (:Doc {id: i})", nil)
	require.NoError(t, err)

	t.Run("one_sided_filter_and_join", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (a:Doc), (b:Doc) WHERE a.id < 50 AND b.id = a.id + 1 RETURN count(*) AS c", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(50), result.Rows[0][0])
	})
	t.Run("join_predicate_count", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (a:Doc), (b:Doc) WHERE b.id = a.id + 1 RETURN count(*) AS c", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(99), result.Rows[0][0])
	})
	t.Run("unfiltered_product_count", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (a:Doc), (b:Doc) RETURN count(*) AS c", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(10000), result.Rows[0][0])
	})
	t.Run("create_from_join", func(t *testing.T) {
		result, err := exec.Execute(ctx,
			"MATCH (a:Doc), (b:Doc) WHERE a.id < 50 AND b.id = a.id + 1 CREATE (a)-[:NEXT]->(b) RETURN count(*) AS c", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		require.Equal(t, int64(50), result.Rows[0][0])
	})
}

func TestGh728_OffsetJoinWhitespaceIndependentAndExact(t *testing.T) {
	// The offset join must parse without spaces around the operator and must
	// compare integer values exactly: above 2^53 a float64 residual would
	// drop the true pair (#692 review).
	exec := newGh728Executor(t)
	ctx := context.Background()
	// a = 2^53+3 (representable), b = 2^53+1 (rounds down in float64).
	_, err := exec.Execute(ctx, "CREATE (:Doc {id: $a}), (:Doc {id: $b})",
		map[string]interface{}{"a": int64(9007199254740995), "b": int64(9007199254740993)})
	require.NoError(t, err)

	for _, predicate := range []string{
		"a.id = b.id + 2",
		"a.id=b.id+2",
		"b.id = a.id - 2",
		"b.id=a.id- 2",
	} {
		result, err := exec.Execute(ctx,
			"MATCH (a:Doc), (b:Doc) WHERE "+predicate+" RETURN count(*) AS c", nil)
		require.NoError(t, err, "predicate %q", predicate)
		require.Equal(t, int64(1), result.Rows[0][0], "predicate %q must match exactly once", predicate)
	}

	// An exact miss: float64 rounding would falsely satisfy a.id = b.id + 1.
	result, err := exec.Execute(ctx,
		"MATCH (a:Doc), (b:Doc) WHERE a.id = b.id + 1 RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), result.Rows[0][0])
}
