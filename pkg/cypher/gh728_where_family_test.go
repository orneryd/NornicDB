package cypher

// gh728_where_family_test.go — regression tests for #728 (WHERE family
// convergence): multi-MATCH WHERE chains, non-boolean WHERE predicates, and
// WITH … WHERE at the start of a correlated CALL body.
//
// Pinned against neo4j:5.26.30-community (issue tables).

import (
	"context"
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

func TestGh728_NonBooleanWhere(t *testing.T) {
	exec := newGh728Executor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:W728 {id: 'a', f: true})", nil)
	require.NoError(t, err)

	t.Run("literal_integer_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "MATCH (n:W728) WHERE 1 RETURN n.id", nil)
		require.Error(t, err)
	})
	t.Run("literal_list_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "MATCH (n:W728) WHERE [1] RETURN n.id", nil)
		require.Error(t, err)
	})
	t.Run("with_literal_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "WITH 1 AS x WHERE x RETURN x", nil)
		require.Error(t, err)
	})
	t.Run("unwind_with_string_arithmetic_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "UNWIND ['a'] AS x WITH x WHERE x + 1 RETURN x", nil)
		require.Error(t, err)
	})
	t.Run("call_yield_where_nonboolean_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "CALL db.labels() YIELD label WHERE label + 1 RETURN label", nil)
		require.Error(t, err)
	})
	t.Run("runtime_string_property_rejected", func(t *testing.T) {
		_, err := exec.Execute(ctx, "MATCH (n:W728) WHERE n.id RETURN n.id", nil)
		require.Error(t, err)
	})
	t.Run("runtime_string_expression_rejected", func(t *testing.T) {
		// #728: a non-boolean WHERE value is a type error in every clause
		// position — computed expressions included.
		_, err := exec.Execute(ctx, "MATCH (n:W728) WHERE n.id + 'z' RETURN n.id", nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Type mismatch")
	})
	t.Run("runtime_bool_property", func(t *testing.T) {
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
