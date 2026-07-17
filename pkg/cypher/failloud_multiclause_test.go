package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ====================================================================================
// BUG: Multi-clause Cypher shapes silently returned CORRUPT results
// ====================================================================================
// Discovered: 2026-07 (fail-loud audit of the string-slicing WITH interpreters)
// Reporter: Cypher executor correctness review
// Impact: The worst failure mode for a graph database — silent data corruption.
//   Multi-clause queries routed into the legacy string interpreters
//   (executeMatchWithClause / executeMatchWithOptionalMatch /
//   executeCompoundMatchOptionalMatch) returned, for a handful of shapes, raw
//   expression text as a column value, a boolean/nil where a real value was
//   expected, skipped DISTINCT deduplication, or dropped every row — with NO
//   error. A caller could not distinguish a wrong answer from a right one.
// Root Cause: These interpreters slice the query as strings and have no
//   evaluation path for the shapes below, yet fell through to a text-substitution
//   fallback (or an empty projection) instead of erroring.
// Fix: unsupportedMultiClauseShape (failloud_guard.go) detects exactly these
//   shapes at the handler entry and returns a clear, actionable error mirroring
//   the existing "unsupported clause after CALL {}" contract. Single-clause and
//   working multi-clause shapes are untouched.
// Neo4j Behavior: Neo4j evaluates all of these correctly; NornicDB does not, so
//   failing loud (rather than lying) is the correct interim contract.
// ====================================================================================

// setupFailLoudGraph creates a tiny Person/KNOWS graph used by the fail-loud
// regression tests. Alice(30) -KNOWS-> Bob(25) -KNOWS-> Carol(40).
func setupFailLoudGraph(t *testing.T) (*StorageExecutor, storage.Engine, context.Context) {
	t.Helper()
	exec, store := newTestExecutor(t)
	ctx := context.Background()
	nodes := []*storage.Node{
		{ID: "a", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice", "age": int64(30)}},
		{ID: "b", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob", "age": int64(25)}},
		{ID: "c", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Carol", "age": int64(40)}},
	}
	for _, n := range nodes {
		if _, err := store.CreateNode(n); err != nil {
			t.Fatalf("create node %s: %v", n.ID, err)
		}
	}
	edges := []*storage.Edge{
		{ID: "e1", StartNode: "a", EndNode: "b", Type: "KNOWS"},
		{ID: "e2", StartNode: "b", EndNode: "c", Type: "KNOWS"},
	}
	for _, e := range edges {
		if err := store.CreateEdge(e); err != nil {
			t.Fatalf("create edge %s: %v", e.ID, err)
		}
	}
	return exec, store, ctx
}

// assertFailLoud asserts that the query returns a fail-loud error whose message
// carries the "unsupported multi-clause query" prefix and the given substring.
func assertFailLoud(t *testing.T, exec *StorageExecutor, ctx context.Context, query, wantSubstr string) {
	t.Helper()
	_, err := exec.Execute(ctx, query, nil)
	require.Error(t, err, "query should fail loud, not return corrupt rows: %s", query)
	assert.Contains(t, err.Error(), "unsupported multi-clause query",
		"error should use the fail-loud contract prefix")
	assert.Contains(t, strings.ToLower(err.Error()), strings.ToLower(wantSubstr),
		"error should name the unsupported shape")
}

// TestBug_FailLoudMultiClause_Symptoms covers the seven confirmed corrupt-output
// shapes. Each subtest asserts the query now returns a clear error instead of
// silently emitting garbage.
func TestBug_FailLoudMultiClause_Symptoms(t *testing.T) {
	exec, _, ctx := setupFailLoudGraph(t)

	t.Run("1_return_distinct_after_with", func(t *testing.T) {
		// Was: 3 rows of nil, deduplication skipped.
		assertFailLoud(t, exec, ctx,
			`MATCH (a:Person) WITH a RETURN DISTINCT a.name AS name`, "RETURN DISTINCT")
	})

	t.Run("1b_return_distinct_after_optional_match", func(t *testing.T) {
		// Was: 3 rows of literal string "DISTINCT a.name".
		assertFailLoud(t, exec, ctx,
			`MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN DISTINCT a.name AS name`, "RETURN DISTINCT")
	})

	t.Run("2_length_path_across_optional_match", func(t *testing.T) {
		// Was: zero rows (path binding lost across the OPTIONAL MATCH join).
		assertFailLoud(t, exec, ctx,
			`MATCH p=(a:Person)-[:KNOWS]->(b:Person) WITH p WHERE a IS NOT NULL OPTIONAL MATCH (b)-[:KNOWS]->(c) RETURN length(p)`,
			"length(<path>)")
	})

	t.Run("3_min_over_node_pattern", func(t *testing.T) {
		// Was: 3 rows of raw ages (no aggregation).
		assertFailLoud(t, exec, ctx,
			`MATCH (a:Person) WITH a RETURN min(a.age) AS m`, "min()/max()/avg()")
	})

	t.Run("3b_max_over_node_pattern", func(t *testing.T) {
		// Was: 3 rows of raw ages (no aggregation).
		assertFailLoud(t, exec, ctx,
			`MATCH (a:Person) WITH a RETURN max(a.age) AS m`, "min()/max()/avg()")
	})

	t.Run("3c_avg_over_node_pattern", func(t *testing.T) {
		// Was: one row of nil (avg has no evaluation case).
		assertFailLoud(t, exec, ctx,
			`MATCH (a:Person) WITH a RETURN avg(a.age) AS m`, "min()/max()/avg()")
	})

	t.Run("4_with_aggregation_then_optional_match", func(t *testing.T) {
		// Was: aggregated column projected as nil for every row.
		assertFailLoud(t, exec, ctx,
			`MATCH (a:Person) WITH a, count(*) AS c OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a.name, c`,
			"aggregation in WITH followed by OPTIONAL MATCH")
	})

	t.Run("5_comma_multi_pattern_with", func(t *testing.T) {
		// Was: zero rows (only the first comma-separated pattern was parsed).
		assertFailLoud(t, exec, ctx,
			`MATCH (a:Person), (b:Person) WITH a, b RETURN a.name, b.name`,
			"comma-separated multi-pattern MATCH")
	})

	t.Run("6_pattern_comprehension_projection", func(t *testing.T) {
		// Was: boolean true instead of the projected list.
		assertFailLoud(t, exec, ctx,
			`MATCH (a:Person) WITH a RETURN [(a)-[:KNOWS]->(b) | b.name] AS friends`,
			"pattern comprehension")
	})

	t.Run("7_zero_length_varlength_in_primary_match", func(t *testing.T) {
		// Was: hop-0 rows leaked the literal string "b.name".
		assertFailLoud(t, exec, ctx,
			`MATCH (a:Person)-[:KNOWS*0..2]->(b) WITH a, b RETURN a.name, b.name`,
			"zero-length variable-length pattern")
	})
}

// TestBug_FailLoudMultiClause_Variations exercises regression variations:
// the guard must still fire when trailing clauses (ORDER BY / SKIP / LIMIT),
// parameters, mixed case, or newlines are present, and it must key off the
// actual shape rather than incidental substrings.
func TestBug_FailLoudMultiClause_Variations(t *testing.T) {
	exec, _, ctx := setupFailLoudGraph(t)

	t.Run("distinct_with_order_by_limit", func(t *testing.T) {
		assertFailLoud(t, exec, ctx,
			`MATCH (a:Person) WITH a RETURN DISTINCT a.name AS name ORDER BY name LIMIT 2`, "RETURN DISTINCT")
	})

	t.Run("distinct_lowercase_keywords", func(t *testing.T) {
		assertFailLoud(t, exec, ctx,
			`match (a:Person) with a return distinct a.name as name`, "RETURN DISTINCT")
	})

	t.Run("min_with_newlines_and_alias", func(t *testing.T) {
		assertFailLoud(t, exec, ctx,
			"MATCH (a:Person)\nWITH a\nRETURN min(a.age) AS youngest", "min()/max()/avg()")
	})

	t.Run("with_aggregation_then_optional_with_parameter", func(t *testing.T) {
		// Parameter must not hide the shape (guard runs pre-substitution).
		_, err := exec.Execute(ctx,
			`MATCH (a:Person) WHERE a.age > $min WITH a, count(*) AS c OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a.name, c`,
			map[string]interface{}{"min": 10})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "aggregation in WITH followed by OPTIONAL MATCH")
	})

	t.Run("comma_multi_pattern_property_map_not_a_top_level_comma", func(t *testing.T) {
		// A comma inside a {property map} is NOT a top-level multi-pattern comma,
		// so this single-pattern query must NOT be rejected by Rule 5.
		res, err := exec.Execute(ctx,
			`MATCH (a:Person {name: 'Alice', age: 30}) WITH a RETURN a.name AS name`, nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		assert.Equal(t, "Alice", res.Rows[0][0])
	})
}

// TestBug_FailLoudMultiClause_SingleClauseUnaffected proves the guard never
// fires for single-clause queries (which are handled correctly elsewhere) or
// for supported multi-clause shapes. These MUST return real data.
func TestBug_FailLoudMultiClause_SingleClauseUnaffected(t *testing.T) {
	exec, _, ctx := setupFailLoudGraph(t)

	t.Run("single_clause_return_distinct", func(t *testing.T) {
		res, err := exec.Execute(ctx, `MATCH (a:Person) RETURN DISTINCT a.name AS name`, nil)
		require.NoError(t, err)
		assert.Len(t, res.Rows, 3)
	})

	t.Run("single_clause_min", func(t *testing.T) {
		res, err := exec.Execute(ctx, `MATCH (a:Person) RETURN min(a.age) AS m`, nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		assert.Equal(t, int64(25), res.Rows[0][0])
	})

	t.Run("single_clause_max", func(t *testing.T) {
		res, err := exec.Execute(ctx, `MATCH (a:Person) RETURN max(a.age) AS m`, nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		assert.Equal(t, int64(40), res.Rows[0][0])
	})

	t.Run("bare_with_then_optional_match", func(t *testing.T) {
		res, err := exec.Execute(ctx,
			`MATCH (a:Person) WITH a OPTIONAL MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name`, nil)
		require.NoError(t, err)
		assert.Len(t, res.Rows, 3)
	})

	t.Run("with_count_projection_return", func(t *testing.T) {
		res, err := exec.Execute(ctx,
			`MATCH (a:Person) WITH a.name AS n, count(*) AS c RETURN n, c`, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, res.Rows)
	})

	t.Run("with_collect_projection", func(t *testing.T) {
		res, err := exec.Execute(ctx,
			`MATCH (a:Person) WITH a RETURN collect(a.name) AS names`, nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
	})

	t.Run("min_over_relationship_path_still_works", func(t *testing.T) {
		// The relationship handler implements min()/length() correctly; the guard
		// must NOT reject it (Rule 3 is node-pattern only).
		res, err := exec.Execute(ctx,
			`MATCH p=(a:Person)-[:KNOWS]->(b:Person) WITH p RETURN min(length(p)) AS ml`, nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		assert.Equal(t, int64(1), toInt64Loud(t, res.Rows[0][0]))
	})

	t.Run("with_scalar_arithmetic_projection", func(t *testing.T) {
		// The string-substitution fallback legitimately computes arithmetic on
		// WITH-bound scalars; the guard must not disturb it.
		res, err := exec.Execute(ctx,
			`MATCH (a:Person) WITH a.age AS age RETURN age * 2 AS doubled`, nil)
		require.NoError(t, err)
		assert.Len(t, res.Rows, 3)
	})

	t.Run("zero_length_varlength_inside_optional_match_works", func(t *testing.T) {
		// A *0.. inside OPTIONAL MATCH is a supported subgraph traversal — it must
		// NOT be rejected (Rule 7 is scoped to the primary MATCH pattern).
		res, err := exec.Execute(ctx,
			`MATCH (a:Person {name: 'Alice'}) OPTIONAL MATCH (a)-[:KNOWS*0..2]->(b) WITH DISTINCT b WHERE b IS NOT NULL RETURN b.name`, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, res.Rows)
	})
}

// toInt64Loud coerces an int-ish result value to int64 for assertions.
func toInt64Loud(t *testing.T, v interface{}) int64 {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	default:
		t.Fatalf("expected int-ish value, got %T (%v)", v, v)
		return 0
	}
}

// TestUnsupportedMultiClauseShape_Unit exercises the detector directly, covering
// the classification boundaries and the empty/degenerate inputs that never reach
// it through a live query (empty string, RETURN-only, no preceding clause).
func TestUnsupportedMultiClauseShape_Unit(t *testing.T) {
	exec, _ := newTestExecutor(t)

	tests := []struct {
		name         string
		query        string
		wantRejected bool
	}{
		{"empty", "", false},
		{"return_only_single_clause", "MATCH (n) RETURN n", false},
		{"single_clause_distinct", "MATCH (n) RETURN DISTINCT n.name", false},
		{"no_return", "MATCH (n) WITH n", false},
		{"with_return_distinct", "MATCH (n) WITH n RETURN DISTINCT n.name", true},
		{"optional_return_distinct", "MATCH (n) OPTIONAL MATCH (n)-[:R]->(m) RETURN DISTINCT n.name", true},
		{"with_min_node", "MATCH (n) WITH n RETURN min(n.age)", true},
		{"with_max_node", "MATCH (n) WITH n RETURN max(n.age)", true},
		{"with_avg_node", "MATCH (n) WITH n RETURN avg(n.age)", true},
		{"with_count_node_ok", "MATCH (n) WITH n RETURN count(n)", false},
		{"with_min_over_rel_ok", "MATCH (a)-[:R]->(b) WITH a, b RETURN min(a.age)", false},
		{"comma_multipattern_with", "MATCH (a), (b) WITH a, b RETURN a, b", true},
		{"pattern_comprehension", "MATCH (a) WITH a RETURN [(a)-[:R]->(b) | b.name] AS f", true},
		{"list_comprehension_not_pattern", "MATCH (a) WITH a RETURN [x IN a.tags | x] AS f", false},
		{"zero_len_primary", "MATCH (a)-[:R*0..2]->(b) WITH a, b RETURN a.name, b.name", true},
		{"zero_len_in_optional_ok", "MATCH (a) OPTIONAL MATCH (a)-[:R*0..2]->(b) WITH b RETURN b.name", false},
		{"with_agg_then_optional", "MATCH (a) WITH a, count(*) AS c OPTIONAL MATCH (a)-[:R]->(b) RETURN a.name, c", true},
		{"length_path_across_optional", "MATCH p=(a)-[:R]->(b) WITH p OPTIONAL MATCH (b)-[:R]->(c) RETURN length(p)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detail, rejected := exec.unsupportedMultiClauseShape(tt.query)
			assert.Equal(t, tt.wantRejected, rejected, "detail=%q", detail)
			if rejected {
				assert.NotEmpty(t, detail, "a rejection must carry an actionable detail")
			}
		})
	}
}

// TestFailLoudHelpers_Unit covers the pure string helpers, including the nil/
// empty and boundary cases.
func TestFailLoudHelpers_Unit(t *testing.T) {
	t.Run("leadingKeyword", func(t *testing.T) {
		assert.True(t, leadingKeyword("DISTINCT a.name", "DISTINCT"))
		assert.True(t, leadingKeyword("  distinct x", "DISTINCT"))
		assert.True(t, leadingKeyword("DISTINCT", "DISTINCT"))
		assert.True(t, leadingKeyword("DISTINCT\nx", "DISTINCT"))
		assert.False(t, leadingKeyword("DISTINCTLY x", "DISTINCT"), "must respect word boundary")
		assert.False(t, leadingKeyword("a.name", "DISTINCT"))
		assert.False(t, leadingKeyword("", "DISTINCT"))
		assert.False(t, leadingKeyword("DIS", "DISTINCT"))
	})

	t.Run("hasTopLevelComma", func(t *testing.T) {
		assert.True(t, hasTopLevelComma("(a), (b)"))
		assert.False(t, hasTopLevelComma("(a {x:1, y:2})"))
		assert.False(t, hasTopLevelComma("a IN [1, 2, 3]"))
		assert.False(t, hasTopLevelComma("(a)"))
		assert.False(t, hasTopLevelComma(""))
		assert.False(t, hasTopLevelComma("'a, b'"), "comma inside a quoted string is not top level")
	})

	t.Run("looksLikePatternComprehension", func(t *testing.T) {
		assert.True(t, looksLikePatternComprehension("[(a)-[:R]->(b) | b.name]"))
		assert.True(t, looksLikePatternComprehension("[ (a)-[:R]-(b) | b ]"))
		assert.True(t, looksLikePatternComprehension("[(b)<-[:R]-(a) | a.name]"), "incoming arrow")
		assert.True(t, looksLikePatternComprehension("[(a)-->(b) | b]"), "bare arrow")
		assert.False(t, looksLikePatternComprehension("[(a)-[:R]->(b)]"), "no pipe projection")
		assert.False(t, looksLikePatternComprehension("[x IN list | x]"), "list comprehension is not a pattern comprehension")
		assert.False(t, looksLikePatternComprehension("[(a) | a]"), "no relationship arrow")
		assert.False(t, looksLikePatternComprehension("a.name"))
		assert.False(t, looksLikePatternComprehension(""))
	})

	t.Run("indexBracketParen", func(t *testing.T) {
		assert.Equal(t, 0, indexBracketParen("[(a)]"))
		assert.Equal(t, 2, indexBracketParen("x [ (a)]"))
		assert.Equal(t, -1, indexBracketParen("[a]"))
		assert.Equal(t, -1, indexBracketParen(""))
	})

	t.Run("projectionUsesFunc", func(t *testing.T) {
		items := []returnItem{{expr: "a.name"}, {expr: "length(p)"}}
		assert.True(t, projectionUsesFunc(items, "length"))
		assert.False(t, projectionUsesFunc(items, "size"))
		assert.False(t, projectionUsesFunc(nil, "length"))
	})
}

// BenchmarkFailLoudGuard_SupportedWithQuery measures the end-to-end cost of a
// supported multi-clause WITH query, which now runs the fail-loud guard on
// entry. Used to prove the guard adds no material overhead to the read path.
func BenchmarkFailLoudGuard_SupportedWithQuery(b *testing.B) {
	exec, store := newTestExecutor(b)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_, _ = store.CreateNode(&storage.Node{
			ID:         storage.NodeID(string(rune('A'+i%26)) + string(rune('0'+i%10)) + "-" + itoaLoud(i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "P", "age": int64(20 + i%50)},
		})
	}
	q := `MATCH (a:Person) WITH a.age AS age, count(*) AS c RETURN age, c`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUnsupportedMultiClauseShape measures the detector in isolation on a
// supported query (the common case: it must scan and return false cheaply).
func BenchmarkUnsupportedMultiClauseShape(b *testing.B) {
	exec, _ := newTestExecutor(b)
	q := `MATCH (a:Person) WITH a.age AS age, count(*) AS c RETURN age, c`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = exec.unsupportedMultiClauseShape(q)
	}
}

func itoaLoud(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [8]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// BenchmarkFailLoudGuard_UncachedWithQuery measures a FULL multi-clause WITH
// query execution with the result cache cleared every iteration, so every call
// pays the guard cost (worst case). Used for the before/after regression check.
func BenchmarkFailLoudGuard_UncachedWithQuery(b *testing.B) {
	exec, store := newTestExecutor(b)
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_, _ = store.CreateNode(&storage.Node{
			ID:         storage.NodeID("p-" + itoaLoud(i)),
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "P", "age": int64(20 + i%50)},
		})
	}
	q := `MATCH (a:Person) WITH a.age AS age, count(*) AS c RETURN age, c`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.ClearQueryCaches()
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			b.Fatal(err)
		}
	}
}
