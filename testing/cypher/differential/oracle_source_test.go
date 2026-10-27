package differential

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOracleReadsExecutableSource(t *testing.T) {
	for _, tc := range []struct {
		query             string
		arbitrary, writes bool
	}{
		{"MATCH (n) RETURN n AS `LIMIT` ORDER BY n.id", false, false},
		{"MATCH (n) RETURN 'LIMIT CREATE' AS x", false, false},
		{"MATCH (n) RETURN n /* ORDER BY n.id */ LIMIT 1", true, false},
		{"MATCH (n) RETURN n ORDER\u00a0BY n.id LIMIT 1", false, false},
		{"CREATE (n {x:'LIMIT'}) RETURN n", false, true},
		{"MATCH (n) RETURN n // SET n.x=1", false, false},
		{"MATCH (n) RETURN n.limit", false, false},
		{"CREATE (n {limit:1})", false, true},
		{"MATCH (n) RETURN n.create", false, false},
		{"MATCH (n) RETURN {limit:1}", false, false},
		{`MATCH (n) RETURN '\u004cIMIT CREATE' AS x`, false, false},
		{`MATCH (n) RETURN \u0027LIMIT CREATE\u0027 AS x`, false, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			require.Equal(t, tc.arbitrary, Arbitrary(tc.query))
			require.Equal(t, tc.writes, Writes(tc.query))
		})
	}
	rows := func(values ...any) Outcome {
		out := Outcome{Columns: []string{"v"}}
		for _, value := range values {
			out.Rows = append(out.Rows, []any{value})
		}
		return out
	}
	require.False(t, Same("MATCH (n) RETURN n AS `LIMIT` ORDER BY n.id", rows(1, 2), rows(9, 8)))
	require.False(t, Same("MATCH (n) RETURN n.limit", rows(1), rows(9)))
	require.True(t, Same("UNWIND [1,2] AS x RETURN x // ORDER BY x", rows(1, 2), rows(2, 1)))
	require.False(t, Same("RETURN [1,2] AS v // collect(", rows([]any{1, 2}), rows([]any{2, 1})))
	require.True(t, Same("UNWIND [1,2] AS x RETURN collect (x) AS v", rows([]any{1, 2}), rows([]any{2, 1})))
}

func TestOracleRetainsDistinctUTF16Units(t *testing.T) {
	// Independent byte fixtures, not the engine's name decoder.
	first := string([]byte{0xed, 0xa0, 0x80})
	second := string([]byte{0xed, 0xa0, 0x81})
	answer := func(name string) Outcome {
		return Outcome{Columns: []string{"v"}, Rows: [][]any{{map[string]any{name: name}}}}
	}
	require.True(t, Same("RETURN {} AS v", answer(first), answer(first)))
	require.False(t, Same("RETURN {} AS v", answer(first), answer(second)))
}

func TestOracleClauseRolesAcrossQueryScopes(t *testing.T) {
	for _, test := range []struct {
		query             string
		arbitrary, writes bool
	}{
		{"MATCH (limit) RETURN limit", false, false},
		{"MATCH (skip) RETURN skip", false, false},
		{"MATCH (n) WITH n AS limit RETURN limit", false, false},
		{"MATCH (n) RETURN n. /* gap */ skip", false, false},
		{"MATCH (n:CREATE {SET:1}) RETURN n", false, false},
		{"UNWIND [1] AS create RETURN create", false, false},
		{"RETURN create(1)", false, false},
		{"RETURN CASE WHEN true THEN limit ELSE skip END", false, false},
		{"WITH 1 AS x RETURN x + limit", false, false},
		{"CREATE (n {limit:1})", false, true},
		{"MATCH (n) SET n.limit=1 RETURN n", false, true},
		{"MATCH (n) RETURN n LIMIT $limit", true, false},
		{"MATCH (n) RETURN n OFFSET 1", true, false},
		{"MATCH (n) RETURN n ORDER BY n.id LIMIT 1", false, false},
		{"MATCH (n) WITH n LIMIT 1 SET n.x=1", true, false},
		{"CALL { MATCH (n) RETURN n LIMIT 1 } RETURN n", true, false},
		{"CALL (n) { WITH n RETURN n LIMIT 1 } RETURN n", true, false},
		{"CALL { MATCH (n) RETURN n ORDER BY n.id LIMIT 1 } RETURN n", false, false},
		{"CALL { MATCH (n) RETURN n ORDER BY n.id } WITH n LIMIT 1 RETURN n", true, false},
		{"WITH 1 AS x ORDER BY x CALL { MATCH (n) RETURN n LIMIT 1 } RETURN x", true, false},
		{"CALL { RETURN {limit:1} AS v } RETURN v", false, false},
		{"CALL { CREATE (n {limit:1}) RETURN n } RETURN n", false, true},
		{"MATCH (n) WHERE EXISTS { MATCH (m) RETURN m LIMIT 1 } RETURN n", true, false},
		{"RETURN count {limit:1}", false, false},
		{"RETURN collect {MATCH:1}", false, false},
		{"RETURN limitβ, βlimit, createβ", false, false},
	} {
		t.Run(test.query, func(t *testing.T) {
			require.Equal(t, test.arbitrary, Arbitrary(test.query))
			require.Equal(t, test.writes, Writes(test.query))
		})
	}
	for _, property := range []string{"limit", "return", "as"} {
		query := "MATCH (n) RETURN n." + property + " AS " + property + " ORDER BY " + property
		require.Equal(t, []int{0}, returnShape(query, []string{property}).sortKeys, query)
	}
	left := Outcome{Columns: []string{"v"}, Rows: [][]any{{1}, {2}}}
	right := Outcome{Columns: []string{"v"}, Rows: [][]any{{2}, {1}}}
	query := "CALL { RETURN 1 ORDER BY 1 } RETURN 2 AS v"
	require.True(t, Same(query, left, right), "child ordering does not order the outer result")
	require.False(t, OrderOnly(query, left, right))
}
