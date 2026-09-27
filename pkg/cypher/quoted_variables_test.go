package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQuotedVariablesMatchNeo4j: backtick-quoted variables, labels, keys and
// parameters in every position read as Neo4j 5.26 reads them (#734); the
// expected columns and rows are Neo4j's.
func TestQuotedVariablesMatchNeo4j(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:P {name: 'a', v: 5, l: [1, 2, 3]})-[:R {w: 1}]->(:P {name: 'b', v: 7, l: [4]}), (:P {name: 'c', v: 9})", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		query   string
		params  map[string]interface{}
		columns []string
		rows    [][]interface{}
	}{
		{"MATCH (`n n`:P) WHERE `n n`.v = 5 RETURN count(*) AS c", nil, []string{"c"}, [][]interface{}{{int64(1)}}},
		{"MATCH (`n n`:P) WHERE `n n`.name IN ['a', 'c'] RETURN count(`n n`) AS c", nil, []string{"c"}, [][]interface{}{{int64(2)}}},
		{"MATCH (`n n`:P {name: 'a'}) RETURN `n n`.name", nil, []string{"`n n`.name"}, [][]interface{}{{"a"}}},
		{"MATCH (`n n`:P)-[`r r`:R]->(`m m`) RETURN `r r`.w AS w, `m m`.name AS b", nil, []string{"w", "b"}, [][]interface{}{{int64(1), "b"}}},
		{"MATCH `p p` = (:P)-[:R]->(:P) RETURN length(`p p`) AS l", nil, []string{"l"}, [][]interface{}{{int64(1)}}},
		{"MATCH (`n n`:P) WITH `n n` ORDER BY `n n`.v DESC LIMIT 1 RETURN `n n`.name AS v", nil, []string{"v"}, [][]interface{}{{"c"}}},
		{"MATCH (`n n`:P) WITH `n n`.v AS `v v` ORDER BY `v v` RETURN collect(`v v`) AS c", nil, []string{"c"}, [][]interface{}{{[]interface{}{int64(5), int64(7), int64(9)}}}},
		{"OPTIONAL MATCH (`n n`:Nope) RETURN `n n` AS v", nil, []string{"v"}, [][]interface{}{{nil}}},
		{"WITH [7, 8, 9] AS `a b` RETURN `a b`[0] AS i, `a b`[1..] AS s, `a b`[-1] AS l", nil, []string{"i", "s", "l"}, [][]interface{}{{int64(7), []interface{}{int64(8), int64(9)}, int64(9)}}},
		{"WITH {k: 3} AS `a b` RETURN `a b`['k'] AS k, `a b`.k AS d, `a b` {.k} AS m", nil, []string{"k", "d", "m"}, [][]interface{}{{int64(3), int64(3), map[string]interface{}{"k": int64(3)}}}},
		{"WITH {k: 1} AS m, 5 AS `a b` RETURN m {.k, `a b`} AS r", nil, []string{"r"}, [][]interface{}{{map[string]interface{}{"k": int64(1), "a b": int64(5)}}}},
		{"WITH [1, 2, 3] AS l RETURN [`e e` IN l WHERE `e e` > 1 | `e e` * 2] AS c, reduce(`s s` = 0, `e e` IN l | `s s` + `e e`) AS r", nil, []string{"c", "r"}, [][]interface{}{{[]interface{}{int64(4), int64(6)}, int64(6)}}},
		{"WITH 1 AS `a``b` RETURN `a``b` AS v, `a``b`", nil, []string{"v", "a`b"}, [][]interface{}{{int64(1), int64(1)}}},
		{"WITH 1 AS x RETURN `x`", nil, []string{"x"}, [][]interface{}{{int64(1)}}},
		{"WITH 1 AS `x` RETURN x + 1, `x` + 1", nil, []string{"x + 1", "`x` + 1"}, [][]interface{}{{int64(2), int64(2)}}},
		{"WITH 2 AS `a b` RETURN `a b` + 1, `a b` * `a b` AS sq", nil, []string{"`a b` + 1", "sq"}, [][]interface{}{{int64(3), int64(4)}}},
		{"WITH 1 AS `ñandú` RETURN `ñandú` + 1 AS v, `ñandú`", nil, []string{"v", "ñandú"}, [][]interface{}{{int64(2), int64(1)}}},
		{"WITH 1 AS `match` RETURN `match` AS v, `match`", nil, []string{"v", "match"}, [][]interface{}{{int64(1), int64(1)}}},
		{"WITH 1 AS `a b`, 2 AS c RETURN *", nil, []string{"a b", "c"}, [][]interface{}{{int64(1), int64(2)}}},
		{"WITH 'x`y' AS `a b` RETURN `a b` AS v, 'q `a b` q' AS s", nil, []string{"v", "s"}, [][]interface{}{{"x`y", "q `a b` q"}}},
		{"WITH {`a b`: 5} AS `a b` RETURN `a b`.`a b` AS v", nil, []string{"v"}, [][]interface{}{{int64(5)}}},
		{"MATCH (`n n`:P {name: 'a'}) // a `comment`\nRETURN `n n`.name AS v", nil, []string{"v"}, [][]interface{}{{"a"}}},
		{"CALL db.labels() YIELD label AS `l l` RETURN `l l` ORDER BY `l l`", nil, []string{"l l"}, [][]interface{}{{"P"}}},
		{"UNWIND [1, 2] AS `x x` CALL { WITH `x x` RETURN `x x` * 2 AS d } RETURN d ORDER BY d", nil, []string{"d"}, [][]interface{}{{int64(2)}, {int64(4)}}},
		{"RETURN $`a b` AS v", map[string]interface{}{"a b": int64(1)}, []string{"v"}, [][]interface{}{{int64(1)}}},
		{"WITH $`a b` + 1 AS `a b` RETURN `a b`, $`p` AS p", map[string]interface{}{"a b": int64(1), "p": "x"}, []string{"a b", "p"}, [][]interface{}{{int64(2), "x"}}},
		{"MATCH (`n n`) WHERE `n n`.name = $name RETURN `n n`.v AS v", map[string]interface{}{"name": "b"}, []string{"v"}, [][]interface{}{{int64(7)}}},
	} {
		result, err := exec.Execute(ctx, tc.query, tc.params)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.columns, result.Columns, tc.query)
		require.Equal(t, tc.rows, result.Rows, tc.query)
	}
}

// TestQuotedVariablesInWrites: quoted variables in CREATE, SET, REMOVE,
// DELETE, MERGE and FOREACH (Neo4j's results).
func TestQuotedVariablesInWrites(t *testing.T) {
	for _, tc := range []struct {
		query string
		rows  [][]interface{}
	}{
		{"CREATE (`n n`:W {v: 1})-[`r r`:T {w: 2}]->(`m m`:W {v: 3}) RETURN `n n`.v + `r r`.w + `m m`.v AS s", [][]interface{}{{int64(6)}}},
		{"MATCH (`n n`:P {name: 'a'}) SET `n n`.v = 50 RETURN `n n`.v AS v", [][]interface{}{{int64(50)}}},
		{"MATCH (`n n`:P {name: 'a'}) SET `n n` += {z: 1} RETURN `n n`.z AS z", [][]interface{}{{int64(1)}}},
		{"MATCH (`n n`:P {name: 'a'}) SET `n n`:Extra RETURN labels(`n n`) AS l", [][]interface{}{{[]interface{}{"P", "Extra"}}}},
		{"MATCH (`n n`:P {name: 'a'}) REMOVE `n n`.v RETURN `n n`.v AS v", [][]interface{}{{nil}}},
		{"MATCH (`n n`:P {name: 'b'}) DETACH DELETE `n n` RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
		{"MERGE (`n n`:P {name: 'q'}) ON CREATE SET `n n`.c = true RETURN `n n`.c AS c", [][]interface{}{{true}}},
		{"MATCH (`n n`:P) FOREACH (`x x` IN [1] | SET `n n`.f = `x x`) RETURN count(*) AS c", [][]interface{}{{int64(2)}}},
	} {
		exec, _ := newTestExecutor(t)
		ctx := context.Background()
		_, err := exec.Execute(ctx, "CREATE (:P {name: 'a', v: 5})-[:R]->(:P {name: 'b', v: 7})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.rows, result.Rows, tc.query)
	}
}

// TestQuotedVariablesStayInternal: internal identifiers never reach the
// client, the statement as written names columns and errors, and statements
// that differ only in quoting are validated and cached apart.
func TestQuotedVariablesStayInternal(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()

	result, err := exec.Execute(ctx, "EXPLAIN MATCH (`n n`) WHERE `n n`.x = 1 RETURN `n n`.x AS v", nil)
	require.NoError(t, err)
	plan, ok := result.Metadata["plan"].(*ExecutionPlan)
	require.True(t, ok)
	require.Equal(t, "MATCH (`n n`) WHERE `n n`.x = 1 RETURN `n n`.x AS v", plan.Query)
	require.NotContains(t, result.Metadata["planString"], "nornicq")
	require.Equal(t, formatPlan(plan), result.Metadata["planString"])
	for _, line := range strings.Split(strings.TrimSpace(result.Metadata["planString"].(string)), "\n") {
		require.Len(t, line, 64, "plan lines keep their width: %q", line)
	}

	_, err = exec.Execute(ctx, "WITH 1 AS `a b` RETURN `a b`.x", nil)
	require.ErrorContains(t, err, "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was Integer")
	require.NotContains(t, err.Error(), "nornicq")

	_, err = exec.Execute(ctx, "RETURN `no such` AS v", nil)
	require.ErrorContains(t, err, "no such")

	for _, name := range []string{"a b", "c d"} {
		result, err = exec.Execute(ctx, "RETURN 1 AS `"+name+"`", nil)
		require.NoError(t, err)
		require.Equal(t, []string{name}, result.Columns)
	}
	result, err = exec.Execute(ctx, "WITH 1 AS `x` RETURN x + 1, `x` + 1", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"x + 1", "`x` + 1"}, result.Columns)
	_, err = exec.Execute(ctx, "WITH 1 AS x RETURN x + 1, x + 1", nil)
	require.ErrorContains(t, err, "Multiple RETURN items project the same column name")
}

// TestCanonicalizeQuotedVariables: the rewrite itself.
func TestCanonicalizeQuotedVariables(t *testing.T) {
	query := "MATCH (n) RETURN n"
	canonical, names := canonicalizeQuotedVariables(query)
	require.Nil(t, names, "no backtick, nothing scanned")
	require.Equal(t, query, canonical)

	canonical, names = canonicalizeQuotedVariables("MATCH (n:`L L` {`k k`: 1}) RETURN n.`p q`")
	require.Nil(t, names, "labels, keys and property names stay as written")
	require.Equal(t, "MATCH (n:`L L` {`k k`: 1}) RETURN n.`p q`", canonical)

	// An identifier the statement already uses is never the internal one.
	first, firstNames := canonicalizeQuotedVariables("WITH 1 AS `a b` RETURN `a b`")
	require.NotNil(t, firstNames)
	var internal string
	for identifier := range firstNames.internal {
		internal = identifier
	}
	collided, _ := canonicalizeQuotedVariables("WITH 1 AS `a b`, 2 AS " + internal + " RETURN `a b`, " + internal)
	require.Contains(t, collided, " 2 AS "+internal+" RETURN", "the user's own variable keeps its name: %s", collided)
	require.NotContains(t, collided, "`a b`")
	require.NotEqual(t, first, collided)

	exec, _ := newTestExecutor(t)
	result, err := exec.Execute(context.Background(), "WITH 1 AS `a b`, 2 AS "+internal+" RETURN `a b`, "+internal, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a b", internal}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(1), int64(2)}}, result.Rows)
}

// TestMissingParameterIsParameterMissing: a referenced parameter that isn't
// given is Neo4j's ParameterMissing, checked before the statement runs
// (#712).
func TestMissingParameterIsParameterMissing(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	for query, missing := range map[string]string{
		"RETURN $missing AS v":                                 "missing",
		"RETURN $`missing one` AS v":                           "missing one",
		"MATCH (n:NoSuchLabelX) WHERE n.x = $missing RETURN n": "missing",
		"RETURN 'x $inString' AS s, $a + $b AS v":              "b",
	} {
		_, err := exec.Execute(ctx, query, map[string]interface{}{"a": int64(1)})
		require.ErrorContains(t, err, "Neo.ClientError.Statement.ParameterMissing: Expected parameter(s): "+missing, query)
	}
	result, err := exec.Execute(ctx, "EXPLAIN RETURN $later AS v", nil)
	require.NoError(t, err, "EXPLAIN runs nothing and needs no parameters")
	require.NotNil(t, result)
}
