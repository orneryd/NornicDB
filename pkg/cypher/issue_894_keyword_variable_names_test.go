package cypher

// NornicDB #894: keywords are valid variable names, as in Neo4j: where,
// optional, union, call, as and distinct in node patterns, WITH, UNWIND and
// RETURN aliases, ORDER BY, WHERE and expressions; distinct and other clause
// keywords as names before ORDER BY, SKIP, LIMIT, WHERE and UNION ALL, after
// a separator and as a projection's first item; DISTINCT read as the keyword
// wherever the rest can be an expression. Answers are Neo4j 5.26.30's.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue894KeywordVariableNames(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:Q {id: 1})", nil)
			require.NoError(t, err)
			run := func(query string) (*ExecuteResult, error) {
				if mode == "auto-commit" {
					return exec.Execute(ctx, query, nil)
				}
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
				return exec.Execute(ctx, query, nil)
			}
			for _, tc := range []struct {
				query string
				rows  [][]interface{}
			}{
				{"MATCH (where:Q) RETURN where.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (where:Q) RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n AS where RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS where RETURN where AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [1] AS where RETURN where AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (where:Q) WHERE where.id = 1 RETURN where.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) RETURN n.id AS where ORDER BY where", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS where WHERE where > 0 RETURN where + 1 AS v", [][]interface{}{{int64(2)}}},
				{"MATCH (optional:Q) RETURN optional.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (optional:Q) RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n AS optional RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS optional RETURN optional AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [1] AS optional RETURN optional AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (optional:Q) WHERE optional.id = 1 RETURN optional.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) RETURN n.id AS optional ORDER BY optional", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS optional WHERE optional > 0 RETURN optional + 1 AS v", [][]interface{}{{int64(2)}}},
				{"MATCH (union:Q) RETURN union.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (union:Q) RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n AS union RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS union RETURN union AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [1] AS union RETURN union AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (union:Q) WHERE union.id = 1 RETURN union.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) RETURN n.id AS union ORDER BY union", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS union WHERE union > 0 RETURN union + 1 AS v", [][]interface{}{{int64(2)}}},
				{"MATCH (call:Q) RETURN call.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (call:Q) RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n AS call RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS call RETURN call AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [1] AS call RETURN call AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (call:Q) WHERE call.id = 1 RETURN call.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) RETURN n.id AS call ORDER BY call", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS call WHERE call > 0 RETURN call + 1 AS v", [][]interface{}{{int64(2)}}},
				{"MATCH (as:Q) RETURN as.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (as:Q) RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n AS as RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS as RETURN as AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [1] AS as RETURN as AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (as:Q) WHERE as.id = 1 RETURN as.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) RETURN n.id AS as ORDER BY as", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS as WHERE as > 0 RETURN as + 1 AS v", [][]interface{}{{int64(2)}}},
				{"MATCH (distinct:Q) RETURN distinct.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (distinct:Q) RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n AS distinct RETURN 1 AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS distinct RETURN distinct AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [1] AS distinct RETURN distinct AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (distinct:Q) WHERE distinct.id = 1 RETURN distinct.id AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) RETURN n.id AS distinct ORDER BY distinct", [][]interface{}{{int64(1)}}},
				{"MATCH (n:Q) WITH n.id AS distinct WHERE distinct > 0 RETURN distinct + 1 AS v", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN distinct + 1 AS v", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN distinct - 1 AS v", [][]interface{}{{int64(-1)}}},
				{"WITH 2 AS distinct RETURN distinct * 2 AS v", [][]interface{}{{int64(4)}}},
				{"WITH 2 AS distinct RETURN distinct / 2 AS v", [][]interface{}{{int64(1)}}},
				{"WITH [5] AS distinct RETURN distinct[0] AS v", [][]interface{}{{[]interface{}{int64(0)}}}},
				{"WITH 1 AS distinct RETURN distinct IS NULL AS v", [][]interface{}{{false}}},
				{"WITH 1 AS distinct RETURN distinct", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN distinct = 1 AS v", [][]interface{}{{true}}},
				{"WITH 1 AS distinct RETURN distinct, 2 AS w", [][]interface{}{{int64(1), int64(2)}}},
				{"RETURN DISTINCT .5 AS v", [][]interface{}{{float64(0.5)}}},
				{"RETURN DISTINCT -1 AS v", [][]interface{}{{int64(-1)}}},
				{"RETURN DISTINCT +1 AS v", [][]interface{}{{int64(1)}}},
				{"RETURN DISTINCT [1] AS v", [][]interface{}{{[]interface{}{int64(1)}}}},
				{"RETURN DISTINCT (1) AS v", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN distinct < 2 AS v", [][]interface{}{{true}}},
				{"WITH 1 AS distinct RETURN distinct ^ 2 AS v", [][]interface{}{{float64(1.0)}}},
				{"WITH 3 AS distinct RETURN distinct % 2 AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [1,1] AS x RETURN DISTINCT x AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [1,1] AS distinct RETURN DISTINCT distinct AS v", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct WITH DISTINCT distinct RETURN distinct AS v", [][]interface{}{{int64(1)}}},
				{"WITH {a:1} AS distinct RETURN distinct.a AS v", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct ORDER BY distinct RETURN distinct AS v", [][]interface{}{{int64(1)}}},
				{"WITH true AS distinct RETURN distinct AND true AS v", [][]interface{}{{true}}},
				{"WITH true AS distinct RETURN distinct OR false AS v", [][]interface{}{{true}}},
				{"WITH true AS distinct RETURN distinct XOR true AS v", [][]interface{}{{false}}},
				{"WITH 'ab' AS distinct RETURN distinct CONTAINS 'a' AS v", [][]interface{}{{true}}},
				{"WITH 1 AS distinct RETURN distinct IS NOT NULL AS v", [][]interface{}{{true}}},
				{"WITH 1 AS distinct RETURN distinct <> 2 AS v", [][]interface{}{{true}}},
				{"WITH 1 AS distinct RETURN distinct >= 2 AS v", [][]interface{}{{false}}},
				{"WITH 1 AS distinct RETURN distinct > 0 AS v", [][]interface{}{{true}}},
				{"WITH 'a' AS distinct RETURN distinct =~ 'a' AS v", [][]interface{}{{true}}},
				{"WITH 1 AS distinct RETURN distinct :: INTEGER AS v", [][]interface{}{{true}}},
				{"WITH 2 AS distinct RETURN distinct *2 AS v", [][]interface{}{{int64(4)}}},
				{"WITH 2 AS x RETURN DISTINCT x AS v", [][]interface{}{{int64(2)}}},
				{"WITH 2 AS distinct RETURN distinct AS distinct", [][]interface{}{{int64(2)}}},
				{"WITH 1 AS distinct RETURN distinct ORDER BY distinct", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN distinct LIMIT 1", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN distinct SKIP 0", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct WITH distinct WHERE distinct = 1 RETURN distinct AS v", [][]interface{}{{int64(1)}}},
				{"WITH 2 AS distinct RETURN distinct (1) AS v", [][]interface{}{{int64(1)}}},
				{"WITH {a:1} AS distinct RETURN distinct{a: 2} AS v", [][]interface{}{{map[string]interface{}{"a": int64(2)}}}},
				{"WITH {a:1} AS distinct RETURN distinct{} AS v", [][]interface{}{{map[string]interface{}{}}}},
				{"UNWIND [1,1] AS order RETURN DISTINCT order", [][]interface{}{{int64(1)}}},
				{"UNWIND [1,1] AS skip RETURN DISTINCT skip", [][]interface{}{{int64(1)}}},
				{"UNWIND [1,1] AS limit RETURN DISTINCT limit", [][]interface{}{{int64(1)}}},
				{"UNWIND [1,1] AS where RETURN DISTINCT where", [][]interface{}{{int64(1)}}},
				{"UNWIND [1,1] AS distinct RETURN count(DISTINCT distinct) AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [2,2] AS distinct RETURN count(distinct * 2) AS v", [][]interface{}{{int64(2)}}},
				{"UNWIND [2,2] AS distinct RETURN sum(distinct * 2) AS v", [][]interface{}{{int64(8)}}},
				{"UNWIND [2,2] AS distinct RETURN sum(distinct + 1) AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [2,2] AS distinct RETURN distinct AS v ORDER BY distinct * 2", [][]interface{}{{int64(2)}, {int64(2)}}},
				{"WITH 2 AS distinct RETURN distinct IS NOT :: INTEGER AS v", [][]interface{}{{false}}},
				{"WITH 2 AS distinct RETURN distinct IS TYPED INTEGER AS v", [][]interface{}{{true}}},
				{"UNWIND [2,2] AS distinct RETURN count(*) AS v, sum(distinct / 2) AS w", [][]interface{}{{int64(2), int64(2)}}},
				{"RETURN DISTINCT {a: 1} AS v", [][]interface{}{{map[string]interface{}{"a": int64(1)}}}},
				{"RETURN DISTINCT{a: 1} AS v", [][]interface{}{{map[string]interface{}{"a": int64(1)}}}},
				{"RETURN DISTINCT {} AS v", [][]interface{}{{map[string]interface{}{}}}},
				{"UNWIND [1,1] AS x RETURN DISTINCT {a: x} AS v", [][]interface{}{{map[string]interface{}{"a": int64(1)}}}},
				{"WITH 2 AS x RETURN DISTINCT x", [][]interface{}{{int64(2)}}},
				{"UNWIND [1,1] AS x RETURN count(DISTINCT {a: x}) AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [1,1] AS x RETURN collect(DISTINCT {a: x}) AS v", [][]interface{}{{[]interface{}{map[string]interface{}{"a": int64(1)}}}}},
				{"UNWIND [1,1] AS x WITH DISTINCT {a: x} AS m RETURN m", [][]interface{}{{map[string]interface{}{"a": int64(1)}}}},
				{"UNWIND [1,1] AS x RETURN count(DISTINCT x * 2) AS v", [][]interface{}{{int64(1)}}},
				{"MATCH (distinct:Q) RETURN distinct:Q AS v", [][]interface{}{{true}}},
				{"MATCH (distinct:Q) RETURN DISTINCT distinct:Q AS v", [][]interface{}{{true}}},
				{"WITH 1 AS distinct RETURN distinct\nORDER BY distinct", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct WITH distinct SKIP 0 RETURN distinct AS v", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct WITH distinct LIMIT 1 RETURN distinct AS v", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN distinct UNION ALL RETURN 1 AS distinct", [][]interface{}{{int64(1)}, {int64(1)}}},
				{"WITH 1 AS distinct RETURN distinct, distinct AS b", [][]interface{}{{int64(1), int64(1)}}},
				{"WITH 1 AS distinct RETURN DISTINCT distinct", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS distinct RETURN DISTINCT distinct, 1 AS w", [][]interface{}{{int64(1), int64(1)}}},
				{"WITH [1] AS distinct RETURN distinct [0] AS v", [][]interface{}{{[]interface{}{int64(0)}}}},
				{"UNWIND [1] AS skip RETURN DISTINCT skip + 1 AS v", [][]interface{}{{int64(2)}}},
				{"UNWIND [1] AS skip RETURN DISTINCT skip - 1", [][]interface{}{{int64(0)}}},
				{"WITH {x:1} AS where RETURN DISTINCT where.x AS v", [][]interface{}{{int64(1)}}},
				{"UNWIND [3] AS limit RETURN DISTINCT limit * 2 AS v", [][]interface{}{{int64(6)}}},
				{"UNWIND [3] AS limit WITH 7 AS distinct, limit RETURN distinct LIMIT 1", [][]interface{}{{int64(7)}}},
				{"UNWIND [3] AS where RETURN DISTINCT where AS v", [][]interface{}{{int64(3)}}},
				{"UNWIND [3] AS where WITH 7 AS distinct, where WITH distinct WHERE where = 3 RETURN distinct AS v", [][]interface{}{{int64(7)}}},
				{"UNWIND [3] AS order RETURN DISTINCT order AS v ORDER BY order", [][]interface{}{{int64(3)}}},
				{"WITH 1 AS distinct RETURN DISTINCT distinct ORDER BY distinct", [][]interface{}{{int64(1)}}},
				{"WITH 3 AS where WITH where WHERE where = 3 RETURN where AS v", [][]interface{}{{int64(3)}}},
				{"UNWIND [3] AS where WITH 7 AS distinct, where RETURN where AS v", [][]interface{}{{int64(3)}}},
				{"UNWIND [3] AS where WITH 7 AS d, where WITH d WHERE where = 3 RETURN d AS v", [][]interface{}{{int64(7)}}},
				{"WITH 3 AS with WITH with WHERE with = 3 RETURN with AS v", [][]interface{}{{int64(3)}}},
				{"WITH 3 AS by RETURN by ORDER BY by", [][]interface{}{{int64(3)}}},
				{"WITH 3 AS return, 1 AS x RETURN x, return", [][]interface{}{{int64(1), int64(3)}}},
				{"WITH 3 AS and RETURN [and] AS v", [][]interface{}{{[]interface{}{int64(3)}}}},
				{"WITH 3 AS then RETURN (then) AS v", [][]interface{}{{int64(3)}}},
				{"WITH 3 AS yield RETURN 1 + yield AS v", [][]interface{}{{int64(4)}}},
				{"WITH 3 AS where, 1 AS x RETURN x, where ORDER BY where", [][]interface{}{{int64(1), int64(3)}}},
				{"WITH 3 AS when RETURN CASE WHEN when = 3 THEN 1 ELSE 0 END AS v", [][]interface{}{{int64(1)}}},
				{"WITH 3 AS optional, 1 AS x WITH x, optional MATCH (n:Q) RETURN optional AS v", [][]interface{}{{int64(3)}}},
				{"WITH 3 AS union, 1 AS x RETURN x, union UNION ALL RETURN 1 AS x, 2 AS union", [][]interface{}{{int64(1), int64(3)}, {int64(1), int64(2)}}},
				{"WITH 3 AS call, 1 AS x WITH x, call CALL { RETURN 1 AS y } RETURN call + y AS v", [][]interface{}{{int64(4)}}},
				{"WITH 1 AS x RETURN x, NOT true AS v", [][]interface{}{{int64(1), false}}},
				{"WITH true AS not RETURN NOT not AS v", [][]interface{}{{false}}},
				// not and case are variables when a clause follows them, after
				// any keyword an expression follows; otherwise they start one.
				{"WITH 1 AS case WITH case WHERE case = 1 RETURN case AS c", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS x, 2 AS case WITH x, case WHERE case = 2 RETURN case AS c", [][]interface{}{{int64(2)}}},
				{"WITH true AS not WITH not WHERE not RETURN not AS n", [][]interface{}{{true}}},
				{"WITH 1 AS x, true AS not WITH x, not WHERE not RETURN x", [][]interface{}{{int64(1)}}},
				{"WITH true AS not, true AS x WITH not, x WHERE x AND not RETURN 1 AS n", [][]interface{}{{int64(1)}}},
				{"WITH true AS not, false AS x WITH not, x WHERE x OR not RETURN 1 AS n", [][]interface{}{{int64(1)}}},
				{"WITH true AS not, false AS x WITH not, x WHERE x XOR not RETURN 1 AS n", [][]interface{}{{int64(1)}}},
				{"WITH false AS not WITH not WHERE NOT not RETURN 1 AS n", [][]interface{}{{int64(1)}}},
				{"WITH [1] AS not, 1 AS x WITH not, x WHERE x IN not RETURN 1 AS n", [][]interface{}{{int64(1)}}},
				{"WITH true AS case WITH case WHERE case RETURN 1 AS n", [][]interface{}{{int64(1)}}},
				{"WITH true AS case, true AS x WITH case, x WHERE x AND case RETURN 1 AS n", [][]interface{}{{int64(1)}}},
				{"WITH [1] AS case, 1 AS x WITH case, x WHERE x IN case RETURN 1 AS n", [][]interface{}{{int64(1)}}},
				{"WITH true AS not WITH not WHERE not RETURN not AS n ORDER BY not", [][]interface{}{{true}}},
				{"WITH true AS not WITH not WHERE not WITH not AS m RETURN m", [][]interface{}{{true}}},
				{"WITH 1 AS x WITH x WHERE not x = 2 RETURN x AS n", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS x WITH x WHERE x = 1 AND not x = 2 RETURN x AS n", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS x WITH x WHERE case when x = 1 then true end RETURN x AS n", [][]interface{}{{int64(1)}}},
				{"WITH 1 AS x RETURN CASE WHEN x = 1 THEN 'a' END AS r", [][]interface{}{{"a"}}},
				{"WITH 1 AS x RETURN x, CASE x WHEN 1 THEN 'a' END AS r", [][]interface{}{{int64(1), "a"}}},
				{"WITH 1 AS x RETURN x, NOT x = 2 AS r", [][]interface{}{{int64(1), true}}},
				{"WITH true AS not RETURN CASE WHEN true THEN not END AS n", [][]interface{}{{true}}},
				{"WITH 3 AS as WITH as WHERE as = 3 RETURN as AS v", [][]interface{}{{int64(3)}}},
				{"WITH 3 AS set RETURN set, 1 AS w", [][]interface{}{{int64(3), int64(1)}}},
				{"WITH 3 AS in, [3] AS l RETURN in IN l AS v", [][]interface{}{{true}}},
			} {
				result, err := run(tc.query)
				require.NoError(t, err, tc.query)
				require.Equal(t, tc.rows, result.Rows, tc.query)
			}
			// Neo4j rejects these: distinct is the keyword there (count(distinct)
			// has no argument; DISTINCT IN [1] reads in as a variable) or a clause
			// keyword reads as a variable that isn't defined.
			for _, query := range []string{
				"WITH 1 AS distinct RETURN distinct IN [1] AS v",
				"WITH 'a' AS distinct RETURN distinct STARTS WITH 'a' AS v",
				"WITH 'ab' AS distinct RETURN distinct ENDS WITH 'b' AS v",
				"WITH 2 AS distinct RETURN distinct IS :: INTEGER AS v",
				"WITH 1 AS distinct RETURN distinct UNION RETURN 2 AS distinct",
				"WITH 2 AS distinct RETURN distinct{.a} AS v",
				"UNWIND [1,1] AS distinct RETURN count(distinct) AS v",
				"UNWIND [1,1] AS distinct RETURN collect(distinct) AS v",
				"UNWIND [2,2] AS distinct RETURN sum(distinct) AS v",
				"WITH 2 AS x RETURN distinct * 2 AS v",
				"WITH 2 AS x RETURN DISTINCT IS NULL AS v",
				"WITH 1 AS x RETURN distinct",
				"RETURN DISTINCT * 2 AS v",
				"UNWIND [1,1] AS x RETURN count(DISTINCT) AS v",
				"UNWIND [1] AS skip WITH 5 AS distinct, skip RETURN distinct SKIP - 1",
			} {
				_, err := run(query)
				require.Error(t, err, query)
				require.True(t, strings.HasPrefix(statusText(err), "Neo.ClientError.Statement.SyntaxError"), "%s: %s", query, statusText(err))
			}
		})
	}
}

// Path aggregates read DISTINCT with the same rule: collect(DISTINCT b.k) is
// the keyword, collect(distinct.k) reads a node named distinct. Answers are
// Neo4j 5.26.30's; collected lists are compared sorted.
func TestIssue894DistinctInPathAggregates(t *testing.T) {
	for _, mode := range []string{"auto-commit", "explicit transaction"} {
		t.Run(mode, func(t *testing.T) {
			exec := newAsyncStackTestExecutor(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (a:P {id: 1})-[:R]->(:P {id: 2, k: 'x'}), (a)-[:R]->(:P {id: 3, k: 'x'}), (:P {id: 4})-[:R]->(:P {id: 5, k: 'y'})", nil)
			require.NoError(t, err)
			if mode == "explicit transaction" {
				_, err := exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
			}
			for _, tc := range []struct {
				query string
				rows  [][]interface{}
			}{
				{"MATCH (a:P)-[:R]->(b:P) RETURN collect(DISTINCT b.k) AS v", [][]interface{}{{[]interface{}{"x", "y"}}}},
				{"MATCH (a:P)-[:R]->(b:P) RETURN a.id AS a, collect(DISTINCT b.k) AS v ORDER BY a", [][]interface{}{{int64(1), []interface{}{"x"}}, {int64(4), []interface{}{"y"}}}},
				{"MATCH p = (a:P)-[:R]->(b:P) RETURN collect(DISTINCT b.k) AS v", [][]interface{}{{[]interface{}{"x", "y"}}}},
				{"MATCH p = (a:P)-[:R]->(b:P) RETURN a.id AS a, collect(DISTINCT b.k) AS v ORDER BY a", [][]interface{}{{int64(1), []interface{}{"x"}}, {int64(4), []interface{}{"y"}}}},
				{"MATCH (a:P)-[:R]->(distinct:P) RETURN collect(distinct.k) AS v", [][]interface{}{{[]interface{}{"x", "x", "y"}}}},
				{"MATCH (a:P)-[:R]->(distinct:P) RETURN count(DISTINCT distinct.k) AS v", [][]interface{}{{int64(2)}}},
				{"MATCH p = (a:P)-[:R]->(distinct:P) RETURN a.id AS a, collect(distinct.id) AS v ORDER BY a", [][]interface{}{{int64(1), []interface{}{int64(2), int64(3)}}, {int64(4), []interface{}{int64(5)}}}},
			} {
				result, err := exec.Execute(ctx, tc.query, nil)
				require.NoError(t, err, tc.query)
				for _, row := range result.Rows {
					for _, value := range row {
						if list, ok := value.([]interface{}); ok {
							sort.Slice(list, func(i, j int) bool { return fmt.Sprint(list[i]) < fmt.Sprint(list[j]) })
						}
					}
				}
				require.Equal(t, tc.rows, result.Rows, tc.query)
			}
		})
	}
}

func TestIssue894ClauseAfterName(t *testing.T) {
	for text, want := range map[string]bool{
		"OPTIONAL MATCH (n)": true, "OPTIONAL": false, "optional + 1": false,
		"ORDER BY x": true, "order": false, "SKIP 1": true, "skip - 1": false,
		"where WHERE where = 3": false, "UNION ALL RETURN 1": true,
	} {
		require.Equal(t, want, startsWithClauseAfterName(text), text)
	}
	for text, want := range map[string]bool{
		"IN [1]": true, "AND x": true, "STARTS WITH 'a'": true, "AS v": true,
		"as = 3": false, "foo": false, "+ 1": true,
	} {
		require.Equal(t, want, nameContinuesExpression(text), text)
	}
}

// The traversal executor's own path aggregation (the direct MATCH route,
// which the pipeline doesn't use for aggregates) reads collect(DISTINCT …)
// with the same rule as the pipeline: the answers match the statements above.
func TestIssue894TraversalPathAggregateDistinct(t *testing.T) {
	exec := newAsyncStackTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (a:P {id: 1})-[:R]->(:P {id: 2, k: 'x'}), (a)-[:R]->(:P {id: 3, k: 'x'}), (:P {id: 4})-[:R]->(:P {id: 5, k: 'y'})", nil)
	require.NoError(t, err)
	sorted := func(rows [][]interface{}) [][]interface{} {
		for _, row := range rows {
			for _, value := range row {
				if list, ok := value.([]interface{}); ok {
					sort.Slice(list, func(i, j int) bool { return fmt.Sprint(list[i]) < fmt.Sprint(list[j]) })
				}
			}
		}
		sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]) < fmt.Sprint(rows[j]) })
		return rows
	}
	result, err := exec.executeMatchWithRelationshipsWithPathSeeded(ctx, "(a:P)-[:R]->(b:P)", "", []returnItem{{expr: "collect(DISTINCT b.k)", alias: "v"}}, nil, nil, "", -1)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{"x", "y"}}}, sorted(result.Rows))
	result, err = exec.executeMatchWithRelationshipsWithPathSeeded(ctx, "(a:P)-[:R]->(b:P)", "", []returnItem{{expr: "a.id", alias: "a"}, {expr: "collect(DISTINCT b.k)", alias: "v"}}, nil, nil, "", -1)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), []interface{}{"x"}}, {int64(4), []interface{}{"y"}}}, sorted(result.Rows))
	result, err = exec.executeMatchWithRelationshipsWithPathSeeded(ctx, "(a:P)-[:R]->(distinct:P)", "", []returnItem{{expr: "collect(distinct.k)", alias: "v"}}, nil, nil, "", -1)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{"x", "x", "y"}}}, sorted(result.Rows))
}

// RETURN's own item check reads a keyword-named first item as a name
// (RETURN union[0]). Answers are Neo4j 5.26.30's.
func TestIssue894KeywordNamedFirstReturnItem(t *testing.T) {
	exec := newAsyncStackTestExecutor(t)
	ctx := context.Background()
	for _, keyword := range []string{"union", "UNION", "where", "order", "skip", "limit"} {
		result, err := exec.Execute(ctx, "WITH [1] AS "+keyword+" RETURN "+keyword+"[0] AS v", nil)
		require.NoError(t, err, keyword)
		require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows, keyword)
	}
}
