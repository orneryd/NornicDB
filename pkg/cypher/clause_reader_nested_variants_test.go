package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClauseReaderNestedProjectionVariants(t *testing.T) {
	for _, q := range []string{
		"WITH {a:1} AS m WHERE (size(keys(m {.a})) = 1) RETURN 1 AS v",
		"WITH {a:1} AS m WHERE size([m {.a}]) = 1 RETURN 1 AS v",
		"WITH {a:1} AS m WHERE all(x IN [m {.a}] WHERE x.a = 1) RETURN 1 AS v",
		"WITH {a:1} AS m WHERE (m {.a}).a = 1 RETURN 1 AS v",
		"WITH {a:1} AS m WHERE (((m {.a}).a)) = 1 RETURN 1 AS v",
		"WITH {a:1} AS m WHERE size (keys (m {.a})) = 1 RETURN 1 AS v",
		"WITH {a:1} AS m WHERE size(/* ()[] */ [m {.a}]) = 1 RETURN 1 AS v",
		"WITH {a:1} AS m WHERE size([{nested: m {.a}}]) = 1 RETURN 1 AS v",
		"WITH {a:1} AS m WHERE size([1,2]) - (m {.a}).a = 1 RETURN 1 AS v",
		"WITH {a:1} AS m WHERE size([1,2]) - (m {.a})['a'] = 1 RETURN 1 AS v",
		"WITH {a:1} AS m WHERE -[m {.a}][0].a = -1 RETURN 1 AS v",
		"WITH {a:1} AS m WHERE -2 < -[m {.a}][0].a RETURN 1 AS v",
		"WITH {a:1} AS m WHERE ((m {.a}).a) / 1 = 1 RETURN 1 AS v",

		"CREATE (:Q {a:1}) WITH 1 AS dummy MATCH (n:Q WHERE size([n {.a}]) = 1) RETURN 1 AS v",
		"CREATE (:Q {a:1}) WITH 1 AS dummy MATCH (n:Q WHERE n {.a} = {a:1}) RETURN 1 AS v",
		"CREATE (:Q {a:1}) WITH 1 AS dummy MATCH (n:Q WHERE ((n {.a}).a = 1)) RETURN 1 AS v",
	} {
		t.Run(q, func(t *testing.T) {
			e, ctx := newUnitExecutor(t)
			r, err := e.Execute(ctx, q, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, r.Rows)
		})
	}
}

func TestNestedProjectionReadersKeepPatternMapErrors(t *testing.T) {
	for _, q := range []string{
		"MATCH (n {.a}) RETURN n",
		"MATCH (n) WHERE (true AND EXISTS((m {a}))) RETURN n",
		"MATCH (n) WHERE EXISTS(((m {bad}))) RETURN n",
		"MATCH (n) WHERE EXISTS /* comment */ ((m {bad})) RETURN n",
		"MATCH (where /* comment */ {.a}) RETURN where",
		"MATCH (n:WHERE {bad}) RETURN n",
		"MATCH (n IS WHERE {bad}) RETURN n",
		"MATCH /* comment */ (n {bad}) RETURN n",
		"MATCH (n), (m {bad}) RETURN m",
		"MATCH ANY (n {bad}) RETURN n",
		"MATCH p = shortestPath((n {bad})) RETURN p",
		"MATCH p = allShortestPaths((n {bad})) RETURN p",
		"MATCH ANY (a {s:'quoted'}) /* comment */ -->(b {bad}) RETURN b",
		"CALL { MATCH ANY (a {s:'quoted'}) /* comment */ -->(b {bad}) RETURN b } RETURN b",
		"MATCH p = (n {bad}) RETURN p",
		"MATCH (n {bad}) /* comment */ <--(m) RETURN n",
		"MATCH (n)--(m {bad}) RETURN n",
		"MATCH (a)-[r {bad}] /* comment */ -> /* comment */ (b) RETURN r",
		"MATCH p = ALL SHORTEST (n {bad}) RETURN p",
		"MATCH SHORTEST 2 GROUPS (n {bad}) RETURN n",
		"MATCH (n) - [r {bad}] -> (m) RETURN r",
		"MATCH (n) /* comment */ - /* comment */ [r {bad}]->(m) RETURN r",
		"MATCH (n) WHERE (n {bad})-->() RETURN n",
		"MATCH (n) WHERE (n)-->(m {bad}) RETURN n",
		"CALL { MATCH (n), (m {bad}) RETURN n } RETURN n",
		"CREATE (n {bad}) RETURN n",
		"MERGE (n {bad}) RETURN n",
	} {
		t.Run(q, func(t *testing.T) {
			err := validateStaticMapKeys(q)
			require.ErrorContains(t, err, "pattern property maps require key-value entries")
			exec, ctx := newUnitExecutor(t)
			_, executeErr := exec.Execute(ctx, q, nil)
			require.Error(t, executeErr)
			require.Contains(t, executeErr.Error(), "Neo.ClientError.Statement.SyntaxError")
		})
	}
}

func TestNestedProjectionReadersMalformedDelimiters(t *testing.T) {
	for _, q := range []string{
		"MATCH (n)-[r {bad} RETURN r",
		"WITH {a:1} AS m WHERE (m {.a} RETURN 1",
		"MATCH (n) WHERE (n {bad} RETURN n",
	} {
		t.Run(q, func(t *testing.T) {
			e, ctx := newUnitExecutor(t)
			_, err := e.Execute(ctx, q, nil)
			require.Error(t, err)
		})
	}
}

func TestNestedProjectionReadersQualifiedFunctionsKeepExpressions(t *testing.T) {
	for _, name := range []string{"plugin.shortestPath", "plugin.allShortestPaths", "plugin.MATCH"} {
		require.NoError(t, validateStaticMapKeys("WITH {a:1} AS m WHERE "+name+"(m {.a}) RETURN 1"))
	}
}

func TestNestedProjectionReadersRejectMalformedChains(t *testing.T) {
	for _, q := range []string{
		"MATCH (n), (m {.a}).x RETURN m",
		"MATCH (n)-[r {.a}][0]->(m) RETURN m",
	} {
		t.Run(q, func(t *testing.T) {
			e, ctx := newUnitExecutor(t)
			_, err := e.Execute(ctx, q, nil)
			require.Error(t, err)
		})
	}
}

// BenchmarkNestedProjectionContext covers adversarial depth and width with one
// projection, keeping the workload independent of query execution and storage.
func BenchmarkNestedProjectionContext(b *testing.B) {
	for _, count := range []int{100, 1000} {
		for _, shape := range []string{"groups", "siblings"} {
			b.Run(fmt.Sprintf("%s/%d", shape, count), func(b *testing.B) {
				query := "WITH {a:1} AS m WHERE " + strings.Repeat("(", count) + "(m {.a}).a" + strings.Repeat(")", count) + "=1 RETURN 1"
				if shape == "siblings" {
					query = "MATCH " + strings.Repeat("(n),", count) + "(m) WHERE (m {.a}).a=1 RETURN 1"
				}
				end := strings.Index(query, "{.a}")
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					context := newMapPatternContext(query)
					if context.insidePatternDelimiter(query, end) {
						b.Fatal("projection classified as pattern map")
					}
				}
			})
		}
	}
}

func TestNestedProjectionExistsPropertyExpression(t *testing.T) {
	for _, q := range []string{
		"WITH {a:1} AS m WHERE exists((m {.a}).a) RETURN 1",
		"WITH {a:1} AS m WHERE exists(((m {.a}).a)) RETURN 1",
		"WITH {a:1} AS m WHERE plugin.EXISTS((m {.a}).a) RETURN 1",
	} {
		require.NoError(t, validateStaticMapKeys(q), q)
	}
}

func TestNestedProjectionQualifiedCommentFunctions(t *testing.T) {
	for _, name := range []string{"plugin. /* namespace */ shortestPath", "plugin. /* namespace */ allShortestPaths", "plugin. /* namespace */ EXISTS", "plugin. // namespace\n shortestPath"} {
		require.NoError(t, validateStaticMapKeys("WITH {a:1} AS m WHERE "+name+"(m {.a}) RETURN 1"), name)
	}
}

func TestNestedProjectionRejectsMalformedSingletonComprehension(t *testing.T) {
	e, ctx := newUnitExecutor(t)
	_, err := e.Execute(ctx, "MATCH (n) WHERE size([(m {bad}) | m]) = 0 RETURN n", nil)
	require.Error(t, err)
	require.Equal(t, "Neo.ClientError.Statement.SyntaxError", errorStatusCode(err))
}
