package cypher

import (
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// NOR-27: a postfix chain that continues after a subscript reads the same
// value as the chain on its own. pages[i].ends is the collected node's list;
// [0] then indexes it, rather than reading a property named "ends[0]".
func TestNOR27PostfixChainAfterSubscriptReadsValue(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		ctx := t.Context()
		_, err := exec.Execute(ctx, "CREATE (:N27Page {i: 0, ends: ['a', 'b']})", nil)
		require.NoError(t, err)

		for _, testCase := range []struct {
			query string
			want  interface{}
		}{
			{"MATCH (p:N27Page) WITH collect(p) AS pages, 0 AS i RETURN pages[i].ends[0] AS v", "a"},
			{"MATCH (p:N27Page) WITH collect(p) AS pages, 0 AS i RETURN pages[i].ends[1] AS v", "b"},
			{"MATCH (p:N27Page) WITH collect(p) AS pages, 0 AS i RETURN pages[i].ends[0..2] AS v", []interface{}{"a", "b"}},
			{"MATCH (p:N27Page) WITH collect(p) AS pages RETURN pages[0].ends[0] AS v", "a"},
			{"WITH [{a: [1, 2]}] AS l, 0 AS i RETURN l[i].a[0] AS v", int64(1)},
			{"WITH [{a: [1, 2]}] AS l, 0 AS i RETURN l[i].a[0..1] AS v", []interface{}{int64(1)}},
		} {
			t.Run(testCase.query, func(t *testing.T) {
				result, err := exec.Execute(ctx, testCase.query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
			})
		}

		// The same chains without the leading subscript keep their values.
		for _, testCase := range []struct {
			query string
			want  interface{}
		}{
			{"MATCH (p:N27Page) RETURN p.ends[0] AS v", "a"},
			{"MATCH (p:N27Page) WITH collect(p) AS pages, 0 AS i RETURN pages[i].ends AS v", []interface{}{"a", "b"}},
			{"MATCH (p:N27Page) WITH collect(p) AS pages, 0 AS i RETURN pages[i].i AS v", int64(0)},
		} {
			t.Run("control/"+testCase.query, func(t *testing.T) {
				result, err := exec.Execute(ctx, testCase.query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
			})
		}
	})
}

// NOR-27: an aggregate's DISTINCT input is a call modifier, not an
// expression, so a subscript on the collected value is valid.
func TestNOR27AggregateDistinctPostfixReadsValue(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, _ := newTestExecutor(t)
		ctx := t.Context()

		for _, testCase := range []struct {
			query string
			want  interface{}
		}{
			{"UNWIND ['a', 'a', 'b'] AS x RETURN collect(DISTINCT x)[0] AS v", "a"},
			{"UNWIND ['a', 'a', 'b'] AS x RETURN collect(DISTINCT x)[0..2] AS v", []interface{}{"a", "b"}},
			{"UNWIND ['a', 'a', 'b'] AS x WITH x ORDER BY x RETURN collect(DISTINCT x)[0] AS v", "a"},
			{"UNWIND ['a', 'a', 'b'] AS x WITH x ORDER BY x RETURN collect(DISTINCT x)[1..] AS v", []interface{}{"b"}},
			{"UNWIND ['a', 'a', 'b'] AS x RETURN collect(DISTINCT x) AS v", []interface{}{"a", "b"}},
			{"UNWIND ['a', 'a', 'b'] AS x RETURN collect(x)[0] AS v", "a"},
			{"UNWIND ['a', 'a', 'b'] AS x WITH collect(DISTINCT x) AS l RETURN l[0] AS v", "a"},
		} {
			t.Run(testCase.query, func(t *testing.T) {
				result, err := exec.Execute(ctx, testCase.query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
			})
		}
	})
}

// NOR-27: a FOREACH over an empty list runs no body, so a body that writes
// the node the same row deleted earlier is a no-op rather than a
// deleted-entity read. A body read that does run keeps EntityNotFound.
func TestNOR27EmptyForeachAfterDeleteIsNoOp(t *testing.T) {
	forEachCypherParserSetting(t, func(t *testing.T) {
		exec, engine := newTestExecutor(t)
		ctx := t.Context()

		for index, body := range []string{
			"SET n.flag = true",
			"SET n.flag = n.flag",
			"SET n:Kept",
			"REMOVE n:N27Target",
		} {
			t.Run(body, func(t *testing.T) {
				id := fmt.Sprintf("case-%d", index)
				_, err := exec.Execute(ctx, "CREATE (:N27Target {id: '"+id+"'})", nil)
				require.NoError(t, err)
				result, err := exec.Execute(ctx, "MATCH (n:N27Target {id: '"+id+"'}) DELETE n FOREACH (x IN [] | "+body+") RETURN 1 AS ok", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
				nodes, err := engine.GetNodesByLabel(ctx, "N27Target")
				require.NoError(t, err)
				for _, node := range nodes {
					require.NotEqual(t, id, node.Properties["id"], "the row's node is deleted and the empty FOREACH writes nothing")
				}
			})
		}

		// A body that runs and reads a deleted property is still Neo4j's
		// EntityNotFound.
		_, err := exec.Execute(ctx, "CREATE (:N27Read {id: 7})", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "MATCH (n:N27Read) DELETE n FOREACH (x IN [1] | SET n.tag = n.id) RETURN 1 AS ok", nil)
		require.Error(t, err)
		code, _ := nornicerrors.Neo4jStatus(err)
		require.Equal(t, "Neo.ClientError.Statement.EntityNotFound", code)

		// A live node's body writes, and an empty list still writes nothing.
		_, err = exec.Execute(ctx, "CREATE (:N27Live {id: 3, flag: 'kept'})", nil)
		require.NoError(t, err)
		result, err := exec.Execute(ctx, "MATCH (n:N27Live) FOREACH (x IN [] | SET n.flag = 'changed') RETURN n.flag AS v", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"kept"}}, result.Rows)
	})
}
