package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// SET x = <source> and SET x += <source> take a map, a node or a relationship
// (a map projection included). A literal of another type is a SyntaxError
// before the statement runs; null is a TypeError when it runs (Neo4j 5.26.30,
// #907).
func TestSetSourceTypesMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "set_sources"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {id: 1, s: 'ab'})-[:R {w: 1}]->(:Q {id: 2, s: 'b'})", nil)
	require.NoError(t, err)
	const bound = "MATCH (n:Q {id: 1})-[r:R]->(o) "
	inRolledBackTransaction := func(query string) (*ExecuteResult, error) {
		_, err := exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		defer func() {
			_, err := exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
		}()
		return exec.Execute(ctx, query, nil)
	}

	for _, testCase := range []struct {
		query string
		want  interface{}
	}{
		{bound + "SET n = o {.s} RETURN n {.*} AS v", map[string]interface{}{"s": "b"}},
		{bound + "SET r = o {.s} RETURN r {.*} AS v", map[string]interface{}{"s": "b"}},
		{bound + "SET n += o {.s} RETURN n {.*} AS v", map[string]interface{}{"id": int64(1), "s": "b"}},
		{bound + "SET r += o {.s} RETURN r {.*} AS v", map[string]interface{}{"s": "b", "w": int64(1)}},
		{"MATCH (n:Q {id: 1}) WITH n, {a: 1, b: 2} AS m SET n = m {.a} RETURN n {.*} AS v", map[string]interface{}{"a": int64(1)}},
		{"MATCH (n:Q {id: 1}) UNWIND [{a: 3}] AS m SET n += m {.a} RETURN n.a AS v", int64(3)},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := inRolledBackTransaction(testCase.query)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
		})
	}

	for _, testCase := range []struct {
		query string
		code  string
	}{
		{bound + "SET n = null RETURN n {.*} AS v", "Neo.ClientError.Statement.TypeError"},
		{bound + "SET r = null RETURN r {.*} AS v", "Neo.ClientError.Statement.TypeError"},
		{bound + "SET n += null RETURN n {.*} AS v", "Neo.ClientError.Statement.TypeError"},
		{bound + "SET r += null RETURN r {.*} AS v", "Neo.ClientError.Statement.TypeError"},
		{bound + "SET n = [1] RETURN n {.*} AS v", "Neo.ClientError.Statement.SyntaxError"},
		{bound + "SET r += [1] RETURN r {.*} AS v", "Neo.ClientError.Statement.SyntaxError"},
		{bound + "SET n = 1 RETURN n {.*} AS v", "Neo.ClientError.Statement.SyntaxError"},
		{bound + "SET r += 'x' RETURN r {.*} AS v", "Neo.ClientError.Statement.SyntaxError"},
		{bound + "SET n = {a: 1} {.*} RETURN n {.*} AS v", "Neo.ClientError.Statement.SyntaxError"},
		// A variable whose type WITH or UNWIND fixes is typed before the
		// statement runs, rows or not; null reads the same for = and +=.
		{"WITH 5 AS s MATCH (n:Q {id: 1}) SET n = s RETURN 1 AS v", "Neo.ClientError.Statement.SyntaxError"},
		{"WITH 5 AS s MATCH (n:Nope) SET n = s RETURN 1 AS v", "Neo.ClientError.Statement.SyntaxError"},
		{"UNWIND [5] AS s MATCH (n:Q {id: 1}) SET n += s RETURN 1 AS v", "Neo.ClientError.Statement.SyntaxError"},
		{"WITH [1] AS s MATCH (n:Q {id: 1}) SET n = s RETURN 1 AS v", "Neo.ClientError.Statement.SyntaxError"},
		{"WITH 'x' AS s MATCH (n:Q {id: 1}) SET n = s RETURN 1 AS v", "Neo.ClientError.Statement.SyntaxError"},
		{"WITH null AS x MATCH (n:Q {id: 1}) SET n = x RETURN 1 AS v", "Neo.ClientError.Statement.TypeError"},
		{"WITH null AS x MATCH (n:Q {id: 1}) SET n += x RETURN 1 AS v", "Neo.ClientError.Statement.TypeError"},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			_, err := inRolledBackTransaction(testCase.query)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, testCase.code, code)
		})
	}
	// Nothing was written.
	result, err := exec.Execute(ctx, "MATCH (n:Q {id: 1}) RETURN n {.*} AS v", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{map[string]interface{}{"id": int64(1), "s": "ab"}}}, result.Rows)

	// Copying from an entity without properties clears them.
	for _, query := range []string{
		"CREATE (e:E) WITH e MATCH (n:Q {id: 1}) SET n = e RETURN n {.*} AS v",
		"CREATE (e:E) WITH e MATCH (:Q {id: 1})-[r:R]->() SET r = e RETURN r {.*} AS v",
	} {
		result, err := inRolledBackTransaction(query)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{map[string]interface{}{}}}, result.Rows, query)
	}
}
