package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// projectionExpressions is the one reading of a projection clause (its
// expressions, aliases dropped) that the row validators share (#823).
func TestProjectionExpressions(t *testing.T) {
	for _, tc := range []struct {
		clause, keyword string
		want            []string
	}{
		{"RETURN DISTINCT a.x AS x, count(b) ORDER BY x LIMIT 3", "RETURN", []string{"a.x", "count(b)"}},
		{"WITH n, m WHERE n.v > 1", "WITH", []string{"n", "m"}},
		{"WITH *", "WITH", nil},
		{"UNWIND range(1, 3) AS i", "UNWIND", []string{"range(1, 3)"}},
		{"UNWIND $list", "UNWIND", nil},
		{"WITH n", "RETURN", nil},
		{"RET", "RETURN", nil},
	} {
		require.Equal(t, tc.want, projectionExpressions(tc.clause, tc.keyword), "%s / %s", tc.keyword, tc.clause)
	}
}

// A CALL subquery that produced no result still yields one row carrying
// the statement's parameters.
func TestCallPipelineRowsFromNilResult(t *testing.T) {
	ctx := withQueryParams(context.Background(), map[string]interface{}{"a": int64(1)})
	rows := callPipelineRowsFromResult(ctx, nil)
	require.Len(t, rows, 1)
	require.Equal(t, int64(1), rows[0]["$a"])
}

// Repeated statements skip validation they already passed (#823), and an
// invalid statement is rejected every time it is sent.
func TestRepeatedStatementValidation(t *testing.T) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		result, err := exec.Execute(ctx, "UNWIND [1, 2] AS x RETURN x * 2 AS y", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}, {int64(4)}}, result.Rows)
		for _, invalid := range []string{
			"RETURN 1 AS x union MATCH (n) FINISH",
			"RETURN 0x AS x",
			"RETURN 1 +",
			"RETURN 1; RETURN 2",
		} {
			_, err := exec.Execute(ctx, invalid, nil)
			require.Error(t, err, "%s (attempt %d)", invalid, i+1)
		}
	}
}

// Every read binds parameters as the same row values: a typed list inside a
// map parameter compares the same in a row read and a fused count (#823).
func TestNestedTypedListParameterBindsAlike(t *testing.T) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"), 0, 0)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Item {v: 'a'}), (:Item {v: 'b'}), (:Item {v: 'c'})", nil)
	require.NoError(t, err)
	params := map[string]interface{}{"p": map[string]interface{}{"vals": []string{"a", "b"}}}
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (n:Item) WHERE n.v IN $p.vals RETURN n.v ORDER BY n.v", [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (n:Item) WHERE n.v IN $p.vals RETURN count(n)", [][]interface{}{{int64(2)}}},
	} {
		result, err := exec.Execute(ctx, tc.query, params)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.want, result.Rows, tc.query)
	}
}
