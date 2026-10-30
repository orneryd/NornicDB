package cypher

// Regression coverage for NornicDB #846: node-pattern properties were compared
// by their Go values, so MATCH (n {n: 1.0}), (n {tags: [1, 2]}) or
// (n {d: date(...)}) missed a stored 1, integer list or date, and MERGE created
// a duplicate node instead of matching. They now use Cypher equality, in
// auto-commit and in explicit transactions.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIssue846NodePatternPropertiesUseCypherEquality(t *testing.T) {
	exec, ctx := newUnitExecutor(t)
	for _, route := range []string{"auto", "tx"} {
		t.Run(route, func(t *testing.T) {
			label := "A846"
			if route == "tx" {
				label = "B846"
			}
			run := func(query string) [][]interface{} {
				t.Helper()
				if route == "tx" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
				}
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				if route == "tx" {
					_, err = exec.Execute(ctx, "COMMIT", nil)
					require.NoError(t, err)
				}
				return result.Rows
			}
			run("CREATE (:" + label + " {id: 1, tags: [1, 2], d: date('2020-01-02'), n: 1})")
			one := [][]interface{}{{int64(1)}}
			for _, pattern := range []string{
				"{tags: [1, 2]}",
				"{tags: [1.0, 2.0]}",
				"{n: 1.0}",
				"{n: 1}",
				"{d: date('2020-01-02')}",
				"{id: 1, tags: [1, 2], d: date('2020-01-02')}",
			} {
				require.Equal(t, one, run("MATCH (x:"+label+" "+pattern+") RETURN x.id"), "MATCH "+pattern)
				require.Equal(t, one, run("MERGE (x:"+label+" "+pattern+") RETURN x.id"), "MERGE "+pattern)
				require.Equal(t, one, run("MATCH (x:"+label+") RETURN count(x)"), "no node created by MERGE "+pattern)
			}
			for _, pattern := range []string{"{n: 2}", "{tags: [2, 1]}", "{d: date('2020-01-03')}", "{tags: [1, 2, 3]}"} {
				require.Empty(t, run("MATCH (x:"+label+" "+pattern+") RETURN x.id"), "MATCH "+pattern)
			}
		})
	}
}

func TestNodePropertiesMatch(t *testing.T) {
	node := &storage.Node{Properties: map[string]interface{}{"n": int64(1), "tags": []int64{1, 2}, "s": "x"}}
	require.True(t, nodePropertiesMatch(node, nil))
	require.True(t, nodePropertiesMatch(node, map[string]interface{}{"n": 1.0, "tags": []interface{}{1.0, int64(2)}, "s": "x"}))
	require.False(t, nodePropertiesMatch(node, map[string]interface{}{"n": nil}))
	require.False(t, nodePropertiesMatch(node, map[string]interface{}{"missing": int64(1)}))
	require.False(t, nodePropertiesMatch(node, map[string]interface{}{"s": "y"}))
	require.False(t, nodePropertiesMatch(node, map[string]interface{}{"n": "1"}))
}
