package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// In a Cypher 25 statement a dynamic label or type may be tested in an
// expression (Neo4j 2026.09): WHERE n:$(e), RETURN n:$any(l), n IS $(e),
// r:$(e), combined with static labels. A Cypher 5 statement keeps Neo4j
// 5.26's SyntaxError there.
func TestCypher25DynamicLabelTests(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "cypher25_dynamic_label_tests"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:DlP:DlM {name: 'Ann'})-[:DlR]->(:DlP {name: 'Bob'})", nil)
	require.NoError(t, err)
	annOnly := [][]interface{}{{"Ann", true}, {"Bob", false}}
	for query, want := range map[string][][]interface{}{
		"MATCH (n:DlP) WHERE n:$('DlM') RETURN n.name AS v":                        {{"Ann"}},
		"WITH 'DlM' AS l MATCH (n:DlP) WHERE n:$(l) RETURN n.name AS v":            {{"Ann"}},
		"MATCH (n:DlP) RETURN n.name AS v, n:$('DlM') AS m ORDER BY v":             annOnly,
		"MATCH (n:DlP) RETURN n.name AS v, n:$any(['DlM', 'X']) AS m ORDER BY v":   annOnly,
		"MATCH (n:DlP) RETURN n.name AS v, n:$all(['DlM', 'DlP']) AS m ORDER BY v": annOnly,
		"MATCH (n:DlP) RETURN n.name AS v, n:DlP&$('DlM') AS m ORDER BY v":         annOnly,
		"MATCH (n:DlP) RETURN n.name AS v, n IS $('DlM') AS m ORDER BY v":          annOnly,
		"MATCH (n:DlP) RETURN n.name AS v, NOT n:$('DlM') AS m ORDER BY v":         {{"Ann", false}, {"Bob", true}},
		"MATCH ()-[r]->() RETURN r:$('DlR') AS m":                                  {{true}},
	} {
		result, err := exec.Execute(ctx, "CYPHER 25 "+query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
		_, err = exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}
