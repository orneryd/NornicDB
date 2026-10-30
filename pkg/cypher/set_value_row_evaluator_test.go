package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A value SET writes is the value the same expression projects: a list of
// comparisons, a subscript, a comprehension, a map member. Recorded on Neo4j
// 5.26.30.
func TestSetWritesTheProjectedValue(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "set_value_row"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:ZS {id: 1, l: [5, 6]})", nil)
	require.NoError(t, err)
	for query, want := range map[string]interface{}{
		"MATCH (n:ZS) SET n.p = [1 > 2] RETURN n.p AS v":                         []interface{}{false},
		"MATCH (n:ZS) SET n.p = [1 + 2, 3] RETURN n.p AS v":                      []interface{}{int64(3), int64(3)},
		"MATCH (n:ZS) SET n.p = n.l[0] + 1 RETURN n.p AS v":                      int64(6),
		"MATCH (n:ZS) SET n.p = [x IN n.l | x * 2] RETURN n.p AS v":              []interface{}{int64(10), int64(12)},
		"MATCH (n:ZS) SET n.p = n.l[1 - 1] RETURN n.p AS v":                      int64(5),
		"MATCH (n:ZS) SET n.p = size([1 > 0, 2]) RETURN n.p AS v":                int64(2),
		"MATCH (n:ZS) SET n.p = [n.id = 1, n.id <> 1] RETURN n.p AS v":           []interface{}{true, false},
		"MATCH (n:ZS) SET n += {q: [1 > 2]} RETURN n.q AS v":                     []interface{}{false},
		"UNWIND [7] AS u MATCH (n:ZS) SET n.p = u + n.id RETURN n.p AS v":        int64(8),
		"MERGE (m:ZM {id: 1}) ON CREATE SET m.p = [1 > 2] RETURN m.p AS v":       []interface{}{false},
		"MATCH (n:ZS) SET n.p = apoc.text.join(['a', 'b'], '-') RETURN n.p AS v": "a-b",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		stored, err := exec.Execute(ctx, "MATCH (n) WHERE n:ZS OR n:ZM RETURN n.p AS p, n.q AS q", nil)
		require.NoError(t, err)
		require.NotEmpty(t, stored.Rows)
	}
}
