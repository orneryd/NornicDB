package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A node pattern whose property map holds null matches no node, a node
// without the property included, and OPTIONAL MATCH keeps its row with null,
// as in Neo4j 5.26.30; the lookup finds that without scanning the label.
func TestNodePatternWithNullPropertyMatchesNothing(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nullprop"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE CONSTRAINT g_id FOR (n:G) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:G {id: 'a'}), (:G {other: 1}), (:G)", nil)
	require.NoError(t, err)

	for statement, want := range map[string][][]interface{}{
		"MATCH (x:G {id: null}) RETURN count(x) AS c":                                                          {{int64(0)}},
		"OPTIONAL MATCH (x:G {id: null}) RETURN x":                                                             {{nil}},
		"WITH null AS v MATCH (x:G {id: v}) RETURN count(x) AS c":                                              {{int64(0)}},
		"MATCH (x:G {id: 'a', other: null}) RETURN count(x) AS c":                                              {{int64(0)}},
		"MATCH (x {id: null}) RETURN count(x) AS c":                                                            {{int64(0)}},
		"UNWIND range(0, 2) AS i OPTIONAL MATCH (x:G {id: CASE WHEN i < 0 THEN 'a' END}) RETURN count(x) AS c": {{int64(0)}},
		"UNWIND ['a', null] AS v OPTIONAL MATCH (x:G {id: v}) RETURN v, x.id AS id":                            {{"a", "a"}, {nil, nil}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}
}
