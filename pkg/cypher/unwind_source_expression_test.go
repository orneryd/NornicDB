package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// UNWIND of an expression that starts with a property, a list joined to it,
// a slice of it or a backticked property, unwinds the expression's list, not
// a property named after the rest of the text. Each statement's outcome is
// Neo4j 5.26.30's.
func TestUnwindSourceStartingWithAProperty(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unwindsource"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Tmp {p: ['a'], q: ['b'], l: [1, 2, 3]})", nil)
	require.NoError(t, err)

	for statement, want := range map[string][][]interface{}{
		"MATCH (n:Tmp) UNWIND n.p + n.q AS v RETURN v":                               {{"a"}, {"b"}},
		"MATCH (n:Tmp) UNWIND n.p + ['z'] AS v RETURN v":                             {{"a"}, {"z"}},
		"MATCH (n:Tmp) UNWIND (n.p + n.q) AS v RETURN v":                             {{"a"}, {"b"}},
		"MATCH (n:Tmp) UNWIND n.l[1..] AS v RETURN v":                                {{int64(2)}, {int64(3)}},
		"MATCH (n:Tmp) UNWIND n.`l` AS v RETURN v":                                   {{int64(1)}, {int64(2)}, {int64(3)}},
		"MATCH (n:Tmp) UNWIND n.p AS v RETURN v":                                     {{"a"}},
		"MATCH (n:Tmp) UNWIND n.missing AS v RETURN v":                               {},
		"MATCH (n:Tmp) UNWIND n.p + n.q AS v RETURN count(v) AS c":                   {{int64(2)}},
		"CREATE (n:Tmp2 {p: ['a'], q: ['b']}) WITH n UNWIND n.p + n.q AS v RETURN v": {{"a"}, {"b"}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		if len(want) == 0 {
			require.Empty(t, result.Rows, statement)
			continue
		}
		require.Equal(t, want, result.Rows, statement)
	}
}
