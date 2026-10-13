package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A variable named and, or or xor is a variable where an operand stands, and
// the operator splits a predicate only where it follows a complete operand,
// in RETURN, WHERE and CASE, as Neo4j 5.26.30 reads them (#1145).
func TestLogicalOperatorsReadVariablesNamedLikeThem(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "opkw"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Doc {id: 'd1'}), (:Code {id: 'c1'})", nil)
	require.NoError(t, err)
	for statement, want := range map[string][][]interface{}{
		"WITH 2 AS and RETURN and < 3 AND true AS v":                                            {{true}},
		"WITH 2 AS and RETURN and > 1 AND and < 3 AS v":                                         {{true}},
		"WITH 2 AS or RETURN or > 1 OR or < 3 AS v":                                             {{true}},
		"WITH 2 AS or RETURN or > 1 AND or < 3 AS v":                                            {{true}},
		"WITH 2 AS xor RETURN xor > 1 XOR xor < 3 AS v":                                         {{false}},
		"WITH 2 AS and WHERE and > 1 AND and < 3 RETURN and AS v":                               {{int64(2)}},
		"WITH 2 AS and RETURN CASE WHEN and > 1 AND and < 3 THEN and * 2 ELSE 0 - and END AS v": {{int64(4)}},
		"WITH true AS distinct RETURN distinct AND true AS v":                                   {{true}},
		"WITH 2 AS x RETURN x > 1 AND NOT x < 3 OR x = 2 AS v":                                  {{true}},
		"MATCH (n:%) WHERE NOT n:Code RETURN n.id AS v":                                         {{"d1"}},
		"WITH 7 AS x RETURN x % 2 = 1 AND x % 3 = 1 AS v":                                       {{true}},
	} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, want, result.Rows, statement)
	}
}
