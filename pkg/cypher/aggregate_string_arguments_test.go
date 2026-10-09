package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A ')' or an escaped quote in a string inside an aggregate's argument is
// text: every scanner that matches a call's parentheses skips quoted text
// (findMatchingDelimiter), as Neo4j 5.26.30 reads it (#907).
func TestAggregateStringArguments(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "aggregate_strings"))
	ctx := context.Background()
	for query, want := range map[string][][]interface{}{
		"UNWIND [1] AS i RETURN i, count('a)b') AS x":                     {{int64(1), int64(1)}},
		"RETURN count('a)b') AS x":                                        {{int64(1)}},
		`RETURN count('it\'s') AS x`:                                      {{int64(1)}},
		`UNWIND [1] AS i RETURN i, count('it\'s') AS x`:                   {{int64(1), int64(1)}},
		`UNWIND [1] AS i RETURN i, collect("a)b") AS x`:                   {{int64(1), []interface{}{"a)b"}}},
		"UNWIND [1] AS i RETURN i, collect({k: 'a)b'}) AS x":              {{int64(1), []interface{}{map[string]interface{}{"k": "a)b"}}}},
		"UNWIND [1] AS i WITH i, collect('a)b') AS x RETURN x":            {{[]interface{}{"a)b"}}},
		"UNWIND ['a)b'] AS s RETURN count(s) AS x, collect(s + ')') AS y": {{int64(1), []interface{}{"a)b)"}}},
		"UNWIND ['a', 'b'] AS s RETURN collect(s + ')')[..1] AS x":        {{[]interface{}{"a)"}}},
		"UNWIND [1, 2] AS i RETURN sum(CASE WHEN i > 1 THEN 1 END) AS x":  {{int64(1)}},
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, want, result.Rows, query)
	}
}

func TestExtractFuncInnerQuotedText(t *testing.T) {
	require.Equal(t, "'a)b'", extractFuncInner("count('a)b')"))
	require.Equal(t, `'it\'s'`, extractFuncInner(`count('it\'s')`))
	require.Equal(t, "", extractFuncInner("count('a)b'"))
	require.Equal(t, "", extractFuncInner("count"))
}
