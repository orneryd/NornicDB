package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPaginationParameters: SKIP and LIMIT read the statement's parameters
// on every projection route (standalone RETURN, after UNWIND, after a
// procedure's YIELD), as Neo4j does.
func TestPaginationParameters(t *testing.T) {
	exec, _ := newTestExecutor(t)
	params := map[string]interface{}{"p": int64(5), "zero": int64(0), "one": int64(1), "two": int64(2)}
	for query, want := range map[string][][]interface{}{
		"RETURN $p AS x LIMIT $zero":                            {},
		"RETURN $p AS x SKIP $one":                              {},
		"RETURN $p AS x LIMIT $one":                             {{int64(5)}},
		"UNWIND [1, 2, 3] AS x RETURN x LIMIT $two":             {{int64(1)}, {int64(2)}},
		"UNWIND [1, 2, 3] AS x RETURN x SKIP $one LIMIT $one":   {{int64(2)}},
		"CALL db.labels() YIELD label RETURN label LIMIT $zero": {},
	} {
		result, err := exec.Execute(context.Background(), query, params)
		require.NoError(t, err, query)
		require.Equal(t, len(want), len(result.Rows), query)
		for i := range want {
			require.Equal(t, want[i], result.Rows[i], query)
		}
	}
}
