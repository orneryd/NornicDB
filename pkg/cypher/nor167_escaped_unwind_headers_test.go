package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNOR167EscapedUnwindHeadersExecute keeps the Unicode escape in the query
// source for both UNWIND forms and the direct projection, then checks the
// result headers returned by Execute against Neo4j's decoded name.
func TestNOR167EscapedUnwindHeadersExecute(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t), QueryCachePolicy{MaxEntries: 0, TTL: 0})
		escapedIdentifier := `a\u0062`
		for _, tc := range []struct {
			query string
			rows  [][]interface{}
		}{
			{"UNWIND ['value'] AS `" + escapedIdentifier + "` RETURN `" + escapedIdentifier + "`", [][]interface{}{{"value"}}},
			{"UNWIND [] AS `" + escapedIdentifier + "` RETURN `" + escapedIdentifier + "`", nil},
			{"RETURN 1 AS `" + escapedIdentifier + "`", [][]interface{}{{int64(1)}}},
		} {
			require.True(t, strings.Contains(tc.query, `\u0062`), "query must contain the literal Unicode escape: %q", tc.query)
			result, err := exec.Execute(t.Context(), tc.query, nil)
			require.NoError(t, err, tc.query)
			require.Equal(t, []string{"ab"}, result.Columns, tc.query)
			if tc.rows == nil {
				require.Empty(t, result.Rows, tc.query)
			} else {
				require.Equal(t, tc.rows, result.Rows, tc.query)
			}
			t.Logf("statement=%q columns=%v rows=%v", tc.query, result.Columns, result.Rows)
		}
	})
}
