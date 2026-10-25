package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

// Framing no longer infers procedure or CALL headers independently. The
// canonical admitted product supplies presence as well as ordered columns.
func TestResultHeaderFramingPresence(t *testing.T) {
	executor := NewStorageExecutor(newTestMemoryEngine(t))
	for _, tc := range []struct {
		query string
		want  bool
	}{
		{"RETURN 1 AS value", true},
		{"RETURN 1 AS value UNION ALL RETURN 2 AS value", true},
		{"CALL db.labels()", true},
		{"CALL db.labels() YIELD label", true},
		{"CALL { RETURN 1 AS value }", true},
		{"CALL { CREATE (:FramingPresence) }", false},
		{"CALL db.labels() FINISH", false},
		{"WITH 1 AS value FINISH", false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			query := "/* header framing presence contract */ " + tc.query
			key := projectionKey(query, resultmetadata.Known(nil), resultmetadata.Known(nil), false)
			_, cached := projectionFactsCache.get(key)
			require.False(t, cached)
			// Presence is a view of the admitted columns, not a separate cold-header reader.
			columns, err := executor.StatementResultColumns(query, RawSource)
			require.NoError(t, err)
			require.Equal(t, tc.want, len(columns) > 0)
		})
	}
}
