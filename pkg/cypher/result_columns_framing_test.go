package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/stretchr/testify/require"
)

// Framing only needs to know whether a branch returns columns. Procedure
// and nested CALL endings must agree with the owner without compiling its
// full scope program on the ordinary execution path.
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
			require.Equal(t, tc.want, executor.statementProducesColumns(t.Context(), query))
			_, cached = projectionFactsCache.get(key)
			require.False(t, cached, "presence-only framing must not compile the scope program")
			columns, err := executor.StatementResultColumns(query, RawSource)
			require.NoError(t, err)
			require.Equal(t, tc.want, len(columns) > 0)
		})
	}
}
