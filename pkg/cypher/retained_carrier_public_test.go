// Author: Codex R17 implementation agent, 30 October 2026.
package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Repeated equal-text mutation stretches must keep distinct occurrences while
// the native batch planner consumes the retained program. WITH preserves the
// two incoming rows, so the second UNWIND produces four rows and reuses nodes.
func TestRetainedCarrierPublicRepeatedUnwindStages(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec := NewStorageExecutor(newTestMemoryEngine(t))
			result, err := exec.Execute(t.Context(), `
				UNWIND $rows AS row
				MERGE (n:RetainedCarrierWindow {id: row.id})
				WITH $rows AS rows UNWIND rows AS row
				MERGE (n:RetainedCarrierWindow {id: row.id})
				RETURN count(n) AS total`, map[string]interface{}{
				"rows": []interface{}{
					map[string]interface{}{"id": int64(1)},
					map[string]interface{}{"id": int64(2)},
				},
			})
			require.NoError(t, err)
			require.Equal(t, []string{"total"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)
			stored, err := exec.Execute(t.Context(), "MATCH (n:RetainedCarrierWindow) RETURN n.id AS id ORDER BY id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, stored.Rows)
		})
	}
}
