package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The six shapes every statement-effect consumer reads: the execution contract
// and the query analysis answer them. The CALL-subquery consumers take their
// answer from that same analysis (see
// TestCallSubqueryUncorrelatedWriteProcedureRunsPerInputRow for the public
// path).
func TestStatementEffectConsumersHonorRegisteredProcedureModes(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		ensureBuiltInProceduresRegistered()
		for _, tc := range []struct {
			name, query string
			writes      bool
		}{
			{"registered write in tail", "MATCH (n:Doc) CALL kp.suppress(n) RETURN n", true},
			{"registered write in subquery", "CALL { MATCH (n:Doc) CALL kp.suppress(n) } RETURN 1 AS value", true},
			{"schema drop", "DROP INDEX effect_control IF EXISTS", true},
			{"read procedure", "CALL db.labels()", false},
			{"literal keyword", "RETURN 'CREATE' AS keyword", false},
			{"property keyword", "MATCH (n:Doc) RETURN n.set AS value", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx := t.Context()
				contract, err := readStatementExecutionContract(ctx, tc.query)
				require.NoError(t, err)
				require.Equal(t, tc.writes, contract.writes, "execution contract")
				t.Run("query analysis", func(t *testing.T) {
					info := mustAnalyze(t, NewQueryAnalyzer(16), ctx, tc.query)
					require.Equal(t, tc.writes, info.Writes())
					require.Equal(t, !tc.writes, info.ReadOnly())
				})
			})
		}
	})
}
