package cypher

import (
	"encoding/json"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/functionsyntaxmatrix"
	"github.com/stretchr/testify/require"
)

// Generated in memory: comments containing quotes, delimiters and function-like
// prose cross argument, map, aggregate and label boundaries. Each public call
// gets an isolated engine; no external fixture or database is required.
func TestFunctionExpressionLexicalCorpus(t *testing.T) {
	cases := append(functionsyntaxmatrix.Cases(), functionsyntaxmatrix.NativeCases()...)
	forEachCypherParserSetting(t, func(t *testing.T) {
		for _, sample := range cases {
			t.Run(sample.ID, func(t *testing.T) {
				exec, _ := newTestExecutor(t)
				result, err := exec.Execute(t.Context(), sample.Query, nil)
				require.NoError(t, err, sample.Query)
				rows, err := json.Marshal(result.Rows)
				require.NoError(t, err)
				require.JSONEq(t, sample.Rows, string(rows), sample.Query)
			})
		}
	})
}
