package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestNumericParameterNameAcrossParsers retains Neo4j's numeric parameter identity.
func TestNumericParameterNameAcrossParsers(t *testing.T) {
	require.Equal(t, []string{"123abc"}, ExtractParameters("RETURN $123abc AS v"))
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			exec, ctx := newUnitExecutor(t)
			result, err := exec.Execute(ctx, "RETURN $123abc AS v", map[string]interface{}{"123abc": int64(7)})
			require.NoError(t, err)
			require.Equal(t, []string{"v"}, result.Columns)
			require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
		})
	}
}
