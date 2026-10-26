package cypher

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestDistinctOperandKeywordContext(t *testing.T) {
	require.False(t, expressionFollowsKeyword("DIſTINCT"))
	require.True(t, expressionFollowsKeyword("distinct"))
	for _, query := range []string{"WITH 7 AS yield WITH DISTINCT yield RETURN yield AS k", "WITH 7 AS call WITH DISTINCT call RETURN call AS k", "WITH 7 AS where WITH DISTINCT where RETURN where AS k", "WITH 7 AS distinct WITH distinct RETURN distinct AS k", "WITH 7 AS distinct RETURN distinct ORDER BY distinct"} {
		t.Run(query, func(t *testing.T) {
			require.Equal(t, strings.LastIndex(query, "RETURN"), topLevelKeywordIndex(originalBraceView(query), "RETURN"))
			for _, parser := range []string{"nornic", "antlr"} {
				t.Run(parser, func(t *testing.T) {
					old := config.GetParserType()
					config.SetParserType(parser)
					defer config.SetParserType(old)
					e := NewStorageExecutor(newTestMemoryEngine(t))
					r, err := e.Execute(t.Context(), query, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
				})
			}
		})
	}
}
