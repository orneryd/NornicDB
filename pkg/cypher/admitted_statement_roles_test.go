package cypher

import (
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestImportedASInProjectionStatements(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			old := config.GetParserType()
			config.SetParserType(parser)
			defer config.SetParserType(old)
			for _, q := range []string{"WITH as RETURN as AS k", "RETURN as AS k", "wItH/*import*/ as RETURN as AS k", "rEtUrN/*expression*/ as AS k"} {
				t.Run(q, func(t *testing.T) {
					e := newFabricCallRowsExecutor(t)
					bridge := &cypherFabricExecutor{base: e}
					cols, rows, err := bridge.executeFragmentQuery(t.Context(), e.cloneWithStorage(e.storage, selectedDatabaseStorage), q, nil, map[string]interface{}{"as": int64(7)})
					require.NoError(t, err)
					require.Equal(t, []string{"k"}, cols)
					require.Equal(t, [][]interface{}{{int64(7)}}, rows)
				})
			}
		})
	}
}
func TestAdmittedProjectionPrefixPreservesFragmentNames(t *testing.T) {
	for _, kw := range []string{"WITH", "RETURN", "CALL"} {
		require.False(t, startsWithKeywordFold(kw+"x as", kw))
		require.True(t, startsWithKeywordFold(kw+"/* c */ as", kw))
		require.Zero(t, testing.AllocsPerRun(100, func() { _ = startsWithKeywordFold(kw+" as", kw) }))
	}
	require.Equal(t, -1, findKeywordIndex(originalBraceView("with AS alias"), "WITH"))
	e := newFabricCallRowsExecutor(t)
	_, err := e.Execute(t.Context(), "CALL as()", nil)
	requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureNotFound")
}
