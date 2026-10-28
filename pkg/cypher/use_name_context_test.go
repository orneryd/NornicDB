package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestFabricUseVariableContext(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			old := config.GetParserType()
			config.SetParserType(parser)
			defer config.SetParserType(old)
			for _, query := range []string{
				"USE comp CALL { USE comp.one WITH 7 AS use WITH DISTINCT use RETURN use AS k } RETURN k",
				"USE comp CALL { USE comp.one WITH 7 AS use RETURN use AS k } RETURN k",
				"USE comp CALL { WITH 7 AS use CALL { USE comp.one RETURN use AS k } RETURN k } RETURN k",
				"USE comp CALL { USE comp.one WITH {use:7} AS n RETURN n.use AS k } RETURN k",
				"USE comp CALL { USE comp.one WITH 7 AS `use` RETURN `use` AS k } RETURN k",
				"USE comp CALL { USE comp.one WITH 7 AS use /* USE fake */ RETURN use AS k } RETURN k",
				"USE GRAPH use RETURN 7 AS k",
			} {
				t.Run(query, func(t *testing.T) {
					e := newFabricCallRowsExecutor(t)
					mgr := e.dbManager.(*testDatabaseManagerAdapter).manager
					require.NoError(t, mgr.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "use"}))
					r, err := e.Execute(t.Context(), query, nil)
					require.NoError(t, err)
					require.Equal(t, []string{"k"}, r.Columns)
					require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
				})
			}
		})
	}
}
func TestUseNameContextReader(t *testing.T) {
	for _, query := range []string{"WITH 7 AS use RETURN use", "WITH 7 AS use WITH DISTINCT use RETURN use", "WITH 7 AS use /* USE fake */ RETURN use"} {
		at := strings.Index(query, "use")
		require.True(t, clauseKeywordUsedAsName(query, at, at+3, "USE"))
		require.Zero(t, testing.AllocsPerRun(100, func() { _ = clauseKeywordUsedAsName(query, at, at+3, "USE") }))
	}
	p := fabric.NewFabricPlanner(nil)
	p.SetClauseKeywordNameReader(clauseKeywordUsedAsName)
	single, err := p.SingleGraphStatement(t.Context(), "USE comp CALL { USE comp.one WITH 7 AS use WITH DISTINCT use RETURN use AS k } RETURN k", cyphertext.AcceptedSource)
	require.NoError(t, err)
	require.Equal(t, "CALL { WITH 7 AS use WITH DISTINCT use RETURN use AS k } RETURN k", single)
}
