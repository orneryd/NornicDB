package cypher

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestMergeActionDanglingClauseRoles(t *testing.T) {
	for _, text := range []string{
		"MERGE (n) ON MATCH SET n.x=1",
		"MERGE (n) ON/*c*/MATCH //c\nSET n.x=1 ON CREATE SET n.y=2",
		"MERGE (n) ON CREATE SET n.x=1 ON MATCH SET n.y=2",
	} {
		require.Equal(t, "SET", lastTopLevelClauseWord(text), text)
		require.Zero(t, testing.AllocsPerRun(100, func() {
			if lastTopLevelClauseWord(text) != "SET" {
				panic("MERGE action role")
			}
		}), text)
	}
	require.Equal(t, "MATCH", lastTopLevelClauseWord("MERGE (n) ON MATCH SET n.x=1 MATCH (m)"))
	prior := config.GetParserType()
	defer config.SetParserType(prior)
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			for i, query := range []string{
				"MERGE (n:MergeRoleSeed {id:1}) ON MATCH SET n.x=7",
				"MERGE (n:MergeRoleSeed {id:1}) ON/*c*/MATCH //c\nSET n.x=7 ON CREATE SET n.y=8",
				"MERGE (n:MergeRoleSeed {id:1}) ON CREATE SET n.y=8 ON MATCH SET n.x=7",
			} {
				t.Run(fmt.Sprintf("valid_%d", i), func(t *testing.T) {
					exec, ctx := newUnitExecutor(t)
					_, err := exec.Execute(ctx, "CREATE (:MergeRoleSeed {id:1})", nil)
					require.NoError(t, err)
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err, query)
					require.Zero(t, result.Stats.NodesCreated)
					got, err := exec.Execute(ctx, "MATCH (n:MergeRoleSeed) RETURN n.x AS x", nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(7)}}, got.Rows)
				})
			}
			for _, query := range []string{
				"MERGE (n:MergeRoleSeed {id:1}) ON MATCH SET",
				"MERGE (n:MergeRoleSeed {id:1}) ON CREATE SET",
				"MERGE (n:MergeRoleSeed {id:1}) ON MATCH SET n.x=7 MATCH",
				"MATCH (n:MergeRoleSeed) SET",
			} {
				exec, ctx := newUnitExecutor(t)
				_, err := exec.Execute(ctx, query, nil)
				require.Error(t, err, query)
				code, _ := nornicerrors.Neo4jStatus(err)
				require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code, query)
			}
			for _, query := range []string{
				"WITH 1 AS on MATCH set = (n:MergeRoleSeed) RETURN on,length(set)",
				"MATCH merge = (n:MergeRoleSeed) RETURN 1,length(merge)",
				"WITH 1 AS on MERGE (n:MergeRoleSeed {id:1}) ON CREATE SET n.x=on MATCH set = (m:MergeRoleSeed) RETURN on,length(set)",
			} {
				exec, ctx := newUnitExecutor(t)
				_, err := exec.Execute(ctx, "CREATE (:MergeRoleSeed {id:1})", nil)
				require.NoError(t, err)
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{int64(1), int64(0)}}, result.Rows, query)
			}
		})
	}
}
