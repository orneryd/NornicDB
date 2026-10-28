package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestPaginationWordsRetainVariableRoles(t *testing.T) {
	for _, name := range []string{"limit", "skip"} {
		for _, template := range []string{
			"USE comp CALL { USE comp.one WITH 7 AS %[1]s WITH DISTINCT %[1]s RETURN %[1]s AS k } RETURN k",
			"USE comp CALL { USE comp.one MATCH %[1]s = (n:FabricStructureName) RETURN n.v AS k } RETURN k",
			"USE comp CALL { USE comp.one CREATE (%[1]s:FabricNameMutation {v:7}) REMOVE %[1]s:FabricNameMutation RETURN %[1]s.v AS k } RETURN k",
			"CREATE %[1]s = (n:PaginationNativeRole) RETURN length(%[1]s) AS k",
			"MERGE %[1]s = (n:PaginationNativeRole {v:7}) RETURN length(%[1]s) AS k",
			"MATCH (%[1]s:PaginationNativeRole) DELETE %[1]s RETURN 7 AS k",
		} {
			query := fmt.Sprintf(template, name)
			t.Run(query, func(t *testing.T) {
				got, rewrite := desugarStandaloneOrderClauses(query)
				t.Logf("ORIGINAL=%q REWRITTEN=%q", query, got)
				require.Equal(t, query, got)
				require.Nil(t, rewrite)
			})
		}
	}
}

func TestPaginationVariableRolesOnPublicPaths(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			old := config.GetParserType()
			config.SetParserType(parser)
			defer config.SetParserType(old)
			for _, name := range []string{"limit", "skip", "p"} {
				for _, tc := range []struct {
					query string
					want  int64
				}{
					{"CREATE %[1]s = (n:PaginationNativeRole) RETURN length(%[1]s) AS k", 0},
					{"MERGE %[1]s = (n:PaginationNativeRole {v:7}) RETURN length(%[1]s) AS k", 0},
					{"CREATE (%[1]s:PaginationNativeRole {v:7}) WITH %[1]s DELETE %[1]s RETURN 7 AS k", 7},
				} {
					q := fmt.Sprintf(tc.query, name)
					t.Run(q, func(t *testing.T) {
						e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pagination_roles"))
						r, err := e.Execute(t.Context(), q, nil)
						require.NoError(t, err)
						require.Equal(t, []string{"k"}, r.Columns)
						require.Equal(t, [][]interface{}{{tc.want}}, r.Rows)
					})
				}
			}
			for _, q := range []string{"WITH 7 AS create CREATE(n:PaginationNativeRole {v:create}) RETURN n.v AS k", "WITH 7 AS merge MERGE(n:PaginationNativeRole {v:merge}) RETURN n.v AS k", "WITH 7 AS remove CREATE(n:PaginationNativeRole {v:remove}) REMOVE n:PaginationNativeRole RETURN n.v AS k"} {
				t.Run(q, func(t *testing.T) {
					e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "pagination_roles"))
					r, err := e.Execute(t.Context(), q, nil)
					require.NoError(t, err)
					require.Equal(t, [][]interface{}{{int64(7)}}, r.Rows)
				})
			}
		})
	}
}
func TestPaginationRolePredicateAndSourceEdits(t *testing.T) {
	for _, word := range []string{"MATCH", "CREATE", "MERGE", "REMOVE", "DELETE", "DISTINCT", "CALL", "STARTS", "LIMIT"} {
		require.True(t, standaloneOperandWord(word))
		require.Zero(t, testing.AllocsPerRun(100, func() { _ = standaloneOperandWord(word) }))
	}
	q := "MATCH limit = (n) LIMIT 1 RETURN limit"
	got, r := desugarStandaloneOrderClauses(q)
	require.Equal(t, "MATCH limit = (n) WITH * LIMIT 1 RETURN limit", got)
	require.NotNil(t, r)
	require.Len(t, r.Edits, 1)
	at := strings.Index(q, "LIMIT 1")
	require.Equal(t, at, r.Edits[0].Original.Start)
	require.Equal(t, at, r.Edits[0].Original.End)
	require.Equal(t, at, r.Edits[0].Accepted.Start)
	require.Equal(t, at+len("WITH * "), r.Edits[0].Accepted.End)
}
