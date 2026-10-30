package cypher

import (
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestLocalBindingNotDeclarationRoles(t *testing.T) {
	for _, query := range []string{"RETURN [not IN [1] | not]", "FOREACH (not IN [1] | CREATE (:T {v:not}))", "RETURN reduce(acc=0,not IN [1] | acc+not)", "RETURN REDUCE(acc=0,not IN [1] | acc+not)"} {
		require.False(t, containsNotInOperator(query), query)
		allocs := testing.AllocsPerRun(100, func() {
			if containsNotInOperator(query) {
				panic("valid local declaration rejected")
			}
		})
		require.Zero(t, allocs, query)
	}
	for _, query := range []string{"RETURN 1 NOT IN [1]", "RETURN reduce(acc=1 NOT IN [1],not IN [1] | acc)", "FOREACH (x IN [1] | CREATE (:T {v:1 NOT IN [1]}))"} {
		require.True(t, containsNotInOperator(query), query)
	}
	for _, tc := range []struct{ name, want string }{{"fİlter", "fİlter"}, {"ſingle", ""}, {"FiLtEr", "FiLtEr"}, {"FOREACH", "FOREACH"}} {
		text := tc.name + "(x IN [1] | x)"
		require.Equal(t, tc.want, bindingFunctionBeforeParen(text, strings.IndexByte(text, '(')), tc.name)
	}
}

func TestLocalBindingMapSelectorRoles(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  [][]interface{}
	}{
		{"WITH {v:0} AS m,7 AS false RETURN false AS literal,m{false}.false AS selected", [][]interface{}{{false, int64(7)}}},
		{"WITH {v:0} AS m,7 AS true RETURN true AS literal,m{true}.true AS selected", [][]interface{}{{true, int64(7)}}},
		{"WITH {v:0} AS m,7 AS null RETURN null AS literal,m{null}.null AS selected", [][]interface{}{{nil, int64(7)}}},
		{"WITH {v:0} AS m,7 AS false RETURN m{k:false}.k AS k", [][]interface{}{{false}}},
		{"WITH {} AS m,null AS n RETURN m{n}.n AS k", [][]interface{}{{nil}}},
		{"WITH {} AS m,7 AS `true` RETURN m{`true`}.`true` AS k", [][]interface{}{{int64(7)}}},
	} {
		exec, ctx := newUnitExecutor(t)
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.want, result.Rows, tc.query)
	}
}

func TestNamedPatternKeywordPrefixRoles(t *testing.T) {
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "path_keyword_roles")
	exec := NewStorageExecutor(store)
	_, err := store.CreateNode(&storage.Node{ID: "source", Labels: []string{"LocalPathSource"}, Properties: map[string]interface{}{"id": "source"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "target", Labels: []string{"LocalPathTarget"}, Properties: map[string]interface{}{"id": "target"}})
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "edge", Type: "LocalPathRel", StartNode: "source", EndNode: "target", Properties: map[string]interface{}{"id": "edge"}}))
	for _, name := range []string{"create", "match", "merge", "p"} {
		pattern := name + " = (s:LocalPathSource)-[:LocalPathRel]->(t:LocalPathTarget)"
		for _, prefix := range []string{"", "CREATE ", "CREATE/*c*/", "MERGE\t"} {
			kinds := make(matchSemanticScope)
			addMatchPatternBindingKinds(kinds, originalBraceView(prefix+pattern))
			require.Equal(t, matchBindingPath, kinds[name], prefix+pattern)
			require.Equal(t, matchBindingNode, kinds["s"], prefix+pattern)
		}
		for _, prefix := range []string{"", "MATCH ", "MATCH/*c*/"} {
			rows, ok := exec.boundPatternRows(t.Context(), originalBraceView(prefix+pattern), nil, nil, nil)
			require.True(t, ok, prefix+pattern)
			require.Len(t, rows, 1, prefix+pattern)
			path, ok := rows[0].values[name].(map[string]interface{})
			require.True(t, ok, prefix+pattern)
			detail, ok := path["_pathResult"].(*PathResult)
			require.True(t, ok, prefix+pattern)
			require.Len(t, detail.Nodes, 2)
			require.Len(t, detail.Relationships, 1)
			require.Equal(t, "source", detail.Nodes[0].Properties["id"])
			require.Equal(t, "target", detail.Nodes[1].Properties["id"])
			require.Equal(t, "edge", detail.Relationships[0].Properties["id"])
		}
		for _, gap := range []string{" ", "/*c*/", " //c\n"} {
			query := "MATCH" + gap + pattern + " RETURN length(" + name + ") AS length,nodes(" + name + ")[0].id AS source,nodes(" + name + ")[1].id AS target,relationships(" + name + ")[0].id AS edge"
			result, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{int64(1), "source", "target", "edge"}}, result.Rows, query)
		}
	}
}

func TestTrueClauseKeywordTriviaRoles(t *testing.T) {
	prior := config.GetParserType()
	defer config.SetParserType(prior)
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			for _, normalized := range []bool{true, false} {
				t.Run(fmt.Sprintf("normalization_%v", normalized), func(t *testing.T) {
					restore := config.WithCypherQueryNormalizationDisabled()
					defer restore()
					config.SetCypherQueryNormalizationEnabled(normalized)
					for _, gap := range []string{" ", "/*c*/", " //c\n"} {
						for _, keyword := range []string{"MATCH", "CREATE", "MERGE"} {
							t.Run(fmt.Sprintf("%s/%q", keyword, gap), func(t *testing.T) {
								exec, ctx := newUnitExecutor(t)
								if keyword == "MATCH" {
									_, err := exec.Execute(ctx, "CREATE (:LocalClauseTrivia {id:7})", nil)
									require.NoError(t, err)
								}
								query := keyword + gap + "(n:LocalClauseTrivia {id:7}) RETURN n.id AS k"
								result, err := exec.Execute(ctx, query, nil)
								require.NoError(t, err, query)
								require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows, query)
							})
						}
					}
				})
			}
		})
	}
}
