package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQualifiedPluginNamesWithLexicalTrivia(t *testing.T) {
	previous := PluginFunctionLookup
	normalization := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { PluginFunctionLookup = previous; config.SetCypherQueryNormalizationEnabled(normalization) })
	for _, normalize := range []bool{true, false} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, explicit := range []bool{false, true} {
			for _, suffix := range []string{"range", "truncate", "shortestPath", "allShortestPaths", "and", "or", "xor", "a b", "a-b", "a`b"} {
				quoted := "`" + strings.ReplaceAll(suffix, "`", "``") + "`"
				forms := []string{"`custom`." + quoted, "custom /*c*/ . " + quoted, "custom . /*c*/ " + quoted, "custom //c\n . " + quoted, "custom . //c\n " + quoted}
				if !strings.ContainsAny(suffix, " -`") {
					for _, form := range []string{"custom /*c*/ . %s", "custom . /*c*/ %s", "custom //c\n . %s", "custom . //c\n %s"} {
						forms = append(forms, fmt.Sprintf(form, suffix))
					}
				}
				for _, call := range forms {
					t.Run(fmt.Sprintf("normalize=%v/explicit=%v/%s", normalize, explicit, call), func(t *testing.T) {
						PluginFunctionLookup = func(name string) (interface{}, bool) {
							return func(value interface{}) interface{} { return value }, name == "custom."+suffix
						}
						executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "lexical_plugin"))
						ctx := context.Background()
						if explicit {
							_, err := executor.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err)
							t.Cleanup(func() { _, _ = executor.Execute(ctx, "ROLLBACK", nil) })
						}
						for _, query := range []string{"RETURN " + call + "('ok') AS v", "UNWIND ['ok'] AS x RETURN " + call + "(x) AS v", "UNWIND ['ok','no'] AS x WITH x WHERE " + call + "(x) = 'ok' RETURN x AS v"} {
							result, err := executor.Execute(ctx, query, nil)
							require.NoError(t, err, query)
							require.Equal(t, [][]interface{}{{"ok"}}, result.Rows)
						}
						count, err := executor.Execute(ctx, "UNWIND range(1,100) AS x RETURN count("+call+"(x)) AS n", nil)
						require.NoError(t, err)
						require.Equal(t, [][]interface{}{{int64(100)}}, count.Rows)
						if explicit {
							_, err := executor.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}
						PluginFunctionLookup = nil
						// Registration changes need a fresh executor, without cached semantics.
						unknown := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "lexical_unknown"))
						_, err = unknown.Execute(ctx, "RETURN "+call+"('ok') AS v", nil)
						requireSemanticDetail(t, err, "UnknownFunction")
						require.ErrorContains(t, err, "custom."+suffix)
					})
				}
			}
		}
	}
}

func TestQualifiedBooleanPropertyOperands(t *testing.T) {
	previous := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
	for _, normalize := range []bool{true, false} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "qualified_boolean"))
		for _, name := range []string{"and", "or", "xor"} {
			for _, dot := range []string{".", " . ", "\t.\n"} {
				property := "x" + dot + name
				for _, predicate := range []string{property + " AND y", "y AND " + property, "(" + property + ") AND y", "CASE WHEN " + property + " THEN y ELSE true END"} {
					query := "WITH {" + name + ":true} AS x, false AS y RETURN " + predicate + " AS v"
					result, err := executor.Execute(context.Background(), query, nil)
					require.NoError(t, err, query)
					require.Equal(t, [][]interface{}{{false}}, result.Rows, query)
				}
			}
		}
	}
}
