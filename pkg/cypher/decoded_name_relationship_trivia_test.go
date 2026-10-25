package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Actual source consumers decide whether trivia is part of a relationship
// declaration; decoded name bytes must never be read as that trivia.
func TestDecodedNameRelationshipCommentFlow(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("normalization=%t", enabled), func(t *testing.T) {
			previous := config.IsCypherQueryNormalizationEnabled()
			config.SetCypherQueryNormalizationEnabled(enabled)
			t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "comment_flow"))
			ctx := context.Background()
			for _, name := range []string{"R|]", "Other", "R/*]*/"} {
				_, err := executor.Execute(ctx, "CREATE (:CommentStart)-[:"+cyphertext.QuoteName(name)+"]->(:CommentEnd)", nil)
				require.NoError(t, err)
			}
			for _, subject := range []struct {
				name, inside, expected string
				count                  int64
			}{
				{"ordinary_control", ":Other", "Other", 1},
				{"quoted_control", ":`R|]`", "R|]", 1},
				{"comment_looking_data", ":`R/*]*/`", "R/*]*/", 1},
				{"before_type", ": /* ] * | WHERE */ `R|]`", "R|]", 1},
				{"after_type", ":`R|]` /* ] * | WHERE */", "R|]", 1},
				{"before_marker", "r /* ] * | WHERE */ :`R|]`", "R|]", 1},
				{"before_variable", "/* ] * | WHERE */ r:`R|]`", "R|]", 1},
				{"line_trivia", ":`R|]` // ] * | WHERE\n", "R|]", 1},
				{"alternative", ":`R|]` /* ] | */ | Other", "R|]", 2},
			} {
				t.Run(subject.name, func(t *testing.T) {
					pattern := "(a:CommentStart)-[" + subject.inside + "]->(b:CommentEnd)"
					t.Run("MATCH", func(t *testing.T) {
						result, err := executor.Execute(ctx, "MATCH "+pattern+" RETURN count(*) AS c", nil)
						require.NoError(t, err)
						require.EqualValues(t, subject.count, result.Rows[0][0])
					})
					t.Run("COUNT_EXISTS", func(t *testing.T) {
						query := "MATCH (a:CommentStart) RETURN COUNT { (a)-[" + subject.inside + "]->() } AS c, EXISTS { (a)-[" + subject.inside + "]->() } AS e ORDER BY c DESC"
						result, err := executor.Execute(ctx, query, nil)
						require.NoError(t, err)
						var total int64
						var present int
						for _, row := range result.Rows {
							total += row[0].(int64)
							if row[1].(bool) {
								present++
							}
						}
						require.EqualValues(t, subject.count, total)
						require.EqualValues(t, subject.count, present)
					})
					t.Run("AST_metadata", func(t *testing.T) {
						ast, err := NewASTBuilder().Build("MATCH " + pattern + " RETURN b")
						require.NoError(t, err)
						require.NotEmpty(t, ast.Clauses)
						require.NotNil(t, ast.Clauses[0].Match)
						require.Len(t, ast.Clauses[0].Match.Patterns, 1)
						relationships := ast.Clauses[0].Match.Patterns[0].Relationships
						require.Len(t, relationships, 1)
						require.Equal(t, subject.expected, relationships[0].Type)
					})
				})
			}
		})
	}
}
