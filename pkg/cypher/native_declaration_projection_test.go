package cypher

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestDeclarationNativePublicMatrix(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	ctx := context.Background()
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		exec := newFabricCallRowsExecutor(t)
		_, err := exec.Execute(ctx, "CREATE (:DeclaredValue)", nil)
		require.NoError(t, err)
		for _, sample := range []struct{ syntax, name string }{
			{"plain", "plain"}, {"`quoted`", "quoted"}, {"`name,part`", "name,part"}, {"`name``part`", "name`part"}, {"```edge```", "`edge`"},
		} {
			body := "CALL db.labels() YIELD label AS " + sample.syntax + " RETURN *"
			for _, entry := range []struct {
				name, query string
				outer       bool
			}{
				{"CALL", body, false},
				{"WITH", "WITH 7 AS value " + body, true},
				{"relative", "WITH 7 AS value CALL () { " + body + " } RETURN *", true},
			} {
				t.Run(fmt.Sprintf("normalize_%v/%s/%s", normalize, entry.name, sample.name), func(t *testing.T) {
					metadata := entry.query
					if entry.name == "relative" {
						metadata = body
					}
					for repeat := 0; repeat < 2; repeat++ {
						require.Equal(t, []string{sample.name}, exec.StatementColumns(metadata))
						result, err := exec.Execute(ctx, entry.query, nil)
						require.NoError(t, err, entry.query)
						explain, err := exec.Execute(ctx, "EXPLAIN "+entry.query, nil)
						require.NoError(t, err, entry.query)
						require.Equal(t, result.Columns, explain.Columns)
						require.Empty(t, explain.Rows)
						require.Len(t, result.Rows, 1)
						column := slices.Index(result.Columns, sample.name)
						require.GreaterOrEqual(t, column, 0)
						require.Equal(t, "DeclaredValue", result.Rows[0][column])
						if entry.outer {
							require.Len(t, result.Columns, 2)
							valueColumn := slices.Index(result.Columns, "value")
							require.GreaterOrEqual(t, valueColumn, 0)
							require.Equal(t, int64(7), result.Rows[0][valueColumn])
						} else {
							require.Equal(t, []string{sample.name}, result.Columns)
						}
					}
				})
			}
			for _, query := range []string{
				"CALL db.labels() YIELD label AS " + sample.syntax,
				"RETURN 7 AS " + sample.syntax,
				"WITH 7 AS " + sample.syntax + " RETURN " + sample.syntax,
				"CALL db.labels() YIELD label AS " + sample.syntax + " RETURN " + sample.syntax + " UNION ALL RETURN 'other' AS " + sample.syntax,
			} {
				for repeat := 0; repeat < 2; repeat++ {
					require.Equal(t, []string{sample.name}, exec.StatementColumns(query), query)
					result, err := exec.Execute(ctx, query, nil)
					require.NoError(t, err, query)
					require.Equal(t, []string{sample.name}, result.Columns, query)
					explain, err := exec.Execute(ctx, "EXPLAIN "+query, nil)
					require.NoError(t, err, query)
					require.Equal(t, result.Columns, explain.Columns, query)
				}
			}
			for _, query := range []string{
				"UNWIND [7] AS " + sample.syntax + " RETURN " + sample.syntax,
				"CYPHER 25 FOR " + sample.syntax + " IN [7] RETURN " + sample.syntax,
				"CYPHER 25 LET " + sample.syntax + " = 7 RETURN " + sample.syntax,
			} {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Equal(t, []string{sample.name}, result.Columns, query)
				require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows, query)
			}
		}
		_, err = exec.Execute(ctx, "CALL db.labels() YIELD label ORDER BY label WHERE label <> '' RETURN label", nil)
		require.Error(t, err)
	}
}

func TestDeclarationNativeRelativeRewriters(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:DeclaredRelative)", nil)
	require.NoError(t, err)
	for _, sample := range []struct{ syntax, name string }{
		{"plain", "plain"}, {"`quoted`", "quoted"}, {"`name,part`", "name,part"}, {"`name``part`", "name`part"},
	} {
		body := "CALL db.labels() YIELD label AS " + sample.syntax + " RETURN *"
		for _, query := range []string{
			"CYPHER 25 WHEN true THEN { " + body + " } ELSE { " + body + " }",
			"CYPHER 25 { " + body + " UNION " + body + " } UNION ALL " + body,
		} {
			for repeat := 0; repeat < 2; repeat++ {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Equal(t, []string{sample.name}, result.Columns, query)
				require.NotEmpty(t, result.Rows, query)
				explain, err := exec.Execute(ctx, "EXPLAIN "+query, nil)
				require.NoError(t, err, query)
				require.Equal(t, result.Columns, explain.Columns, query)
			}
		}
	}
}

func TestDeclarationNativeASTCallProjection(t *testing.T) {
	builder := NewASTBuilder()
	for _, sample := range []struct{ syntax, name string }{
		{"plain", "plain"}, {"`quoted`", "quoted"}, {"`name,part`", "name,part"}, {"`name``part`", "name`part"}, {"```edge```", "`edge`"},
	} {
		call := builder.parseCall("CALL db.labels() YIELD label /* AS ignored, other */ AS " + sample.syntax)
		require.Equal(t, []string{sample.name}, call.Yield)
		require.Equal(t, "db.labels", call.Procedure)
		require.False(t, isPlainVariableName("`"+sample.name+"`"))
	}
}

// Native declarations are usable only after complete-statement admission.
// This also holds on empty input, after cached valid executions, and when the
// API requests only metadata. Relative declaration readers themselves never
// initiate admission.
func TestDeclarationNativeInvalidSyntaxAdmission(t *testing.T) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	ctx := context.Background()
	queries := []string{
		"WITH 7 AS value CALL db.labels() YIELD label ORDER BY label WHERE label <> '' RETURN *",
		"UNWIND [] AS value CALL db.labels() YIELD label AS `name,part` ORDER BY `name,part` WHERE true RETURN *",
		"WITH 7 AS value RETURN value AS `unterminated",
	}
	if driver := fabricCallRowsReference(t); driver != nil {
		for _, query := range queries {
			_, err := neo4j.ExecuteQuery(ctx, driver, query, nil, neo4j.EagerResultTransformer)
			var status *neo4j.Neo4jError
			require.ErrorAs(t, err, &status, query)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", status.Code, query)
		}
	}
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, composite := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				for index, query := range queries {
					for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
						t.Run(fmt.Sprintf("normalize_%v/composite_%v/transaction_%v/query_%d/%s", normalize, composite, explicit, index, mode), func(t *testing.T) {
							exec := newFabricCallRowsExecutor(t)
							statement := query
							if composite {
								exec = fabricCallRowsCompositeExecutor(t, exec)
								statement = "USE comp.one " + statement
							}
							for repeat := 0; repeat < 2; repeat++ {
								_, err := exec.Execute(ctx, "RETURN 7 AS cached", nil)
								require.NoError(t, err)
								if explicit {
									_, err = exec.Execute(ctx, "BEGIN", nil)
									require.NoError(t, err)
								}
								_, err = exec.Execute(ctx, mode+statement, nil)
								require.Error(t, err, query)
								require.Contains(t, statusText(err), "Neo.ClientError.Statement.SyntaxError", query)
								if explicit {
									_, err = exec.Execute(ctx, "ROLLBACK", nil)
									require.NoError(t, err)
								}
							}
						})
					}
				}
			}
		}
	}
}
