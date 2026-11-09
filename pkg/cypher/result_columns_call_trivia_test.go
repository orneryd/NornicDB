package cypher

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// CALL classification, structure and scope must consume the same token frame.
// Comments on either side of an import list are whitespace with normalization
// on or off, and a procedure's later brace must never become its CALL body.
func TestStatementFactsCallFrameTrivia(t *testing.T) {
	ctx := context.Background()
	var reference neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reference.Close(ctx)) })
	}
	for _, normalized := range []bool{false, true} {
		t.Run(fmt.Sprint("normalization=", normalized), func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			config.SetCypherQueryNormalizationEnabled(normalized)
			for _, variable := range []string{"x", "__fabric_import_0"} {
				for _, frame := range []struct {
					call, importing string
				}{
					{"CALL /* scope */ (/* import */ " + variable + ") ", ""},
					{"CALL (" + variable + ") /* body */ ", ""},
					{"CALL // scope\n (" + variable + ") ", ""},
					{"CALL (" + variable + ") // body\n ", ""},
					{"CALL /* scope */ (*) ", ""},
					{"CALL /* body */ ", "WITH " + variable + " "},
				} {
					query := "UNWIND [1,2] AS " + variable + " " + frame.call + "{ " + frame.importing + "RETURN " + variable + " + 1 AS v } RETURN v ORDER BY v"
					t.Run(query, func(t *testing.T) {
						executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call-frame-trivia"))
						for _, explicit := range []bool{false, true} {
							if explicit {
								_, err := executor.Execute(ctx, "BEGIN", nil)
								require.NoError(t, err)
								defer func() { _, err := executor.Execute(ctx, "ROLLBACK", nil); require.NoError(t, err) }()
							}
							for _, prefix := range []string{"", "PROFILE ", "EXPLAIN "} {
								result, err := executor.Execute(ctx, prefix+query, nil)
								require.NoError(t, err)
								require.Equal(t, []string{"v"}, result.Columns)
								if prefix != "EXPLAIN " {
									require.Equal(t, [][]interface{}{{int64(2)}, {int64(3)}}, result.Rows)
								}
								if reference != nil {
									want, err := neo4j.ExecuteQuery(ctx, reference, prefix+query, nil, neo4j.EagerResultTransformer)
									require.NoError(t, err)
									require.Equal(t, want.Keys, result.Columns)
									require.Len(t, result.Rows, len(want.Records))
								}
							}
						}
						require.Equal(t, []string{"v"}, executor.StatementColumns(query))
						composite := "USE comp " + strings.Replace(query, "{ "+frame.importing, "{ "+frame.importing+"USE comp.remote ", 1)
						executor.StatementFacts(composite, resultmetadata.KnownScope(nil), false, RawSource).Visit(func(statement resultmetadata.Statement) {
							var call *resultmetadata.Call
							for _, clause := range statement.Clauses {
								if clause.Call != nil {
									call = clause.Call
								}
							}
							require.NotNil(t, call)
							require.Equal(t, []string{variable}, call.Imports)
						}, func() { t.Fatal("owner has no continuation") }, func(reason resultmetadata.Unresolved) { t.Fatal(reason.Err) })
					})
				}
			}
		})
	}
}
