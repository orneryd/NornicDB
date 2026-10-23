package cypher

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/statementfactstest"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCanonicalStatementPublicMatrix(t *testing.T) {
	require.Len(t, statementfactstest.Cases(), 148)
	ctx := context.Background()
	var reference neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reference.Close(ctx)) })
	}
	for _, normalized := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalization=%v", normalized), func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			config.SetCypherQueryNormalizationEnabled(normalized)
			for _, sample := range statementfactstest.Cases() {
				t.Run(sample.Name, func(t *testing.T) {
					executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j"))
					columns, err := executor.StatementResultColumns(sample.Query)
					if sample.Code != "" {
						require.Error(t, err)
						requireStatusCode(t, err, sample.Code)
					} else {
						require.NoError(t, err)
						require.Equal(t, sample.Columns, columns)
					}
					for _, explicit := range []bool{false, true} {
						for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
							t.Run(fmt.Sprintf("explicit=%v/mode=%s", explicit, prefix), func(t *testing.T) {
								if explicit {
									_, err := executor.Execute(ctx, "BEGIN", nil)
									require.NoError(t, err)
									defer executor.Execute(ctx, "ROLLBACK", nil)
								}
								result, err := executor.Execute(ctx, prefix+sample.Query, nil)
								if sample.Code != "" {
									require.Error(t, err)
									requireStatusCode(t, err, sample.Code)
								} else {
									require.NoError(t, err)
									require.Equal(t, sample.Columns, result.Columns)
								}
							})
						}
					}
					if reference != nil {
						result, err := neo4j.ExecuteQuery(ctx, reference, "EXPLAIN "+sample.Query, nil, neo4j.EagerResultTransformer)
						if sample.Code != "" {
							require.Error(t, err)
							status, ok := err.(*neo4j.Neo4jError)
							require.True(t, ok, "%v", err)
							require.Equal(t, sample.Code, status.Code)
						} else {
							require.NoError(t, err)
							require.Equal(t, sample.Columns, result.Keys)
						}
					}
				})
			}
		})
	}
}

func TestCanonicalBoundIdentities(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "bound-identities"))
	query := "WITH 1 AS `écho` WITH `écho` + 1 AS `écho` RETURN [echo IN [1] | echo] AS local, `écho` AS result"
	program := executor.statementProjectionProgram(query, resultmetadata.KnownScope(nil), false)
	require.True(t, program.complete, "%v", program.err)
	require.NotNil(t, program.bound)
	ids := map[statementBindingID]statementBinding{}
	for _, binding := range program.bound.bindings {
		ids[binding.id] = binding
	}
	var prior statementBindingID
	for _, use := range program.bound.uses {
		require.NotZero(t, use.binding)
		definition, found := ids[use.binding]
		require.True(t, found)
		require.Equal(t, use.name, definition.name)
		require.GreaterOrEqual(t, use.start, 0)
		require.LessOrEqual(t, use.end, len(query))
		if use.name == "écho" {
			if prior != 0 {
				require.NotEqual(t, prior, use.binding, "a WITH definition shadows the previous value")
			}
			prior = use.binding
		}
	}
	require.NotZero(t, prior)
}
