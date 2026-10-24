package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSourceColumnAgreementRejectsBeforeWrites(t *testing.T) {
	ctx := context.Background()
	query := "CREATE (:SourceColumnEffect) RETURN 1 /* column */ + 2 UNION ALL CREATE (:SourceColumnEffect) RETURN 1 + 2"
	if reference := fabricCallRowsReference(t); reference != nil {
		_, err := neo4j.ExecuteQuery(ctx, reference, query, nil, neo4j.EagerResultTransformer)
		require.Error(t, err)
		status, ok := err.(*neo4j.Neo4jError)
		require.True(t, ok)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", status.Code)
	}
	for _, normalized := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			for _, mode := range []string{"", "EXPLAIN ", "PROFILE "} {
				t.Run(fmt.Sprintf("normalization=%v/explicit=%v/mode=%q", normalized, explicit, mode), func(t *testing.T) {
					restore := config.WithCypherQueryNormalizationDisabled()
					defer restore()
					config.SetCypherQueryNormalizationEnabled(normalized)
					store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "neo4j")
					executor := NewStorageExecutor(store)
					if explicit {
						_, err := executor.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer executor.Execute(ctx, "ROLLBACK", nil)
					}
					_, executionErr := executor.Execute(ctx, mode+query, nil)
					nodes, err := store.GetNodesByLabel("SourceColumnEffect")
					require.NoError(t, err)
					t.Logf("source-column agreement: executionError=%v, visibleNodes=%d", executionErr, len(nodes))
					require.Empty(t, nodes, "column agreement is resolved before graph effects")
					require.Error(t, executionErr)
					requireStatusCode(t, executionErr, "Neo.ClientError.Statement.SyntaxError")
				})
			}
		}
	}
}
