package bolt

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/statementfactstest"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltCanonicalStatementMatrix(t *testing.T) {
	ctx := context.Background()
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "tenant"))
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	for _, normalized := range []bool{false, true} {
		t.Run(fmt.Sprintf("normalization=%v", normalized), func(t *testing.T) {
			restore := config.WithCypherQueryNormalizationDisabled()
			defer restore()
			config.SetCypherQueryNormalizationEnabled(normalized)
			for _, sample := range statementfactstest.Cases() {
				for _, explicit := range []bool{false, true} {
					for _, prefix := range []string{"", "EXPLAIN ", "PROFILE "} {
						t.Run(fmt.Sprintf("%s/explicit=%v/mode=%s", sample.Name, explicit, prefix), func(t *testing.T) {
							session := driver.NewSession(ctx, neo4j.SessionConfig{})
							defer session.Close(ctx)
							var result neo4j.ResultWithContext
							var failure error
							if explicit {
								tx, err := session.BeginTransaction(ctx)
								require.NoError(t, err)
								defer tx.Rollback(ctx)
								result, failure = tx.Run(ctx, prefix+sample.Query, nil)
							} else {
								result, failure = session.Run(ctx, prefix+sample.Query, nil)
							}
							var columns []string
							if failure == nil {
								columns, failure = result.Keys()
							}
							if failure == nil {
								_, failure = result.Collect(ctx)
							}
							if sample.Code != "" {
								require.Error(t, failure)
								var status *neo4j.Neo4jError
								require.True(t, errors.As(failure, &status), "%v", failure)
								require.Equal(t, sample.Code, status.Code)
							} else {
								require.NoError(t, failure)
								require.Equal(t, sample.Columns, columns)
							}
						})
					}
				}
			}
		})
	}
}
