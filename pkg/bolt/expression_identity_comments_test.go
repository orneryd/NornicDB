package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/testutil/expressionidentitymatrix"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpressionIdentityGeneratedBolt(t *testing.T) {
	runExpressionIdentityBolt(t, expressionidentitymatrix.Cases())
}

func runExpressionIdentityBolt(t *testing.T, cases []expressionidentitymatrix.Case) {
	prior := config.IsCypherQueryNormalizationEnabled()
	t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(prior) })
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(base, "neo4j"))
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	for _, normalize := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("normalize=%t/explicit=%t", normalize, explicit), func(t *testing.T) {
				config.SetCypherQueryNormalizationEnabled(normalize)
				for _, sample := range cases {
					func() {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
						defer session.Close(ctx)
						var result neo4j.ResultWithContext
						var queryErr error
						if explicit {
							tx, err := session.BeginTransaction(ctx)
							require.NoError(t, err)
							defer tx.Rollback(ctx)
							result, queryErr = tx.Run(ctx, sample.Query, nil)
						} else {
							result, queryErr = session.Run(ctx, sample.Query, nil)
						}
						rows := make([][]interface{}, 0)
						if queryErr == nil {
							for result.Next(ctx) {
								rows = append(rows, result.Record().Values)
							}
							queryErr = result.Err()
						}
						if assert.NoError(t, queryErr, "%s: %s", sample.ID, sample.Query) {
							encoded, err := json.Marshal(rows)
							require.NoError(t, err)
							assert.JSONEq(t, sample.Rows, string(encoded), sample.ID)
						}
					}()
				}
			})
		}
	}
}
