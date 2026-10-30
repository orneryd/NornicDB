package bolt

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPR1612BoltCommentAdmission(t *testing.T) {
	ctx := context.Background()
	restore := config.WithCypherQueryNormalizationDisabled()
	defer restore()
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(engine, "tenant"))
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	for _, normalize := range []bool{false, true} {
		config.SetCypherQueryNormalizationEnabled(normalize)
		for _, explicit := range []bool{false, true} {
			for _, tc := range []struct{ query, code string }{
				{"CREATE (:R1612Invalid) /* incomplete", "Neo.ClientError.Statement.SyntaxError"},
				{"/* incomplete", "Neo.DatabaseError.Statement.ExecutionFailed"},
				{"PROFILE /* closed */", "Neo.ClientError.Statement.SyntaxError"},
				{"PROFILE WITH 1 AS x FINISH", ""},
			} {
				t.Run(fmt.Sprintf("normalize=%v/explicit=%v/%s", normalize, explicit, tc.query), func(t *testing.T) {
					session := driver.NewSession(ctx, neo4j.SessionConfig{})
					defer session.Close(ctx)
					run := func(ctx context.Context, query string, params map[string]any) (neo4j.ResultWithContext, error) {
						return session.Run(ctx, query, params)
					}
					if explicit {
						tx, err := session.BeginTransaction(ctx)
						require.NoError(t, err)
						defer tx.Rollback(ctx)
						run = tx.Run
					}
					result, err := run(ctx, tc.query, nil)
					if err == nil {
						_, err = result.Consume(ctx)
					}
					if tc.code == "" {
						require.NoError(t, err)
						keys, err := result.Keys()
						require.NoError(t, err)
						require.Empty(t, keys)
						return
					}
					var status *neo4j.Neo4jError
					require.ErrorAs(t, err, &status)
					require.Equal(t, tc.code, status.Code)
				})
			}
		}
	}
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, "MATCH (n:R1612Invalid) RETURN count(n)", nil)
	require.NoError(t, err)
	record, err := result.Single(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), record.Values[0])
}
