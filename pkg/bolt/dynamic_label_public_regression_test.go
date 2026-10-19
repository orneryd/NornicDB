package bolt

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/dynamiclabels"
	"github.com/stretchr/testify/require"
)

func TestDynamicLabelPublicBolt(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "dynamic_public")
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	var reference neo4j.DriverWithContext
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reference.Close(ctx)) })
	}
	for _, normalization := range []string{"true", "false"} {
		t.Run("normalization="+normalization, func(t *testing.T) {
			previous := config.IsCypherQueryNormalizationEnabled()
			config.SetCypherQueryNormalizationEnabled(normalization == "true")
			t.Cleanup(func() { config.SetCypherQueryNormalizationEnabled(previous) })
			for _, explicit := range []bool{false, true} {
				for _, c := range dynamiclabels.Cases() {
					t.Run(fmt.Sprintf("tx=%t/%s", explicit, c.Name), func(t *testing.T) {
						check := func(d neo4j.DriverWithContext, database string) {
							session := d.NewSession(ctx, neo4j.SessionConfig{DatabaseName:database})
							defer session.Close(ctx)
							run := func(ctx context.Context, query string, params map[string]any) (neo4j.ResultWithContext, error) { return session.Run(ctx, query, params) }
							if explicit {
								tx, err := session.BeginTransaction(ctx)
								require.NoError(t, err)
								defer tx.Close(ctx)
								run = tx.Run
							}
							for _, query := range []string{"MATCH (n) DETACH DELETE n", dynamiclabels.Fixture} {
								result, err := run(ctx, query, nil)
								if !require.NoError(t, err) { return }
								_, err = result.Consume(ctx)
								if !require.NoError(t, err) { return }
							}
							result, err := run(ctx, c.Query, c.Params)
							var rows [][]any
							if err == nil {
								var records []*neo4j.Record
								records, err = result.Collect(ctx)
								rows = make([][]any, len(records))
								for i, record := range records { rows[i] = record.Values }
							}
							if c.Code != "" {
								var status *neo4j.Neo4jError
								require.True(t, errors.As(err, &status), "%v", err)
								require.Equal(t, c.Code, status.Code)
								return
							}
							if require.NoError(t, err, c.Query) { require.Equal(t, c.Rows, rows, c.Query) }
						}
						if reference != nil && !c.Cypher25 { t.Run("neo4j-5.26", func(t *testing.T) { check(reference, "neo4j") }) }
						check(driver, "dynamic_public")
					})
				}
			}
		})
	}
}
