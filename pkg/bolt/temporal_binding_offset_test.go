package bolt

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/temporaloffset"
	"github.com/stretchr/testify/require"
)

func TestTemporalBindingBoltTemporalOffsetMutationRoutes(t *testing.T) {
	badger, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badger.Close()) })
	store := storage.NewNamespacedEngine(badger, "temporal_binding")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, driver.Close(ctx)) })
	t.Run("opaque map binding", func(t *testing.T) {
		session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "temporal_binding"})
		defer session.Close(ctx)
		point := neo4j.Point2D{SpatialRefId: 7203, X: 1.25, Y: 2.5}
		r, err := session.Run(ctx, "MERGE (n:Opaque) SET n += $m RETURN n.v", map[string]any{"m": map[string]any{"v": point}})
		require.NoError(t, err)
		records, err := r.Collect(ctx)
		require.NoError(t, err)
		require.Equal(t, point, records[0].Values[0])
	})
	london, err := time.LoadLocation("Europe/London")
	require.NoError(t, err)
	for _, nanos := range []int64{0, 123456789} {
		instant := time.Unix(1792888200, nanos).In(london)
		text := instant.Format(time.RFC3339Nano) + "[Europe/London]"
		for _, explicit := range []bool{false, true} {
			for _, c := range temporaloffset.Cases(text) {
				t.Run(fmt.Sprintf("nanos=%d/tx=%t/%s", nanos, explicit, c.Name), func(t *testing.T) {
					session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "temporal_binding", AccessMode: neo4j.AccessModeWrite})
					defer session.Close(ctx)
					params := map[string]any{"t": instant, "m": map[string]any{"t": instant, "ts": []any{instant}}}
					run := func(ctx context.Context, query string, params map[string]any) (neo4j.ResultWithContext, error) {
						return session.Run(ctx, query, params)
					}
					var tx neo4j.ExplicitTransaction
					if explicit {
						tx, err = session.BeginTransaction(ctx)
						require.NoError(t, err)
						defer tx.Close(ctx)
						run = tx.Run
					}
					result, err := run(ctx, "MATCH (n) DETACH DELETE n", nil)
					require.NoError(t, err)
					_, err = result.Consume(ctx)
					require.NoError(t, err)
					if c.Setup != "" {
						result, err = run(ctx, c.Setup, nil)
						require.NoError(t, err)
						_, err = result.Consume(ctx)
						require.NoError(t, err)
					}
					check := func(result neo4j.ResultWithContext, err error) {
						t.Helper()
						require.NoError(t, err)
						records, err := result.Collect(ctx)
						require.NoError(t, err)
						require.Len(t, records, 1)
						require.Equal(t, []any{int64(1792888200), text, int64(1792888200), text}, records[0].Values)
					}
					check(run(ctx, c.Query, params))
					if explicit {
						require.NoError(t, tx.Commit(ctx))
					}
					check(session.Run(ctx, c.Read, nil))
				})
			}
		}
	}
}
