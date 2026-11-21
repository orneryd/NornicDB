package bolt

// Batch 2 earlier-issue probe (L506) on the final build (research only, untracked).

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestZZEI2BoltSuppressedLabelScan(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, pending := range []bool{false, true} {
			if pending && !explicit {
				continue
			}
			t.Run(fmt.Sprintf("explicit=%t/pending=%t", explicit, pending), func(t *testing.T) {
				ctx := t.Context()
				engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
				_, port := startBoltIntegrationServer(t, storage.NewNamespacedEngine(engine, "nornic"), boltIntegrationExplicitTx)
				driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
				require.NoError(t, err)
				defer driver.Close(ctx)
				peer := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
				defer peer.Close(ctx)
				peerRun := nor160Run(func(ctx context.Context, query string, params map[string]interface{}) (neo4j.ResultWithContext, error) {
					return peer.Run(ctx, query, params)
				})
				nor160Collect(t, ctx, peerRun, "CREATE (:ZZHidden {id:1})", nil)
				session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
				defer session.Close(ctx)
				run := nor160Run(func(ctx context.Context, query string, params map[string]interface{}) (neo4j.ResultWithContext, error) {
					return session.Run(ctx, query, params)
				})
				if explicit {
					tx, err := session.BeginTransaction(ctx)
					require.NoError(t, err)
					defer tx.Close(ctx)
					run = tx.Run
				}
				if pending {
					nor160Collect(t, ctx, run, "MATCH (n:ZZHidden) SET n.touched = true", nil)
				}
				require.Len(t, nor160Collect(t, ctx, run, "MATCH (n:ZZHidden) RETURN n.id", nil), 1)
				nor160Collect(t, ctx, peerRun, "MATCH (n:ZZHidden) CALL kp.suppress(n)", nil)
				control := nor160Collect(t, ctx, peerRun, "MATCH (n:ZZHidden) RETURN n.id", nil)
				rows := nor160Collect(t, ctx, run, "MATCH (n:ZZHidden) RETURN n.id", nil)
				t.Logf("ZZEI2 L506 Bolt explicit=%t pending=%t: fresh autocommit rows=%d, same-transaction rescan rows=%d", explicit, pending, len(control), len(rows))
				require.Empty(t, control, "fresh autocommit control")
				require.Empty(t, rows, "live suppression must also apply to the transaction label scan")
			})
		}
	}
}
