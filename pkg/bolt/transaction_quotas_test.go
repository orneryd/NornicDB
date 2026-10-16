package bolt

import (
	"context"
	"fmt"
	"testing"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltTransactionQuotas(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	manager, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close(); _ = base.Close() })
	require.NoError(t, manager.CreateDatabase("quota"))
	require.NoError(t, manager.SetDatabaseLimits("quota", &multidb.Limits{Storage: multidb.StorageLimits{MaxNodes: 1}}))
	server := NewWithDatabaseManager(&Config{Port: 0, ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, manager)
	port := startBoltTestServer(t, server)
	driver, err := neo4jdriver.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "quota"})
			defer func() { _ = session.Close(ctx) }()
			query := "UNWIND [1, 2] AS id CREATE (:Quota {id: id})"
			if explicit {
				tx, err := session.BeginTransaction(ctx)
				require.NoError(t, err)
				defer func() { _ = tx.Close(ctx) }()
				result, err := tx.Run(ctx, query, nil)
				require.NoError(t, err)
				_, err = result.Consume(ctx)
				require.NoError(t, err)
				err = tx.Commit(ctx)
				require.ErrorContains(t, err, "max_nodes")
			} else {
				result, err := session.Run(ctx, query, nil)
				if err == nil {
					_, err = result.Consume(ctx)
				}
				require.ErrorContains(t, err, "max_nodes")
			}
			engine, err := manager.GetStorage("quota")
			require.NoError(t, err)
			count, err := engine.NodeCount()
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}
