package bolt

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	neo4jconfig "github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltDatabaseManagerExecuteWriteUniqueMerge_BadgerDefaultNamespaceWithLookupIndex(t *testing.T) {
	base, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.Close()
	})

	server := NewWithDatabaseManager(&Config{
		Port:            0,
		MaxConnections:  32,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, mgr)
	port := startBoltTestServer(t, server)

	ctx := context.Background()
	driver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://127.0.0.1:%d", port),
		neo4jdriver.NoAuth(),
		func(config *neo4jconfig.Config) {
			config.MaxTransactionRetryTime = 5 * time.Second
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = driver.Close(context.Background())
	})
	require.NoError(t, driver.VerifyConnectivity(ctx))

	setup := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: "nornic",
	})
	_, err = setup.Run(ctx,
		"CREATE INDEX nornicdb_terraform_resource_uid_lookup IF NOT EXISTS FOR (n:TerraformResource) ON (n.uid)",
		nil,
	)
	require.NoError(t, err)
	_, err = setup.Run(ctx,
		"CREATE CONSTRAINT terraform_resource_uid_unique IF NOT EXISTS FOR (n:TerraformResource) REQUIRE n.uid IS UNIQUE",
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, setup.Close(ctx))

	const concurrency = 8
	var wg sync.WaitGroup
	failures := make([]string, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			session := driver.NewSession(ctx, neo4jdriver.SessionConfig{
				AccessMode:   neo4jdriver.AccessModeWrite,
				DatabaseName: "nornic",
			})
			defer func() {
				_ = session.Close(ctx)
			}()
			_, execErr := session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
				result, runErr := tx.Run(ctx,
					`UNWIND $rows AS row
MERGE (r:TerraformResource {uid: row.uid})
SET r.name = row.name`,
					map[string]any{
						"rows": []map[string]any{
							{
								"uid":  "badger-dbmanager-same-uid",
								"name": fmt.Sprintf("session%d", idx),
							},
						},
					},
				)
				if runErr != nil {
					return nil, runErr
				}
				_, consumeErr := result.Consume(ctx)
				return nil, consumeErr
			})
			if execErr != nil {
				failures[idx] = execErr.Error()
			}
		}(i)
	}
	wg.Wait()

	for idx, msg := range failures {
		if msg != "" {
			t.Errorf("session %d failed: %s", idx, msg)
		}
	}

	check := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeRead,
		DatabaseName: "nornic",
	})
	defer func() {
		_ = check.Close(ctx)
	}()
	result, err := check.Run(ctx,
		"MATCH (r:TerraformResource {uid: $uid}) RETURN count(r)",
		map[string]any{"uid": "badger-dbmanager-same-uid"},
	)
	require.NoError(t, err)
	record, err := result.Single(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, record.Values[0])
}
