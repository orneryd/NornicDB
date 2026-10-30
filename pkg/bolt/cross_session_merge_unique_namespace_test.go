package bolt

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	neo4jconfig "github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestBoltCrossSessionMergeUniqueConflict_EshuDefaultNamespaceExplicitTx pins
// the explicit transaction shape Eshu uses with DEFAULT_DATABASE=nornic.
// Retried MERGE attempts must observe a peer transaction's committed UNIQUE
// value through the nornic schema cache, not repeatedly choose CREATE and
// surface commit-time UNIQUE violations.
func TestBoltCrossSessionMergeUniqueConflict_EshuDefaultNamespaceExplicitTx(t *testing.T) {
	baseStore := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() {
		_ = baseStore.Close()
	})
	store := storage.NewNamespacedEngine(baseStore, "nornic")
	_, port := startBoltIntegrationServerWithExplicitTx(t, store)

	setup := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, setup,
		"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE")
	require.NoError(t, setup.Close())

	const concurrency = 8
	var wg sync.WaitGroup
	failures := make([]string, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := runBoltExplicitMergeTx(t, port,
				"TerraformResource", "uid", "eshu-same-uid",
				fmt.Sprintf("session%d", idx))
			if err != nil {
				failures[idx] = err.Error()
			}
		}(i)
	}
	wg.Wait()

	for idx, msg := range failures {
		if msg != "" {
			t.Errorf("session %d failed: %s", idx, msg)
		}
	}

	check := openBoltTestConn(t, port)
	records := runBoltQueryAndCollectRecords(t, check,
		"MATCH (r:TerraformResource {uid: 'eshu-same-uid'}) RETURN count(r)")
	if len(records) != 1 || len(records[0]) < 1 {
		t.Fatalf("expected one count row, got %v", records)
	}
	count, ok := records[0][0].(int64)
	if !ok {
		t.Fatalf("expected int64 count, got %T (value=%v)", records[0][0], records[0][0])
	}
	if count != 1 {
		t.Errorf("expected exactly one TerraformResource node, got %d", count)
	}
}

// The default-namespace population exercises the
// real database-manager Bolt path used by Eshu's NornicDB deployment. A
// commit-time UNIQUE conflict must become a successful retry because the next
// ExecuteWrite attempt can MATCH the committed nornic-scoped node.
type uniqueMergeTestBackend uint8

const (
	uniqueMergeTestMemory uniqueMergeTestBackend = iota
	uniqueMergeTestBadger
	uniqueMergeTestWAL
)

type uniqueMergeTestInput uint8

const (
	uniqueMergeTestDirect uniqueMergeTestInput = iota
	uniqueMergeTestUnwindRows
)

// Every backend keeps its original schema/input population while sharing the
// actual concurrent driver/manager commit-and-read lifecycle.
func TestBoltDatabaseManagerExecuteWriteUniqueMerge_BackendPopulations(t *testing.T) {
	const constraint = "CREATE CONSTRAINT terraform_resource_uid_unique IF NOT EXISTS FOR (n:TerraformResource) REQUIRE n.uid IS UNIQUE"
	const lookup = "CREATE INDEX nornicdb_terraform_resource_uid_lookup IF NOT EXISTS FOR (n:TerraformResource) ON (n.uid)"
	cases := []struct {
		name          string
		backend       uniqueMergeTestBackend
		input         uniqueMergeTestInput
		uid           string
		schemaQueries []string
	}{
		{"memory default", uniqueMergeTestMemory, uniqueMergeTestDirect, "dbmanager-same-uid", []string{"CREATE CONSTRAINT tr_uid IF NOT EXISTS FOR (r:TerraformResource) REQUIRE r.uid IS UNIQUE"}},
		{"badger lookup", uniqueMergeTestBadger, uniqueMergeTestUnwindRows, "badger-dbmanager-same-uid", []string{constraint, lookup}},
		{"WAL lookup", uniqueMergeTestWAL, uniqueMergeTestUnwindRows, "prod-stack-same-uid", []string{constraint, lookup}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var base storage.Engine
			switch tc.backend {
			case uniqueMergeTestMemory:
				base = fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			case uniqueMergeTestBadger, uniqueMergeTestWAL:
				badger, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
				if badger != nil {
					fixtureowner.Own(t, badger)
				}
				require.NoError(t, err)
				base = badger
				if tc.backend == uniqueMergeTestWAL {
					wal, err := storage.NewWAL(t.TempDir(), nil)
					require.NoError(t, err)
					base = storage.NewWALEngine(badger, wal)
				}
			}
			mgr, err := multidb.NewDatabaseManager(t.Context(), base, nil)
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
			// Constraint precedes optional lookup-index DDL, preserving the original no-op order (#884).
			for _, query := range tc.schemaQueries {
				_, err = setup.Run(ctx, query, nil)
				require.NoError(t, err)
			}
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
						params := map[string]any{"uid": tc.uid, "name": fmt.Sprintf("session%d", idx)}
						query := "MERGE (r:TerraformResource {uid: $uid}) SET r.name = $name"
						if tc.input == uniqueMergeTestUnwindRows {
							query = `UNWIND $rows AS row
MERGE (r:TerraformResource {uid: row.uid})
SET r.name = row.name`
							params = map[string]any{"rows": []map[string]any{{"uid": tc.uid, "name": fmt.Sprintf("session%d", idx)}}}
						}
						result, runErr := tx.Run(ctx, query, params)

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
				map[string]any{"uid": tc.uid},
			)
			require.NoError(t, err)
			record, err := result.Single(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 1, record.Values[0])

		})
	}
}

func TestBoltDatabaseManagerMergeExistingNodeAfterLateSchemaBootstrap(t *testing.T) {
	badger, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	if badger != nil {
		fixtureowner.Own(t, badger)
	}
	require.NoError(t, err)
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	base := storage.NewWALEngine(badger, wal)
	mgr, err := multidb.NewDatabaseManager(t.Context(), base, nil)
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
		"CREATE (r:TerraformResource {uid: $uid, name: 'before'})",
		map[string]any{"uid": "late-schema-existing"},
	)
	require.NoError(t, err)
	// The constraint first: its own index serves the uid seeks, and the index
	// statement after it is a no-op, as in Neo4j. The other order fails with
	// IndexAlreadyExists (#884).
	_, err = setup.Run(ctx,
		"CREATE CONSTRAINT terraform_resource_uid_unique IF NOT EXISTS FOR (n:TerraformResource) REQUIRE n.uid IS UNIQUE",
		nil,
	)
	require.NoError(t, err)
	_, err = setup.Run(ctx,
		"CREATE INDEX nornicdb_terraform_resource_uid_lookup IF NOT EXISTS FOR (n:TerraformResource) ON (n.uid)",
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, setup.Close(ctx))
	schema := base.GetSchemaForNamespace("nornic")
	nodeID, valueFound, constraintExists, cacheComplete := schema.LookupUniqueConstraintValue("TerraformResource", "uid", "late-schema-existing")
	require.True(t, constraintExists, "expected TerraformResource.uid constraint to exist")
	require.True(t, cacheComplete, "expected TerraformResource.uid cache to be complete after DDL backfill")
	require.True(t, valueFound, "expected TerraformResource.uid cache to contain the preexisting node")
	require.NotEmpty(t, nodeID, "expected unique cache to point at the preexisting node")
	require.Regexp(t, "^nornic:", string(nodeID), "expected schema backfill to register storage-prefixed node IDs")
	require.NotEmpty(t, schema.PropertyIndexLookup("TerraformResource", "uid", "late-schema-existing"), "expected lookup index to contain the preexisting node")

	session := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: "nornic",
	})
	_, err = session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, runErr := tx.Run(ctx,
			`UNWIND $rows AS row
MERGE (r:TerraformResource {uid: row.uid})
SET r.name = row.name`,
			map[string]any{
				"rows": []map[string]any{
					{"uid": "late-schema-existing", "name": "after"},
				},
			},
		)
		if runErr != nil {
			return nil, runErr
		}
		_, consumeErr := result.Consume(ctx)
		return nil, consumeErr
	})
	require.NoError(t, err)
	require.NoError(t, session.Close(ctx))

	check := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeRead,
		DatabaseName: "nornic",
	})
	defer func() {
		_ = check.Close(ctx)
	}()
	result, err := check.Run(ctx,
		"MATCH (r:TerraformResource {uid: $uid}) RETURN r.name, count(r)",
		map[string]any{"uid": "late-schema-existing"},
	)
	require.NoError(t, err)
	record, err := result.Single(ctx)
	require.NoError(t, err)
	require.Equal(t, "after", record.Values[0])
	require.EqualValues(t, 1, record.Values[1])
}
