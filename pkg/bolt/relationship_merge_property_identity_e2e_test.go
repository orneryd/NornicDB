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

func TestBoltExecuteWriteConcurrentRelationshipPropertyIdentityPersists(t *testing.T) {
	badger, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	base := storage.NewWALEngine(badger, wal)
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
	for _, query := range []string{
		"CREATE (:Repository {id: 'repository'})",
		"CREATE (:Package:PackageRegistryPackage {uid: 'package'})",
	} {
		result, runErr := setup.Run(ctx, query, nil)
		require.NoError(t, runErr)
		_, consumeErr := result.Consume(ctx)
		require.NoError(t, consumeErr)
	}
	require.NoError(t, setup.Close(ctx))

	writeIdentity := func(scopeID, evidenceSource, generationID string) error {
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{
			AccessMode:   neo4jdriver.AccessModeWrite,
			DatabaseName: "nornic",
		})
		defer func() {
			_ = session.Close(ctx)
		}()
		_, executeErr := session.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
			result, runErr := tx.Run(ctx, `UNWIND $rows AS row
MATCH (source:Repository {id: row.repository_id})
MATCH (target:Package {uid: row.package_id})
MERGE (source)-[rel:PUBLISHES {
  scope_id: row.scope_id,
  evidence_source: row.evidence_source
}]->(target)
SET rel.generation_id = row.generation_id,
    rel.evidence_kinds = row.evidence_kinds`, map[string]any{
				"rows": []map[string]any{{
					"repository_id":   "repository",
					"package_id":      "package",
					"scope_id":        scopeID,
					"evidence_source": evidenceSource,
					"generation_id":   generationID,
					"evidence_kinds":  []string{"ASSERTION"},
				}},
			})
			if runErr != nil {
				return nil, runErr
			}
			_, consumeErr := result.Consume(ctx)
			return nil, consumeErr
		})
		return executeErr
	}

	require.NoError(t, writeIdentity("scope-a", "source-a", "generation-a"))
	require.NoError(t, writeIdentity("scope-b", "source-b", "generation-b"))
	require.NoError(t, writeIdentity("scope-a", "source-a", "generation-a-retry"))
	readIdentities := func() [][]any {
		check := driver.NewSession(ctx, neo4jdriver.SessionConfig{
			AccessMode:   neo4jdriver.AccessModeRead,
			DatabaseName: "nornic",
		})
		defer func() {
			_ = check.Close(ctx)
		}()
		result, readErr := check.Run(ctx, `MATCH (:Repository {id: 'repository'})-[rel:PUBLISHES]->(:Package {uid: 'package'})
RETURN rel.scope_id, rel.evidence_source
ORDER BY rel.scope_id`, nil)
		require.NoError(t, readErr)
		records, collectErr := result.Collect(ctx)
		require.NoError(t, collectErr)
		rows := make([][]any, len(records))
		for i, record := range records {
			rows[i] = record.Values
		}
		return rows
	}
	require.Equal(t, [][]any{
		{"scope-a", "source-a"},
		{"scope-b", "source-b"},
	}, readIdentities(), "prime the shared per-database query cache before the peer commits")

	const concurrency = 8
	start := make(chan struct{})
	errs := make(chan error, concurrency)
	var ready sync.WaitGroup
	var writers sync.WaitGroup
	ready.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			ready.Done()
			<-start
			errs <- writeIdentity("scope-c", "source-c", "generation-concurrent")
		}()
	}
	ready.Wait()
	close(start)
	writers.Wait()
	close(errs)
	for writeErr := range errs {
		require.NoError(t, writeErr)
	}

	require.Equal(t, [][]any{
		{"scope-a", "source-a"},
		{"scope-b", "source-b"},
		{"scope-c", "source-c"},
	}, readIdentities(), "managed transaction commit must invalidate the shared read cache")
}

func TestBoltProcedureWriteCommitInvalidatesSharedReadCache(t *testing.T) {
	badger, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	wal, err := storage.NewWAL(t.TempDir(), nil)
	require.NoError(t, err)
	base := storage.NewWALEngine(badger, wal)
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.Close()
	})

	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 8}, &mockExecutor{}, mgr)
	port := startBoltTestServer(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	driver, err := neo4jdriver.NewDriverWithContext(
		fmt.Sprintf("bolt://127.0.0.1:%d", port),
		neo4jdriver.NoAuth(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = driver.Close(context.Background())
	})
	require.NoError(t, driver.VerifyConnectivity(ctx))

	readCount := func(query string) int64 {
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{
			AccessMode:   neo4jdriver.AccessModeRead,
			DatabaseName: "nornic",
		})
		defer func() {
			_ = session.Close(ctx)
		}()
		result, readErr := session.Run(ctx, query, nil)
		require.NoError(t, readErr)
		record, singleErr := result.Single(ctx)
		require.NoError(t, singleErr)
		count, ok := record.Values[0].(int64)
		require.True(t, ok)
		return count
	}
	const cachedRead = "MATCH (n:DynamicWrite {id: 'procedure'}) RETURN count(n)"
	require.Zero(t, readCount(cachedRead), "prime the shared per-database query cache")

	writeSession := driver.NewSession(ctx, neo4jdriver.SessionConfig{
		AccessMode:   neo4jdriver.AccessModeWrite,
		DatabaseName: "nornic",
	})
	_, err = writeSession.ExecuteWrite(ctx, func(tx neo4jdriver.ManagedTransaction) (any, error) {
		result, runErr := tx.Run(ctx, "CALL apoc.cypher.runMany($statements, $params)", map[string]any{
			"statements": `CREATE (:DynamicWrite {id:"procedure"})`,
			"params":     map[string]any{},
		})
		if runErr != nil {
			return nil, runErr
		}
		records, collectErr := result.Collect(ctx)
		if collectErr != nil {
			return nil, collectErr
		}
		for _, record := range records {
			if len(record.Values) < 2 {
				continue
			}
			if row, ok := record.Values[1].(map[string]any); ok {
				if message, ok := row["error"].(string); ok {
					return nil, fmt.Errorf("apoc.cypher.runMany inner write: %s", message)
				}
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
	require.NoError(t, writeSession.Close(ctx))

	require.Equal(t, int64(1), readCount("MATCH (n:DynamicWrite {id: 'procedure'}) RETURN count(n) AS total"), "procedure write must be durable")
	require.Equal(t, int64(1), readCount(cachedRead), "successful explicit commit must evict stale reads even when the outer CALL hides its write")
}
