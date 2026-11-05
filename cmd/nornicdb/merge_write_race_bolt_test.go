package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// MERGE statements that write the node they match, racing over Bolt through
// the production executor (NOR-117). In Neo4j 5.26 every statement succeeds
// and one node exists: a later writer waits for the earlier one. Before,
// the later commit failed with a UNIQUE violation.

var boltMergeWriteShapes = []string{
	"MERGE (n:Doc {id: $k}) ON CREATE SET n.c = 1 ON MATCH SET n.m = $w",
	"MERGE (n:Doc {id: $k}) SET n.w = $w",
	"MERGE (n:Doc {id: $k}) SET n += {w: $w}",
	"UNWIND [$k] AS k MERGE (n:Doc {id: k}) ON MATCH SET n.m = $w",
	"MERGE (n:Doc {id: $k})",
}

func startMergeRaceBolt(t *testing.T, async bool) neo4j.DriverWithContext {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = async
	db, err := nornicdb.Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	server := bolt.New(&bolt.Config{Port: 0, MaxConnections: 64, ReadBufferSize: 8192, WriteBufferSize: 8192}, NewDBQueryExecutor(db))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		<-served
	})
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://%s", listener.Addr()), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	_, err = runBolt(context.Background(), driver.NewSession(context.Background(), neo4j.SessionConfig{}), "CREATE CONSTRAINT doc_id IF NOT EXISTS FOR (n:Doc) REQUIRE n.id IS UNIQUE")
	require.NoError(t, err)
	return driver
}

// runBoltMerge runs query in a session of its own, auto-commit or in an
// explicit transaction, without the driver's retries.
func runBoltMerge(ctx context.Context, driver neo4j.DriverWithContext, explicit bool, query string, params map[string]any) error {
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	if !explicit {
		result, err := session.Run(ctx, query, params)
		if err == nil {
			_, err = result.Consume(ctx)
		}
		return err
	}
	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return err
	}
	result, err := tx.Run(ctx, query, params)
	if err == nil {
		_, err = result.Consume(ctx)
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func boltRows(t *testing.T, driver neo4j.DriverWithContext, query string, params map[string]any) []any {
	t.Helper()
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	result, err := session.Run(ctx, query, params)
	require.NoError(t, err)
	record, err := result.Single(ctx)
	require.NoError(t, err)
	return record.Values
}

// TestBoltMergeWritersOfOneNodeSerialize: the second statement starts while
// the first one's transaction holds the node; the key wait, not timing,
// orders them.
func TestBoltMergeWritersOfOneNodeSerialize(t *testing.T) {
	driver := startMergeRaceBolt(t, false)
	waits := make(chan struct{}, 64)
	t.Cleanup(storage.SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waits <- struct{}{}:
		default:
		}
	}))
	ctx := context.Background()
	for _, query := range boltMergeWriteShapes[:2] {
		for _, explicit := range []bool{false, true} {
			key := fmt.Sprintf("%s/%v", query, explicit)
			boltRows(t, driver, "CREATE (n:Doc {id: $k}) RETURN count(n)", map[string]any{"k": key})
			for len(waits) > 0 {
				<-waits
			}
			session := driver.NewSession(ctx, neo4j.SessionConfig{})
			first, err := session.BeginTransaction(ctx)
			require.NoError(t, err)
			result, err := first.Run(ctx, query, map[string]any{"k": key, "w": int64(1)})
			require.NoError(t, err)
			_, err = result.Consume(ctx)
			require.NoError(t, err)
			done := make(chan error, 1)
			go func() { done <- runBoltMerge(ctx, driver, explicit, query, map[string]any{"k": key, "w": int64(2)}) }()
			var secondErr error
			waited := false
			select {
			case <-waits:
				waited = true
			case secondErr = <-done:
			}
			require.NoError(t, first.Commit(ctx), key)
			if waited {
				secondErr = <-done
			}
			_ = session.Close(ctx)
			require.NoError(t, secondErr, key)
			require.True(t, waited, key)
			require.Equal(t, []any{int64(1)}, boltRows(t, driver, "MATCH (n:Doc {id: $k}) RETURN count(n)", map[string]any{"k": key}), key)
		}
	}
}

// raceBoltMerges runs every shape from several sessions at once, auto-commit
// and in explicit transactions, and returns the failures and node counts
// that aren't one.
func raceBoltMerges(t *testing.T, driver neo4j.DriverWithContext, prefix string) []string {
	t.Helper()
	ctx := context.Background()
	var failures []string
	var mu sync.Mutex
	for s, query := range boltMergeWriteShapes {
		for _, explicit := range []bool{false, true} {
			for round := 0; round < 8; round++ {
				key := fmt.Sprintf("%s-%d-%v-%d", prefix, s, explicit, round)
				var start, done sync.WaitGroup
				start.Add(1)
				for w := 0; w < 6; w++ {
					done.Add(1)
					go func() {
						defer done.Done()
						start.Wait()
						if err := runBoltMerge(ctx, driver, explicit, query, map[string]any{"k": key, "w": int64(w)}); err != nil {
							mu.Lock()
							failures = append(failures, fmt.Sprintf("%s explicit=%v: %v", query, explicit, err))
							mu.Unlock()
						}
					}()
				}
				start.Done()
				done.Wait()
				if count := boltRows(t, driver, "MATCH (n:Doc {id: $k}) RETURN count(n)", map[string]any{"k": key})[0]; count != int64(1) {
					failures = append(failures, fmt.Sprintf("%s explicit=%v: %v nodes", query, explicit, count))
				}
			}
		}
	}
	return failures
}

func TestBoltConcurrentMergeWritersConverge(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%v", async), func(t *testing.T) {
			require.Empty(t, raceBoltMerges(t, startMergeRaceBolt(t, async), "nornic"))
		})
	}
	// The same race on the Neo4j reference, when one is configured.
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		t.Run("neo4j", func(t *testing.T) {
			reference, err := neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
			require.NoError(t, err)
			ctx := context.Background()
			defer reference.Close(ctx)
			prefix := fmt.Sprintf("nor117-%d", time.Now().UnixNano())
			_, err = runBolt(ctx, reference.NewSession(ctx, neo4j.SessionConfig{}), "CREATE CONSTRAINT doc_id IF NOT EXISTS FOR (n:Doc) REQUIRE n.id IS UNIQUE")
			require.NoError(t, err)
			failures := raceBoltMerges(t, reference, prefix)
			boltRows(t, reference, "MATCH (n:Doc) WHERE n.id STARTS WITH $p DELETE n RETURN count(*)", map[string]any{"p": prefix})
			require.Empty(t, failures)
		})
	}
}
