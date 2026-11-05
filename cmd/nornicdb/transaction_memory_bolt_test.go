package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

// Transaction memory limits over Bolt, through the production executor
// (#1343). Neo4j 5.26 (neo-ref) fails such a statement with
// Neo.TransientError.General.MemoryPoolOutOfMemoryError, GQL status 50N42,
// rolls its explicit transaction back and keeps the session usable.

const boltHeavyCollect = "UNWIND range(1, 200000) AS x RETURN size(collect(x)) AS n"

func startTransactionMemoryBolt(t *testing.T, configure func(*nornicdb.Config)) neo4j.DriverWithContext {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	configure(config)
	db, err := nornicdb.Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
		cypher.ConfigureTransactionMemory(0, cypher.TransactionMemoryLimits{})
	})
	server := bolt.New(&bolt.Config{Port: 0, MaxConnections: 32, ReadBufferSize: 8192, WriteBufferSize: 8192}, NewDBQueryExecutor(db))
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
	return driver
}

func requireBoltMemoryError(t *testing.T, err error, code, setting string) {
	t.Helper()
	var neoErr *neo4j.Neo4jError
	require.True(t, errors.As(err, &neoErr), "%v", err)
	require.Equal(t, code, neoErr.Code)
	require.True(t, strings.HasPrefix(neoErr.Msg, "The allocation of an extra "), neoErr.Msg)
	require.True(t, strings.HasSuffix(neoErr.Msg, ". "+setting+" threshold reached"), neoErr.Msg)
	require.Equal(t, "50N42", neoErr.GqlStatus)
}

func runBolt(ctx context.Context, session neo4j.SessionWithContext, query string) ([]*neo4j.Record, error) {
	result, err := session.Run(ctx, query, nil)
	if err != nil {
		return nil, err
	}
	return result.Collect(ctx)
}

func TestBoltTransactionMemoryLimit(t *testing.T) {
	driver := startTransactionMemoryBolt(t, func(config *nornicdb.Config) {
		config.Memory.TransactionMax = 1 << 20
	})
	ctx := context.Background()
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err := runBolt(ctx, session, boltHeavyCollect)
	requireBoltMemoryError(t, err, "Neo.ClientError.General.TransactionOutOfMemoryError", "db.memory.transaction.max")
	records, err := runBolt(ctx, session, "RETURN 1 AS x")
	require.NoError(t, err, "the session stays usable")
	require.Equal(t, int64(1), records[0].Values[0])

	// An explicit transaction over the limit is rolled back.
	tx, err := session.BeginTransaction(ctx)
	require.NoError(t, err)
	result, err := tx.Run(ctx, "CREATE (:BoltMemTx)", nil)
	require.NoError(t, err)
	_, err = result.Consume(ctx)
	require.NoError(t, err)
	result, err = tx.Run(ctx, boltHeavyCollect, nil)
	if err == nil {
		_, err = result.Collect(ctx)
	}
	requireBoltMemoryError(t, err, "Neo.ClientError.General.TransactionOutOfMemoryError", "db.memory.transaction.max")
	require.Error(t, tx.Commit(ctx), "the failed transaction can't commit")
	records, err = runBolt(ctx, session, "MATCH (n:BoltMemTx) RETURN count(n) AS c")
	require.NoError(t, err)
	require.Equal(t, int64(0), records[0].Values[0], "the failed transaction's write was rolled back")
}

func TestBoltTransactionMemoryPoolConcurrentAggregations(t *testing.T) {
	driver := startTransactionMemoryBolt(t, func(config *nornicdb.Config) {
		config.Memory.TransactionTotalMax = 1 << 40
	})
	ctx := context.Background()
	// One statement first: the driver initialises its connector lazily, and
	// concurrent first connections race inside it.
	warm := driver.NewSession(ctx, neo4j.SessionConfig{})
	_, err := runBolt(ctx, warm, "RETURN 1")
	require.NoError(t, err)
	// Size the pool from one statement's peak, so it holds about two.
	peak := measureBoltStatementPeak(t, warm, boltHeavyCollect)
	_ = warm.Close(ctx)
	cypher.ConfigureTransactionMemory(peak*5/2, cypher.TransactionMemoryLimits{})
	const heavy = 6
	succeeded, failed := 0, 0
	for round := 0; round < 20 && (succeeded == 0 || failed == 0); round++ {
		var wg sync.WaitGroup
		start := make(chan struct{})
		heavyErrors := make([]error, heavy)
		heavyValues := make([]interface{}, heavy)
		smallErrors := make([]error, heavy)
		for worker := 0; worker < heavy; worker++ {
			wg.Add(2)
			go func(worker int) {
				defer wg.Done()
				session := driver.NewSession(ctx, neo4j.SessionConfig{})
				defer session.Close(ctx)
				<-start
				records, err := runBolt(ctx, session, boltHeavyCollect+fmt.Sprintf(", %d AS tag", round*heavy+worker))
				heavyErrors[worker] = err
				if err == nil {
					heavyValues[worker] = records[0].Values[0]
				}
			}(worker)
			go func(worker int) {
				defer wg.Done()
				session := driver.NewSession(ctx, neo4j.SessionConfig{})
				defer session.Close(ctx)
				<-start
				_, smallErrors[worker] = runBolt(ctx, session, "UNWIND range(1, 100) AS x RETURN collect(x) AS xs")
			}(worker)
		}
		close(start)
		wg.Wait()
		for worker := 0; worker < heavy; worker++ {
			require.NoError(t, smallErrors[worker], "a small statement failed")
			if heavyErrors[worker] == nil {
				succeeded++
				require.Equal(t, int64(200000), heavyValues[worker])
				continue
			}
			failed++
			requireBoltMemoryError(t, heavyErrors[worker], "Neo.TransientError.General.MemoryPoolOutOfMemoryError", "dbms.memory.transaction.total.max")
		}
		global, _ := cypher.TransactionMemoryPoolUsage("nornic")
		require.Zero(t, global, "the pool holds no memory once the statements ended")
	}
	require.Positive(t, succeeded, "no heavy statement completed under the pool")
	require.Positive(t, failed, "the pool never refused a heavy statement")
	// The server is still up.
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	_, err = runBolt(ctx, session, "RETURN 1")
	require.NoError(t, err)
}

// measureBoltStatementPeak is the most the pools held while one statement
// ran alone (a limit must be configured, so it is tracked).
func measureBoltStatementPeak(t *testing.T, session neo4j.SessionWithContext, query string) int64 {
	t.Helper()
	var peak int64
	done := make(chan error, 1)
	go func() {
		_, err := runBolt(context.Background(), session, query)
		done <- err
	}()
	for {
		select {
		case err := <-done:
			require.NoError(t, err)
			if peak == 0 {
				// Too fast to sample: at least the collected list.
				peak = 4 << 20
			}
			return peak
		default:
			if used, _ := cypher.TransactionMemoryPoolUsage("nornic"); used > peak {
				peak = used
			}
		}
	}
}
