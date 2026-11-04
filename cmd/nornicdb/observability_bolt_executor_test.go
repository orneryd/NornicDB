// Package main: NornicDB issue #563 defect 1 regression coverage for the
// database-scoped executors Bolt serves.
//
// Bolt no longer configures its own database-scoped executors: runServe wires
// (*server.Server).DatabaseScopedExecutorOptions into
// bolt.Config.DatabaseExecutorOptions, so both protocols build them from one
// host factory (query-result cache policy, statement-cache owner, settings,
// logger, search and mutation wiring). Historically
// DBQueryExecutor.ConfigureDatabaseExecutor was Bolt's own wiring point; it
// copied embedder/inference-manager from the base executor but never its
// logger/threshold, so every Bolt query logged to io.Discard regardless of
// NORNICDB_SLOW_QUERY_THRESHOLD.
//
// These tests construct their executors exactly as Bolt's session path does
// now — through the host factory — so the regression they guard stays on the
// wiring production actually uses.
package main

import (
	"context"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newBoltExecutorHost builds the host side runServe wires to Bolt: the HTTP
// server whose DatabaseScopedExecutorOptions is the canonical per-database
// executor factory, plus the database manager Bolt shares with it. Bolt's
// session path calls that factory once per database-scoped executor
// construction (pkg/bolt/server.go Session.databaseScopedExecutorOptions).
func newBoltExecutorHost(t *testing.T, db *nornicdb.DB) *server.Server {
	t.Helper()
	authStorage := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = authStorage.Close() })
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{}, authStorage)
	require.NoError(t, err)
	httpConfig := server.DefaultConfig()
	// Never starts a listener: the tests only need the host's executor
	// configuration, exactly as Bolt obtains it.
	httpConfig.Port = 0
	// Keep the HTTP server's own external-embedder bootstrap out of these
	// tests; the embedder under test is the DB's.
	httpConfig.EmbeddingEnabled = false
	httpConfig.MCPEnabled = false
	httpServer, err := server.New(db, authenticator, httpConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Stop(ctx)
	})
	return httpServer
}

func TestProductionExecutorsShareQueryStatistics(t *testing.T) {
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	db, err := nornicdb.Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	base := db.GetCypherExecutor()
	_, err = base.Execute(ctx, "CALL db.stats.collect('QUERIES')", nil)
	require.NoError(t, err)
	// Mirrors pkg/bolt/server.go's database-scoped construction: the executor
	// comes from the host factory, so it inherits the base executor's query
	// statistics the same way every Bolt session does.
	httpServer := newBoltExecutorHost(t, db)
	protocol, err := cypher.NewDatabaseScopedExecutor(httpServer.DatabaseScopedExecutorOptions("nornic", ""))
	require.NoError(t, err)
	_, err = protocol.Execute(ctx, "RETURN 530 AS sharedValue", nil)
	require.NoError(t, err)
	transaction, err := newTxScopedExecutor(db, "nornic")
	require.NoError(t, err)
	result, err := transaction.Execute(ctx, "CALL db.stats.retrieve('QUERIES')", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, "RETURN 530 AS sharedValue", result.Rows[0][1].(map[string]interface{})["query"])
}

// TestDatabaseScopedBoltExecutor_InheritsLoggerAndSlowQueryThreshold proves
// that a database-scoped executor built the way pkg/bolt/server.go builds one
// for every Bolt session (through the host factory that runServe wires into
// bolt.Config.DatabaseExecutorOptions) emits a slow_query record on the base
// executor's configured logger/threshold.
func TestDatabaseScopedBoltExecutor_InheritsLoggerAndSlowQueryThreshold(t *testing.T) {
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false

	db, err := nornicdb.Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	baseExec := db.GetCypherExecutor()
	require.NotNil(t, baseExec)
	te := observability.NewTestEnv(t)
	te.CaptureRecords()
	baseExec.SetLogger(te.Logger)
	baseExec.SetSlowQueryThreshold(1 * time.Nanosecond)

	// Mirrors pkg/bolt/server.go's per-database session executor: the host
	// factory configures the fresh StorageExecutor before it is published.
	httpServer := newBoltExecutorHost(t, db)
	exec, err := cypher.NewDatabaseScopedExecutor(httpServer.DatabaseScopedExecutorOptions("nornic", ""))
	require.NoError(t, err)

	// A trivial RETURN can complete within one Windows clock tick and report
	// zero elapsed time, below even this 1 ns threshold. Exercise real query
	// work so this wiring test actually reaches the slow-query emission gate.
	result, err := exec.Execute(context.Background(), testutil.SlowQueryWorkload, nil)
	require.NoError(t, err)
	require.EqualValues(t, 50005000, result.Rows[0][0])

	var found bool
	for _, rec := range te.LoggedRecords() {
		if rec["event"] == "slow_query" {
			found = true
			break
		}
	}
	require.True(t, found,
		"the Bolt database-scoped executor did not inherit the base executor's logger/threshold (NornicDB issue #563 defect 1)")
}
