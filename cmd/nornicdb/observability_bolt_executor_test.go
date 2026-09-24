// Package main: NornicDB issue #563 defect 1 regression coverage for the
// last of the 4 construction sites review finding C3 flagged as untested —
// DBQueryExecutor.ConfigureDatabaseExecutor, the sole wiring point
// pkg/bolt/server.go's newDatabaseScopedCypherExecutor calls (via the
// databaseExecutorConfigurator interface) for every Bolt database-scoped
// executor, with or without an explicit database. Historically this
// function copied embedder/inference-manager from the base executor but
// never copied its logger/threshold, so every Bolt query logged to
// io.Discard regardless of NORNICDB_SLOW_QUERY_THRESHOLD.
package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

// TestConfigureDatabaseExecutor_InheritsLoggerAndSlowQueryThreshold proves
// that a *cypher.StorageExecutor built by protocol adapters (mirroring what
// pkg/bolt/server.go's newDatabaseScopedCypherExecutor does for every Bolt
// multi-database session) and then passed through
// DBQueryExecutor.ConfigureDatabaseExecutor emits a slow_query record on
// the base executor's configured logger/threshold. Verified by mutation:
// commenting out either SetLogger or SetSlowQueryThreshold in
// ConfigureDatabaseExecutor's base-executor block fails this test.
func TestConfigureDatabaseExecutor_InheritsLoggerAndSlowQueryThreshold(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nornicdb-bolt-executor-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false

	db, err := nornicdb.Open(tmpDir, config)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	baseExec := db.GetCypherExecutor()
	require.NotNil(t, baseExec)
	te := observability.NewTestEnv(t)
	te.CaptureRecords()
	baseExec.SetLogger(te.Logger)
	baseExec.SetSlowQueryThreshold(1 * time.Nanosecond)

	qe := NewDBQueryExecutor(db)

	// Mirrors pkg/bolt/server.go's newDatabaseScopedCypherExecutor: build a
	// fresh StorageExecutor for a database-scoped Bolt session, then hand it
	// to ConfigureDatabaseExecutor for production wiring.
	storageEngine := db.GetStorage()
	exec := cypher.NewStorageExecutor(storageEngine)
	qe.ConfigureDatabaseExecutor(exec, "nornic", storageEngine)

	_, err = exec.Execute(context.Background(), "RETURN 1", nil)
	require.NoError(t, err)

	var found bool
	for _, rec := range te.LoggedRecords() {
		if rec["event"] == "slow_query" {
			found = true
			break
		}
	}
	require.True(t, found,
		"ConfigureDatabaseExecutor's executor did not inherit the base executor's logger/threshold (NornicDB issue #563 defect 1)")
}
