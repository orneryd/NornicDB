// SPDX-License-Identifier: MIT
package storage_test

// protocolServerFixture is the one test-only owner of a real database, real
// Badger engine and real server for the protocol-level regressions in this
// package (the NOR136 acknowledged-write acceptance and the PR1655 commit-read
// protocol fixtures). It owns the directory, configuration, database, engine
// and server, so it alone performs startup, the native-engine unwrap, the
// fixture-owner tracking, close and reopen. Fault wiring and scenario seed
// statements stay with the tests that need them.

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// protocolServerOptions are the fixture's typed private knobs. They are the
// only values that differ between the protocol fixtures; the rest of the
// configuration below is one shared low-memory, default-decay, disabled
// embedding and disabled native-lifecycle setup.
type protocolServerOptions struct {
	// writeBehind enables the write-behind (async write) path.
	writeBehind bool
	// flushInterval pins the initial write-behind rotation delay; 0 keeps the
	// documented auto/adaptive behaviour (a supported production configuration
	// value, not a production change).
	flushInterval time.Duration
	// manualDrain stops the existing worker seam; explicit flushes still work.
	manualDrain bool
	// logger receives the engine and server records; nil discards them.
	logger *slog.Logger
}

// protocolServerFixture is one real server over a real Badger engine, with the
// directory and configuration retained so the same store can be closed and
// reopened.
type protocolServerFixture struct {
	dir         string
	config      *nornicdb.Config
	db          *nornicdb.DB
	engine      *storage.BadgerEngine
	server      *server.Server
	closed      bool
	manualDrain bool
}

// openProtocolServerFixture opens the fixture in a fresh temporary directory.
// The caller owns its lifetime; each start also registers its own close so the
// canonical fixture-owner audit (which runs first) always sees a closed engine.
func openProtocolServerFixture(t *testing.T, opts protocolServerOptions) *protocolServerFixture {
	t.Helper()
	f := &protocolServerFixture{dir: t.TempDir(), manualDrain: opts.manualDrain}
	f.config = nornicdb.DefaultConfig()
	f.config.Auth.Enabled = false
	f.config.Memory.DecayEnabled = false
	f.config.Memory.AutoLinksEnabled = false
	f.config.Memory.EmbeddingEnabled = false
	f.config.Database.AsyncWritesEnabled = opts.writeBehind
	f.config.Database.AsyncFlushInterval = opts.flushInterval
	f.config.Database.MVCCLifecycleEnabled = false
	f.config.Database.WALAutoCompactionEnabled = false
	f.config.Storage.Mode = "low"
	f.config.Logger = opts.logger
	if f.config.Logger == nil {
		f.config.Logger = slog.New(slog.DiscardHandler)
	}
	f.start(t)
	return f
}

// start opens the store and the server. It is called once at construction and
// again after close for the reopen check.
func (f *protocolServerFixture) start(t *testing.T) {
	t.Helper()
	database, err := nornicdb.Open(f.dir, f.config)
	if err != nil && database != nil {
		_ = database.Close()
	}
	require.NoError(t, err)
	base := storage.UnwrapEngine(database.GetBaseStorageForManager())
	native, ok := base.(*storage.BadgerEngine)
	if !ok {
		_ = database.Close()
		t.Fatalf("real Badger fixture required, got %T", base)
	}
	fixtureowner.Track(t, native, storage.ErrStorageClosed)
	if f.manualDrain && f.config.Database.AsyncWritesEnabled {
		storage.NOR140StopReplayWorkerForTest(native)
	}
	f.db, f.engine, f.server, f.closed = database, native, nil, false
	t.Cleanup(func() { f.close(t) })
	serverConfig := server.DefaultConfig()
	serverConfig.Port = 0
	serverConfig.Headless = true
	serverConfig.EmbeddingEnabled = false
	serverConfig.ProcessConfig = f.config
	srv, err := server.New(database, nil, serverConfig)
	if err != nil {
		_ = database.Close()
		require.NoError(t, err)
	}
	f.server = srv
	require.NoError(t, srv.Start())
}

// close stops the server and closes the store. It is idempotent so the reopen
// check and the test cleanup can both call it.
func (f *protocolServerFixture) close(t *testing.T) {
	if f == nil || f.closed {
		return
	}
	f.closed = true
	var stopErr, closeErr error
	if f.server != nil {
		stopErr = f.server.Stop(context.Background())
	}
	if f.db != nil {
		closeErr = f.db.Close()
	}
	require.NoError(t, errors.Join(stopErr, closeErr))
}

// seed writes the fixture graph through the production Cypher executor and makes
// it durable, so a writer's snapshot validates against a stored head.
func (f *protocolServerFixture) seed(t *testing.T, statements ...string) {
	t.Helper()
	store, err := f.server.GetDatabaseManager().GetStorage("nornic")
	require.NoError(t, err)
	exec := cypher.NewStorageExecutor(store)
	for _, statement := range statements {
		_, err := exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err, statement)
	}
	require.NoError(t, f.engine.FlushWriteBehind(context.Background()))
}
