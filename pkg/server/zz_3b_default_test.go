package server

// Batch-3b probe (research only, untracked; same text on both builds): the
// 60,000-node HTTP DELETE on a server whose database configuration is
// nornicdb.DefaultConfig() unchanged (unlike the shared server fixture, which
// sets Memory.DecayEnabled=false, Memory.AutoLinksEnabled=false,
// Memory.SearchCursorMax=128, Database.AsyncWritesEnabled=false and
// Database.MVCCRetentionMaxVersions=100). Server: DefaultConfig with Port 0
// and EmbeddingEnabled=false (no embedder in the lab); authentication on, as
// the HTTP route needs a token. Same sampling as TestZZ3BServerDeleteHolder.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func zz3bDefaultServer(tb testing.TB) (*Server, *auth.Authenticator) {
	tmpDir, err := os.MkdirTemp("", "zz3b-default-*")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { os.RemoveAll(tmpDir) })
	config := nornicdb.DefaultConfig()
	tb.Logf("ZZ3B DEFAULT database config: Database.MVCCRetentionMaxVersions=%d Database.AsyncWritesEnabled=%v Memory.DecayEnabled=%v Memory.AutoLinksEnabled=%v Memory.SearchCursorMax=%d",
		config.Database.MVCCRetentionMaxVersions, config.Database.AsyncWritesEnabled, config.Memory.DecayEnabled, config.Memory.AutoLinksEnabled, config.Memory.SearchCursorMax)
	tb.Logf("ZZ3B DEFAULT full database config: %+v", *config)
	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: true, JWTSecret: []byte("test-secret-key-for-testing-only-32b")},
		storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	if err != nil {
		tb.Fatal(err)
	}
	if err := zz3bCreateAdmin(authenticator); err != nil {
		tb.Fatal(err)
	}
	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	tb.Logf("ZZ3B DEFAULT server config: %+v", *serverConfig)
	srv, err := New(db, authenticator, serverConfig)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	})
	return srv, authenticator
}

func TestZZ3BDefaultServerDelete(t *testing.T) {
	runs := 20
	if os.Getenv("ZZ3B_RUNS") != "" {
		fmt.Sscan(os.Getenv("ZZ3B_RUNS"), &runs)
	}
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "10m")
	srv, a := zz3bDefaultServer(t)
	zz3bHolderRunsOn(t, srv, a, runs, t.Logf)
}
