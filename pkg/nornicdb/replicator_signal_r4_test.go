package nornicdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/stretchr/testify/require"
)

func TestDBR4SignalsReplicatorBeforeJoiningItsConsumer(t *testing.T) {
	for key, value := range map[string]string{
		"NORNICDB_CLUSTER_MODE":                  "raft",
		"NORNICDB_CLUSTER_NODE_ID":               "r4-db-stop",
		"NORNICDB_CLUSTER_BIND_ADDR":             "127.0.0.1:0",
		"NORNICDB_CLUSTER_RAFT_BOOTSTRAP":        "false",
		"NORNICDB_CLUSTER_RAFT_PEERS":            "127.0.0.1:1",
		"NORNICDB_CLUSTER_RAFT_ELECTION_TIMEOUT": "1h",
		"NORNICDB_CLUSTER_TLS_ENABLED":           "false",
	} {
		t.Setenv(key, value)
	}
	cfg := DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	native := db.GetReplicator()
	require.NotNil(t, native)
	provider, ok := native.(interface{ LifetimeContext() context.Context })
	require.True(t, ok, "the native owner must expose its lifetime to its real consumers")
	entered, waited := make(chan struct{}), make(chan error, 1)
	require.True(t, db.RunBackgroundTask(func() {
		close(entered)
		waited <- native.WaitForLeader(provider.LifetimeContext())
	}))
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Error("DB.Close joined a native consumer before signaling the replicator it waits on")
		// Controlled old-product escape; the real DB retains/joins the worker.
		require.NoError(t, native.Shutdown())
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("DB.Close did not finish after the native owner was stopped")
		}
	}
	select {
	case err := <-waited:
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, replication.ErrClosed), "wait outcome: %v", err)
	case <-time.After(time.Second):
		t.Fatal("native consumer completion was lost")
	}
	require.ErrorIs(t, provider.LifetimeContext().Err(), context.Canceled)
}
