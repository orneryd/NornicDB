package main

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// db.embedding.pause / resume / status over Bolt with authentication
// (#1021): an administrator pauses and resumes, in auto-commit and in an
// explicit transaction, on a user database and on system; a viewer is
// refused and changes nothing.
func TestEmbeddingPauseOverBolt(t *testing.T) {
	ctx := context.Background()
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = true
	cfg.Database.AsyncWritesEnabled = false
	db, err := nornicdb.Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	manager, err := multidb.NewDatabaseManager(db.GetBaseStorageForManager(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })

	authConfig := auth.DefaultAuthConfig()
	authConfig.SecurityEnabled = true
	authConfig.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	authenticator, err := auth.NewAuthenticator(authConfig, storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	require.NoError(t, err)
	_, err = authenticator.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)
	_, err = authenticator.CreateUser("viewer", "password123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)

	config := bolt.DefaultConfig()
	config.Authenticator = bolt.NewAuthenticatorAdapter(authenticator)
	config.RequireAuth = true
	server := bolt.NewWithDatabaseManager(config, NewDBQueryExecutor(db), manager)
	// Every user may use every database here: what is under test is the
	// procedures' own administrator requirement.
	server.SetDatabaseAccessMode(auth.FullDatabaseAccessMode)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		require.NoError(t, server.Close())
		require.NoError(t, <-served)
	})

	session := func(user, database string) neo4j.SessionWithContext {
		t.Helper()
		driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.BasicAuth(user, "password123", ""))
		require.NoError(t, err)
		t.Cleanup(func() { _ = driver.Close(ctx) })
		s := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: database})
		t.Cleanup(func() { _ = s.Close(ctx) })
		return s
	}
	state := func(s neo4j.SessionWithContext, statement string) bool {
		t.Helper()
		result, err := s.Run(ctx, statement, nil)
		require.NoError(t, err)
		record, err := result.Single(ctx)
		require.NoError(t, err)
		paused, _ := record.Get("paused")
		_, hasPending := record.Get("pending")
		require.True(t, hasPending)
		return paused.(bool)
	}

	admin := session("admin", "nornic")
	require.False(t, state(admin, "CALL db.embedding.status() YIELD paused, pending RETURN paused, pending"))
	require.True(t, state(admin, "CALL db.embedding.pause()"))
	require.True(t, state(admin, "CALL db.embedding.pause()"), "idempotent")
	require.True(t, db.EmbeddingStatus().Paused)
	require.True(t, db.GetEmbedQueue().Paused())

	viewer := session("viewer", "nornic")
	for _, statement := range []string{"CALL db.embedding.resume()", "CALL db.embedding.status()"} {
		result, err := viewer.Run(ctx, statement, nil)
		if err == nil {
			_, err = result.Consume(ctx)
		}
		var neoErr *neo4j.Neo4jError
		require.True(t, errors.As(err, &neoErr), "%s: %v", statement, err)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", neoErr.Code)
	}
	require.True(t, db.EmbeddingStatus().Paused, "a viewer cannot resume")

	system := session("admin", "system")
	require.True(t, state(system, "CALL db.embedding.status()"))
	tx, err := admin.BeginTransaction(ctx)
	require.NoError(t, err)
	result, err := tx.Run(ctx, "CALL db.embedding.resume()", nil)
	require.NoError(t, err)
	record, err := result.Single(ctx)
	require.NoError(t, err)
	paused, _ := record.Get("paused")
	require.Equal(t, false, paused)
	require.NoError(t, tx.Commit(ctx))
	require.False(t, db.EmbeddingStatus().Paused)
	require.False(t, db.GetEmbedQueue().Paused())
	require.False(t, state(system, "CALL db.embedding.status()"))
}
