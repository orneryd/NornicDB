package multidb

import (
	"context"
	"errors"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-30. A managed write through the
// public entity API (no caller context) takes the applying engine's work
// context. While the engine is open the write applies; once the engine has
// closed, the write is refused by the engine's write decider as a closed
// store, not by the cancellation of the engine's own work context.
func TestManagedWriteTakesEngineLifetime(t *testing.T) {
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	manager, err := NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	const db = "managed_lifetime"
	require.NoError(t, manager.CreateDatabase(t.Context(), DatabaseCreateRequest{Name: db}))
	require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, &Limits{Storage: StorageLimits{MaxNodes: 100}}))
	engine, err := manager.GetStorage(db)
	require.NoError(t, err)

	_, err = engine.CreateNode(&storage.Node{ID: "open", Properties: map[string]any{"v": "applied"}})
	require.NoError(t, err, "an open engine applies the managed write")
	node, err := engine.GetNode("open")
	require.NoError(t, err)
	require.Equal(t, "applied", node.Properties["v"])

	require.NoError(t, base.Close())
	_, err = engine.CreateNode(&storage.Node{ID: "closed"})
	require.Error(t, err, "a closed engine refuses the managed write")
	require.ErrorIs(t, err, storage.ErrStorageClosed)
	require.False(t, errors.Is(err, context.Canceled), "the closed store, not its cancelled work context, refuses the write")
}
