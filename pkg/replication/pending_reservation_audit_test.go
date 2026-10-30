package replication

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

func TestPendingReplicationRetainsByteReservation(t *testing.T) {
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer base.Close()
	peer := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	defer peer.Close()
	adapter, err := NewStorageAdapterWithWAL(base, t.TempDir())
	require.NoError(t, err)
	defer adapter.Close()
	remote, err := NewStorageAdapterWithWAL(peer, t.TempDir())
	require.NoError(t, err)
	defer remote.Close()
	local := NewStandaloneReplicator(DefaultConfig(), adapter)
	require.NoError(t, local.Start(context.Background()))
	defer local.Shutdown()
	route := &managedRouteReplicator{Replicator: local, peer: remote}
	manager, err := multidb.NewDatabaseManager(t.Context(), NewReplicatedEngine(base, route, time.Second), nil)
	require.NoError(t, err)
	defer manager.Close()
	const db = "pending_quota"
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: db}))
	view, err := manager.GetStorage(db)
	require.NoError(t, err)
	at := time.Unix(1700000000, 0)
	for _, id := range []storage.NodeID{"n", "m"} {
		_, err = view.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, CreatedAt: at, UpdatedAt: at})
		require.NoError(t, err)
	}
	prior, err := storage.GetStoredNode(view, "n")
	require.NoError(t, err)
	growth := storage.CopyNode(prior)
	growth.Properties = map[string]any{"payload": strings.Repeat("x", 4096)}
	before, _, _ := manager.GetStorageSize(db)
	require.NoError(t, view.UpdateNode(storage.NodeUpdateRequest{After: growth}))
	ceiling, _, _ := manager.GetStorageSize(db)
	require.Greater(t, ceiling, before)
	require.NoError(t, view.UpdateNode(storage.NodeUpdateRequest{After: prior}))
	restored, _, _ := manager.GetStorageSize(db)
	require.Equal(t, before, restored)
	require.NoError(t, manager.SetDatabaseLimits(t.Context(), db, &multidb.Limits{Storage: multidb.StorageLimits{MaxBytes: ceiling}}))
	route.delay = true
	err = view.UpdateNode(storage.NodeUpdateRequest{After: growth})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	pending := route.pending
	require.NotNil(t, pending)
	route.delay = false
	other := storage.CopyNode(growth)
	other.ID = "m"
	otherErr := view.UpdateNode(storage.NodeUpdateRequest{After: other})
	assert.ErrorIs(t, otherErr, multidb.ErrStorageLimitExceeded, "the pending command already reserved this only growth allowance")
	require.NoError(t, route.applyPending(adapter, pending))
	recount, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	exact, _, _ := recount.GetStorageSize(db)
	assert.LessOrEqual(t, exact, ceiling, "a late commit and a competing admitted write must not exceed the byte quota")
}
