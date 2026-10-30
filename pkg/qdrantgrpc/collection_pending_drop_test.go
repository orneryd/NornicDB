package qdrantgrpc

import (
	"context"
	"sync/atomic"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

// qdrantDropSyncFault fails one durable Sync of the native storage owner,
// which the DROP owner reaches after each catalog transition.
type qdrantDropSyncFault struct {
	*storage.BadgerEngine
	syncCalls atomic.Int32
	failSync  atomic.Int32
}

func (e *qdrantDropSyncFault) Sync() error {
	if e.syncCalls.Add(1) == e.failSync.Load() {
		return storage.ErrConflict
	}
	return e.BadgerEngine.Sync()
}

// Author: NOR-288 lane (Claude), 2026-10-30. A collection delete whose final
// metadata Sync fails leaves the name reserved by the DROP owner. A later
// public delete reaches the owner, which finishes that pending drop, and the
// name can be created again. A live non-collection database stays protected.
func TestCollectionDeleteFinishesPendingDrop(t *testing.T) {
	ctx := context.Background()
	native, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true}))
	require.NoError(t, err)
	faults := &qdrantDropSyncFault{BadgerEngine: native}
	dbm, err := multidb.NewDatabaseManager(t.Context(), faults, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbm.Close() })
	collections, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	service := NewCollectionsService(collections, nil)
	require.NoError(t, collections.Create(ctx, "c", 4, qpb.Distance_Cosine))
	require.NoError(t, dbm.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "plain"}))

	// DROP Syncs once after its intent and once after the final removal.
	faults.failSync.Store(faults.syncCalls.Load() + 2)
	_, err = service.Delete(ctx, &qpb.DeleteCollection{CollectionName: "c"})
	require.Error(t, err, "the accepted outcome is reported, not hidden")
	require.False(t, dbm.Exists("c"))
	require.ErrorIs(t, dbm.CreateDatabase(ctx, multidb.DatabaseCreateRequest{Name: "c"}), multidb.ErrDatabaseExists, "the pending name stays fenced")

	resp, err := service.Delete(ctx, &qpb.DeleteCollection{CollectionName: "c"})
	require.NoError(t, err, "a later delete reaches the DROP owner and finishes the pending drop")
	require.True(t, resp.Result)
	require.NoError(t, collections.Create(ctx, "c", 4, qpb.Distance_Cosine), "the fence cleared")

	_, err = service.Delete(ctx, &qpb.DeleteCollection{CollectionName: "plain"})
	require.Error(t, err, "a live database that is not a collection is never dropped by Qdrant")
	require.True(t, dbm.Exists("plain"))
	_, err = service.Delete(ctx, &qpb.DeleteCollection{CollectionName: "absent"})
	require.Error(t, err)
}
