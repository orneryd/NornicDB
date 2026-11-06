package qdrantgrpc

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-31. A Qdrant collection delete whose
// request is cancelled while the DROP owner purges the collection's data. The
// owner still finishes the drop, so the collection can be created again with
// no further delete and no restart.
func TestCollectionDeleteFinishesWhenRequestCancelledDuringPurge(t *testing.T) {
	native, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), LowMemory: true}))
	require.NoError(t, err)
	faults := &qdrantDropSyncFault{BadgerEngine: native}
	dbm, err := multidb.NewDatabaseManager(t.Context(), faults, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbm.Close() })
	collections, err := NewDatabaseCollectionStore(dbm, nil)
	require.NoError(t, err)
	service := NewCollectionsService(collections, nil)
	require.NoError(t, collections.Create(t.Context(), "c", 4, qpb.Distance_Cosine))

	syncsBefore := faults.syncCalls.Load()
	faults.purgeEntered = make(chan struct{}, 1)
	faults.purgePermit = make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := service.Delete(ctx, &qpb.DeleteCollection{CollectionName: "c"})
		result <- err
	}()
	select {
	case <-faults.purgeEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the collection delete did not reach its data purge")
	}
	cancel()
	close(faults.purgePermit)
	select {
	case <-result:
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled collection delete did not return")
	}
	// The owner syncs once after the intent and once after the final removal.
	require.Eventually(t, func() bool {
		return faults.syncCalls.Load() >= syncsBefore+2 && !dbm.Exists("c")
	}, 10*time.Second, 10*time.Millisecond, "the DROP owner finishes the accepted drop")
	require.NoError(t, collections.Create(t.Context(), "c", 4, qpb.Distance_Cosine), "the name is free without another delete")
}
