package qdrantgrpc

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-30. Close-phase check of the Qdrant
// accepted-work consumer: a collection delete through the gRPC service and the
// DROP owner, with engine Close before, during (the accepted catalog command
// held in flight) and after the delete. Every phase returns, an acknowledged
// delete is durable, and the reopened collection is either intact or absent
// and creatable again.
func TestCollectionDeleteClosePhases(t *testing.T) {
	for _, phase := range []string{"before", "during", "after"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			open := func() *storage.BadgerEngine {
				t.Helper()
				native, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, LowMemory: true}))
				require.NoError(t, err)
				return native
			}
			hold := storagefault.NewCloseHold(open(), "system")
			t.Cleanup(hold.Release)
			dbm, err := multidb.NewDatabaseManager(t.Context(), hold, nil)
			require.NoError(t, err)
			collections, err := NewDatabaseCollectionStore(dbm, nil)
			require.NoError(t, err)
			service := NewCollectionsService(collections, nil)
			require.NoError(t, collections.Create(ctx, "c", 4, qpb.Distance_Cosine))
			remove := func(out chan<- error) {
				_, err := service.Delete(ctx, &qpb.DeleteCollection{CollectionName: "c"})
				out <- err
			}

			deleted := make(chan error, 1)
			closed := make(chan error, 1)
			var deleteErr error
			switch phase {
			case "before":
				require.NoError(t, dbm.Close())
				go remove(deleted)
				require.Error(t, qdrantClosePhaseWithin(t, "delete on a closed owner", deleted))
			case "during":
				entered := hold.Arm()
				go remove(deleted)
				qdrantClosePhaseWithin(t, "the held catalog command", entered)
				go func() { closed <- dbm.Close() }()
				qdrantClosePhaseWithin(t, "native Close", hold.CloseEntered())
				hold.Release()
				require.NoError(t, qdrantClosePhaseWithin(t, "Close with a delete in flight", closed))
				deleteErr = qdrantClosePhaseWithin(t, "delete across Close", deleted)
			case "after":
				go remove(deleted)
				require.NoError(t, qdrantClosePhaseWithin(t, "delete", deleted))
				go func() { closed <- dbm.Close() }()
				require.NoError(t, qdrantClosePhaseWithin(t, "Close after a delete", closed))
			}

			again, err := multidb.NewDatabaseManager(t.Context(), open(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = again.Close() })
			reopened, err := NewDatabaseCollectionStore(again, nil)
			require.NoError(t, err)
			if again.Exists("c") {
				require.NotEqual(t, "after", phase, "an acknowledged delete is durable")
				require.False(t, phase == "during" && deleteErr == nil, "a delete acknowledged across Close is durable")
				return
			}
			require.NotEqual(t, "before", phase, "a refused delete removed nothing")
			require.NoError(t, reopened.Create(ctx, "c", 4, qpb.Distance_Cosine), "a finished delete leaves the name reusable")
		})
	}
}

func qdrantClosePhaseWithin[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not return", what)
	}
	var zero T
	return zero
}
