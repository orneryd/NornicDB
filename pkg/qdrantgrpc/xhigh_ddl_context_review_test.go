package qdrantgrpc

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

func TestXHighCollectionLifecycleCancellation(t *testing.T) {
	for _, operation := range []string{"create", "drop"} {
		t.Run(operation, func(t *testing.T) {
			manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			collections, err := NewDatabaseCollectionStore(manager, nil)
			require.NoError(t, err)
			if operation == "drop" {
				require.NoError(t, collections.Create(context.Background(), "xhigh_collection", 4, qpb.Distance_Cosine))
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if operation == "drop" {
				err = collections.Drop(ctx, "xhigh_collection")
				if !collections.Exists("xhigh_collection") {
					t.Error("already-canceled DROP deleted the existing collection")
				}
			} else {
				err = collections.Create(ctx, "xhigh_collection", 4, qpb.Distance_Cosine)
				if collections.Exists("xhigh_collection") {
					t.Error("already-canceled CREATE published a new collection")
				}
			}
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}
