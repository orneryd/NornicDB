package qdrantgrpc

import (
	"context"
	"testing"

	qpb "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/require"
)

func TestCollectionCatalogPreservesRequestContext(t *testing.T) {
	store := newTestCollectionStore(t).(*databaseCollectionStore)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, store.Create(canceled, "canceled_catalog_create", 3, qpb.Distance_Cosine), context.Canceled)
	require.False(t, store.dbManager.Exists("canceled_catalog_create"))

	const collection = "catalog_context"
	require.NoError(t, store.Create(t.Context(), collection, 3, qpb.Distance_Cosine))
	var received context.Context
	originalDrop := store.dropDatabase
	store.dropDatabase = func(ctx context.Context, name string) error {
		received = ctx
		return originalDrop(ctx, name)
	}
	require.ErrorIs(t, store.Drop(canceled, collection), context.Canceled)
	require.Nil(t, received, "canceled metadata admission must not reach the drop callback")
	{
		exists, existsErr := store.Exists(t.Context(), collection)
		require.NoError(t, existsErr)
		require.True(t, exists, "canceled admission must preserve collection metadata")
	}

	request, finish := context.WithCancel(t.Context())
	defer finish()
	require.NoError(t, store.Drop(request, collection))
	require.Same(t, request, received)
	require.False(t, store.dbManager.Exists(collection))
}

func TestCollectionReadsPreserveCancellationInsteadOfMissing(t *testing.T) {
	store := newTestCollectionStore(t).(*databaseCollectionStore)
	const name = "canceled_collection_reads"
	require.NoError(t, store.Create(t.Context(), name, 3, qpb.Distance_Cosine))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	exists, err := store.Exists(ctx, name)
	require.False(t, exists)
	require.ErrorIs(t, err, context.Canceled)
	engine, meta, err := store.Open(ctx, name)
	require.Nil(t, engine)
	require.Nil(t, meta)
	require.ErrorIs(t, err, context.Canceled)
	meta, err = store.GetMeta(ctx, name)
	require.Nil(t, meta)
	require.ErrorIs(t, err, context.Canceled)
	names, err := store.List(ctx)
	require.Nil(t, names)
	require.ErrorIs(t, err, context.Canceled)
	service := NewCollectionsService(store, nil)
	existsResponse, rpcErr := service.CollectionExists(ctx, &qpb.CollectionExistsRequest{CollectionName: name})
	require.Nil(t, existsResponse)
	require.ErrorIs(t, rpcErr, context.Canceled)
	updateResponse, rpcErr := service.Update(ctx, &qpb.UpdateCollection{CollectionName: name})
	require.Nil(t, updateResponse)
	require.ErrorIs(t, rpcErr, context.Canceled)
	snapshots := NewSnapshotsService(&Config{}, store, nil, t.TempDir(), nil)
	created, rpcErr := snapshots.Create(ctx, &qpb.CreateSnapshotRequest{CollectionName: name})
	require.Nil(t, created)
	require.ErrorIs(t, rpcErr, context.Canceled)
	listed, rpcErr := snapshots.List(ctx, &qpb.ListSnapshotsRequest{CollectionName: name})
	require.Nil(t, listed)
	require.ErrorIs(t, rpcErr, context.Canceled)
	deleted, rpcErr := snapshots.Delete(ctx, &qpb.DeleteSnapshotRequest{CollectionName: name, SnapshotName: "snapshot"})
	require.Nil(t, deleted)
	require.ErrorIs(t, rpcErr, context.Canceled)
	existsResponse, rpcErr = service.CollectionExists(t.Context(), &qpb.CollectionExistsRequest{CollectionName: name})
	require.NoError(t, rpcErr)
	require.True(t, existsResponse.Result.Exists)

	exists, err = store.Exists(t.Context(), name)
	require.NoError(t, err)
	require.True(t, exists)
}
