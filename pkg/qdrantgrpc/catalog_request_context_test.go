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
	require.Same(t, canceled, received, "the drop callback receives the exact operation context")
	require.True(t, store.Exists(collection), "canceled admission must preserve collection metadata")

	request, finish := context.WithCancel(t.Context())
	defer finish()
	require.NoError(t, store.Drop(request, collection))
	require.Same(t, request, received)
	require.False(t, store.dbManager.Exists(collection))
}
