package nornicdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateNodeWithIDSupportsIdempotentImportCheck(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	created, err := db.CreateNodeWithID(ctx, "beir-abc", []string{"BEIRDocument"}, map[string]interface{}{"beir_id": "abc"})
	require.NoError(t, err)
	require.Equal(t, "beir-abc", created.ID)

	loaded, err := db.GetNode(ctx, "beir-abc")
	require.NoError(t, err)
	require.Equal(t, "abc", loaded.Properties["beir_id"])

	_, err = db.CreateNodeWithID(ctx, "beir-abc", []string{"BEIRDocument"}, nil)
	require.Error(t, err)
}

func TestWaitForEmbeddingsRequiresEnabledWorker(t *testing.T) {
	db := openTestDB(t)
	require.Error(t, db.WaitForEmbeddings(context.Background()))
}
