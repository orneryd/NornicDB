package replication_test

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReplicatedDeleteInvalidatesWarmLocalQuery(t *testing.T) {
	base, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	defer base.Close()
	_, err = base.CreateNode(&storage.Node{ID: "library:item", Labels: []string{"Document"}})
	require.NoError(t, err)
	adapter, err := replication.NewStorageAdapter(base)
	require.NoError(t, err)
	view := storage.NewNamespacedEngine(replication.NewReplicatedEngine(base, nil, 0), "library")
	executor := cypher.NewStorageExecutorWithQueryCachePolicy(view, 10, time.Minute)
	read := func(want int) {
		t.Helper()
		result, executeErr := executor.Execute(context.Background(), "MATCH (n) WHERE elementId(n)=$id RETURN n", map[string]interface{}{"id": "item"})
		require.NoError(t, executeErr)
		require.Len(t, result.Rows, want)
	}

	read(1)
	read(1)
	require.NoError(t, adapter.ApplyCommand(&replication.Command{
		Type:      replication.CmdDeleteNode,
		Data:      []byte("library:item"),
		Timestamp: time.Now(),
	}))
	read(0)
}
