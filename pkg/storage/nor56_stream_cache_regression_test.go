package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNOR56StreamFullNodeCacheRespectsSnapshot(t *testing.T) {
	ctx := context.Background()
	engine := fixtureowner.Own(t, fixtureowner.Track(t, NewMemoryEngine(), ErrStorageClosed))
	_, err := engine.CreateNode(ctx, &Node{ID: "tenant:n", Properties: map[string]interface{}{"value": "old"}})
	require.NoError(t, err)
	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	before, err := engine.GetNode(ctx, "tenant:n")
	require.NoError(t, err)
	after := CopyNode(before)
	after.Properties["value"] = "new"
	require.NoError(t, engine.UpdateNode(ctx, NodeUpdateRequest{Before: before, After: after}))
	_, err = engine.AllNodes(ctx)
	require.NoError(t, err)
	var values []interface{}
	require.NoError(t, tx.StreamNodesWithOptions(ctx, StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: true}, func(n *Node) error { values = append(values, n.Properties["value"]); return nil }))
	require.Equal(t, []interface{}{"old"}, values, "snapshot stream must not use a newer cached image")
}
