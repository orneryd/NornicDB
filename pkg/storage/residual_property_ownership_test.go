package storage_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Ownership is exercised through real ingress and subsequent caller reuse.
// The physical image after reopen is the independent storage-codec oracle.
func TestResidualPropertyOwnership(t *testing.T) {
	for _, route := range []string{"direct", "transaction", "buffered"} {
		t.Run(route, func(t *testing.T) {
			dir := t.TempDir()
			open := func() *storage.BadgerEngine {
				b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, WriteBehind: route == "buffered", WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				return b
			}
			b := open()
			t.Cleanup(func() { _ = b.Close() })
			duration := &cypher.CypherDuration{Days: 1}
			props := map[string]any{"nested": map[string]any{"list": []any{duration, "original"}}}
			metadata := map[string]any{"nested": []any{int64(1), "original"}}
			node := &storage.Node{ID: "test:owned", Properties: props, EmbedMeta: metadata}
			if route == "direct" {
				_, err := b.CreateNode(node)
				require.NoError(t, err)
			} else {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(route == "buffered"))
				_, err = tx.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, tx.CommitContext(context.Background()))
			}
			duration.Days = 9
			props["nested"].(map[string]any)["list"].([]any)[1] = "caller reused"
			metadata["nested"].([]any)[1] = "caller reused"
			first, err := b.GetStoredNode(node.ID)
			require.NoError(t, err)
			list := first.Properties["nested"].(map[string]any)["list"].([]any)
			require.Equal(t, int64(1), list[0].(*cypher.CypherDuration).Days)
			require.Equal(t, "original", list[1])
			require.Equal(t, "original", first.EmbedMeta["nested"].([]any)[1])
			// Reusing a returned body must not mutate any retained cache/overlay.
			list[0].(*cypher.CypherDuration).Days = 7
			list[1] = "reader reused"
			first.EmbedMeta["nested"].([]any)[1] = "reader reused"
			second, err := b.GetStoredNode(node.ID)
			require.NoError(t, err)
			require.Equal(t, "original", second.Properties["nested"].(map[string]any)["list"].([]any)[1])
			require.Equal(t, "original", second.EmbedMeta["nested"].([]any)[1])
			require.NoError(t, b.FlushWriteBehind())
			require.NoError(t, b.Close())
			b = open()
			cold, err := b.GetStoredNode(node.ID)
			require.NoError(t, err)
			require.Equal(t, cold.Properties, second.Properties)
			require.Equal(t, cold.EmbedMeta, second.EmbedMeta)
		})
	}
}

func TestResidualUint64PropertyList(t *testing.T) {
	b, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	_, err = b.CreateNode(&storage.Node{ID: "test:uints", Properties: map[string]any{"xs": []uint64{1, math.MaxUint64}}})
	require.NoError(t, err)
	n, err := b.GetStoredNode("test:uints")
	require.NoError(t, err)
	require.Equal(t, []any{int64(1), uint64(math.MaxUint64)}, n.Properties["xs"])
}
