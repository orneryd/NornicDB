// Author: Lane #1644 (Codex). The public clear operation must not acknowledge
// a required node read that failed before the requested embeddings were cleared.
package storage

import (
	"context"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestFoundationPublicClearReadOutcome(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "healthy"
		if corrupt {
			name = "read-failure"
		}
		t.Run(name, func(t *testing.T) {
			base := createTestBadgerEngine(t)
			_, err := base.CreateNode(&Node{ID: "test:clear", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 2, 3}}})
			require.NoError(t, err)
			if corrupt {
				require.NoError(t, base.withUpdate(func(txn *badger.Txn) error {
					return txn.Set(nodeKey("test:clear"), []byte("invalid-clear-node-envelope"))
				}))
				base.cacheDeleteNode("test:clear", nodeCacheAllForms)
			}
			cleared, err := base.ClearAllEmbeddingsForPrefix("test:")
			if corrupt {
				require.Error(t, err, "a failed read cannot be acknowledged as an empty successful clear")
				require.Zero(t, cleared)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, cleared)
			node, err := base.GetStoredNode("test:clear")
			require.NoError(t, err)
			require.Empty(t, node.ChunkEmbeddings)
		})
	}
}

// Both enumeration and point reads must include acknowledged buffered data.
func TestFoundationPublicClearBufferedView(t *testing.T) {
	base := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	bufferedCreate(t, base, &Node{ID: "test:buffered-clear", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 2, 3}}})
	require.Greater(t, base.writeBehind.PendingOps(), 0)
	before, err := base.GetStoredNode("test:buffered-clear")
	require.NoError(t, err)
	require.NotEmpty(t, before.ChunkEmbeddings)
	cleared, err := base.ClearAllEmbeddingsForPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, 1, cleared, "an acknowledged node belongs to the clear operation's issued read view")
	after, err := base.GetStoredNode("test:buffered-clear")
	require.NoError(t, err)
	require.Empty(t, after.ChunkEmbeddings)
}

func TestFoundationPublicNativeScanReadOutcome(t *testing.T) {
	for _, opts := range []StreamNodesOptions{{WithEmbeddings: true}, {QueryNodes: true}, {Projection: []string{"name"}}} {
		t.Run(fmt.Sprintf("full=%v/query=%v/projection=%v", opts.WithEmbeddings, opts.QueryNodes, opts.Projection), func(t *testing.T) {
			base := createTestBadgerEngine(t)
			_, err := base.CreateNode(&Node{ID: "test:scan", Labels: []string{"Doc"}, Properties: map[string]any{"name": "visible"}})
			require.NoError(t, err)
			seen := 0
			require.NoError(t, base.StreamNodesWithOptions(context.Background(), opts, func(*Node) error { seen++; return nil }))
			require.Equal(t, 1, seen)
			require.NoError(t, base.withUpdate(func(txn *badger.Txn) error { return txn.Set(nodeKey("test:scan"), []byte("invalid-scan-envelope")) }))
			base.cacheDeleteNode("test:scan", nodeCacheAllForms)
			err = base.StreamNodesWithOptions(context.Background(), opts, func(*Node) error { return nil })
			require.Error(t, err, "a required corrupt node cannot become a successful empty scan")
		})
	}
}
