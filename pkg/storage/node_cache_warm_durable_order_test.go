package storage

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The durable delete must hide an already cached node before either writer's
// cache tail runs. Exercise both callback orders, including the earlier test's
// missing observation while both callbacks are still paused.
func TestNodeCacheWarmEntryCannotOutliveDurableDelete(t *testing.T) {
	for _, newerFirst := range []bool{false, true} {
		name := "older-callback-first"
		if newerFirst {
			name = "newer-callback-first"
		}
		t.Run(name, func(t *testing.T) {
			b := NewMemoryEngine()
			t.Cleanup(func() { _ = b.Close() })
			node := &Node{ID: "test:warm-durable-delete", Labels: []string{"WarmCache"}, ChunkEmbeddings: [][]float32{{1, 0}}}
			_, err := b.CreateNode(node)
			require.NoError(t, err)
			_, err = b.GetNode(node.ID)
			require.NoError(t, err)
			require.Contains(t, b.nodeCache, node.ID)
			_, err = b.GetFirstNodeByLabel("WarmCache")
			require.NoError(t, err)
			entered := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			released := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			var once [2]sync.Once
			unblock := func(i int) { once[i].Do(func() { close(released[i]) }) }
			var phase atomic.Int32
			restore := setCommitTailHook(func() {
				if i := int(phase.Add(1)) - 1; i < len(entered) {
					close(entered[i])
					<-released[i]
				}
			})
			t.Cleanup(func() { unblock(0); unblock(1); restore() })
			wait := func(i int) {
				t.Helper()
				select {
				case <-entered[i]:
				case <-time.After(10 * time.Second):
					t.Fatal("write did not reach its durable boundary")
				}
			}
			written, deleted := make(chan error, 1), make(chan error, 1)
			updated := copyNode(node)
			updated.ChunkEmbeddings = [][]float32{{0, 1}}
			go func() { written <- b.UpdateNode(updated) }()
			wait(0)
			go func() { deleted <- b.DeleteNode(node.ID) }()
			wait(1)
			require.NoError(t, b.withView(func(txn *badger.Txn) error {
				_, err := txn.Get(nodeKey(node.ID))
				require.ErrorIs(t, err, badger.ErrKeyNotFound)
				return nil
			}))
			readMissing := func(stage string) {
				t.Helper()
				_, err := b.GetNode(node.ID)
				assert.ErrorIs(t, err, ErrNotFound, stage+": GetNode")
				_, err = b.GetQueryNode(node.ID)
				assert.ErrorIs(t, err, ErrNotFound, stage+": GetQueryNode")
				if stored, ok := any(b).(interface {
					GetStoredNode(NodeID) (*Node, error)
				}); ok {
					_, err = stored.GetStoredNode(node.ID)
					assert.ErrorIs(t, err, ErrNotFound, stage+": GetStoredNode")
				}
				_, err = b.GetNodeProjected(node.ID, []string{})
				assert.ErrorIs(t, err, ErrNotFound, stage+": GetNodeProjected")
				batch, err := b.BatchGetNodes([]NodeID{node.ID})
				assert.NoError(t, err)
				assert.Empty(t, batch, stage+": BatchGetNodes")
				batch, err = b.BatchGetNodesWithoutEmbeddings([]NodeID{node.ID})
				assert.NoError(t, err)
				assert.Empty(t, batch, stage+": BatchGetNodesWithoutEmbeddings")
				visible, answered := b.RelationshipEndpointVisible(node.ID)
				assert.True(t, answered)
				assert.False(t, visible, stage+": endpoint visibility")
				firstNode, err := b.GetFirstNodeByLabel("WarmCache")
				assert.NoError(t, err, stage+": label first")
				assert.Nil(t, firstNode, stage+": label first")
			}
			readMissing("both callbacks paused")
			first := 0
			if newerFirst {
				first = 1
			}
			unblock(first)
			if newerFirst {
				require.NoError(t, <-deleted)
			} else {
				require.NoError(t, <-written)
			}
			readMissing("first callback completed")
			unblock(1 - first)
			if newerFirst {
				require.NoError(t, <-written)
			} else {
				require.NoError(t, <-deleted)
			}
			readMissing("both callbacks completed")
		})
	}
}
