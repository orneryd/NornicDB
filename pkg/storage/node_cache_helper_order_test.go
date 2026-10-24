package storage

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Develop serializes sidecar writes with unfinished body publishers; release
// retains its shared-barrier sidecar route and must revoke their tokens.
const nodeCacheSidecarSerializesPublishers = false

func TestNodeCacheDurableHelperMutations(t *testing.T) {
	for _, sidecar := range []bool{false, true} {
		name := "clear-suppression"
		if sidecar {
			name = "embedding-sidecar"
		}
		t.Run(name, func(t *testing.T) {
			b := NewMemoryEngine()
			t.Cleanup(func() { _ = b.Close() })
			b.SetDecayEnabled(!sidecar)
			node := &Node{ID: "test:durable-helper", VisibilitySuppressed: !sidecar, ChunkEmbeddings: [][]float32{{1, 0}}}
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
			created := make(chan error, 1)
			go func() { _, err := b.CreateNode(node); created <- err }()
			wait(0)
			updated := copyNode(node)
			updated.ChunkEmbeddings = [][]float32{{0, 1}}
			updated.EmbedMeta = map[string]interface{}{"chunk_count": 1}
			changed := make(chan error, 1)
			go func() {
				if sidecar {
					changed <- b.UpdateNodeEmbeddingSidecar(updated)
				} else {
					_, err := b.EnqueueDeindexIfSuppressed(string(node.ID), false)
					changed <- err
				}
			}()
			if sidecar && nodeCacheSidecarSerializesPublishers {
				// Establish that the public sidecar has queued its exclusive
				// barrier, rather than relying on a sleep or scheduler timing.
				deadline := time.Now().Add(10 * time.Second)
				for b.writeBarrier.TryRLock() {
					b.writeBarrier.RUnlock()
					require.True(t, time.Now().Before(deadline), "sidecar did not queue its barrier")
					runtime.Gosched()
				}
				select {
				case <-entered[1]:
					t.Fatal("sidecar overtook an unfinished body publisher")
				default:
				}
				unblock(0)
				require.NoError(t, <-created)
			}
			wait(1)
			var durable *Node
			require.NoError(t, b.withView(func(txn *badger.Txn) error {
				item, err := txn.Get(nodeKey(node.ID))
				if err != nil {
					return err
				}
				return item.Value(func(value []byte) error {
					durable, err = b.decodeNodeWithEmbeddings(txn, value, node.ID)
					return err
				})
			}))
			if !sidecar || !nodeCacheSidecarSerializesPublishers {
				unblock(0)
				require.NoError(t, <-created)
			}
			if sidecar && nodeCacheSidecarSerializesPublishers {
				unblock(1)
				require.NoError(t, <-changed)
			}
			got, readErr := b.GetNode(node.ID)
			_, queryErr := b.GetQueryNode(node.ID)
			if !sidecar || !nodeCacheSidecarSerializesPublishers {
				unblock(1)
				require.NoError(t, <-changed)
			}
			require.NoError(t, readErr, "older callback must not hide a newer visible node")
			require.NoError(t, queryErr)
			if sidecar {
				require.Equal(t, updated.ChunkEmbeddings, durable.ChunkEmbeddings)
				require.Equal(t, durable.ChunkEmbeddings, got.ChunkEmbeddings)
			} else {
				require.False(t, durable.VisibilitySuppressed)
				require.False(t, got.VisibilitySuppressed)
			}
		})
	}
}

// Generic helpers may write node or embedding keys without requesting a warm
// publication token. They must still revoke older publishers at durability.
func TestNodeCacheGenericDataWriteRevokesPublisher(t *testing.T) {
	for name, key := range map[string][]byte{
		"node":      nodeKey("test:generic"),
		"embedding": embeddingKey("test:generic", 0),
		"sidecar":   embeddingMetaKey("test:generic"),
	} {
		for _, route := range []string{"withUpdate", "commitEngineWrite", "withUpdateUnits", "managedUpdate", "managedWriteBatch"} {
			t.Run(name+"/"+route, func(t *testing.T) {
				b := NewMemoryEngine()
				t.Cleanup(func() { _ = b.Close() })
				before := b.nodeCacheGen.current()
				write := func(txn *badger.Txn) error { return txn.Set(key, []byte{1}) }
				var err error
				switch route {
				case "managedUpdate":
					err = b.db.Update(write)
				case "managedWriteBatch":
					w := b.db.NewWriteBatch(unknownNodeCacheWrites(nodeCacheUnknownManaged))
					defer w.Cancel()
					err = w.Set(key, []byte{1})
					if err == nil {
						err = w.Flush()
					}
				case "withUpdate":
					err = b.withUpdate(unknownNodeCacheWrites(nodeCacheUnknownManaged), write)
				case "commitEngineWrite":
					err = b.commitEngineWrite(unknownNodeCacheWrites(nodeCacheUnknownManaged), func(cw *commitWriter) error { return cw.writeOnce(write) })
				case "withUpdateUnits":
					err = b.withUpdateUnits(unknownNodeCacheWrites(nodeCacheUnknownManaged), []func(*badger.Txn) error{write})
				}
				require.NoError(t, err)
				require.Greater(t, b.nodeCacheGen.current(), before, "a data write cannot use the metadata-only exemption")
			})
		}
	}
}
