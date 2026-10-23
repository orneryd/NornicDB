package storage

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// Each operation lands between two measured public reads. Inspect eligibility
// before the second read can conceal a cooled cache by filling it again.
func TestNodeCacheWriterEntranceWarmth(t *testing.T) {
	kinds := []string{
		"node-create", "node-update", "node-delete", "edge-create", "edge-update", "edge-delete",
		"bulk-node-create", "bulk-node-delete", "bulk-edge-create", "bulk-edge-delete",
		"sidecar", "sidecar-clear", "separate-embedding-replace", "separate-embedding-clear", "schema-token", "schema-drop",
		"access-meta-put", "access-meta-delete", "catalog-put", "catalog-delete",
		"manual-node-suppression", "manual-edge-suppression", "pending-add", "pending-remove",
		"transaction-create-vectors", "transaction-update-vectors", "transaction-delete",
		"buffered-create-vectors", "buffered-update-vectors", "buffered-delete",
		"transaction-raw-vector", "transaction-raw-meta-delete",
		"buffered-raw-vector", "buffered-raw-meta-delete", "recovered-raw-vector", "recovered-raw-meta-delete",
	}
	for _, kind := range kinds {
		t.Run(kind, func(t *testing.T) {
			var b *BadgerEngine
			if len(kind) >= 9 && kind[:9] == "buffered-" {
				b = newWriteBehindTestEngine(t, time.Hour)
			} else {
				b = NewMemoryEngine().BadgerEngine
				t.Cleanup(func() { _ = b.Close() })
			}
			b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"v"}})
			vector := make([]float32, 20000)
			vector[0] = 1
			target := &Node{ID: "test:scope-untouched", Properties: map[string]any{"v": int64(7)}}
			other := &Node{ID: "test:scope-other", Properties: map[string]any{"v": int64(1)}, ChunkEmbeddings: [][]float32{vector}}
			peer := &Node{ID: "test:scope-peer", Properties: map[string]any{"v": int64(2)}}
			victim := &Node{ID: "test:scope-victim", Properties: map[string]any{"v": int64(3)}}
			for _, node := range []*Node{target, other, peer, victim} {
				_, err := b.CreateNode(node)
				require.NoError(t, err)
			}
			edge := &Edge{ID: "test:scope-edge", StartNode: other.ID, EndNode: peer.ID, Type: "SCOPE"}
			require.NoError(t, b.CreateEdge(edge))
			require.NoError(t, b.FlushWriteBehind())
			_, err := b.GetNode(target.ID)
			require.NoError(t, err)
			warm := func() (bool, uint64) {
				b.nodeCacheMu.RLock()
				defer b.nodeCacheMu.RUnlock()
				_, ok := b.cachedNodeLocked(target.ID, false, 0)
				return ok, b.nodeCacheInvalidationTs
			}
			before, fenceBefore := warm()
			require.True(t, before)
			created := &Node{ID: "test:scope-created", Properties: map[string]any{"v": int64(3)}, ChunkEmbeddings: [][]float32{vector}}
			createdEdge := &Edge{ID: "test:scope-created-edge", StartNode: other.ID, EndNode: peer.ID, Type: "SCOPE"}
			switch kind {
			case "node-create":
				_, err = b.CreateNode(created)
			case "node-update":
				other.Properties["v"] = int64(4)
				err = b.UpdateNode(other)
			case "node-delete":
				err = b.DeleteNode(victim.ID)
			case "edge-create":
				err = b.CreateEdge(createdEdge)
			case "edge-update":
				edge.Properties = map[string]any{"v": int64(4)}
				err = b.UpdateEdge(edge)
			case "edge-delete":
				err = b.DeleteEdge(edge.ID)
			case "bulk-node-create":
				err = b.BulkCreateNodes([]*Node{created})
			case "bulk-node-delete":
				err = b.BulkDeleteNodes([]NodeID{victim.ID})
			case "bulk-edge-create":
				err = b.BulkCreateEdges([]*Edge{createdEdge})
			case "bulk-edge-delete":
				err = b.BulkDeleteEdges([]EdgeID{edge.ID})
			case "separate-embedding-replace":
				err = b.replaceSeparateEmbeddingChunks(other.ID, [][]float32{{1, 2}})
			case "separate-embedding-clear":
				err = b.replaceSeparateEmbeddingChunks(other.ID, nil)
			case "sidecar", "sidecar-clear":
				if kind == "sidecar-clear" {
					other.ChunkEmbeddings = nil
				} else {
					other.ChunkEmbeddings[0][0] = 2
				}
				err = b.UpdateNodeEmbeddingSidecar(other)
			case "schema-token":
				err = NewNamespacedEngine(b, "test").GetSchema().RegisterTokens([]string{"ScopeNew"}, []string{"SCOPE_NEW"})
			case "schema-drop":
				err = b.dropNamespaceSchema("schema-only")
			case "access-meta-put":
				err = b.PutAccessMeta(string(other.ID), &knowledgepolicy.AccessMetaEntry{})
			case "access-meta-delete":
				err = b.DeleteAccessMeta(string(other.ID))
			case "catalog-put":
				err = b.PutIndexEntryCatalog(string(other.ID), &IndexEntryCatalog{})
			case "catalog-delete":
				err = b.DeleteIndexEntryCatalog(string(other.ID))
			case "manual-node-suppression":
				err = b.SetNodeManuallySuppressed(other.ID, true)
			case "manual-edge-suppression":
				err = b.SetEdgeManuallySuppressed(edge.ID, true)
			case "pending-add":
				b.AddToPendingEmbeddings(other.ID)
			case "pending-remove":
				b.MarkNodeEmbedded(other.ID)
			default:
				raw := kind == "transaction-raw-vector" || kind == "buffered-raw-vector" || kind == "recovered-raw-vector"
				deletingMeta := kind == "transaction-raw-meta-delete" || kind == "buffered-raw-meta-delete" || kind == "recovered-raw-meta-delete"
				if raw || deletingMeta {
					writes, deletes := map[string][]byte{}, map[string]bool{}
					if raw {
						kvs, encodeErr := buildEmbeddingChunkWriteKVs(other.ID, 0, []float32{2, 0})
						require.NoError(t, encodeErr)
						for _, kv := range kvs {
							writes[string(kv.key)] = kv.val
						}
					} else {
						deletes[string(embeddingMetaKey(other.ID))] = true
					}
					if kind == "recovered-raw-vector" || kind == "recovered-raw-meta-delete" {
						// Exercise the live replay boundary; reopening cannot preserve a process cache.
						err = b.applyBufferedCommits([]bufferedCommit{{namespace: "test", recovered: true, writes: writes, deletes: deletes}})
					} else {
						tx, beginErr := b.BeginTransaction()
						require.NoError(t, beginErr)
						require.NoError(t, tx.SetNamespace("test"))
						require.NoError(t, tx.SetImplicit(kind == "buffered-raw-vector" || kind == "buffered-raw-meta-delete"))
						for key, value := range writes {
							tx.bufferSet([]byte(key), value)
						}
						for key := range deletes {
							tx.bufferDelete([]byte(key))
						}
						err = tx.Commit()
					}
				} else {
					tx, beginErr := b.BeginTransaction()
					require.NoError(t, beginErr)
					require.NoError(t, tx.SetNamespace("test"))
					require.NoError(t, tx.SetImplicit(kind == "buffered-create-vectors" || kind == "buffered-update-vectors" || kind == "buffered-delete"))
					switch kind {
					case "transaction-create-vectors", "buffered-create-vectors":
						_, err = tx.CreateNode(created)
					case "transaction-update-vectors", "buffered-update-vectors":
						other.Properties["v"] = int64(4)
						other.ChunkEmbeddings[0][0] = 2
						err = tx.UpdateNode(other)
					case "transaction-delete", "buffered-delete":
						err = tx.DeleteNode(victim.ID)
					default:
						t.Fatalf("unhandled writer %q", kind)
					}
					require.NoError(t, err)
					err = tx.Commit()
				}
				require.NoError(t, err)
				err = b.FlushWriteBehind()
			}
			require.NoError(t, err)
			after, fenceAfter := warm()
			misses := atomic.LoadInt64(&b.cacheMisses)
			hits := atomic.LoadInt64(&b.cacheHits)
			got, err := b.GetNode(target.ID)
			require.NoError(t, err)
			require.Equal(t, int64(7), got.Properties["v"])
			t.Logf("eligible %t->%t fence %d->%d public misses %+d", before, after, fenceBefore, fenceAfter, atomic.LoadInt64(&b.cacheMisses)-misses)
			require.True(t, after, "known writer cooled an unrelated node")
			require.Equal(t, misses, atomic.LoadInt64(&b.cacheMisses), "measured second read must stay warm")
			require.Greater(t, atomic.LoadInt64(&b.cacheHits), hits)
		})
	}
}
