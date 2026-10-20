package storage

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/stretchr/testify/require"
)

// The helper appended below is the exact base's old encoding/write path,
// renamed for this test. Its hydration preserves unknown metadata just as
// that binary's GetNode does. The new-writer mutations use the public API.
func TestEmbeddingRangeManifestOldWriterRoundTrip(t *testing.T) {
	for _, shape := range []string{"extra_chunk", "extra_shards", "extra_chunk_freshness", "extra_shards_freshness"} {
		t.Run(shape, func(t *testing.T) {
			engine := NewMemoryEngine().BadgerEngine
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			engine.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Include: []string{"text"}})
			body := &Node{ID: NodeID("test:downgrade-" + shape), Labels: []string{"Doc"}, Properties: map[string]any{"text": "unchanged source"}}
			_, err := engine.CreateNode(body)
			require.NoError(t, err)
			initial := CopyNode(body)
			initial.ChunkEmbeddings = [][]float32{{1, 2}}
			initial.EmbedMeta = map[string]any{"chunk_count": 1, "embedding_dimensions": 2}
			require.NoError(t, engine.UpdateNodeEmbeddingSidecar(initial))
			require.NoError(t, engine.withView(func(txn *badger.Txn) error {
				item, err := txn.Get(embeddingMetaKey(body.ID))
				require.NoError(t, err)
				require.Equal(t, embeddingRangeManifestBound, item.UserMeta(), "only the new range owner binds its freshly derived layout")
				return nil
			}))
			held, err := engine.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, held.Rollback()) })

			// Actual old hydration removes only the content stamp. It does not
			// invent, invalidate or maintain the unknown layout entry.
			older, err := engine.GetNodeWithoutEmbeddings(body.ID)
			require.NoError(t, err)
			require.NoError(t, engine.withView(func(txn *badger.Txn) error {
				meta, fresh, err := engine.loadEmbeddingSidecar(txn, older, older.ID)
				require.NoError(t, err)
				require.True(t, fresh)
				delete(meta, embeddingContentUpdatedAtKey)
				older.EmbedMeta = meta
				return nil
			}))
			require.Equal(t, []byte{1, 1, 0}, older.EmbedMeta[embeddingRangeManifestKey])
			obsolete := [][]byte{embeddingKey(body.ID, 1)}
			older.ChunkEmbeddings = [][]float32{{3, 4}, {5, 6}}
			if strings.HasPrefix(shape, "extra_shards") {
				large := make([]float32, maxEmbeddingValueBytes/4+100)
				for index := range large {
					large[index] = float32(index)
				}
				older.ChunkEmbeddings = [][]float32{large}
				kvs, err := buildEmbeddingChunkWriteKVs(body.ID, 0, large)
				require.NoError(t, err)
				require.Greater(t, len(kvs), 1)
				obsolete = nil
				for _, kv := range kvs[1:] {
					obsolete = append(obsolete, kv.key)
				}
			}
			older.EmbedMeta["chunk_count"] = len(older.ChunkEmbeddings)
			older.EmbedMeta["embedding_dimensions"] = len(older.ChunkEmbeddings[0])
			require.NoError(t, engine.persistSidecarBeforeManifest(older))
			require.NoError(t, engine.withView(func(txn *badger.Txn) error {
				item, err := txn.Get(embeddingMetaKey(body.ID))
				require.NoError(t, err)
				require.Zero(t, item.UserMeta(), "old txn.Set cannot preserve new entry-level provenance")
				return item.Value(func(data []byte) error {
					var record embeddingRangeManifestRecord
					require.NoError(t, decodeValue(data, &record))
					require.Equal(t, []byte{1, 1, 0}, record.Layout, "old encoding really preserved the stale but valid manifest")
					return nil
				})
			}))

			if strings.HasSuffix(shape, "_freshness") {
				// A source-preserving transaction must not launder an old
				// writer's unbound layout into a trusted range.
				retained, err := engine.GetNode(body.ID)
				require.NoError(t, err)
				retained.Properties["views"] = int64(1)
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.UpdateNode(retained))
				require.NoError(t, tx.Commit())
				saved, err := engine.GetNode(body.ID)
				require.NoError(t, err)
				require.Equal(t, older.ChunkEmbeddings, saved.ChunkEmbeddings, "the fixture must preserve the source and its actual old-writer layout")
				body, err = engine.GetNodeWithoutEmbeddings(body.ID)
				require.NoError(t, err)
				require.NoError(t, engine.withView(func(txn *badger.Txn) error {
					item, err := txn.Get(embeddingMetaKey(body.ID))
					require.NoError(t, err)
					require.Zero(t, item.UserMeta(), "ordinary freshness encoding cannot establish layout provenance")
					for _, key := range obsolete {
						_, err := txn.Get(key)
						require.NoError(t, err, "the obsolete old-writer keys must still challenge the final replacement")
					}
					return nil
				}))
			}
			current := CopyNode(body)
			current.ChunkEmbeddings = [][]float32{{7, 8}}
			current.EmbedMeta = map[string]any{"chunk_count": 1, "embedding_dimensions": 2}
			require.NoError(t, engine.UpdateNodeEmbeddingSidecar(current))
			require.NoError(t, engine.withView(func(txn *badger.Txn) error {
				item, err := txn.Get(embeddingMetaKey(body.ID))
				require.NoError(t, err)
				require.Equal(t, embeddingRangeManifestBound, item.UserMeta(), "bounded fallback repairs and binds the current layout")
				for _, key := range obsolete {
					_, err := txn.Get(key)
					require.ErrorIs(t, err, badger.ErrKeyNotFound, "a supported old writer cannot leave an undiscoverable obsolete chunk/part")
				}
				return nil
			}))
			saved, err := engine.GetNode(body.ID)
			require.NoError(t, err)
			require.Equal(t, current.ChunkEmbeddings, saved.ChunkEmbeddings)
			require.NotContains(t, saved.EmbedMeta, embeddingRangeManifestKey)
			old, err := held.GetNode(body.ID)
			require.NoError(t, err)
			require.Equal(t, initial.ChunkEmbeddings, old.ChunkEmbeddings)
		})
	}
}

// Exact base 16923aef778025fab3490f6a57549763b536f3d5 public writer; only helper names changed.
func (b *BadgerEngine) persistSidecarBeforeManifest(node *Node) error {
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurPut)
	if node == nil {
		return ErrInvalidData
	}
	if node.ID == "" {
		return ErrInvalidID
	}
	if !strings.Contains(string(node.ID), ":") {
		return localizedError(localization.StorageClientNodeIDNamespaceUnprefixed(string(node.ID)), nil)
	}
	if err := b.ensureOpen(); err != nil {
		return err
	}
	// The test seam runs before taking the exclusive publication barrier so
	// it can land a competing business write. The real commit rechecks the
	// source below, after acquiring the barrier.
	if hook := embeddingSourceCheckedHook.Load(); hook != nil {
		if err := b.withView(func(txn *badger.Txn) error {
			return b.checkEmbeddingSourceInTxn(txn, node)
		}); err != nil {
			return err
		}
		(*hook)()
	}

	meta := make(map[string]any, util.SafePreallocSum(len(node.EmbedMeta), 1))
	for key, value := range node.EmbedMeta {
		meta[key] = value
	}
	// A nonempty range always has discoverable metadata, even when its
	// caller supplied vectors alone. Reads can then establish its presence
	// without an embedding-prefix scan (and hydrate the requested vectors).
	if len(node.ChunkEmbeddings) > 0 {
		if _, exists := meta["chunk_count"]; !exists {
			meta["chunk_count"] = len(node.ChunkEmbeddings)
		}
	}
	// No embedding state means the record is DELETED: the content stamp alone
	// is not metadata, and a record holding only the stamp would surface as an
	// empty EmbedMeta on reads.
	writeMeta := len(meta) > 0
	if writeMeta {
		if !node.UpdatedAt.IsZero() {
			meta[embeddingContentUpdatedAtKey] = node.UpdatedAt.UTC().Format(time.RFC3339Nano)
		}
	}
	metaBytes, err := encodeValue(meta)
	if err != nil {
		return fmt.Errorf("failed to encode embedding metadata for node %s: %w", node.ID, err)
	}

	// One atomic engine write (#703): check the stored node, then delete the
	// previous chunk vectors and metadata record, write the new chunk
	// vectors, write or delete the metadata record at its reserved chunk
	// index, and remove the pending-embeddings marker. Readers see the old
	// embedding state or the new one — never a mix.
	var units []func(txn *badger.Txn) error
	metaKey := embeddingMetaKey(node.ID)
	if writeMeta {
		units = append(units, func(txn *badger.Txn) error { return txn.Set(metaKey, metaBytes) })
	} else {
		units = append(units, func(txn *badger.Txn) error { return txn.Delete(metaKey) })
	}
	units = append(units, func(txn *badger.Txn) error { return txn.Delete(pendingEmbedKey(node.ID)) })
	notification := &commitNotification{}
	defer b.completeCommitNotification(notification)
	err = b.commitEmbeddingSidecar(node.ID, len(node.ChunkEmbeddings) > 0 || writeMeta, notification, func(cw *commitWriter) error {
		if err := cw.writeOnce(func(txn *badger.Txn) error {
			return b.checkEmbeddingSourceInTxn(txn, node)
		}); err != nil {
			return err
		}
		if err := cw.replaceEmbeddingKeysBeforeManifest(node.ID, node.ChunkEmbeddings); err != nil {
			return err
		}
		for _, unit := range units {
			if err := cw.write(unit); err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, badger.ErrConflict) {
		return ErrEmbeddingSourceChanged
	}
	if err != nil {
		return err
	}

	// The full-node cache may hold a copy without the new embedding state;
	// drop it so the next read re-hydrates from the sidecar. Body cache and
	// counts are untouched: the node record did not change.
	b.cacheDeleteNode(node.ID)
	// Search indexes and listeners still need the updated node (mutation
	// notification to the embedding queue keeps its own loop guard).
	notification.ops = []Operation{{Type: OpUpdateNode, Node: node}}
	return nil
}

func (cw *commitWriter) replaceEmbeddingKeysBeforeManifest(id NodeID, embeddings [][]float32, references ...embeddingReferenceView) error {
	var cleanup embeddingKeyCleanupOptions
	if len(references) > 0 {
		cleanup.references = &references[0]
	}
	if _, err := cw.removeEmbeddingKeys(id, 0, cleanup); err != nil {
		return err
	}
	return writeEmbeddingChunks(id, embeddings, func(key, value []byte) error {
		return cw.write(func(txn *badger.Txn) error { return txn.Set(key, value) })
	})
}
