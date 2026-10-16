package storage

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEmbeddingCleanup_RestoreLegacyOrphans(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		t.Run(fmt.Sprintf("released=%v", legacy), func(t *testing.T) {
			source, err := NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			defer source.Close()
			id := NodeID("test:restored-inline")
			live := seedPriorEmbeddingNode(t, source, "test:restored-live\x00", false)
			if legacy {
				seedPriorEmbeddingNode(t, source, id, false)
			} else {
				_, err = source.CreateNode(separatelyStoredNode(id))
				require.NoError(t, err)
			}
			require.NoError(t, source.withUpdate(func(txn *badger.Txn) error {
				body, separate, err := source.encodeNodeInTxn(txn, "test", &Node{
					ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v2"}, ChunkEmbeddings: [][]float32{{7, 8}},
				})
				if err != nil {
					return err
				}
				require.False(t, separate)
				if err = txn.Set(nodeKey(id), body); err != nil {
					return err
				}
				if err = txn.Delete(embeddingRepairReadyKey); err != nil {
					return err
				}
				prefix := embeddingPrefix(id)
				if legacy {
					prefix = legacyEmbeddingPrefix(id)
				}
				opts := badgerIteratorOptions()
				opts.Prefix = prefix
				it := txn.NewIterator(opts)
				defer it.Close()
				// The released cleanup retained borrowed iterator keys. Build
				// that old-store shape rather than relying on the fixed writer.
				for it.Rewind(); it.Valid(); it.Next() {
					if err = txn.Delete(it.Item().Key()); err != nil {
						return err
					}
				}
				return nil
			}))
			require.Equal(t, 14, separatorIDKeyCount(t, source, id))
			backup := filepath.Join(t.TempDir(), "old.backup")
			require.NoError(t, source.Backup(backup))
			for _, action := range []string{"delete", "update", "transaction_delete", "transaction_update"} {
				t.Run(action, func(t *testing.T) {
					dir := t.TempDir()
					b, err := NewBadgerEngine(dir)
					require.NoError(t, err)
					defer func() { require.NoError(t, b.Close()) }()
					require.NoError(t, b.Restore(backup))
					require.Zero(t, separatorIDKeyCount(t, b, id), "repair precedes admission of reads")
					require.Equal(t, live.ChunkEmbeddings, embeddingCleanupColdNode(t, b, live.ID).ChunkEmbeddings)
					next, err := b.GetNode(id)
					require.NoError(t, err)
					require.Equal(t, [][]float32{{7, 8}}, next.ChunkEmbeddings)
					require.True(t, next.embeddingKeysKnown)
					require.False(t, next.embeddingKeysPresent)
					next.Properties["k"] = "v3"
					switch action {
					case "delete":
						require.NoError(t, b.DeleteNode(id))
					case "update":
						require.NoError(t, b.UpdateNode(next))
					default:
						tx, err := b.BeginTransaction()
						require.NoError(t, err)
						if action == "transaction_delete" {
							require.NoError(t, tx.DeleteNode(id))
						} else {
							require.NoError(t, tx.UpdateNode(next))
						}
						require.NoError(t, tx.Commit())
					}
					require.NoError(t, b.Close())
					b, err = NewBadgerEngine(dir)
					require.NoError(t, err)
					require.Zero(t, separatorIDKeyCount(t, b, id))
					require.Equal(t, live.ChunkEmbeddings, embeddingCleanupColdNode(t, b, live.ID).ChunkEmbeddings)
					removed, err := b.RepairOrphanedEmbeddingKeys()
					require.NoError(t, err)
					require.Zero(t, removed)
				})
			}
		})
	}
}
