package storage

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// setReleasedStoreVersion gives a store the version released builds wrote,
// whose delete and shrink paths could leave orphaned embedding keys.
func setReleasedStoreVersion(txn *badger.Txn) error {
	version := make([]byte, 8)
	binary.BigEndian.PutUint64(version, uint64(storageVersionEdgeAdjacencyV3))
	return txn.Set(mvccSchemaVersionKey(), version)
}

// reopenReleasedStore checks that an old store needs the upgrade, then opens
// it as the operator's authorized upgrade does.
func reopenReleasedStore(t *testing.T, dir string) *BadgerEngine {
	t.Helper()
	_, err := NewBadgerEngine(dir)
	var upgrade *ErrStorageUpgradeRequired
	require.ErrorAs(t, err, &upgrade)
	require.Equal(t, storageVersionEdgeAdjacencyV3, upgrade.OnDisk)
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, AllowStorageUpgrade: true})
	require.NoError(t, err)
	version, err := b.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionEmbedKeysV4, version)
	return b
}

// An old #1218 shrink persisted an inline body but left 14 of 16 chunk
// keys. Reopen that exact disk shape before exercising the new fast path.
// This is an upgrade fixture, not a new API sequence that creates orphans.
func TestEmbeddingCleanup_LegacyShrinkOrphans(t *testing.T) {
	for _, action := range []string{"delete", "update", "transaction_delete", "transaction_update"} {
		t.Run(action, func(t *testing.T) {
			dir := t.TempDir()
			b, err := NewBadgerEngine(dir)
			require.NoError(t, err)
			id := NodeID("test:r3-old-shrink")
			_, err = b.CreateNode(separatelyStoredNode(id))
			require.NoError(t, err)
			require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
				body, separate, err := b.encodeNodeInTxn(txn, "test", &Node{
					ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"k": "v2"}, ChunkEmbeddings: [][]float32{{7, 8}},
				})
				if err != nil {
					return err
				}
				require.False(t, separate)
				if err = txn.Set(nodeKey(id), body); err != nil {
					return err
				}
				if err := setReleasedStoreVersion(txn); err != nil {
					return err
				}
				// Run the pre-#1218 cleanup verbatim: Badger retains each borrowed
				// key until commit, while the iterator reuses it on Next.
				opts := badgerIteratorOptions()
				opts.Prefix = embeddingPrefix(id)
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					if err := txn.Delete(it.Item().Key()); err != nil {
						return err
					}
				}
				return nil
			}))
			require.Equal(t, 14, countEmbeddingKeys(t, b, id))
			require.NoError(t, b.Close())
			b = reopenReleasedStore(t, dir)
			defer b.Close()
			require.Zero(t, countEmbeddingKeys(t, b, id), "the upgrade repairs the legacy range before absence can be trusted")
			next, err := b.GetNode(id)
			require.NoError(t, err)
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
			t.Logf("after %s: %d old chunk keys remain", action, countEmbeddingKeys(t, b, id))
			require.Zero(t, countEmbeddingKeys(t, b, id), "inline flags do not prove that an older store has no orphaned chunks")
		})
	}
}

func TestEmbeddingCleanup_ExplicitRepair(t *testing.T) {
	b := newTestEngine(t)
	separate := separatelyStoredNode("test:live-separate")
	_, err := b.CreateNode(separate)
	require.NoError(t, err)
	sidecar := &Node{ID: "other:live-sidecar", ChunkEmbeddings: [][]float32{{3, 4}}}
	_, err = b.CreateNode(&Node{ID: sidecar.ID})
	require.NoError(t, err)
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(sidecar))
	// More owners and keys than either repair batch, including an owner ID
	// that prefixes a live owner's ID. Missing bodies are repaired as well.
	const owners = embeddingRepairBatchSize + 1
	const keys = embeddingRepairBatchSize + 3
	require.NoError(t, b.withUpdate(func(txn *badger.Txn) error {
		for i := 0; i < owners; i++ {
			id := NodeID(fmt.Sprintf("test:orphan-%03d", i))
			if i == 0 {
				id = "test:live"
			}
			for j := 0; j < keys; j++ {
				if err := txn.Set(embeddingKey(id, j), []byte{0}); err != nil {
					return err
				}
			}
		}
		return nil
	}))
	removed, err := b.RepairOrphanedEmbeddingKeys()
	require.NoError(t, err)
	require.Equal(t, owners*keys, removed)
	for i := 0; i < owners; i++ {
		id := NodeID(fmt.Sprintf("test:orphan-%03d", i))
		if i == 0 {
			id = "test:live"
		}
		require.Zero(t, countEmbeddingKeys(t, b, id))
	}
	require.Equal(t, 16, countEmbeddingKeys(t, b, separate.ID))
	require.Equal(t, 2, countEmbeddingKeys(t, b, sidecar.ID))
	require.Equal(t, separate.ChunkEmbeddings, embeddingCleanupColdNode(t, b, separate.ID).ChunkEmbeddings)
	require.Equal(t, sidecar.ChunkEmbeddings, embeddingCleanupColdNode(t, b, sidecar.ID).ChunkEmbeddings)
	removed, err = b.RepairOrphanedEmbeddingKeys()
	require.NoError(t, err)
	require.Zero(t, removed, "repair is idempotent")
}
