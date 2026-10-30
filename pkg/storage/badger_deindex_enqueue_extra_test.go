package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestBadgerEngine_EnqueueDeindexIfSuppressed_DecayDisabledNoop(t *testing.T) {
	engine := createTestBadgerEngine(t)
	engine.SetDecayEnabled(false)

	changed, err := engine.EnqueueDeindexIfSuppressed("test:any", false)
	require.NoError(t, err)
	require.False(t, changed)
}

func TestClearTombstonesForEntityInTxn_Branches(t *testing.T) {
	engine := createTestBadgerEngine(t)

	t.Run("missing catalog is no-op", func(t *testing.T) {
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return clearTombstonesForEntityInTxn(txn, "test:missing")
		}))
	})

	t.Run("corrupt catalog payload is no-op", func(t *testing.T) {
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			if err := txn.Set(indexEntryCatalogKey("test:corrupt"), []byte("not-msgpack")); err != nil {
				return err
			}
			return clearTombstonesForEntityInTxn(txn, "test:corrupt")
		}))
	})

	t.Run("clears tombstones, deindexed flag, and work item", func(t *testing.T) {
		entityID := "test:entity-1"
		workID := "deindex:" + entityID
		idxKeys := [][]byte{
			{prefixLabelIndex, 'l', 0x00, 'n'},
			{prefixEdgeTypeIndex, 't', 0x00, 'e'},
		}
		cat := &IndexEntryCatalog{
			TargetID:    entityID,
			TargetScope: "NODE",
			IndexKeys:   idxKeys,
			Deindexed:   true,
		}
		encCat, err := msgpack.Marshal(cat)
		require.NoError(t, err)

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			if err := txn.Set(indexEntryCatalogKey(entityID), encCat); err != nil {
				return err
			}
			for _, k := range idxKeys {
				if err := txn.Set(indexTombstoneKey(k), []byte{1}); err != nil {
					return err
				}
			}
			if err := enqueueWorkItemInTxn(txn, entityID, "NODE"); err != nil {
				return err
			}
			// Sanity check explicit key shape used by clear helper.
			return txn.Set(deindexWorkItemKey(workID), []byte{1})
		}))

		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			return clearTombstonesForEntityInTxn(txn, entityID)
		}))

		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			// Tombstones removed.
			for _, k := range idxKeys {
				_, err := txn.Get(indexTombstoneKey(k))
				require.ErrorIs(t, err, badger.ErrKeyNotFound)
			}

			// Work item removed.
			_, err := txn.Get(deindexWorkItemKey(workID))
			require.ErrorIs(t, err, badger.ErrKeyNotFound)

			// Catalog persisted with Deindexed=false.
			item, err := txn.Get(indexEntryCatalogKey(entityID))
			require.NoError(t, err)
			return item.Value(func(v []byte) error {
				var got IndexEntryCatalog
				require.NoError(t, msgpack.Unmarshal(v, &got))
				require.False(t, got.Deindexed)
				return nil
			})
		}))
	})
}
