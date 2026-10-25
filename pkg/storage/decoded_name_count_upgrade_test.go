package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestDecodedNamePositionalCountUpgrade(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
			require.NoError(t, engine.Close())
		}
	})
	for _, namespace := range []string{"left", "right"} {
		store := NewNamespacedEngine(engine, namespace)
		start, err := store.CreateNode(&Node{ID: "start", Labels: []string{"A\x00B"}})
		require.NoError(t, err)
		end, err := store.CreateNode(&Node{ID: "end", Labels: []string{"A\x00B"}})
		require.NoError(t, err)
		require.NoError(t, store.CreateEdge(&Edge{ID: "edge", StartNode: start, EndNode: end, Type: "C"}))
	}
	require.NoError(t, engine.FlushWriteBehind())
	// This fixture is the pre-upgrade format: namespace NUL label NUL type.
	// It is evidence for migration, not another production key writer.
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		for _, sub := range []byte{prefixMVCCMetaEdgeTypeStartLabelCount, prefixMVCCMetaEdgeTypeEndLabelCount} {
			prefix := []byte{prefixMVCCMeta, sub}
			it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
			var keys [][]byte
			for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
				keys = append(keys, it.Item().KeyCopy(nil))
			}
			it.Close()
			for _, key := range keys {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			for _, namespace := range []string{"left", "right"} {
				key := append([]byte{prefixMVCCMeta, sub}, []byte(namespace+"\x00A\x00B\x00C")...)
				if err := txn.Set(key, encodeDerivedCount(1)); err != nil {
					return err
				}
			}
		}
		return txn.Set(edgeTypeCountReadyKey, []byte{1})
	}))
	for _, phase := range []string{"upgrade", "clean reopen"} {
		require.NoError(t, engine.Close())
		engine, err = NewBadgerEngine(dir)
		require.NoError(t, err)
		t.Run(phase, func(t *testing.T) {
			for _, reader := range []func(string, string) (int64, error){engine.EdgeCountByStartLabel, engine.EdgeCountByEndLabel} {
				count, err := reader("A\x00B", "C")
				require.NoError(t, err)
				require.EqualValues(t, 2, count, "the unscoped reader decodes the exact label across both namespaces")
				count, err = reader("A", "B\x00C")
				require.NoError(t, err)
				require.Zero(t, count, "the distinct tuple owns no edges")
			}
			for _, namespace := range []string{"left", "right"} {
				store := NewNamespacedEngine(engine, namespace)
				count, err := store.EdgeCountByStartLabel("A\x00B", "C")
				require.NoError(t, err)
				require.EqualValues(t, 1, count)
				count, err = store.EdgeCountByEndLabel("A", "B\x00C")
				require.NoError(t, err)
				require.Zero(t, count)
			}
		})
	}
}
