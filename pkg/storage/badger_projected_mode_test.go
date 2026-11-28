package storage

import (
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestProjectedStoreModeIsPinnedAndDefaultLeavesExistingStoreUntouched(t *testing.T) {
	for _, projected := range []bool{false, true} {
		dir := t.TempDir()
		open := func(mode bool) (*BadgerEngine, error) {
			return NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, Projected: mode})
		}
		engine, err := open(projected)
		require.NoError(t, err)
		require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
			_, markerErr := txn.Get(projectedStoreMarkerKey)
			if projected {
				require.NoError(t, markerErr)
			} else {
				require.ErrorIs(t, markerErr, badger.ErrKeyNotFound)
			}
			return nil
		}))
		_, err = engine.CreateNode(t.Context(), &Node{ID: "ns:mode", Properties: map[string]any{"x": int64(1)}})
		require.NoError(t, err)
		require.NoError(t, engine.Close())
		_, err = open(!projected)
		require.ErrorIs(t, err, ErrInvalidData)
		engine, err = open(projected)
		require.NoError(t, err)
		require.NoError(t, engine.Close())
	}
}
