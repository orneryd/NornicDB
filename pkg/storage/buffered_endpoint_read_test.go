package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestEndpointExistenceReadUsesStoredReadContract(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	check := func(id NodeID) {
		t.Helper()
		_, want := engine.GetStoredNode(id)
		image, got := engine.getNodeSnapshotForRead(id, false, storedRead, true)
		require.Nil(t, image, "existence reads must never expose an internal cache image")
		if want == nil {
			require.NoError(t, got)
			return
		}
		require.Error(t, got)
		require.Equal(t, want.Error(), got.Error())
		require.Equal(t, errors.Is(want, ErrNotFound), errors.Is(got, ErrNotFound))
	}
	check("nornic:absent")
	_, err := engine.getNodeSnapshotForRead("nornic:absent", false, visibleRead, true)
	require.ErrorIs(t, err, ErrInvalidData)
	writeBehindCommit(t, engine, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(&Node{ID: "nornic:pending", Properties: map[string]any{"title": "value"}})
		return err
	})
	check("nornic:pending")
	writeBehindCommit(t, engine, func(tx *BadgerTransaction) error { return tx.DeleteGraphNode("nornic:pending", CascadeEdges) })
	check("nornic:pending")

	// A present but malformed record must still raise the decode error. A
	// key-membership-only shortcut would incorrectly admit this endpoint.
	err = engine.withUpdate(func(txn *badger.Txn) error { return txn.Set(nodeKey("nornic:corrupt"), []byte{0xff}) })
	require.NoError(t, err)
	_, err = engine.GetStoredNode("nornic:corrupt")
	require.Error(t, err)
	check("nornic:corrupt")
	require.NoError(t, engine.Close())
	check("nornic:pending")
}
