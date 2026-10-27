package storage

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestNodeReferenceUsesCapturedSnapshot(t *testing.T) {
	b := newReadSnapshotTestEngine(t, false)
	first, err := b.CreateNode(&Node{ID: "test:reference-first", Labels: []string{"Reference"}})
	require.NoError(t, err)
	view, err := b.captureReadView(nil, readViewOptions{scopeKind: readNodeIDsScope, nodeIDs: []NodeID{first}})
	require.NoError(t, err)
	require.NotNil(t, view)
	defer view.Close()
	ref, found, err := b.snapshotNodeRef(view.txn, first)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, first, ref.id)
	live, ok := b.idDict.lookupNodeNumID(first)
	require.True(t, ok)
	require.Equal(t, live, ref.num)

	later, err := b.CreateNode(&Node{ID: "test:reference-later", Labels: []string{"Reference"}})
	require.NoError(t, err)
	_, liveFound := b.idDict.lookupNodeNumID(later)
	require.True(t, liveFound)
	missing, found, err := b.snapshotNodeRef(view.txn, later)
	require.NoError(t, err)
	require.False(t, found, "the reference must use the supplied snapshot, not the live dictionary")
	require.Equal(t, later, missing.id)
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		current, found, err := b.snapshotNodeRef(txn, later)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, later, current.id)
		return nil
	}))
}

func TestNodeReferencePreservesScopedBookmarkDiagnostics(t *testing.T) {
	b := newReadSnapshotTestEngine(t, false)
	id, err := b.CreateNode(&Node{ID: "test:reference-body", Labels: []string{"Reference"}})
	require.NoError(t, err)
	for _, n := range []int{0, 1, 7, 8, 9} {
		t.Run(strconv.Itoa(n)+"-bytes", func(t *testing.T) {
			value := make([]byte, n)
			if n == 8 {
				binary.BigEndian.PutUint64(value, 42)
			}
			require.NoError(t, b.withUpdate(updateRequest{}, func(txn *badger.Txn) error {
				return txn.Set(nodeIDForwardKey(id), value)
			}))
			require.NoError(t, b.withView(func(txn *badger.Txn) error {
				ref, found, err := b.snapshotNodeRef(txn, id)
				reader := b.newNodeIndexReader(txn, "test:", labelIndexPrefix("Reference"), id)
				defer reader.Close()
				if n != 8 {
					require.ErrorIs(t, err, ErrInvalidData)
					require.False(t, found)
					require.ErrorIs(t, reader.Err(), ErrInvalidData)
				} else {
					require.NoError(t, err)
					require.True(t, found)
					require.Equal(t, uint64(42), ref.num)
					require.NoError(t, reader.Err())
					require.Equal(t, ref.num, reader.afterNum)
				}
				return nil
			}))
		})
	}
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		missing := NodeID("test:reference-absent")
		_, found, err := b.snapshotNodeRef(txn, missing)
		require.NoError(t, err)
		require.False(t, found)
		reader := b.newNodeIndexReader(txn, "test:", labelIndexPrefix("Reference"), missing)
		defer reader.Close()
		require.ErrorIs(t, reader.Err(), ErrNotFound)
		return nil
	}))
}
