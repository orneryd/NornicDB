// SPDX-License-Identifier: MIT
package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestTransactionLegacySnapshotViewDelegatesAndPreservesCallbackError(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, _, edgeID := seedSnapshotAdjacencyGraph(t, engine, 0)
	key := engine.mvccEdgeHeadKeyStringLookup(edgeID)
	require.NotEmpty(t, key)
	legacy := &BadgerTransaction{engine: engine}
	callbackErr := errors.New("snapshot callback rejected decoded application state")
	called := 0
	for _, returned := range []error{nil, callbackErr} {
		err := legacy.withSnapshotViewLocked(func(view *badger.Txn) error {
			called++
			item, err := view.Get(key)
			if err != nil {
				return err
			}
			require.Positive(t, item.Version(), "callback receives a real committed native view")
			return returned
		})
		if returned == nil {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, callbackErr)
		}
	}
	require.Equal(t, 2, called)
}

func TestTransactionSnapshotHeadMissingPhysicalBindingPreservesLogicalConflict(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, _, edgeID := seedSnapshotAdjacencyGraph(t, engine, 0)
	key := engine.mvccEdgeHeadKeyStringLookup(edgeID)
	require.NotEmpty(t, key)
	pinned, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pinned.Rollback() })
	readVersion := MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 10}
	legacy := &BadgerTransaction{engine: engine, readTS: readVersion}
	pinned.readTS = readVersion
	for _, tc := range []struct {
		name string
		tx   *BadgerTransaction
		key  []byte
	}{
		{"legacy_reader", legacy, key},
		{"nil_key", pinned, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conflict, err := tc.tx.snapshotHeadConflict(tc.key, readVersion)
			require.NoError(t, err)
			require.False(t, conflict, "missing physical binding must not fabricate a conflict")
			newer := readVersion
			newer.CommitSequence++
			conflict, err = tc.tx.snapshotHeadConflict(tc.key, newer)
			require.NoError(t, err)
			require.True(t, conflict, "logical conflict must be checked before physical fallback")
		})
	}
}

func TestTransactionSnapshotHeadAbsentMetadataDoesNotInventConflict(t *testing.T) {
	engine := createTestBadgerEngine(t)
	// Allocate a valid dictionary-backed head key without publishing an edge.
	// This is an absent metadata read, not a malformed/empty-key error probe.
	var key []byte
	require.NoError(t, engine.withUpdate(func(view *badger.Txn) error {
		var err error
		key, err = engine.mvccEdgeHeadKeyString(view, EdgeID("test:unpublished-edge"))
		return err
	}))
	require.NotEmpty(t, key)
	require.NoError(t, engine.withView(func(view *badger.Txn) error {
		_, err := view.Get(key)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	conflict, err := reader.snapshotHeadConflict(key, MVCCVersion{})
	require.NoError(t, err)
	require.False(t, conflict, "an absent valid head is not evidence of a peer publication")
}
