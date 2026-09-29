package storage

import (
	"path/filepath"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestBadgerStartupRepairsLegacyEdgeAdjacency(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "badger")
	options := BadgerOptions{DataDir: dataDir}
	engine, err := NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
			_ = engine.Close()
		}
	})

	startID := NodeID("repair:owner")
	endID := NodeID("repair:item")
	edgeID := EdgeID("repair:relationship")
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: startID, Labels: []string{"Owner"}},
		{ID: endID, Labels: []string{"Item"}},
	}))
	require.NoError(t, engine.BulkCreateEdges([]*Edge{{
		ID: edgeID, StartNode: startID, EndNode: endID, Type: "HAS",
	}}))
	head, err := engine.loadEdgeMVCCHead(edgeID)
	require.NoError(t, err)

	// Older bulk writers persisted the edge body and head without these keys.
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		outgoingKey, err := engine.mvccOutgoingAdjacencyKeyString(txn, startID, edgeID, head.Version)
		if err != nil {
			return err
		}
		incomingKey, err := engine.mvccIncomingAdjacencyKeyString(txn, endID, edgeID, head.Version)
		if err != nil {
			return err
		}
		for _, key := range [][]byte{outgoingKey, incomingKey} {
			if _, err := txn.Get(key); err != nil {
				return err
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, engine.writeSchemaVersion(storageVersionPropKeyDictV2))
	require.NoError(t, engine.Close())
	engine = nil

	_, err = NewBadgerEngineWithOptions(options)
	var upgradeErr *ErrStorageUpgradeRequired
	require.ErrorAs(t, err, &upgradeErr)
	require.Equal(t, storageVersionPropKeyDictV2, upgradeErr.OnDisk)
	require.Equal(t, storageVersionCurrent, upgradeErr.Current)

	options.AllowStorageUpgrade = true
	engine, err = NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	version, err := engine.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionCurrent, version)
	reader, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, reader.SetNamespace("repair"))
	t.Cleanup(func() {
		if reader.Status == TxStatusActive {
			_ = reader.Rollback()
		}
	})

	outgoing, err := reader.GetOutgoingEdges(startID)
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	require.Equal(t, edgeID, outgoing[0].ID)
	incoming, err := reader.GetIncomingEdges(endID)
	require.NoError(t, err)
	require.Len(t, incoming, 1)
	require.Equal(t, edgeID, incoming[0].ID)
}

func TestV3MigrationPreservesRetainedLegacyEdgeHistory(t *testing.T) {
	options := BadgerOptions{
		DataDir:       filepath.Join(t.TempDir(), "badger"),
		EngineOptions: EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100}},
	}
	engine, err := NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
			_ = engine.Close()
		}
	})
	startID, endID := NodeID("repair:a"), NodeID("repair:b")
	edgeID := EdgeID("repair:history")
	require.NoError(t, engine.BulkCreateNodes([]*Node{{ID: startID}, {ID: endID}}))
	require.NoError(t, engine.BulkCreateEdges([]*Edge{{ID: edgeID, StartNode: startID, EndNode: endID, Type: "HAS", Properties: map[string]any{"n": int64(1)}}}))
	created, err := engine.loadEdgeMVCCHead(edgeID)
	require.NoError(t, err)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		out, err := engine.mvccOutgoingAdjacencyKeyString(txn, startID, edgeID, created.Version)
		if err != nil {
			return err
		}
		in, err := engine.mvccIncomingAdjacencyKeyString(txn, endID, edgeID, created.Version)
		if err != nil {
			return err
		}
		if err := txn.Delete(out); err != nil {
			return err
		}
		return txn.Delete(in)
	}))
	require.NoError(t, engine.UpdateEdge(&Edge{ID: edgeID, StartNode: startID, EndNode: endID, Type: "HAS", Properties: map[string]any{"n": int64(2)}}))
	require.NoError(t, engine.writeSchemaVersion(storageVersionPropKeyDictV2))
	require.NoError(t, engine.Close())
	engine = nil
	options.AllowStorageUpgrade = true
	engine, err = NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	historical, err := engine.GetOutgoingEdgesVisibleAt(startID, created.Version)
	require.NoError(t, err)
	require.Len(t, historical, 1)
	require.Equal(t, int64(1), historical[0].Properties["n"])
}

func TestV3MigrationPreservesDeletedLegacyEdgeHistory(t *testing.T) {
	options := BadgerOptions{
		DataDir:       filepath.Join(t.TempDir(), "badger"),
		EngineOptions: EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100}},
	}
	engine, err := NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
			_ = engine.Close()
		}
	})
	startID, endID := NodeID("repair:from"), NodeID("repair:to")
	edgeID := EdgeID("repair:deleted")
	require.NoError(t, engine.BulkCreateNodes([]*Node{{ID: startID}, {ID: endID}}))
	require.NoError(t, engine.BulkCreateEdges([]*Edge{{ID: edgeID, StartNode: startID, EndNode: endID, Type: "HAS"}}))
	created, err := engine.loadEdgeMVCCHead(edgeID)
	require.NoError(t, err)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		out, err := engine.mvccOutgoingAdjacencyKeyString(txn, startID, edgeID, created.Version)
		if err != nil {
			return err
		}
		in, err := engine.mvccIncomingAdjacencyKeyString(txn, endID, edgeID, created.Version)
		if err != nil {
			return err
		}
		if err := txn.Delete(out); err != nil {
			return err
		}
		return txn.Delete(in)
	}))
	require.NoError(t, engine.DeleteEdge(edgeID))
	require.NoError(t, engine.writeSchemaVersion(storageVersionPropKeyDictV2))
	require.NoError(t, engine.Close())
	engine = nil
	options.AllowStorageUpgrade = true
	engine, err = NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	historical, err := engine.GetOutgoingEdgesVisibleAt(startID, created.Version)
	require.NoError(t, err)
	require.Len(t, historical, 1)
	require.Equal(t, edgeID, historical[0].ID)
	current, err := engine.GetOutgoingEdges(startID)
	require.NoError(t, err)
	require.Empty(t, current)
}

// TestRepairArchivedEdgeAdjacencyToleratesLegacyVersionKeys pins the
// V2→V3 migration's tolerance for the pre-fixed-width MVCC version key
// layout: [prefixMVCCEdge][string edgeID][0x00][version 16B]. Stores that
// predate the fixed-width rewrite keep such keys, and the archived-adjacency
// repair plus the runtime version scans must accept them (#752-style startup
// failure: "invalid mvcc edge version key: len=61").
func TestRepairArchivedEdgeAdjacencyToleratesLegacyVersionKeys(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = engine.Close()
	})

	startID := NodeID("legacy:owner")
	endID := NodeID("legacy:item")
	edgeID := EdgeID("legacy:relationship")
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: startID, Labels: []string{"Owner"}},
		{ID: endID, Labels: []string{"Item"}},
	}))
	edge := &Edge{ID: edgeID, StartNode: startID, EndNode: endID, Type: "HAS"}
	require.NoError(t, engine.BulkCreateEdges([]*Edge{edge}))
	head, err := engine.loadEdgeMVCCHead(edgeID)
	require.NoError(t, err)

	// Drop the adjacency keys so the repair has something to restore.
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		outgoingKey, err := engine.mvccOutgoingAdjacencyKeyString(txn, startID, edgeID, head.Version)
		if err != nil {
			return err
		}
		incomingKey, err := engine.mvccIncomingAdjacencyKeyString(txn, endID, edgeID, head.Version)
		if err != nil {
			return err
		}
		for _, key := range [][]byte{outgoingKey, incomingKey} {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	}))

	// Write a legacy variable-length version key exactly as the old writer
	// did: [prefix][string edgeID][0x00][version 16B].
	legacyKey := append([]byte{prefixMVCCEdge}, []byte(edgeID)...)
	legacyKey = append(legacyKey, 0x00)
	legacyKey = append(legacyKey, encodeMVCCSortVersion(head.Version)...)
	require.Equal(t, 1+len(edgeID)+1+16, len(legacyKey), "legacy layout length")
	payload, err := encodeMVCCEdgeRecord(edge, false)
	require.NoError(t, err)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		return txn.Set(legacyKey, payload)
	}))

	// The archived-adjacency repair must accept the legacy key.
	require.NoError(t, engine.repairArchivedEdgeAdjacency())

	// The runtime version scan must yield the legacy entry with its version.
	found := false
	_, _, err = engine.withViewEdgeMVCCVersionsFromKey([]byte{prefixMVCCEdge}, 100, func(id EdgeID, version MVCCVersion, tombstoned bool) error {
		if id == edgeID {
			found = true
			require.False(t, tombstoned)
			require.Equal(t, head.Version, version)
		}
		return nil
	})
	require.NoError(t, err)
	require.True(t, found, "legacy edge version key must be visible to the runtime scan")

	// Adjacency must be restored for the legacy-keyed edge.
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		outgoingKey, err := engine.mvccOutgoingAdjacencyKeyString(txn, startID, edgeID, head.Version)
		if err != nil {
			return err
		}
		if _, err := txn.Get(outgoingKey); err != nil {
			return err
		}
		incomingKey, err := engine.mvccIncomingAdjacencyKeyString(txn, endID, edgeID, head.Version)
		if err != nil {
			return err
		}
		_, err = txn.Get(incomingKey)
		return err
	}))

	// Malformed keys still error rather than being silently skipped.
	_, err = extractEdgeVersionFromVersionKey([]byte{prefixMVCCEdge, 0, 0})
	require.Error(t, err)
	bad := append([]byte{prefixMVCCEdge}, []byte("legacy-shape-without-separator")...)
	_, err = extractEdgeVersionFromVersionKey(bad)
	require.Error(t, err)
}
