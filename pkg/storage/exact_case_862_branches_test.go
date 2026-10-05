package storage

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Branches of the #862 rebuild helpers used by the v2→v3 migration.

// oversizedName is longer than Badger's maximum key size, so any index key
// that holds it can't be written.
var oversizedName = strings.Repeat("L", 70_000)

func writeRawNode(t *testing.T, engine *BadgerEngine, node *Node) {
	t.Helper()
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		namespace, _, _ := ParseDatabasePrefix(string(node.ID))
		body, _, err := engine.encodeNodeInTxn(txn, namespace, node)
		if err != nil {
			return err
		}
		return txn.Set(nodeKey(node.ID), body)
	}))
}

func writeRawEdge(t *testing.T, engine *BadgerEngine, edge *Edge) {
	t.Helper()
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		namespace, _, _ := ParseDatabasePrefix(string(edge.ID))
		body, err := engine.encodeEdgeInTxn(txn, namespace, edge)
		if err != nil {
			return err
		}
		return txn.Set(edgeKey(edge.ID), body)
	}))
}

func writeRawValue(t *testing.T, engine *BadgerEngine, key, value []byte) {
	t.Helper()
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	}))
}

func TestForEachStoredRecordInChunks(t *testing.T) {
	engine := createTestBadgerEngine(t)
	for _, id := range []NodeID{"test:a", "test:b", "test:c"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	var logged bytes.Buffer
	scan := storedRecordScan{prefix: prefixNode, batchSize: 1, logEvery: 2, log: slog.New(slog.NewTextHandler(&logged, nil)), message: "scan progress", unit: "nodes"}
	var visited []string
	processed, err := engine.forEachStoredRecordInChunks(context.Background(), scan, func(txn *badger.Txn, key, value []byte) (int, bool, error) {
		visited = append(visited, string(key[1:]))
		return 1, true, nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, processed)
	require.Equal(t, []string{"test:a", "test:b", "test:c"}, visited, "one write per chunk resumes at the next key")
	require.Contains(t, logged.String(), "scan progress")
	require.Contains(t, logged.String(), "nodes=2")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = engine.forEachStoredRecordInChunks(ctx, scan, func(*badger.Txn, []byte, []byte) (int, bool, error) { return 0, true, nil })
	require.ErrorIs(t, err, context.Canceled)
}

func TestRebuildEdgeTypeIndexBranches(t *testing.T) {
	t.Run("skips short keys and untyped edges; rebuilds the typed one", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:a"})
		require.NoError(t, err)
		_, err = engine.CreateNode(&Node{ID: "test:b"})
		require.NoError(t, err)
		require.NoError(t, engine.CreateEdge(&Edge{ID: "test:typed", StartNode: "test:a", EndNode: "test:b", Type: "Knows"}))
		writeRawEdge(t, engine, &Edge{ID: "test:untyped", StartNode: "test:a", EndNode: "test:b"})
		writeRawValue(t, engine, []byte{prefixEdge}, []byte{0})

		var nilCtx context.Context
		processed, err := engine.rebuildEdgeTypeIndex(nilCtx)
		require.NoError(t, err)
		require.Equal(t, 2, processed)
		edges, err := engine.GetEdgesByType("Knows")
		require.NoError(t, err)
		require.Len(t, edges, 1)
	})
	t.Run("decode error", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		writeRawValue(t, engine, edgeKey("test:bad"), []byte{0xFF, 0x00})
		_, err := engine.rebuildEdgeTypeIndex(context.Background())
		require.ErrorContains(t, err, "decode edge for edge-type index")
	})
	t.Run("write error", func(t *testing.T) {
		engine, _ := createTestBadgerEngineOnDisk(t)
		_, err := engine.CreateNode(&Node{ID: "test:a"})
		require.NoError(t, err)
		writeRawEdge(t, engine, &Edge{ID: "test:huge", StartNode: "test:a", EndNode: "test:a", Type: oversizedName})
		_, err = engine.rebuildEdgeTypeIndex(context.Background())
		require.ErrorContains(t, err, "write edge-type index")
	})
	t.Run("closed engine", func(t *testing.T) {
		engine, err := NewBadgerEngineInMemory()
		require.NoError(t, err)
		require.NoError(t, engine.Close())
		_, err = engine.rebuildEdgeTypeIndex(context.Background())
		require.ErrorContains(t, err, "clear edge-type index before rebuild")
	})
}

func TestRebuildLabelAndEdgeBetweenIndexSkipsAndWriteErrors(t *testing.T) {
	t.Run("label index skips short keys and nodes without a database", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"L"}})
		require.NoError(t, err)
		writeRawValue(t, engine, []byte{prefixNode}, []byte{0})
		writeRawValue(t, engine, nodeKey("nodatabase"), []byte{0})
		processed, err := engine.rebuildLabelIndex(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, processed)
	})
	t.Run("label index write error", func(t *testing.T) {
		engine, _ := createTestBadgerEngineOnDisk(t)
		writeRawNode(t, engine, &Node{ID: "test:huge", Labels: []string{oversizedName}})
		_, err := engine.rebuildLabelIndex(context.Background())
		require.ErrorContains(t, err, "write label index")
	})
	t.Run("edge-between index write error", func(t *testing.T) {
		engine, _ := createTestBadgerEngineOnDisk(t)
		_, err := engine.CreateNode(&Node{ID: "test:a"})
		require.NoError(t, err)
		writeRawEdge(t, engine, &Edge{ID: "test:huge", StartNode: "test:a", EndNode: "test:a", Type: oversizedName})
		_, err = engine.rebuildEdgeBetweenIndex(context.Background())
		require.ErrorContains(t, err, "edge-between index")
	})
}

func putCatalog(t *testing.T, engine *BadgerEngine, cat *IndexEntryCatalog) {
	t.Helper()
	require.NoError(t, engine.PutIndexEntryCatalog(cat.TargetID, cat))
}

func TestRewriteIndexEntryCatalogsBranches(t *testing.T) {
	t.Run("leaves catalogs without a current entity", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		stale := [][]byte{[]byte("stale")}
		putCatalog(t, engine, &IndexEntryCatalog{TargetID: "test:missing", TargetScope: "NODE", IndexKeys: stale})
		putCatalog(t, engine, &IndexEntryCatalog{TargetID: "nodatabase", TargetScope: "NODE", IndexKeys: stale})
		putCatalog(t, engine, &IndexEntryCatalog{TargetID: "test:other", TargetScope: "OTHER", IndexKeys: stale})
		require.NoError(t, engine.rewriteIndexEntryCatalogs(context.Background()))
		for _, id := range []string{"test:missing", "nodatabase", "test:other"} {
			cat, err := engine.GetIndexEntryCatalog(id)
			require.NoError(t, err)
			require.Equal(t, stale, cat.IndexKeys, id)
		}
	})
	t.Run("sets a node's current keys", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		_, err := engine.CreateNode(&Node{ID: "test:n", Labels: []string{"A", "B"}})
		require.NoError(t, err)
		current, err := engine.GetIndexEntryCatalog("test:n")
		require.NoError(t, err)
		putCatalog(t, engine, &IndexEntryCatalog{TargetID: "test:n", TargetScope: "NODE", IndexKeys: current.IndexKeys[:1]})
		require.NoError(t, engine.rewriteIndexEntryCatalogs(context.Background()))
		cat, err := engine.GetIndexEntryCatalog("test:n")
		require.NoError(t, err)
		require.Equal(t, current.IndexKeys, cat.IndexKeys)
	})
	t.Run("decode errors", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		writeRawValue(t, engine, indexEntryCatalogKey("test:garbage"), []byte{0xC1})
		require.ErrorContains(t, engine.rewriteIndexEntryCatalogs(context.Background()), "decode index entry catalog")

		engine = createTestBadgerEngine(t)
		writeRawValue(t, engine, nodeKey("test:badnode"), []byte{0xFF, 0x00})
		putCatalog(t, engine, &IndexEntryCatalog{TargetID: "test:badnode", TargetScope: "NODE"})
		require.ErrorContains(t, engine.rewriteIndexEntryCatalogs(context.Background()), "decode node")

		engine = createTestBadgerEngine(t)
		writeRawValue(t, engine, edgeKey("test:badedge"), []byte{0xFF, 0x00})
		putCatalog(t, engine, &IndexEntryCatalog{TargetID: "test:badedge", TargetScope: "EDGE"})
		require.ErrorContains(t, engine.rewriteIndexEntryCatalogs(context.Background()), "decode edge")
	})
	t.Run("a closed transaction fails the body read", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			txn.Discard()
			require.Error(t, readStoredBody(txn, nodeKey("test:x"), func([]byte) error { return nil }))
			return nil
		}))
	})
	require.False(t, sameIndexKeys([][]byte{{1}}, [][]byte{{1}, {2}}))
	require.False(t, sameIndexKeys([][]byte{{1}}, [][]byte{{2}}))
	require.True(t, sameIndexKeys([][]byte{{1}}, [][]byte{{1}}))
}

func TestLegacyPolicyMetadataTransferBranches(t *testing.T) {
	for _, prefix := range []byte{0x11, 0x12, 0x13} {
		t.Run(fmt.Sprintf("malformed-%02x", prefix), func(t *testing.T) {
			engine := createTestBadgerEngine(t)
			key := append([]byte{prefix}, []byte("test:malformed")...)
			writeRawValue(t, engine, key, []byte{0xC1})
			require.ErrorContains(t, engine.migrateLegacyPolicyMetadata(context.Background()), "decode legacy")
			require.NoError(t, engine.withView(func(txn *badger.Txn) error { _, err := txn.Get(key); return err }))
		})
	}
	t.Run("serialized metadata with separator bytes is not a relationship head", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		entityID := "test:entity\x00suffix"
		entry := &knowledgepolicy.AccessMetaEntry{TargetID: entityID, Fixed: knowledgepolicy.AccessMetaFixedFields{AccessCount: 42}}
		value, err := msgpack.Marshal(entry)
		require.NoError(t, err)
		writeRawValue(t, engine, append([]byte{0x11}, []byte(entityID)...), value)
		require.NoError(t, engine.migrateLegacyPolicyMetadata(context.Background()))
		got, err := engine.GetAccessMeta(entityID)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, int64(42), got.Fixed.AccessCount)
	})
	t.Run("relationship prefixes in index markers are restored", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		for _, prefix := range []byte{0x18, 0x19, 0x03} {
			key := []byte{prefix, 1, 2}
			writeRawValue(t, engine, append([]byte{0x17}, key...), nil)
		}
		require.NoError(t, engine.migrateLegacyPolicyMetadata(context.Background()))
		for _, prefix := range []byte{prefixEdgeBetweenIndex, prefixEdgeBetweenHead, prefixLabelIndex} {
			require.True(t, hasTombstoneKey(t, engine, []byte{prefix, 1, 2}))
		}
		restoreRelationshipPrefix(nil)
	})
}

func TestRebuildCaseSensitiveIndexesErrors(t *testing.T) {
	t.Run("label index", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		writeRawValue(t, engine, nodeKey("test:bad"), []byte{0xFF, 0x00})
		require.ErrorContains(t, engine.rebuildCaseSensitiveIndexes(), "rebuild label index")
	})
	t.Run("edge-type index", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		writeRawValue(t, engine, edgeKey("test:bad"), []byte{0xFF, 0x00})
		require.ErrorContains(t, engine.rebuildCaseSensitiveIndexes(), "rebuild edge-type index")
	})
	t.Run("edge-between index", func(t *testing.T) {
		engine, _ := createTestBadgerEngineOnDisk(t)
		_, err := engine.CreateNode(&Node{ID: "test:a"})
		require.NoError(t, err)
		// A type-index key (type + 10 bytes) fits Badger's 65,000-byte key
		// limit; an edge-between key (type + 26 bytes) doesn't.
		writeRawEdge(t, engine, &Edge{ID: "test:long", StartNode: "test:a", EndNode: "test:a", Type: strings.Repeat("T", 64_985)})
		require.ErrorContains(t, engine.rebuildCaseSensitiveIndexes(), "rebuild edge-between index")
	})
	t.Run("catalogs", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		writeRawValue(t, engine, indexEntryCatalogKey("test:garbage"), []byte{0xC1})
		require.ErrorContains(t, engine.rebuildCaseSensitiveIndexes(), "rewrite index entry catalogs")
	})
	t.Run("deindexed tombstone move", func(t *testing.T) {
		engine := createTestBadgerEngine(t)
		require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
			require.NoError(t, moveIndexTombstonesInTxn(txn, [][]byte{[]byte("old")}, [][]byte{[]byte("new")}))
			return nil
		}))
		require.False(t, hasTombstoneKey(t, engine, []byte("old")))
		require.True(t, hasTombstoneKey(t, engine, []byte("new")))
		require.NoError(t, engine.withView(func(txn *badger.Txn) error {
			txn.Discard()
			require.Error(t, moveIndexTombstonesInTxn(txn, [][]byte{[]byte("old")}, nil))
			require.Error(t, moveIndexTombstonesInTxn(txn, nil, [][]byte{[]byte("new")}))
			return nil
		}))
	})
}

func hasTombstoneKey(t *testing.T, engine *BadgerEngine, key []byte) bool {
	t.Helper()
	found := false
	require.NoError(t, engine.withView(func(txn *badger.Txn) error {
		found = hasIndexTombstone(txn, key)
		return nil
	}))
	return found
}

func TestStartupUpgradeFailsOnAFailedV2ToV3IndexRebuild(t *testing.T) {
	options := BadgerOptions{DataDir: filepath.Join(t.TempDir(), "badger")}
	engine, err := NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:a", Labels: []string{"L"}})
	require.NoError(t, err)
	writeRawValue(t, engine, nodeKey("test:bad"), []byte{0xFF, 0x00})
	require.NoError(t, engine.writeSchemaVersion(storageVersionPropKeyDictV2))
	require.NoError(t, engine.Close())

	options.AllowStorageUpgrade = true
	_, err = NewBadgerEngineWithOptions(options)
	require.ErrorContains(t, err, "migration v2→v3 failed")
}

// The async engine's endpoint-label counts overlay pending writes by exact
// label: a pending update of a stored relationship replaces its counts.
func TestAsyncEngineEndpointLabelCountsAreCaseSensitive(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	_, err := base.CreateNode(&Node{ID: "test:a", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: "test:b", Labels: []string{"Thing"}})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "R"}))

	ae := NewAsyncEngine(base, &AsyncEngineConfig{FlushInterval: time.Hour})
	t.Cleanup(func() { _ = ae.Close() })
	require.NoError(t, ae.UpdateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]interface{}{"w": 1}}))
	require.NoError(t, ae.CreateEdge(&Edge{ID: "test:f", StartNode: "test:a", EndNode: "test:b", Type: "R"}))

	for _, tc := range []struct {
		start bool
		label string
		typ   string
		want  int64
	}{
		{true, "Person", "R", 2}, {true, "person", "R", 0},
		{false, "Thing", "R", 2}, {false, "thing", "R", 0},
	} {
		count := func() (int64, error) {
			if tc.start {
				return ae.EdgeCountByStartLabel(tc.label, tc.typ)
			}
			return ae.EdgeCountByEndLabel(tc.label, tc.typ)
		}
		got, err := count()
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "%+v", tc)
	}
}

// Relabelling a node in a transaction that also created a relationship from
// it moves the relationship's endpoint-label count to the new label.
func TestTransactionRelabelMovesEndpointLabelCounts(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"Thing"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:a", Labels: []string{"person"}}))
	require.NoError(t, tx.Commit())

	for label, want := range map[string]int64{"person": 1, "Person": 0} {
		count, err := engine.EdgeCountByStartLabel(label, "R")
		require.NoError(t, err)
		require.Equal(t, want, count, label)
	}
}
