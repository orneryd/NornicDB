package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// legacyLowerCaseKey returns key as version 3 wrote it: the label or
// relationship-type name in a label index, edge-type index, edge-between or
// count key lower-cased (#862). Other keys are returned as they are.
func legacyLowerCaseKey(key []byte) []byte {
	lower := func(start, end int) []byte {
		out := append([]byte(nil), key[:start]...)
		out = append(out, bytes.ToLower(key[start:end])...)
		return append(out, key[end:]...)
	}
	switch {
	case key[0] == prefixLabelIndex || key[0] == prefixEdgeTypeIndex:
		return lower(1, len(key)-9)
	case key[0] == prefixEdgeBetweenIndex:
		return lower(17, len(key)-9)
	case key[0] == prefixEdgeBetweenHead:
		return lower(17, len(key))
	case key[0] == prefixMVCCMeta && len(key) > 2 && (key[1] == prefixMVCCMetaLabelCount || key[1] == prefixMVCCMetaEdgeTypeCount ||
		key[1] == prefixMVCCMetaEdgeTypeStartLabelCount || key[1] == prefixMVCCMetaEdgeTypeEndLabelCount):
		separator := bytes.IndexByte(key[2:], 0)
		return lower(2+separator+1, len(key))
	}
	return key
}

// rewriteAsVersion3 rewrites the store's derived keys and the deindex
// catalogs and tombstones into version 3's lower-cased form, merging the
// counts of names that differ only in case.
func rewriteAsVersion3(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		counts := map[string]int64{}
		type entry struct{ key, value []byte }
		var entries []entry
		for _, prefix := range [][]byte{{prefixLabelIndex}, {prefixEdgeTypeIndex}, {prefixEdgeBetweenIndex}, {prefixEdgeBetweenHead},
			{prefixMVCCMeta, prefixMVCCMetaLabelCount}, {prefixMVCCMeta, prefixMVCCMetaEdgeTypeCount},
			{prefixMVCCMeta, prefixMVCCMetaEdgeTypeStartLabelCount}, {prefixMVCCMeta, prefixMVCCMetaEdgeTypeEndLabelCount},
			accessMetaKey("")} {
			it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
			for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
				value, err := it.Item().ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}
				entries = append(entries, entry{it.Item().KeyCopy(nil), value})
			}
			it.Close()
		}
		for _, e := range entries {
			if err := txn.Delete(e.key); err != nil {
				return err
			}
		}
		for _, e := range entries {
			switch {
			case e.key[0] == prefixMVCCMeta && !bytes.HasPrefix(e.key, accessMetaKey("")):
				count, err := decodeDerivedCount(e.value)
				if err != nil {
					return err
				}
				counts[string(legacyLowerCaseKey(e.key))] += count
			case bytes.HasPrefix(e.key, indexTombstoneKey(nil)):
				if err := txn.Set(indexTombstoneKey(legacyLowerCaseKey(e.key[len(indexTombstoneKey(nil)):])), e.value); err != nil {
					return err
				}
			case bytes.HasPrefix(e.key, accessMetaKey("")):
				var entry knowledgepolicy.AccessMetaEntry
				if err := msgpack.Unmarshal(e.value, &entry); err != nil {
					return err
				}
				for index, key := range entry.IndexKeys {
					entry.IndexKeys[index] = legacyLowerCaseKey(key)
				}
				if err := putAccessMetaInTxn(txn, string(e.key[len(accessMetaKey("")):]), &entry); err != nil {
					return err
				}
			default:
				if err := txn.Set(legacyLowerCaseKey(e.key), e.value); err != nil {
					return err
				}
			}
		}
		for key, count := range counts {
			if err := txn.Set([]byte(key), encodeDerivedCount(count)); err != nil {
				return err
			}
		}
		return txn.Set(cleanShutdownMarkerKey, []byte{1})
	}))
	require.NoError(t, engine.writeSchemaVersion(storageVersionPropKeyDictV2))
}

func keysUnder(t *testing.T, engine *BadgerEngine, prefix ...byte) []string {
	t.Helper()
	var keys []string
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, string(it.Item().KeyCopy(nil)))
		}
		return nil
	}))
	sort.Strings(keys)
	return keys
}

func snapshotGraphDigest(t *testing.T, db *badger.DB) ([]byte, int, int) {
	t.Helper()
	digest := sha256.New()
	counts := []int{0, 0}
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		for index, prefix := range []byte{prefixNode, prefixEdge} {
			iterator := txn.NewIterator(badgerPrefixIteratorOptions([]byte{prefix}))
			for iterator.Rewind(); iterator.ValidForPrefix([]byte{prefix}); iterator.Next() {
				digest.Write(iterator.Item().Key())
				if err := iterator.Item().Value(func(value []byte) error { _, err := digest.Write(value); return err }); err != nil {
					iterator.Close()
					return err
				}
				counts[index]++
			}
			iterator.Close()
		}
		return nil
	}))
	return digest.Sum(nil), counts[0], counts[1]
}

func TestInstalledSnapshotCombinedUpgrade(t *testing.T) {
	source := os.Getenv("NORNICDB_STORAGE_MIGRATION_SNAPSHOT")
	if source == "" {
		t.Skip("requires an offline installed-database snapshot")
	}
	resolved, err := filepath.EvalSymlinks(source)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(resolved, "/private/tmp/nornicdb-v3-installed-snapshot-"), "only scratch snapshots are allowed")
	copyDir := filepath.Join(t.TempDir(), "badger")
	require.NoError(t, exec.Command("/bin/cp", "-cR", source, copyDir).Run())
	raw, err := badger.Open(badger.DefaultOptions(copyDir).WithLogger(nil))
	require.NoError(t, err)
	before, nodes, edges := snapshotGraphDigest(t, raw)
	var observed uint64
	require.NoError(t, raw.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(mvccSchemaVersionKey())
		if err != nil {
			return err
		}
		if err := item.Value(func(value []byte) error { observed = binary.BigEndian.Uint64(value); return nil }); err != nil {
			return err
		}
		if observed == storageVersionEdgeAdjacencyV3 {
			version := make([]byte, 8)
			binary.BigEndian.PutUint64(version, storageVersionPropKeyDictV2)
			return txn.Set(mvccSchemaVersionKey(), version)
		}
		return nil
	}))
	require.NoError(t, raw.Close())
	t.Logf("offline snapshot version=%d, nodes=%d, edges=%d; only scratch clone replays the combined upgrade", observed, nodes, edges)
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: copyDir, AllowStorageUpgrade: true})
	require.NoError(t, err)
	version, err := engine.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionEdgeAdjacencyV3, version)
	require.NoError(t, engine.Close())
	raw, err = badger.Open(badger.DefaultOptions(copyDir).WithReadOnly(true).WithLogger(nil))
	require.NoError(t, err)
	after, afterNodes, afterEdges := snapshotGraphDigest(t, raw)
	require.NoError(t, raw.Close())
	require.Equal(t, nodes, afterNodes)
	require.Equal(t, edges, afterEdges)
	require.Equal(t, before, after, "node and relationship bodies must remain byte-for-byte unchanged")
	t.Logf("combined upgrade preserves %d node bodies and %d relationship bodies, SHA-256=%x", nodes, edges, after)
}

func TestStoragePrefixAssignmentsRemainStable(t *testing.T) {
	seen := map[byte]bool{}
	for index, prefix := range []byte{
		prefixNode, prefixEdge, prefixLabelIndex, prefixOutgoingIndex, prefixIncomingIndex,
		prefixEdgeTypeIndex, prefixPendingEmbed, prefixEmbedding, prefixSchema,
		prefixTemporalIndex, prefixTemporalHead, prefixMVCCNode, prefixMVCCEdge,
		prefixMVCCNodeHead, prefixMVCCEdgeHead, prefixMVCCMeta, prefixEdgeBetweenIndex, prefixEdgeBetweenHead,
	} {
		require.Equal(t, byte(index+1), prefix, "original prefix %02x must not be reassigned", index+1)
		require.False(t, seen[prefix])
		seen[prefix] = true
	}
	for index, prefix := range []byte{
		prefixIDDictNodeForward, prefixIDDictEdgeForward, prefixIDDictCounter,
		prefixIDDictNodeReverse, prefixIDDictEdgeReverse, prefixIDFreelist,
		prefixPropKeyForward, prefixPropKeyReverse, prefixPropKeyCounter,
		prefixMVCCOutgoingAdj, prefixMVCCIncomingAdj, prefixMVCCPruneFloor,
	} {
		require.Equal(t, byte(index+0x1A), prefix, "allocated prefix %02x must remain stable", index+0x1A)
		require.False(t, seen[prefix])
		seen[prefix] = true
	}
}

func TestMigrationV2ToV3PreservesLegacyPolicyMetadata(t *testing.T) {
	options := BadgerOptions{DataDir: filepath.Join(t.TempDir(), "badger"), AllowStorageUpgrade: true}
	engine, err := NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
			_ = engine.Close()
		}
	})
	node := &Node{ID: "test:legacy", Labels: []string{"Person"}}
	_, err = engine.CreateNode(node)
	require.NoError(t, err)
	catalog, err := engine.GetIndexEntryCatalog(string(node.ID))
	require.NoError(t, err)
	catalog.Deindexed = true
	meta := &knowledgepolicy.AccessMetaEntry{
		TargetID: string(node.ID), TargetScope: knowledgepolicy.ScopeNode,
		Fixed:         knowledgepolicy.AccessMetaFixedFields{AccessCount: 42},
		Overflow:      map[string]interface{}{"custom": int64(7)},
		KalmanFilters: map[string]*knowledgepolicy.KalmanPropertyState{"rate": {FilteredValue: 3.5}},
	}
	work := &DeindexWorkItem{WorkItemID: "deindex:test:legacy", TargetID: string(node.ID), TargetScope: "NODE", Status: "pending", RetryCount: 2}
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		for _, record := range []struct {
			prefix byte
			id     string
			value  interface{}
		}{
			{0x11, string(node.ID), meta}, {0x12, string(node.ID), catalog}, {0x13, work.WorkItemID, work},
		} {
			value, err := msgpack.Marshal(record.value)
			if err != nil {
				return err
			}
			if err := txn.Set(append([]byte{record.prefix}, []byte(record.id)...), value); err != nil {
				return err
			}
		}
		if err := txn.Set(append([]byte{0x17}, catalog.IndexKeys[0]...), []byte{}); err != nil {
			return err
		}
		return txn.Delete(accessMetaKey(string(node.ID)))
	}))
	require.NoError(t, engine.writeSchemaVersion(storageVersionPropKeyDictV2))
	require.NoError(t, engine.Close())
	engine = nil
	engine, err = NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	got, err := engine.GetAccessMeta(string(node.ID))
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, int64(42), got.Fixed.AccessCount)
	require.Equal(t, int64(7), got.Overflow["custom"])
	require.Equal(t, 3.5, got.KalmanFilters["rate"].FilteredValue)
	require.True(t, got.Deindexed)
	gotWork, err := engine.GetDeindexWorkItem(work.WorkItemID)
	require.NoError(t, err)
	require.Equal(t, work, gotWork)
	require.True(t, hasTombstoneKey(t, engine, got.IndexKeys[0]))
}

func TestMigrationV3LegacyRelationshipHeadCatalogCollision(t *testing.T) {
	options := BadgerOptions{DataDir: filepath.Join(t.TempDir(), "badger"), AllowStorageUpgrade: true}
	engine, err := NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
			_ = engine.Close()
		}
	})
	start := NodeID("nornic:007a09cd-ab24-4fbf-a307-07cb74ed2469")
	end := NodeID("nornic:f2e09b17-6bc2-4f12-ac71-85ae3b346949")
	for _, id := range []NodeID{start, end} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Room"}})
		require.NoError(t, err)
	}
	edge := &Edge{ID: "nornic:legacy-edge", StartNode: start, EndNode: end, Type: "in_room"}
	require.NoError(t, engine.CreateEdge(edge))
	legacyKey := append([]byte{0x12}, []byte(string(start)+"\x00"+string(end)+"\x00"+edge.Type)...)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		return txn.Set(legacyKey, []byte(edge.ID))
	}))
	require.NoError(t, engine.writeSchemaVersion(storageVersionPropKeyDictV2))
	require.NoError(t, engine.Close())
	engine = nil
	engine, err = NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	version, err := engine.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionEdgeAdjacencyV3, version)
	got, err := engine.GetEdge(edge.ID)
	require.NoError(t, err)
	require.Equal(t, edge.ID, got.ID)
	catalog, err := engine.GetIndexEntryCatalog(string(edge.ID))
	require.NoError(t, err)
	require.NotNil(t, catalog)
	require.Equal(t, string(edge.ID), catalog.TargetID)
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(legacyKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
}

func TestMigrationV2ToV3MakesLabelAndTypeKeysExactCase(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "badger")
	options := BadgerOptions{DataDir: dataDir}
	engine, err := NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
			_ = engine.Close()
		}
	})

	upper, lower, both := NodeID("case:upper"), NodeID("case:lower"), NodeID("case:both")
	for _, node := range []*Node{
		{ID: upper, Labels: []string{"Person"}},
		{ID: lower, Labels: []string{"person"}},
		{ID: both, Labels: []string{"Person", "Other"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	for _, edge := range []*Edge{
		{ID: "case:KNOWS", StartNode: upper, EndNode: lower, Type: "KNOWS"},
		{ID: "case:knows", StartNode: upper, EndNode: lower, Type: "knows"},
		{ID: "case:KNOWS2", StartNode: both, EndNode: lower, Type: "KNOWS"},
	} {
		require.NoError(t, engine.CreateEdge(edge))
	}
	// A deindexed node and relationship: their tombstones must follow the
	// new keys.
	for _, id := range []string{string(upper), "case:KNOWS"} {
		catalog, err := engine.GetIndexEntryCatalog(id)
		require.NoError(t, err)
		require.NotNil(t, catalog)
		require.NoError(t, engine.WriteIndexTombstones(catalog.IndexKeys))
		catalog.Deindexed = true
		require.NoError(t, engine.PutIndexEntryCatalog(id, catalog))
	}
	exactLabelKeys := keysUnder(t, engine, prefixLabelIndex)
	exactTypeKeys := keysUnder(t, engine, prefixEdgeTypeIndex)
	exactBetweenKeys := keysUnder(t, engine, prefixEdgeBetweenIndex)
	exactHeadKeys := keysUnder(t, engine, prefixEdgeBetweenHead)
	exactTombstones := keysUnder(t, engine, indexTombstoneKey(nil)...)
	require.Len(t, exactHeadKeys, 3, "one head per (start, end, type)")

	rewriteAsVersion3(t, engine)
	require.Len(t, keysUnder(t, engine, prefixEdgeBetweenHead), 2, "version 3 shared one head for KNOWS and knows")
	require.NoError(t, engine.Close())
	engine = nil

	_, err = NewBadgerEngineWithOptions(options)
	var upgradeErr *ErrStorageUpgradeRequired
	require.ErrorAs(t, err, &upgradeErr)
	require.Equal(t, storageVersionPropKeyDictV2, upgradeErr.OnDisk)

	options.AllowStorageUpgrade = true
	engine, err = NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	version, err := engine.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionEdgeAdjacencyV3, version)

	require.Equal(t, exactLabelKeys, keysUnder(t, engine, prefixLabelIndex))
	require.Equal(t, exactTypeKeys, keysUnder(t, engine, prefixEdgeTypeIndex))
	require.Equal(t, exactBetweenKeys, keysUnder(t, engine, prefixEdgeBetweenIndex))
	require.Equal(t, exactHeadKeys, keysUnder(t, engine, prefixEdgeBetweenHead))
	require.Equal(t, exactTombstones, keysUnder(t, engine, indexTombstoneKey(nil)...))
	for _, id := range []string{string(upper), "case:KNOWS"} {
		catalog, err := engine.GetIndexEntryCatalog(id)
		require.NoError(t, err)
		require.True(t, catalog.Deindexed)
		for _, key := range catalog.IndexKeys {
			require.Contains(t, exactTombstones, string(indexTombstoneKey(key)), id)
		}
	}
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(cleanShutdownMarkerKey)
		require.ErrorIs(t, err, badger.ErrKeyNotFound, "derived indexes are rebuilt before serving")
		return nil
	}))

	ids := func(nodes []*Node) []string {
		out := make([]string, 0, len(nodes))
		for _, node := range nodes {
			out = append(out, string(node.ID))
		}
		sort.Strings(out)
		return out
	}
	nodes, err := engine.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Equal(t, []string{"case:both", "case:upper"}, ids(nodes))
	nodes, err = engine.GetNodesByLabel("person")
	require.NoError(t, err)
	require.Equal(t, []string{"case:lower"}, ids(nodes))
	nodes, err = engine.GetNodesByLabel("PERSON")
	require.NoError(t, err)
	require.Empty(t, nodes)

	for label, want := range map[string]int64{"Person": 2, "person": 1, "PERSON": 0, "Other": 1} {
		count, err := engine.NodeCountByLabel(label)
		require.NoError(t, err)
		require.Equal(t, want, count, label)
	}
	for edgeType, want := range map[string]int64{"KNOWS": 2, "knows": 1, "Knows": 0} {
		count, err := engine.EdgeCountByType(edgeType)
		require.NoError(t, err)
		require.Equal(t, want, count, edgeType)
	}
	edge := engine.GetEdgeBetween(upper, lower, "knows")
	require.NotNil(t, edge)
	require.Equal(t, EdgeID("case:knows"), edge.ID)
	edge = engine.GetEdgeBetween(upper, lower, "KNOWS")
	require.NotNil(t, edge)
	require.Equal(t, EdgeID("case:KNOWS"), edge.ID)
	require.Nil(t, engine.GetEdgeBetween(upper, lower, "Knows"))
	edges, err := engine.GetEdgesByType("knows")
	require.NoError(t, err)
	require.Len(t, edges, 1)
	require.True(t, strings.HasSuffix(string(edges[0].ID), "knows"))
}
