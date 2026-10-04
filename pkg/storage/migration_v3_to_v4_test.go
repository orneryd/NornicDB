package storage

import (
	"bytes"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
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
			{prefixIndexEntryCatalog}, {prefixIndexTombstone}} {
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
			case e.key[0] == prefixMVCCMeta:
				count, err := decodeDerivedCount(e.value)
				if err != nil {
					return err
				}
				counts[string(legacyLowerCaseKey(e.key))] += count
			case e.key[0] == prefixIndexEntryCatalog:
				catalog, err := engine.GetIndexEntryCatalog(string(e.key[1:]))
				if err != nil {
					return err
				}
				for i, key := range catalog.IndexKeys {
					catalog.IndexKeys[i] = legacyLowerCaseKey(key)
				}
				if err := putIndexEntryCatalogInTxn(txn, catalog.TargetID, catalog); err != nil {
					return err
				}
			case e.key[0] == prefixIndexTombstone:
				if err := txn.Set(indexTombstoneKey(legacyLowerCaseKey(e.key[1:])), e.value); err != nil {
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
	require.NoError(t, engine.writeSchemaVersion(storageVersionEdgeAdjacencyV3))
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

func TestMigrationV3ToV4MakesLabelAndTypeKeysExactCase(t *testing.T) {
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
	exactTombstones := keysUnder(t, engine, prefixIndexTombstone)
	require.Len(t, exactHeadKeys, 3, "one head per (start, end, type)")

	rewriteAsVersion3(t, engine)
	require.Len(t, keysUnder(t, engine, prefixEdgeBetweenHead), 2, "version 3 shared one head for KNOWS and knows")
	require.NoError(t, engine.Close())
	engine = nil

	_, err = NewBadgerEngineWithOptions(options)
	var upgradeErr *ErrStorageUpgradeRequired
	require.ErrorAs(t, err, &upgradeErr)
	require.Equal(t, storageVersionEdgeAdjacencyV3, upgradeErr.OnDisk)

	options.AllowStorageUpgrade = true
	engine, err = NewBadgerEngineWithOptions(options)
	require.NoError(t, err)
	version, err := engine.readSchemaVersion()
	require.NoError(t, err)
	require.Equal(t, storageVersionLabelCaseV4, version)

	require.Equal(t, exactLabelKeys, keysUnder(t, engine, prefixLabelIndex))
	require.Equal(t, exactTypeKeys, keysUnder(t, engine, prefixEdgeTypeIndex))
	require.Equal(t, exactBetweenKeys, keysUnder(t, engine, prefixEdgeBetweenIndex))
	require.Equal(t, exactHeadKeys, keysUnder(t, engine, prefixEdgeBetweenHead))
	require.Equal(t, exactTombstones, keysUnder(t, engine, prefixIndexTombstone))
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
