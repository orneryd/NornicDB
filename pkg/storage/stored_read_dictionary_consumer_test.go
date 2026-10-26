package storage

import (
	"bytes"
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The public diagnostic/repair needs only labels/endpoints/type. Keep a valid
// record envelope and break only one excluded property to catch full-body
// substitution, using the existing persisted codec rather than another grammar.
func TestStoredReadDictionaryConsumersExcludePayload(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for _, id := range []NodeID{"test:a", "test:b", "test:lone"} {
		_, err = engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"key": "excluded"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:link", StartNode: "test:a", EndNode: "test:b", Type: "REL", Properties: map[string]any{"key": "excluded"}}))
	dropDictionaryEntries(t, engine, []NodeID{"test:lone"}, []EdgeID{"test:link"})
	require.NoError(t, engine.Close())
	engine, err = NewBadgerEngine(dir)
	require.NoError(t, err)
	require.NoError(t, engine.RebuildMVCCHeads(context.Background()))
	require.NoError(t, engine.Close())
	engine, err = NewBadgerEngine(dir)
	require.NoError(t, err)
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		for _, key := range [][]byte{nodeKey("test:lone"), edgeKey("test:link")} {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			data, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			value := bytes.LastIndex(data, []byte("excluded"))
			require.Positive(t, value, "fixture must locate its one actual encoded property")
			require.Equal(t, byte(0xa8), data[value-1], "the persisted codec must encode the known fixture string as fixstr")
			data[value-1] = 0xc1
			if err := txn.Set(key, data); err != nil {
				return err
			}
		}
		return nil
	}))
	_, err = engine.GetStoredNode("test:lone")
	require.Error(t, err, "full node hydration must reject the excluded value")
	_, err = engine.GetStoredEdge("test:link")
	require.Error(t, err, "full edge hydration must reject the excluded value")
	report, err := engine.CheckIDDictionary()
	require.NoError(t, err)
	assert.Equal(t, []NodeID{"test:lone"}, report.MismatchedNodes, "labels remain valid even when an excluded property is invalid")
	require.Equal(t, []EdgeID{"test:link"}, report.MismatchedEdges)
	report, err = engine.RepairIDDictionary()
	require.NoError(t, err)
	assert.Equal(t, []NodeID{"test:lone"}, report.RestoredNodes, "label facts must not hydrate excluded properties")
	assert.Equal(t, []EdgeID{"test:link"}, report.RestoredEdges, "endpoint/type facts must not hydrate excluded properties")
	require.Empty(t, report.UnresolvedNodes)
	require.Empty(t, report.UnresolvedEdges)
}

// A rebuilding label index still uses the diagnostic's existing physical
// snapshot; invalid excluded payload must not change label membership.
func TestStoredReadDictionaryBuildingExcludesPayload(t *testing.T) {
	engine, err := NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	_, err = engine.CreateNode(&Node{ID: "test:labelled", Labels: []string{"Doc"}, Properties: map[string]any{"key": "excluded"}})
	require.NoError(t, err)
	wipeLabelIndexAndMarker(t, engine, prefixLabelIndex)
	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("test:labelled"))
		if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		value := bytes.LastIndex(data, []byte("excluded"))
		require.Positive(t, value)
		require.Equal(t, byte(0xa8), data[value-1])
		data[value-1] = 0xc1
		return txn.Set(nodeKey("test:labelled"), data)
	}))
	_, err = engine.GetStoredNode("test:labelled")
	require.Error(t, err, "full hydration is a real failing negative control")
	report, err := engine.CheckIDDictionary()
	require.NoError(t, err)
	require.Empty(t, report.Nodes)
	require.Empty(t, report.MismatchedNodes)
}
