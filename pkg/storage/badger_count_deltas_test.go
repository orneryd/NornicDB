package storage

import (
	"encoding/binary"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// TestCountDeltaPrefixesDisjoint pins the delta-key prefixes against the
// positional (label, type) count prefixes: they are different dimensions and
// must never share a key byte.
func TestCountDeltaPrefixesDisjoint(t *testing.T) {
	distinct := map[byte]string{}
	for _, prefix := range []struct {
		value byte
		name  string
	}{
		{prefixMVCCMetaLabelCount, "label count"},
		{prefixMVCCMetaEdgeTypeCount, "edge-type count"},
		{prefixMVCCMetaLabelCountDelta, "label count delta"},
		{prefixMVCCMetaEdgeTypeCountDelta, "edge-type count delta"},
		{prefixMVCCMetaEdgeTypeStartLabelCount, "start (label, type) count"},
		{prefixMVCCMetaEdgeTypeEndLabelCount, "end (label, type) count"},
	} {
		if previous, exists := distinct[prefix.value]; exists {
			t.Fatalf("prefix collision: %s and %s share 0x%02X", previous, prefix.name, prefix.value)
		}
		distinct[prefix.value] = prefix.name
	}
}

// TestCountDeltaFoldCollapsesIntoBase pins the fold: persisted deltas sum into
// the base key and are deleted, and reads agree before and after.
func TestCountDeltaFoldCollapsesIntoBase(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	ns := "ns1"
	_, err = engine.CreateNode(&Node{ID: NodeID(ns + ":a"), Labels: []string{"Fold"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: NodeID(ns + ":b"), Labels: []string{"Fold"}})
	require.NoError(t, err)

	before, err := engine.NodeCountByLabelInNamespace(ns, "Fold")
	require.NoError(t, err)
	require.Equal(t, int64(2), before)

	require.NoError(t, engine.foldLabelCountDeltas(ns, "Fold"))

	var baseCount, remainingDeltas int64
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(labelCountKey(ns, "Fold"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			count, decodeErr := decodeDerivedCount(val)
			require.NoError(t, decodeErr)
			baseCount = count
			return nil
		})
	}))
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(countDeltaPrefixFor(countDeltaKindLabel, ns, "Fold")))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(countDeltaPrefixFor(countDeltaKindLabel, ns, "Fold")); it.Next() {
			remainingDeltas++
		}
		return nil
	}))
	require.Equal(t, int64(2), baseCount)
	require.Zero(t, remainingDeltas)

	after, err := engine.NodeCountByLabelInNamespace(ns, "Fold")
	require.NoError(t, err)
	require.Equal(t, int64(2), after)

	// Further writes append fresh deltas on top of the folded base.
	_, err = engine.CreateNode(&Node{ID: NodeID(ns + ":c"), Labels: []string{"Fold"}})
	require.NoError(t, err)
	after, err = engine.NodeCountByLabelInNamespace(ns, "Fold")
	require.NoError(t, err)
	require.Equal(t, int64(3), after)
}

// TestCountDeltaCrossNamespaceFastPath pins the by-name summation: deltas for
// the same label in two namespaces both contribute to NodeCountByLabel.
func TestCountDeltaCrossNamespaceFastPath(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	_, err = engine.CreateNode(&Node{ID: "ns1:a", Labels: []string{"Shared"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "ns2:b", Labels: []string{"Shared"}})
	require.NoError(t, err)

	total, err := engine.NodeCountByLabel("Shared")
	require.NoError(t, err)
	require.Equal(t, int64(2), total)
}

// TestCountDeltaMalformedEdgeTypeBaseRepair pins the read-side repair of a
// malformed edge-type base count: the read recomputes from the edge-type
// index and returns the correct count.
func TestCountDeltaMalformedEdgeTypeBaseRepair(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	require.NoError(t, engine.withUpdate(func(txn *badger.Txn) error {
		malformed := make([]byte, 4)
		binary.BigEndian.PutUint32(malformed, 7)
		return txn.Set(edgeTypeCountKey("ns1", "T"), malformed)
	}))

	_, err = engine.CreateNode(&Node{ID: "ns1:a"})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "ns1:b"})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{ID: "ns1:e1", Type: "T", StartNode: "ns1:a", EndNode: "ns1:b"})
	require.NoError(t, err)

	count, err := engine.EdgeCountByTypeInNamespace("ns1", "T")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

// TestCountDeltaFoldAtClose pins graceful-close folding: the deltas of a
// cleanly closed engine are folded into the base keys, and a reopen reports
// the same counts.
func TestCountDeltaFoldAtClose(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)

	_, err = engine.CreateNode(&Node{ID: "ns1:a", Labels: []string{"CloseFold"}})
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	var baseCount, remainingDeltas int64
	require.NoError(t, reopened.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(labelCountKey("ns1", "CloseFold"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			count, decodeErr := decodeDerivedCount(val)
			require.NoError(t, decodeErr)
			baseCount = count
			return nil
		})
	}))
	require.NoError(t, reopened.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(countDeltaPrefixFor(countDeltaKindLabel, "ns1", "CloseFold")))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(countDeltaPrefixFor(countDeltaKindLabel, "ns1", "CloseFold")); it.Next() {
			remainingDeltas++
		}
		return nil
	}))
	require.Equal(t, int64(1), baseCount)
	require.Zero(t, remainingDeltas)

	count, err := reopened.NodeCountByLabelInNamespace("ns1", "CloseFold")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}
