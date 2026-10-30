package storage

// Regression test for issue #183. The reporter saw `MATCH (n:Foo)` returning
// zero rows after restart for restored / pre-existing nodes, while
// `labels(n)` and property-based selection still found them.
//
// The hypothesis in the report was that the label-lookup index used by
// MATCH (n:Label) is in-memory and not rebuilt from storage. On the
// current codebase, the label index is on-disk under prefixLabelIndex
// (0x03) and is written transactionally with every node create/update,
// so it survives a clean close/reopen by construction. These tests pin
// that contract end-to-end so a future refactor that moves the label
// index back into RAM (or splits it into a hot cache that depends on
// the asyncengine in-memory map) trips a regression here, before
// shipping.

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLabelIndex_SurvivesEngineReopen — the canonical bug-report
// reproduction. Create a labelled node, close the engine cleanly,
// reopen the same data dir, and confirm MATCH-by-label still returns
// the node. Bug as reported would surface as zero rows here.
func TestLabelIndex_SurvivesEngineReopen(t *testing.T) {
	dir := t.TempDir()

	// First open: create and label a node, close cleanly.
	engine1, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine1 != nil {
		fixtureowner.Own(t, engine1)
	}
	require.NoError(t, err)

	node := &Node{
		ID:         "test:n1",
		Labels:     []string{"Foo"},
		Properties: map[string]any{"k": int64(1)},
	}
	_, err = engine1.CreateNode(node)
	require.NoError(t, err)

	pre, err := engine1.GetNodesByLabel("Foo")
	require.NoError(t, err)
	require.Len(t, pre, 1, "in-session GetNodesByLabel must find the node")
	require.NoError(t, engine1.Close())

	// Reopen the same dir.
	engine2, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	// MATCH (n:Foo) → must still find the node.
	post, err := engine2.GetNodesByLabel("Foo")
	require.NoError(t, err)
	require.Len(t, post, 1,
		"label-index must persist across restart; got 0 nodes after reopen — this is the v1.1.1 bug-report shape")
	assert.Equal(t, NodeID("test:n1"), post[0].ID)
	assert.Contains(t, post[0].Labels, "Foo")

	// AllNodes must also see the node — confirms the storage itself is intact.
	all, err := engine2.AllNodes()
	require.NoError(t, err)
	assert.Len(t, all, 1, "node body itself must survive reopen")
}

// TestLabelIndex_BulkCreatedNodesSurviveReopen — the bug report
// describes a 5026-node restored corpus where MATCH (c:Concept) climbed
// 0 → 2657 only as the embed worker *touched* nodes. The trigger there
// was bulk creates (snapshot replay). Pin that BulkCreateNodes also
// writes the label index, so reopen sees them all.
func TestLabelIndex_BulkCreatedNodesSurviveReopen(t *testing.T) {
	for _, tc := range []struct {
		name, label, idFormat, writeNamespace, readNamespace string
		count                                                int
		bulk, wipe, properties, checkBefore                  bool
	}{
		{name: "bulk-persisted", label: "Concept", idFormat: "test:concept-%05d", count: 250, bulk: true, properties: true, checkBefore: true},
		{name: "bulk-rebuilt", label: "Concept", idFormat: "test:concept-%05d", count: 250, bulk: true, wipe: true, checkBefore: true},
		{name: "namespaced-rebuilt", label: "Memory", idFormat: "ns-rebuild-%02d", writeNamespace: "nornic", readNamespace: "nornic", count: 30, wipe: true},
		{name: "namespaced-persisted", label: "NSLabel", idFormat: "ns-routed-%02d", writeNamespace: "nornic", readNamespace: "nornic", count: 10},
		{name: "namespaced-write-raw-read", label: "AsyncOnly", idFormat: "test:async-%03d", writeNamespace: "test", count: 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			before, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
			if before != nil {
				fixtureowner.Own(t, before)
			}
			require.NoError(t, err)
			var writer Engine = before
			if tc.writeNamespace != "" {
				writer = NewNamespacedEngine(before, tc.writeNamespace)
			}
			nodes := make([]*Node, tc.count)
			for i := range nodes {
				nodes[i] = &Node{ID: NodeID(fmt.Sprintf(tc.idFormat, i)), Labels: []string{tc.label}}
				if tc.properties {
					nodes[i].Properties = map[string]any{"i": int64(i)}
				}
			}
			if tc.bulk {
				require.NoError(t, before.BulkCreateNodes(nodes))
			} else {
				for _, node := range nodes {
					_, err = writer.CreateNode(node)
					require.NoError(t, err)
				}
			}
			if tc.readNamespace != "" {
				_, err = NewNamespacedEngine(before, "other").CreateNode(&Node{ID: "scope-control", Labels: []string{tc.label}})
				require.NoError(t, err)
			}
			if tc.wipe {
				wipeLabelIndexAndMarker(t, before, prefixLabelIndex)
			}
			if tc.checkBefore {
				got, err := before.GetNodesByLabel(tc.label)
				require.NoError(t, err)
				// A wiped index has no ready marker, so reads use the stored
				// node bodies and still see every node.
				require.Len(t, got, tc.count, "pre-reopen label read")
			}
			require.NoError(t, before.Close())
			after, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
			require.NoError(t, err)
			t.Cleanup(func() { _ = after.Close() })
			if tc.wipe {
				require.True(t, waitForLabelIndexReady(t, after, 5*time.Second), "backfill marker must land within5s")
			}
			var reader Engine = after
			if tc.readNamespace != "" {
				reader = NewNamespacedEngine(after, tc.readNamespace)
			}
			got, err := reader.GetNodesByLabel(tc.label)
			require.NoError(t, err)
			assert.Len(t, got, tc.count, "persisted or rebuilt label entries must survive reopen")
			if tc.readNamespace != "" {
				control, err := NewNamespacedEngine(after, "other").GetNodesByLabel(tc.label)
				require.NoError(t, err)
				require.Len(t, control, 1, "other namespace must retain its own label row")
				assert.Equal(t, NodeID("scope-control"), control[0].ID)
				all, err := after.GetNodesByLabel(tc.label)
				require.NoError(t, err)
				assert.Len(t, all, tc.count+1, "raw read must include both scopes")
			}
		})
	}
}

// TestLabelIndex_MultiLabelSurvivesReopen — the bug report cites
// `MATCH (c:Aprodan:Academic:Concept)` returning 0 against 227 true.
// Pin: multi-label nodes write a label-index entry per label, and
// every label is independently queryable after reopen.
func TestLabelIndex_MultiLabelSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	engine1, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine1 != nil {
		fixtureowner.Own(t, engine1)
	}
	require.NoError(t, err)

	node := &Node{
		ID:     "test:multi-1",
		Labels: []string{"Aprodan", "Academic", "Concept"},
	}
	_, err = engine1.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, engine1.Close())

	engine2, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	for _, label := range []string{"Aprodan", "Academic", "Concept"} {
		got, err := engine2.GetNodesByLabel(label)
		require.NoError(t, err)
		require.Len(t, got, 1, "label %q must surface the node after reopen", label)
		assert.Equal(t, NodeID("test:multi-1"), got[0].ID)
	}
}

// TestLabelIndex_DiskKeyExistsAfterReopen — peek directly at the
// Badger key space after reopen and confirm the label-index keys
// (prefix 0x03) are present. This pins the on-disk shape, so a
// future refactor that moves label storage out of Badger surfaces
// here rather than producing the silent zero-rows symptom.
func TestLabelIndex_DiskKeyExistsAfterReopen(t *testing.T) {
	dir := t.TempDir()

	engine1, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine1 != nil {
		fixtureowner.Own(t, engine1)
	}
	require.NoError(t, err)
	_, err = engine1.CreateNode(&Node{ID: "test:disk-1", Labels: []string{"DiskCheck"}})
	require.NoError(t, err)
	require.NoError(t, engine1.Close())

	// Reopen and walk the label-index key range directly.
	engine2, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	var found int
	require.NoError(t, engine2.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte{prefixLabelIndex}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
			found++
		}
		return nil
	}))
	assert.Greater(t, found, 0,
		"prefixLabelIndex (0x03) range must contain entries after reopen; got 0 — would surface as MATCH (n:Label) returning zero rows")
}

// TestLabelIndex_IDDictionaryReverseMapPopulatedOnReopen — the
// label-index value path goes through idDict.lookupNodeIDByNum to map
// the 8-byte numID back to a string node ID. If the reverse map is
// empty after reopen, GetNodesByLabel silently skips every entry and
// returns []*Node{} with no error — exactly the bug-report shape.
// Pin that loadFromBadger populates nodeReverse so the lookup
// succeeds.
func TestLabelIndex_IDDictionaryReverseMapPopulatedOnReopen(t *testing.T) {
	dir := t.TempDir()

	engine1, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine1 != nil {
		fixtureowner.Own(t, engine1)
	}
	require.NoError(t, err)
	_, err = engine1.CreateNode(&Node{ID: "test:dict-1", Labels: []string{"L"}})
	require.NoError(t, err)

	// Capture the numID assigned during the first session so we can
	// confirm the reverse map carries it after reopen.
	num, ok := engine1.idDict.lookupNodeNumID("test:dict-1")
	require.True(t, ok, "node must have a numID after CreateNode")
	require.NoError(t, engine1.Close())

	engine2, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	gotID, ok := engine2.idDict.lookupNodeIDByNum(num)
	require.True(t, ok,
		"id-dict reverse map must hold numID %d after reopen; without this, GetNodesByLabel silently drops every label-index hit", num)
	assert.Equal(t, NodeID("test:dict-1"), gotID)
}

// TestLabelIndex_QueryMatchesPropertyAndLabelFunctions — the bug
// report's contradictory observation was that bare MATCH (n:Label)
// returned 0 while `'Label' IN labels(n)` and property-based selection
// returned the right count. Pin that all three paths agree after
// reopen so a regression on any one surfaces against the others.
func TestLabelIndex_QueryMatchesPropertyAndLabelFunctions(t *testing.T) {
	dir := t.TempDir()

	engine1, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine1 != nil {
		fixtureowner.Own(t, engine1)
	}
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		_, err := engine1.CreateNode(&Node{
			ID:         NodeID(fmt.Sprintf("test:agree-%02d", i)),
			Labels:     []string{"Agree"},
			Properties: map[string]any{"i": int64(i)},
		})
		require.NoError(t, err)
	}
	require.NoError(t, engine1.Close())

	engine2, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	// Path 1: GetNodesByLabel — the index path.
	byIndex, err := engine2.GetNodesByLabel("Agree")
	require.NoError(t, err)

	// Path 2: AllNodes + filter on labels — the post-decode path that
	// the bug report says "still works".
	all, err := engine2.AllNodes()
	require.NoError(t, err)
	var byScan []*Node
	for _, n := range all {
		for _, l := range n.Labels {
			if l == "Agree" {
				byScan = append(byScan, n)
				break
			}
		}
	}

	require.Len(t, byIndex, 10)
	require.Len(t, byScan, 10)
	assert.Equal(t, len(byScan), len(byIndex),
		"index path and scan-and-filter path must agree after reopen; mismatch is the v1.1.1 bug shape")
}
