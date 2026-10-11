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
	"testing"

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
	engine1, err := NewBadgerEngine(dir)
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
	engine2, err := NewBadgerEngine(dir)
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
	dir := t.TempDir()

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)

	// 250 nodes is enough to flush across multiple Badger pages; pin the
	// index entries are written per-node.
	const n = 250
	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		nodes[i] = &Node{
			ID:         NodeID(fmt.Sprintf("test:concept-%05d", i)),
			Labels:     []string{"Concept"},
			Properties: map[string]any{"i": int64(i)},
		}
	}
	require.NoError(t, engine1.BulkCreateNodes(nodes))

	pre, err := engine1.GetNodesByLabel("Concept")
	require.NoError(t, err)
	require.Len(t, pre, n)
	require.NoError(t, engine1.Close())

	engine2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	post, err := engine2.GetNodesByLabel("Concept")
	require.NoError(t, err)
	assert.Len(t, post, n,
		"BulkCreateNodes must write label-index entries that survive reopen; got %d/%d", len(post), n)
}

// TestLabelIndex_MultiLabelSurvivesReopen — the bug report cites
// `MATCH (c:Aprodan:Academic:Concept)` returning 0 against 227 true.
// Pin: multi-label nodes write a label-index entry per label, and
// every label is independently queryable after reopen.
func TestLabelIndex_MultiLabelSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)

	node := &Node{
		ID:     "test:multi-1",
		Labels: []string{"Aprodan", "Academic", "Concept"},
	}
	_, err = engine1.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, engine1.Close())

	engine2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine2.Close() })

	for _, label := range []string{"Aprodan", "Academic", "Concept"} {
		got, err := engine2.GetNodesByLabel(label)
		require.NoError(t, err)
		require.Len(t, got, 1, "label %q must surface the node after reopen", label)
		assert.Equal(t, NodeID("test:multi-1"), got[0].ID)
	}
}

// TestLabelIndex_AsyncEngineFlushesLabelEntriesBeforeClose — the
// AsyncEngine wraps BadgerEngine and maintains its own in-memory
// labelIndex map for hot reads. Close() forces a final flush so all
// pending writes hit Badger. Pin that the flushed data carries label
// index entries — otherwise the underlying BadgerEngine would have
// the node body but no label-index row.
// Writes apply synchronously, so every create carries its label-index
// entries into Badger immediately. Close() then persists everything; pin
// that the flushed data carries label index entries — otherwise the
// underlying BadgerEngine would have the node body but no label-index row.
func TestLabelIndex_BadgerPersistsLabelEntriesAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	inner, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	store := NewNamespacedEngine(inner, "test")

	for i := 0; i < 50; i++ {
		_, err := store.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("test:async-%03d", i)),
			Labels: []string{"AsyncOnly"},
		})
		require.NoError(t, err)
	}
	require.NoError(t, inner.Close())

	// Reopen JUST the BadgerEngine.
	engine, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	got, err := engine.GetNodesByLabel("AsyncOnly")
	require.NoError(t, err)
	assert.Len(t, got, 50,
		"writes must persist label-index entries; reopened BadgerEngine sees %d/50 nodes by label",
		len(got))
}

// TestLabelIndex_DiskKeyExistsAfterReopen — peek directly at the
// Badger key space after reopen and confirm the label-index keys
// (prefix 0x03) are present. This pins the on-disk shape, so a
// future refactor that moves label storage out of Badger surfaces
// here rather than producing the silent zero-rows symptom.
func TestLabelIndex_DiskKeyExistsAfterReopen(t *testing.T) {
	dir := t.TempDir()

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	_, err = engine1.CreateNode(&Node{ID: "test:disk-1", Labels: []string{"DiskCheck"}})
	require.NoError(t, err)
	require.NoError(t, engine1.Close())

	// Reopen and walk the label-index key range directly.
	engine2, err := NewBadgerEngine(dir)
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

	engine1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	_, err = engine1.CreateNode(&Node{ID: "test:dict-1", Labels: []string{"L"}})
	require.NoError(t, err)

	// Capture the numID assigned during the first session so we can
	// confirm the reverse map carries it after reopen.
	num, ok := engine1.idDict.lookupNodeNumID("test:dict-1")
	require.True(t, ok, "node must have a numID after CreateNode")
	require.NoError(t, engine1.Close())

	engine2, err := NewBadgerEngine(dir)
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

	engine1, err := NewBadgerEngine(dir)
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

	engine2, err := NewBadgerEngine(dir)
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

// TestLabelIndex_RoutedThroughNamespacedEngineSurvivesReopen — the
// production stack puts a NamespacedEngine in front of the
// BadgerEngine to scope reads/writes to a single database. Pin that
// the namespace-prefix wrapper does not break the label-index flow
// across reopen.
func TestLabelIndex_RoutedThroughNamespacedEngineSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	inner1, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	ns1 := NewNamespacedEngine(inner1, "nornic")

	for i := 0; i < 10; i++ {
		// NamespacedEngine.CreateNode prefixes IDs internally; passing
		// an unprefixed ID ("user-1") simulates the Cypher executor's
		// CREATE shape.
		_, err := ns1.CreateNode(&Node{
			ID:     NodeID(fmt.Sprintf("ns-routed-%02d", i)),
			Labels: []string{"NSLabel"},
		})
		require.NoError(t, err)
	}
	require.NoError(t, inner1.Close())

	inner2, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = inner2.Close() })
	ns2 := NewNamespacedEngine(inner2, "nornic")

	got, err := ns2.GetNodesByLabel("NSLabel")
	require.NoError(t, err)
	assert.Len(t, got, 10,
		"NamespacedEngine label query must surface namespace-scoped nodes after reopen; got %d/10", len(got))
}
