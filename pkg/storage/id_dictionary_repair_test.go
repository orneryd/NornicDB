package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// dropDictionaryEntries deletes the dictionary keys of the given entities, as
// a rolled-back transaction could leave them before #907's I29 fix.
func dropDictionaryEntries(t *testing.T, engine *BadgerEngine, nodes []NodeID, edges []EdgeID) {
	t.Helper()
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
		for _, id := range nodes {
			num, ok := engine.idDict.lookupNodeNumID(id)
			require.True(t, ok, id)
			require.NoError(t, txn.Delete(nodeIDForwardKey(id)))
			require.NoError(t, txn.Delete(append([]byte{prefixIDDictNodeReverse}, encodeNumID(num)...)))
		}
		for _, id := range edges {
			num, ok := engine.idDict.lookupEdgeNumID(id)
			require.True(t, ok, id)
			require.NoError(t, txn.Delete(edgeIDForwardKey(id)))
			require.NoError(t, txn.Delete(append([]byte{prefixIDDictEdgeReverse}, encodeNumID(num)...)))
		}
		return nil
	}))
}

// CheckIDDictionary finds the stored entities without a dictionary entry;
// RepairIDDictionary restores their original numeric IDs where the index keys
// identify them unambiguously, so traversal, label lookups and counts are
// right again, and reports the rest (#907, I29).
func TestCheckAndRepairIDDictionary(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	for _, node := range []*Node{
		{ID: "test:a", Labels: []string{"Doc"}}, {ID: "test:b", Labels: []string{"Doc", "Source"}},
		{ID: "test:c", Labels: []string{"Doc"}}, {ID: "test:bare"},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	for _, edge := range []*Edge{
		{ID: "test:link", StartNode: "test:a", EndNode: "test:b", Type: "CITES"},
		{ID: "test:twin1", StartNode: "test:a", EndNode: "test:c", Type: "SEES"},
		{ID: "test:twin2", StartNode: "test:a", EndNode: "test:c", Type: "SEES"},
		{ID: "test:kept", StartNode: "test:b", EndNode: "test:c", Type: "CITES"},
		{ID: "test:fromBare", StartNode: "test:bare", EndNode: "test:c", Type: "SEES"},
	} {
		require.NoError(t, engine.CreateEdge(edge))
	}
	report, err := engine.CheckIDDictionary()
	require.NoError(t, err)
	require.Empty(t, report.Nodes, "a healthy store has no gaps")
	require.Empty(t, report.Edges)
	countBefore, err := engine.EdgeCountByType("CITES")
	require.NoError(t, err)
	require.Equal(t, int64(2), countBefore)

	dropDictionaryEntries(t, engine, []NodeID{"test:b", "test:bare"}, []EdgeID{"test:link", "test:twin1", "test:twin2", "test:fromBare"})
	require.NoError(t, engine.Close())
	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)

	report, err = engine.CheckIDDictionary()
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:b", "test:bare"}, report.Nodes)
	require.ElementsMatch(t, []EdgeID{"test:link", "test:twin1", "test:twin2", "test:fromBare"}, report.Edges)
	outgoing, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Empty(t, outgoing, "lost relationships aren't traversed")

	report, err = engine.RepairIDDictionary()
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:b"}, report.RestoredNodes)
	require.Equal(t, []NodeID{"test:bare"}, report.UnresolvedNodes, "an unlabelled node isn't identified")
	require.Equal(t, []EdgeID{"test:link"}, report.RestoredEdges)
	require.ElementsMatch(t, []EdgeID{"test:twin1", "test:twin2", "test:fromBare"}, report.UnresolvedEdges, "two alike aren't told apart, nor one from an unresolved node")

	outgoing, err = engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	require.Equal(t, EdgeID("test:link"), outgoing[0].ID)
	sources, err := engine.GetNodesByLabel("Source")
	require.NoError(t, err)
	require.Len(t, sources, 1)
	countAfter, err := engine.EdgeCountByType("CITES")
	require.NoError(t, err)
	require.Equal(t, countBefore, countAfter, "relationships between repaired nodes are counted again")

	// The repair persists: reopened, only the unresolved remain.
	require.NoError(t, engine.Close())
	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	report, err = engine.CheckIDDictionary()
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:bare"}, report.Nodes)
	require.ElementsMatch(t, []EdgeID{"test:twin1", "test:twin2", "test:fromBare"}, report.Edges)
}

// A restart that rebuilds the MVCC heads gives a lost entity a new number
// while its label and adjacency keys keep the old one, as in the Personal
// Documents store: CheckIDDictionary reports it as mismatched, and
// RepairIDDictionary points its entry and MVCC head back at the old number
// (#907, I29).
func TestRepairIDDictionaryMismatchedAfterHeadRebuild(t *testing.T) {
	dir := t.TempDir()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	for _, node := range []*Node{{ID: "test:a", Labels: []string{"Doc"}}, {ID: "test:lone", Labels: []string{"Doc"}}, {ID: "test:c", Labels: []string{"Doc"}}} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	for _, edge := range []*Edge{
		{ID: "test:link", StartNode: "test:a", EndNode: "test:c", Type: "CITES", Properties: map[string]any{"stage": "interpretation"}},
		{ID: "test:kept", StartNode: "test:c", EndNode: "test:a", Type: "CITES"},
	} {
		require.NoError(t, engine.CreateEdge(edge))
	}
	linkNum, _ := engine.idDict.lookupEdgeNumID("test:link")
	// A node with relationships can't lose its entry and be renumbered: the
	// rebuild can't decode its relationships, which name it by number.
	dropDictionaryEntries(t, engine, []NodeID{"test:lone"}, []EdgeID{"test:link"})
	require.NoError(t, engine.Close())
	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	require.NoError(t, engine.RebuildMVCCHeads(context.Background()))
	newLinkNum, ok := engine.idDict.lookupEdgeNumID("test:link")
	require.True(t, ok, "the rebuild gave the lost relationship an entry")
	require.NotEqual(t, linkNum, newLinkNum)
	require.NoError(t, engine.Close())
	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)

	report, err := engine.CheckIDDictionary()
	require.NoError(t, err)
	require.Empty(t, report.Nodes)
	require.Empty(t, report.Edges)
	require.Equal(t, []NodeID{"test:lone"}, report.MismatchedNodes)
	require.Equal(t, []EdgeID{"test:link"}, report.MismatchedEdges)
	outgoing, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Empty(t, outgoing)

	report, err = engine.RepairIDDictionary()
	require.NoError(t, err)
	require.Equal(t, []NodeID{"test:lone"}, report.RestoredNodes)
	require.Equal(t, []EdgeID{"test:link"}, report.RestoredEdges)
	require.Empty(t, report.UnresolvedNodes)
	require.Empty(t, report.UnresolvedEdges)
	restoredNum, _ := engine.idDict.lookupEdgeNumID("test:link")
	require.Equal(t, linkNum, restoredNum)
	_, stale := engine.idDict.lookupEdgeIDByNum(newLinkNum)
	require.False(t, stale, "the rebuild's number no longer resolves")
	check := func(t *testing.T, engine *BadgerEngine) {
		t.Helper()
		outgoing, err := engine.GetOutgoingEdges("test:a")
		require.NoError(t, err)
		require.Len(t, outgoing, 1)
		require.Equal(t, "interpretation", outgoing[0].Properties["stage"])
		outgoing, err = engine.GetOutgoingEdges("test:c")
		require.NoError(t, err)
		require.Len(t, outgoing, 1)
		docs, err := engine.GetNodesByLabel("Doc")
		require.NoError(t, err)
		require.Len(t, docs, 3)
		count, err := engine.EdgeCountByType("CITES")
		require.NoError(t, err)
		require.Equal(t, int64(2), count)
		report, err := engine.CheckIDDictionary()
		require.NoError(t, err)
		require.Empty(t, report.MismatchedNodes)
		require.Empty(t, report.MismatchedEdges)
	}
	check(t, engine)
	require.NoError(t, engine.Close())
	engine, err = fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	check(t, engine)
}

// Pointing an entry at another number when the previous number has no MVCC
// head moves nothing and still replaces the entry (#907, I29).
func TestRestoreIDEntryWithoutPreviousHead(t *testing.T) {
	engine := createTestBadgerEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:x", Labels: []string{"Doc"}})
	require.NoError(t, err)
	previous, ok := engine.idDict.lookupNodeNumID("test:x")
	require.True(t, ok)
	require.NoError(t, engine.db.Update(func(txn *badger.Txn) error { return txn.Delete(mvccNodeHeadKey(previous)) }))
	target := previous + 100
	require.NoError(t, engine.restoreIDEntry("test:x", "", target))
	got, _ := engine.idDict.lookupNodeNumID("test:x")
	require.Equal(t, target, got)
	_, stale := engine.idDict.lookupNodeIDByNum(previous)
	require.False(t, stale)
	require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(mvccNodeHeadKey(target))
		require.ErrorIs(t, err, badger.ErrKeyNotFound, "no head to move")
		_, err = txn.Get(append([]byte{prefixIDDictNodeReverse}, encodeNumID(previous)...))
		require.ErrorIs(t, err, badger.ErrKeyNotFound, "the old reverse entry is gone")
		return nil
	}))
}
