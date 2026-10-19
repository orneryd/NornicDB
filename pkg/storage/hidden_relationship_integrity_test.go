package storage

import (
	"sort"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

// hiddenGraph is a graph whose relationships the visibility layer hides in
// every way: e1 is suppressed on demand, e2 has decayed, e3 is visible but
// ends at a suppressed node, and e4 is the only relationship of lone, also
// suppressed. e5 is unrelated and visible.
//
//	hub -[e1:SUPPRESSED]-> peer1 <-[e4:SUPPRESSED]- lone
//	hub <-[e2:DECAYED]- peer2 <-[e5:LINK]- other
//	hub -[e3:LINK]-> hiddenPeer
func setupHiddenGraph(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	engine.SetDecayEnabled(true)
	bindings, err := knowledgepolicy.BuildBindingTable(
		map[string]*knowledgepolicy.DecayProfileBundle{"stale": {
			Name: "stale", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential,
			HalfLifeSeconds: 3600, VisibilityThreshold: 0.5, ScoreFrom: knowledgepolicy.ScoreFromCreated,
			Enabled: true, DecayEnabled: true,
		}},
		map[string]*knowledgepolicy.DecayProfileBinding{"stale_edges": {
			Name: "stale_edges", ProfileRef: "stale", TargetEdgeType: "DECAYED", IsEdge: true,
		}},
		nil, nil,
	)
	require.NoError(t, err)
	engine.GetSchemaForNamespace("test").SetBindingTable(bindings)

	for id, label := range map[NodeID]string{
		"test:hub": "Hub", "test:lone": "Hub", "test:peer1": "Peer", "test:peer2": "Peer",
		"test:hiddenPeer": "Peer", "test:other": "Other",
	} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{label}})
		require.NoError(t, err)
	}
	old := time.Now().Add(-30 * 24 * time.Hour)
	for _, edge := range []*Edge{
		{ID: "test:e1", StartNode: "test:hub", EndNode: "test:peer1", Type: "SUPPRESSED"},
		{ID: "test:e2", StartNode: "test:peer2", EndNode: "test:hub", Type: "DECAYED", CreatedAt: old, UpdatedAt: old},
		{ID: "test:e3", StartNode: "test:hub", EndNode: "test:hiddenPeer", Type: "LINK"},
		{ID: "test:e4", StartNode: "test:lone", EndNode: "test:peer1", Type: "SUPPRESSED"},
		{ID: "test:e5", StartNode: "test:other", EndNode: "test:peer2", Type: "LINK"},
	} {
		require.NoError(t, engine.CreateEdge(edge))
	}
	if engine.writeBehind != nil {
		// Suppression marks stored records.
		require.NoError(t, engine.FlushWriteBehind())
	}
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:e1", true))
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:e4", true))
	require.NoError(t, engine.SetNodeManuallySuppressed("test:hiddenPeer", true))

	for _, id := range []EdgeID{"test:e1", "test:e2", "test:e4"} {
		_, err := engine.GetEdge(id)
		require.ErrorIs(t, err, ErrNotFound, "%s is hidden from reads", id)
		_, err = engine.GetStoredEdge(id)
		require.NoError(t, err, "%s is stored", id)
	}
	_, err = engine.GetNode("test:hiddenPeer")
	require.ErrorIs(t, err, ErrNotFound)
}

func deleteInTransaction(t *testing.T, engine *BadgerEngine, deleteNode func(*BadgerTransaction) error) error {
	t.Helper()
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, deleteNode(tx))
	return tx.Commit()
}

// edgeIDLister returns a function listing the sorted IDs of a read's edges.
func edgeIDLister(t *testing.T) func([]*Edge, error) []string {
	return func(edges []*Edge, err error) []string {
		t.Helper()
		require.NoError(t, err)
		ids := make([]string, 0, len(edges))
		for _, edge := range edges {
			ids = append(ids, string(edge.ID))
		}
		sort.Strings(ids)
		return ids
	}
}

// requireHubDeleted checks storage after the hub's delete: none of the
// hidden relationships survives its deleted endpoint, in the records, the
// adjacency and type indexes and the counters.
func requireHubDeleted(t *testing.T, engine *BadgerEngine) {
	t.Helper()
	storedEdgeIDs := edgeIDLister(t)
	for _, id := range []EdgeID{"test:e1", "test:e2", "test:e3"} {
		_, err := engine.GetStoredEdge(id)
		require.ErrorIs(t, err, ErrNotFound, "%s went with its endpoint", id)
	}
	_, err := engine.GetStoredNode("test:hub")
	require.ErrorIs(t, err, ErrNotFound)

	require.Equal(t, []string{"test:e4"}, storedEdgeIDs(engine.GetStoredIncomingEdges("test:peer1")))
	require.Empty(t, storedEdgeIDs(engine.GetStoredOutgoingEdges("test:peer2")))
	require.Empty(t, storedEdgeIDs(engine.GetStoredIncomingEdges("test:hiddenPeer")))
	require.Empty(t, storedEdgeIDs(engine.GetStoredEdgesByType("DECAYED")))
	require.Equal(t, []string{"test:e4"}, storedEdgeIDs(engine.GetStoredEdgesByType("SUPPRESSED")))

	count := func(n int64, err error) int64 {
		t.Helper()
		require.NoError(t, err)
		return n
	}
	require.EqualValues(t, 2, count(engine.EdgeCount()), "e4 and e5 remain")
	require.EqualValues(t, 1, count(engine.EdgeCountByType("SUPPRESSED")))
	require.EqualValues(t, 0, count(engine.EdgeCountByType("DECAYED")))
	require.EqualValues(t, 1, count(engine.EdgeCountByType("LINK")))
	require.EqualValues(t, 1, count(engine.EdgeCountByStartLabel("Hub", "SUPPRESSED")), "lone's e4")
	require.EqualValues(t, 1, count(engine.EdgeCountByEndLabel("Peer", "SUPPRESSED")))
	require.EqualValues(t, 0, count(engine.EdgeCountByStartLabel("Peer", "DECAYED")))
	require.EqualValues(t, 0, count(engine.EdgeCountByEndLabel("Hub", "DECAYED")))
	require.EqualValues(t, 0, count(engine.EdgeCountByStartLabel("Hub", "LINK")))
	require.EqualValues(t, 1, count(engine.EdgeCountByEndLabel("Peer", "LINK")), "only e5: e3's hidden end is counted down")
}

// A node delete removes every relationship that connects the node, those the
// visibility layer hides included, with or without write-behind; a
// non-DETACH delete fails while only hidden relationships connect the node,
// as Neo4j's DeleteConnectedNode does for any relationship.
func TestNodeDeleteRemovesHiddenRelationships(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		name := "write-behind off"
		if writeBehind {
			name = "write-behind on"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			open := func() *BadgerEngine {
				engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: writeBehind, WriteBehindInterval: time.Hour})
				require.NoError(t, err)
				return engine
			}
			engine := open()
			setupHiddenGraph(t, engine)

			err := deleteInTransaction(t, engine, func(tx *BadgerTransaction) error {
				return tx.DeleteConnectedNode("test:lone")
			})
			var connected *NodeStillConnectedError
			require.ErrorAs(t, err, &connected, "a suppressed relationship still connects lone")
			_, err = engine.GetStoredNode("test:lone")
			require.NoError(t, err)
			_, err = engine.GetStoredEdge("test:e4")
			require.NoError(t, err)

			require.NoError(t, deleteInTransaction(t, engine, func(tx *BadgerTransaction) error {
				edges, err := tx.IncidentEdges("test:hub")
				require.NoError(t, err)
				require.Equal(t, []string{"test:e1", "test:e2", "test:e3"}, edgeIDLister(t)(edges, nil))
				return tx.DeleteNode("test:hub")
			}))
			if writeBehind {
				// Before the flush the buffered delete is what a transaction
				// reads; the engine's records follow at the flush.
				tx, err := engine.BeginTransaction()
				require.NoError(t, err)
				incident := func(id NodeID) []string {
					edges, err := tx.IncidentEdges(id)
					return edgeIDLister(t)(edges, err)
				}
				require.Equal(t, []string{"test:e4"}, incident("test:peer1"))
				require.Equal(t, []string{"test:e5"}, incident("test:peer2"))
				require.Empty(t, incident("test:hiddenPeer"))
				require.NoError(t, tx.Rollback())
				require.NoError(t, engine.FlushWriteBehind())
			}
			requireHubDeleted(t, engine)

			require.NoError(t, engine.Close())
			engine = open()
			t.Cleanup(func() { _ = engine.Close() })
			requireHubDeleted(t, engine)
		})
	}
}

// An acknowledged buffered delete replays after a crash with the hidden
// relationships it removed: the commit's recorded writes include them.
func TestWriteBehindReplayRemovesHiddenRelationships(t *testing.T) {
	dir := t.TempDir()
	engine, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
	setupHiddenGraph(t, engine)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.DeleteNode("test:hub"))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	crashClose(t, engine, wal)

	recovered, wal2 := newWriteBehindWALTestEngine(t, dir, time.Hour)
	t.Cleanup(func() { _ = wal2.Close(); _ = recovered.Close() })
	_, err = RecoverBufferedCommits(recovered, wal2)
	require.NoError(t, err)
	requireHubDeleted(t, recovered)
}
