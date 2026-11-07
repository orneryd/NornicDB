package storage

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// A rebuild derives MVCC heads from archived versions, so an entity that was
// deleted and recreated keeps the archived tombstone as the latest version it
// can see. The stored body is that entity's live state, so the rebuilt head
// must report it as live: a tombstoned head beside a live body lets pruning
// reclaim the entity's numeric identity, and the compact relationship bodies
// that address it no longer resolve.
func TestRebuildMVCCHeadsReplacesTombstonedHeadOfRecreatedNode(t *testing.T) {
	engine := createTestBadgerEngine(t)
	ctx := t.Context()

	_, err := engine.CreateNode(ctx, &Node{ID: "test:a", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(ctx, &Node{ID: "test:b", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	require.NoError(t, engine.DeleteNode(ctx, "test:a"))
	_, err = engine.CreateNode(ctx, &Node{ID: "test:a", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(ctx, &Edge{
		ID:        "test:e",
		StartNode: "test:a",
		EndNode:   "test:b",
		Type:      "LINK",
	}))

	require.NoError(t, engine.RebuildMVCCHeads(ctx))
	head, err := engine.GetNodeCurrentHead(ctx, "test:a")
	require.NoError(t, err)
	require.False(t, head.Tombstoned, "the live body must not keep its archived tombstone as the current head")

	deleted, err := engine.PruneMVCCVersions(ctx, MVCCPruneOptions{})
	require.NoError(t, err)
	require.Positive(t, deleted, "the superseded node tombstone must remain reclaimable history")

	edge, err := engine.GetEdge(ctx, "test:e")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:a"), edge.StartNode)
	require.Equal(t, NodeID("test:b"), edge.EndNode)
	edges, err := engine.AllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	_, err = engine.GetNode(ctx, "test:a")
	require.NoError(t, err)

	report, err := engine.CheckIDDictionary()
	require.NoError(t, err)
	require.Empty(t, report.Nodes)
	require.Empty(t, report.Edges)
}

// The edge keyspace rebuilds heads through the same bootstrap, so a deleted
// and recreated relationship must also keep a live head; otherwise pruning
// removes the numeric edge identity from the dictionary and its index keys
// resolve to nothing.
func TestRebuildMVCCHeadsReplacesTombstonedHeadOfRecreatedEdge(t *testing.T) {
	engine := createTestBadgerEngine(t)
	ctx := t.Context()

	_, err := engine.CreateNode(ctx, &Node{ID: "test:a", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(ctx, &Node{ID: "test:b", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	edge := &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}
	require.NoError(t, engine.CreateEdge(ctx, edge))
	require.NoError(t, engine.DeleteEdge(ctx, "test:e"))
	require.NoError(t, engine.CreateEdge(ctx, edge))

	require.NoError(t, engine.RebuildMVCCHeads(ctx))
	head, err := engine.GetEdgeCurrentHead(ctx, "test:e")
	require.NoError(t, err)
	require.False(t, head.Tombstoned, "the live relationship body must not keep its archived tombstone as the current head")

	deleted, err := engine.PruneMVCCVersions(ctx, MVCCPruneOptions{})
	require.NoError(t, err)
	require.Positive(t, deleted)

	recreated, err := engine.GetEdge(ctx, "test:e")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:a"), recreated.StartNode)
	edges, err := engine.AllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 1)

	report, err := engine.CheckIDDictionary()
	require.NoError(t, err)
	require.Empty(t, report.Edges)
}

// The recreated entity's dictionary entry is persisted, so the same lifecycle
// must hold across an engine reopen; the repeated rebuild and prune confirm
// the repair is idempotent rather than a one-shot rewrite.
func TestPruneAfterRebuildKeepsRecreatedNodeReadableAcrossReopen(t *testing.T) {
	engine, dir := createTestBadgerEngineOnDisk(t)
	ctx := t.Context()

	_, err := engine.CreateNode(ctx, &Node{ID: "test:a", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(ctx, &Node{ID: "test:b", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	require.NoError(t, engine.DeleteNode(ctx, "test:a"))
	_, err = engine.CreateNode(ctx, &Node{ID: "test:a", Labels: []string{"Endpoint"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(ctx, &Edge{
		ID:        "test:e",
		StartNode: "test:a",
		EndNode:   "test:b",
		Type:      "LINK",
	}))
	require.NoError(t, engine.RebuildMVCCHeads(ctx))
	_, err = engine.PruneMVCCVersions(ctx, MVCCPruneOptions{})
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	reopened, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	_, err = reopened.GetNode(ctx, "test:a")
	require.NoError(t, err)
	edge, err := reopened.GetEdge(ctx, "test:e")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:a"), edge.StartNode)
	edges, err := reopened.AllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 1)
	report, err := reopened.CheckIDDictionary()
	require.NoError(t, err)
	require.Empty(t, report.Nodes)
	require.Empty(t, report.Edges)

	require.NoError(t, reopened.RebuildMVCCHeads(ctx))
	_, err = reopened.PruneMVCCVersions(ctx, MVCCPruneOptions{})
	require.NoError(t, err)
	head, err := reopened.GetNodeCurrentHead(ctx, "test:a")
	require.NoError(t, err)
	require.False(t, head.Tombstoned)
	edges, err = reopened.AllEdges(ctx)
	require.NoError(t, err)
	require.Len(t, edges, 1)
}
