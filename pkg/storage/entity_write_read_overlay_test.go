package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR-213 refreshed read images: a no-op write that takes an entity's write
// lock after a peer committed that entity must read it as last committed.
// Point reads already did; these regressions require every collection,
// stream, adjacency, equality and stored-read consumer of the transaction to
// compose the same latest committed image, without staging a write and
// without advancing the rest of the pinned snapshot.
const (
	refreshedDocID       = NodeID("test:doc")
	refreshedRenewedID   = NodeID("test:renewed")
	refreshedBystanderID = NodeID("test:bystander")
	refreshedDoomedID    = NodeID("test:doomed")
	refreshedPartID      = NodeID("test:part")
	refreshedPartTwoID   = NodeID("test:part2")

	refreshedEdgeID       = EdgeID("test:edge")
	refreshedOwnEdgeID    = EdgeID("test:own-edge")
	refreshedLinkID       = EdgeID("test:link")
	refreshedDoomedEdgeID = EdgeID("test:doomed-edge")
)

// refreshedReadFixture is one pinned transaction over a small graph whose
// populations the tests warm before a peer commits. The peer commits after the
// transaction's snapshot, then the transaction takes the write locks of the
// entities it "renews" (a no-op write) and must read their committed images.
type refreshedReadFixture struct {
	engine *MemoryEngine
	tx     *BadgerTransaction
}

func newRefreshedReadFixture(t *testing.T) *refreshedReadFixture {
	t.Helper()
	ctx := t.Context()
	engine := newTestMemoryEngine(t)
	for _, node := range []*Node{
		{ID: refreshedDocID, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "doc", "value": int64(1), "peer": int64(1)}},
		{ID: refreshedRenewedID, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "renewed", "peer": int64(1)}},
		{ID: refreshedBystanderID, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "bystander", "peer": int64(1)}},
		{ID: refreshedDoomedID, Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": "doomed", "peer": int64(1)}},
		{ID: refreshedPartID, Labels: []string{"Part"}, Properties: map[string]interface{}{"id": "part"}},
		{ID: refreshedPartTwoID, Labels: []string{"Part"}, Properties: map[string]interface{}{"id": "part2"}},
	} {
		_, err := engine.CreateNode(ctx, node)
		require.NoError(t, err, node.ID)
	}
	for _, edge := range []*Edge{
		{ID: refreshedEdgeID, StartNode: refreshedDocID, EndNode: refreshedPartID, Type: "HAS", Properties: map[string]interface{}{"value": int64(1), "peer": int64(1)}},
		{ID: refreshedOwnEdgeID, StartNode: refreshedDocID, EndNode: refreshedPartID, Type: "HAS", Properties: map[string]interface{}{"value": int64(1), "peer": int64(1)}},
		{ID: refreshedLinkID, StartNode: refreshedDocID, EndNode: refreshedPartTwoID, Type: "LINK", Properties: map[string]interface{}{"value": int64(1), "peer": int64(1)}},
		{ID: refreshedDoomedEdgeID, StartNode: refreshedDocID, EndNode: refreshedPartTwoID, Type: "HAS", Properties: map[string]interface{}{"peer": int64(1)}},
	} {
		require.NoError(t, engine.CreateEdge(ctx, edge), edge.ID)
	}
	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	return &refreshedReadFixture{engine: engine, tx: tx}
}

// warmRefreshedReadRoutes reads every supported collection route before the
// peer commits, so each route's population, index and equality cache is the
// pre-change snapshot the overlay must keep in step with the point reads.
func warmRefreshedReadRoutes(t *testing.T, f *refreshedReadFixture) {
	t.Helper()
	ctx := t.Context()
	tx := f.tx

	nodes, err := tx.GetNodesByLabel(ctx, "Doc")
	require.NoError(t, err)
	require.Equal(t, []string{"test:bystander", "test:doc", "test:doomed", "test:renewed"}, nodeIDs(nodes))
	requireNodePeers(t, nodes, map[NodeID]interface{}{
		refreshedDocID:       int64(1),
		refreshedRenewedID:   int64(1),
		refreshedBystanderID: int64(1),
		refreshedDoomedID:    int64(1),
	}, "warming Doc population")

	requireNodePeers(t, collectStreamNodes(t, func(visit func(*Node) error) error {
		return tx.StreamNodesByLabelProjected(ctx, "Doc", nil, visit)
	}), map[NodeID]interface{}{
		refreshedDocID:       int64(1),
		refreshedRenewedID:   int64(1),
		refreshedBystanderID: int64(1),
		refreshedDoomedID:    int64(1),
	}, "warming full Doc label stream")

	requireNodePeers(t, collectStreamNodes(t, func(visit func(*Node) error) error {
		return tx.StreamNodesByLabelProjected(ctx, "Doc", []string{"peer"}, visit)
	}), map[NodeID]interface{}{
		refreshedDocID:       int64(1),
		refreshedRenewedID:   int64(1),
		refreshedBystanderID: int64(1),
		refreshedDoomedID:    int64(1),
	}, "warming projected Doc label stream")

	requireNodePeers(t, collectStreamNodes(t, func(visit func(*Node) error) error {
		return tx.StreamNodesWithOptions(ctx, StreamNodesOptions{}, visit)
	}), map[NodeID]interface{}{
		refreshedDocID:       int64(1),
		refreshedRenewedID:   int64(1),
		refreshedBystanderID: int64(1),
		refreshedDoomedID:    int64(1),
		refreshedPartID:      nil,
		refreshedPartTwoID:   nil,
	}, "warming all-node stream")

	all, err := tx.AllNodes(ctx)
	require.NoError(t, err)
	require.Len(t, all, 6)
	requireNodePeers(t, all, map[NodeID]interface{}{
		refreshedDocID:       int64(1),
		refreshedRenewedID:   int64(1),
		refreshedBystanderID: int64(1),
		refreshedDoomedID:    int64(1),
		refreshedPartID:      nil,
		refreshedPartTwoID:   nil,
	}, "warming AllNodes")

	stored, err := tx.GetStoredNodesByLabel(ctx, "Doc")
	require.NoError(t, err)
	requireNodePeers(t, stored, map[NodeID]interface{}{
		refreshedDocID:       int64(1),
		refreshedRenewedID:   int64(1),
		refreshedBystanderID: int64(1),
		refreshedDoomedID:    int64(1),
	}, "warming stored Doc population")

	requireNodePeers(t, collectStreamNodes(t, func(visit func(*Node) error) error {
		return tx.StreamStoredNodesByLabelProjected(ctx, "Doc", []string{"peer"}, visit)
	}), map[NodeID]interface{}{
		refreshedDocID:       int64(1),
		refreshedRenewedID:   int64(1),
		refreshedBystanderID: int64(1),
		refreshedDoomedID:    int64(1),
	}, "warming stored projected Doc stream")

	candidates, complete, err := tx.LookupNodesByEquality(ctx, "Doc", map[string]interface{}{"peer": int64(1)})
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, []string{"test:bystander", "test:doc", "test:doomed", "test:renewed"}, nodeIDs(candidates))

	doc, err := tx.GetNode(ctx, refreshedDocID)
	require.NoError(t, err)
	require.Equal(t, int64(1), doc.Properties["peer"])
	queryDoc, err := tx.GetQueryNode(ctx, refreshedDocID)
	require.NoError(t, err)
	require.Equal(t, int64(1), queryDoc.Properties["peer"])
	projectedDoc, err := tx.GetNodeProjected(ctx, refreshedDocID, []string{"peer"})
	require.NoError(t, err)
	require.Equal(t, int64(1), projectedDoc.Properties["peer"])
	storedDoc, err := tx.GetStoredNode(ctx, refreshedDocID)
	require.NoError(t, err)
	require.Equal(t, int64(1), storedDoc.Properties["peer"])

	edges, err := tx.AllEdges(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"test:doomed-edge", "test:edge", "test:link", "test:own-edge"}, edgeIDs(edges))
	requireEdgePeers(t, edges, map[EdgeID]interface{}{
		refreshedEdgeID:       int64(1),
		refreshedOwnEdgeID:    int64(1),
		refreshedLinkID:       int64(1),
		refreshedDoomedEdgeID: int64(1),
	}, "warming AllEdges")

	hasEdges, err := tx.GetEdgesByType(ctx, "HAS")
	require.NoError(t, err)
	require.Equal(t, []string{"test:doomed-edge", "test:edge", "test:own-edge"}, edgeIDs(hasEdges))
	requireEdgePeers(t, hasEdges, map[EdgeID]interface{}{
		refreshedEdgeID:       int64(1),
		refreshedOwnEdgeID:    int64(1),
		refreshedDoomedEdgeID: int64(1),
	}, "warming HAS materialized type population")

	for _, edgeType := range []string{"HAS", ""} {
		want := refreshedEdgesOfType(map[EdgeID]interface{}{
			refreshedEdgeID:       int64(1),
			refreshedOwnEdgeID:    int64(1),
			refreshedLinkID:       int64(1),
			refreshedDoomedEdgeID: int64(1),
		}, edgeType)
		requireEdgePeers(t, collectStreamEdges(t, func(visit func(*Edge) error) error {
			return tx.StreamEdgesByType(ctx, edgeType, visit)
		}), want, "warming streamed edge type "+edgeType)
		requireEdgePeers(t, collectStreamEdges(t, func(visit func(*Edge) error) error {
			return tx.StreamStoredEdgesByType(ctx, edgeType, visit)
		}), want, "warming stored edge type "+edgeType)
	}

	outgoing, err := tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: refreshedDocID})
	require.NoError(t, err)
	require.Len(t, outgoing, 4)
	incoming, err := tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadIncoming, Node: refreshedPartID})
	require.NoError(t, err)
	require.Equal(t, []string{"test:edge", "test:own-edge"}, edgeIDs(incoming))
	requireEdgePeers(t, incoming, map[EdgeID]interface{}{
		refreshedEdgeID:    int64(1),
		refreshedOwnEdgeID: int64(1),
	}, "warming incoming adjacency")

	between, err := tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadBetween, Node: refreshedDocID, Other: refreshedPartID})
	require.NoError(t, err)
	require.Equal(t, []string{"test:edge", "test:own-edge"}, edgeIDs(between))

	edge, err := tx.GetEdgeBetween(ctx, refreshedDocID, refreshedPartID, "HAS")
	require.NoError(t, err)
	require.NotNil(t, edge)
	require.Equal(t, int64(1), edge.Properties["peer"])

	matched, err := tx.MatchEdgesBetween(ctx, refreshedDocID, refreshedPartID, "HAS", []string{"peer"}, func(edge *Edge) bool {
		return edge.Properties["peer"] == int64(1)
	})
	require.NoError(t, err)
	require.Equal(t, []string{"test:edge", "test:own-edge"}, edgeIDs(matched))

	storedEdge, err := tx.GetStoredEdge(ctx, refreshedEdgeID)
	require.NoError(t, err)
	require.Equal(t, int64(1), storedEdge.Properties["peer"])
	pointEdge, err := tx.GetEdge(ctx, refreshedEdgeID)
	require.NoError(t, err)
	require.Equal(t, int64(1), pointEdge.Properties["peer"])
}

// commitPeerUpdates applies, through its own committed transactions, the
// post-snapshot commits the pinned transaction must observe through its write
// locks only. Unlocked entities stay at the pinned snapshot for every
// snapshot-scoped route.
func commitPeerUpdates(t *testing.T, f *refreshedReadFixture) {
	t.Helper()
	peerNode := func(id NodeID, mutate func(*Node)) {
		t.Helper()
		ctx := t.Context()
		peer, err := f.engine.BeginTransaction(ctx)
		require.NoError(t, err)
		require.NoError(t, peer.SetNamespace("test"))
		node, err := peer.GetNode(ctx, id)
		require.NoError(t, err)
		mutate(node)
		require.NoError(t, peer.UpdateGraphNode(ctx, NodeUpdateRequest{After: node, Policy: ClientNodeEdits}))
		require.NoError(t, peer.Commit())
	}
	peerEdge := func(id EdgeID, mutate func(*Edge)) {
		t.Helper()
		ctx := t.Context()
		peer, err := f.engine.BeginTransaction(ctx)
		require.NoError(t, err)
		require.NoError(t, peer.SetNamespace("test"))
		edge, err := peer.GetEdge(ctx, id)
		require.NoError(t, err)
		mutate(edge)
		require.NoError(t, peer.UpdateEdge(ctx, EdgeUpdateRequest{After: edge}))
		require.NoError(t, peer.Commit())
	}

	peerNode(refreshedDocID, func(node *Node) {
		node.Properties["peer"] = int64(2)
		node.Properties["fresh"] = int64(7)
		node.Labels = append(node.Labels, "New")
	})
	peerNode(refreshedRenewedID, func(node *Node) {
		node.Properties["peer"] = int64(2)
	})
	peerNode(refreshedBystanderID, func(node *Node) {
		node.Properties["peer"] = int64(9)
		node.Labels = append(node.Labels, "Changed")
	})
	peerNode(refreshedDoomedID, func(node *Node) {
		node.Properties["peer"] = int64(8)
	})
	peerEdge(refreshedEdgeID, func(edge *Edge) {
		edge.Properties["peer"] = int64(2)
		edge.Properties["fresh"] = int64(7)
	})
	peerEdge(refreshedOwnEdgeID, func(edge *Edge) {
		edge.Properties["peer"] = int64(2)
	})
	peerEdge(refreshedDoomedEdgeID, func(edge *Edge) {
		edge.Properties["peer"] = int64(8)
	})
	peerEdge(refreshedLinkID, func(edge *Edge) {
		edge.Properties["peer"] = int64(9)
	})

	_, err := f.engine.CreateNode(t.Context(), &Node{
		ID:         NodeID("test:added"),
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"id": "added", "peer": int64(2), "fresh": int64(2)},
	})
	require.NoError(t, err)
}

func (f *refreshedReadFixture) lockNode(t *testing.T, id NodeID) *Node {
	t.Helper()
	latest, err := f.tx.LockNodeForWrite(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, latest, "%s: fixture premise is a peer commit after the snapshot", id)
	return latest
}

func (f *refreshedReadFixture) lockEdge(t *testing.T, id EdgeID) *Edge {
	t.Helper()
	latest, err := f.tx.LockEdgeForWrite(t.Context(), id)
	require.NoError(t, err)
	require.NotNil(t, latest, "%s: fixture premise is a peer commit after the snapshot", id)
	return latest
}

func TestWriteOwnedReadImagesComposeLatestCommittedCollections(t *testing.T) {
	f := newRefreshedReadFixture(t)
	warmRefreshedReadRoutes(t, f)
	commitPeerUpdates(t, f)

	ctx := t.Context()
	latest := f.lockNode(t, refreshedDocID)
	require.Equal(t, int64(2), latest.Properties["peer"], "the lock must hand the caller the committed predecessor")
	again, err := f.tx.LockNodeForWrite(ctx, refreshedDocID)
	require.NoError(t, err)
	require.NotNil(t, again)
	require.Equal(t, int64(2), again.Properties["peer"], "re-reading the held lock keeps the refreshed image")
	f.lockEdge(t, refreshedEdgeID)
	f.lockEdge(t, refreshedOwnEdgeID)

	docPeers := map[NodeID]interface{}{
		refreshedDocID:       int64(2),
		refreshedRenewedID:   int64(1),
		refreshedBystanderID: int64(1),
		refreshedDoomedID:    int64(1),
	}
	allNodePeers := map[NodeID]interface{}{
		refreshedDocID:       int64(2),
		refreshedRenewedID:   int64(1),
		refreshedBystanderID: int64(1),
		refreshedDoomedID:    int64(1),
		refreshedPartID:      nil,
		refreshedPartTwoID:   nil,
	}
	allEdgePeers := map[EdgeID]interface{}{
		refreshedEdgeID:       int64(2),
		refreshedOwnEdgeID:    int64(2),
		refreshedLinkID:       int64(1),
		refreshedDoomedEdgeID: int64(1),
	}

	t.Run("materialized label population", func(t *testing.T) {
		nodes, err := f.tx.GetNodesByLabel(ctx, "Doc")
		require.NoError(t, err)
		require.Equal(t, []string{"test:bystander", "test:doc", "test:doomed", "test:renewed"}, nodeIDs(nodes))
		requireNodePeers(t, nodes, docPeers, "materialized Doc population")
	})

	t.Run("streamed full label population", func(t *testing.T) {
		requireNodePeers(t, collectStreamNodes(t, func(visit func(*Node) error) error {
			return f.tx.StreamNodesByLabelProjected(ctx, "Doc", nil, visit)
		}), docPeers, "streamed Doc population")
	})

	t.Run("streamed projected label population", func(t *testing.T) {
		nodes := collectStreamNodes(t, func(visit func(*Node) error) error {
			return f.tx.StreamNodesByLabelProjected(ctx, "Doc", []string{"peer"}, visit)
		})
		requireNodePeers(t, nodes, docPeers, "streamed projected Doc population")
		doc := nodeWithID(t, nodes, refreshedDocID)
		require.Equal(t, int64(2), doc.Properties["peer"])
	})

	t.Run("streamed all nodes", func(t *testing.T) {
		requireNodePeers(t, collectStreamNodes(t, func(visit func(*Node) error) error {
			return f.tx.StreamNodesWithOptions(ctx, StreamNodesOptions{}, visit)
		}), allNodePeers, "all-node stream")
	})

	t.Run("materialized AllNodes", func(t *testing.T) {
		nodes, err := f.tx.AllNodes(ctx)
		require.NoError(t, err)
		require.Len(t, nodes, 6)
		requireNodePeers(t, nodes, allNodePeers, "AllNodes")
	})

	t.Run("stored label population", func(t *testing.T) {
		nodes, err := f.tx.GetStoredNodesByLabel(ctx, "Doc")
		require.NoError(t, err)
		requireNodePeers(t, nodes, docPeers, "stored Doc population")
		requireNodePeers(t, collectStreamNodes(t, func(visit func(*Node) error) error {
			return f.tx.StreamStoredNodesByLabelProjected(ctx, "Doc", []string{"peer"}, visit)
		}), docPeers, "stored projected Doc stream")
	})

	t.Run("point reads agree", func(t *testing.T) {
		doc, err := f.tx.GetNode(ctx, refreshedDocID)
		require.NoError(t, err)
		require.Equal(t, int64(2), doc.Properties["peer"])
		require.Equal(t, int64(7), doc.Properties["fresh"])
		require.Contains(t, doc.Labels, "New")
		queryDoc, err := f.tx.GetQueryNode(ctx, refreshedDocID)
		require.NoError(t, err)
		require.Equal(t, int64(2), queryDoc.Properties["peer"])
		projectedDoc, err := f.tx.GetNodeProjected(ctx, refreshedDocID, []string{"peer"})
		require.NoError(t, err)
		require.Equal(t, int64(2), projectedDoc.Properties["peer"])
		storedDoc, err := f.tx.GetStoredNode(ctx, refreshedDocID)
		require.NoError(t, err)
		require.Equal(t, int64(2), storedDoc.Properties["peer"])
		pinned, err := f.tx.GetNode(ctx, refreshedBystanderID)
		require.NoError(t, err)
		require.Equal(t, int64(1), pinned.Properties["peer"], "an unlocked peer commit stays outside the snapshot")
		_, err = f.tx.GetNode(ctx, NodeID("test:added"))
		require.ErrorIs(t, err, ErrNotFound, "a node committed after the snapshot is not part of it")
	})

	t.Run("equality cache matches changed and new properties and labels", func(t *testing.T) {
		changed, complete, err := f.tx.LookupNodesByEquality(ctx, "Doc", map[string]interface{}{"peer": int64(2)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, []string{"test:doc"}, nodeIDs(changed))

		newProperty, complete, err := f.tx.LookupNodesByEquality(ctx, "Doc", map[string]interface{}{"fresh": int64(7)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, []string{"test:doc"}, nodeIDs(newProperty))

		oldProperty, complete, err := f.tx.LookupNodesByEquality(ctx, "Doc", map[string]interface{}{"peer": int64(1)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, []string{"test:bystander", "test:doomed", "test:renewed"}, nodeIDs(oldProperty),
			"the refreshed entity no longer matches its old property value")

		newLabel, complete, err := f.tx.LookupNodesByEquality(ctx, "New", map[string]interface{}{"peer": int64(2)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, []string{"test:doc"}, nodeIDs(newLabel))

		newLabelProperty, complete, err := f.tx.LookupNodesByEquality(ctx, "New", map[string]interface{}{"fresh": int64(7)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Equal(t, []string{"test:doc"}, nodeIDs(newLabelProperty))

		unlockedLabel, complete, err := f.tx.LookupNodesByEquality(ctx, "Changed", map[string]interface{}{"peer": int64(9)})
		require.NoError(t, err)
		require.True(t, complete)
		require.Empty(t, unlockedLabel, "an unlocked entity's new label stays outside the snapshot")
	})

	t.Run("materialized edge populations", func(t *testing.T) {
		edges, err := f.tx.AllEdges(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"test:doomed-edge", "test:edge", "test:link", "test:own-edge"}, edgeIDs(edges))
		requireEdgePeers(t, edges, allEdgePeers, "AllEdges")

		hasEdges, err := f.tx.GetEdgesByType(ctx, "HAS")
		require.NoError(t, err)
		require.Equal(t, []string{"test:doomed-edge", "test:edge", "test:own-edge"}, edgeIDs(hasEdges))
		requireEdgePeers(t, hasEdges, map[EdgeID]interface{}{
			refreshedEdgeID:       int64(2),
			refreshedOwnEdgeID:    int64(2),
			refreshedDoomedEdgeID: int64(1),
		}, "materialized HAS type population")

		link, err := f.tx.GetEdgesByType(ctx, "LINK")
		require.NoError(t, err)
		requireEdgePeers(t, link, map[EdgeID]interface{}{refreshedLinkID: int64(1)}, "materialized LINK type population")
	})

	t.Run("streamed edge populations", func(t *testing.T) {
		for _, edgeType := range []string{"HAS", ""} {
			want := refreshedEdgesOfType(allEdgePeers, edgeType)
			requireEdgePeers(t, collectStreamEdges(t, func(visit func(*Edge) error) error {
				return f.tx.StreamEdgesByType(ctx, edgeType, visit)
			}), want, "streamed edge type "+edgeType)
			requireEdgePeers(t, collectStreamEdges(t, func(visit func(*Edge) error) error {
				return f.tx.StreamStoredEdgesByType(ctx, edgeType, visit)
			}), want, "stored streamed edge type "+edgeType)
		}
	})

	t.Run("adjacency and between reads", func(t *testing.T) {
		outgoing, err := f.tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: refreshedDocID})
		require.NoError(t, err)
		require.Equal(t, []string{"test:doomed-edge", "test:edge", "test:link", "test:own-edge"}, edgeIDs(outgoing))
		requireEdgePeers(t, outgoing, allEdgePeers, "outgoing adjacency")

		incoming, err := f.tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadIncoming, Node: refreshedPartID})
		require.NoError(t, err)
		requireEdgePeers(t, incoming, map[EdgeID]interface{}{
			refreshedEdgeID:    int64(2),
			refreshedOwnEdgeID: int64(2),
		}, "incoming adjacency")

		incomingPartTwo, err := f.tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadIncoming, Node: refreshedPartTwoID})
		require.NoError(t, err)
		requireEdgePeers(t, incomingPartTwo, map[EdgeID]interface{}{
			refreshedLinkID:       int64(1),
			refreshedDoomedEdgeID: int64(1),
		}, "incoming adjacency of an unlocked type")

		storedOutgoing, err := f.tx.GetStoredOutgoingEdges(ctx, refreshedDocID)
		require.NoError(t, err)
		requireEdgePeers(t, storedOutgoing, allEdgePeers, "stored outgoing adjacency")
		storedIncoming, err := f.tx.GetStoredIncomingEdges(ctx, refreshedPartID)
		require.NoError(t, err)
		requireEdgePeers(t, storedIncoming, map[EdgeID]interface{}{
			refreshedEdgeID:    int64(2),
			refreshedOwnEdgeID: int64(2),
		}, "stored incoming adjacency")

		between, err := f.tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadBetween, Node: refreshedDocID, Other: refreshedPartID})
		require.NoError(t, err)
		require.Equal(t, []string{"test:edge", "test:own-edge"}, edgeIDs(between))
		requireEdgePeers(t, between, map[EdgeID]interface{}{
			refreshedEdgeID:    int64(2),
			refreshedOwnEdgeID: int64(2),
		}, "between adjacency")

		// The between route keeps its documented current-view contract for a
		// write it holds no lock for; only the locked image is the overlay's.
		betweenUnlocked, err := f.tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadBetween, Node: refreshedDocID, Other: refreshedPartTwoID})
		require.NoError(t, err)
		require.Equal(t, []string{"test:doomed-edge", "test:link"}, edgeIDs(betweenUnlocked))

		edge, err := f.tx.GetEdgeBetween(ctx, refreshedDocID, refreshedPartID, "HAS")
		require.NoError(t, err)
		require.NotNil(t, edge)
		require.Equal(t, int64(2), edge.Properties["peer"])

		matched, err := f.tx.MatchEdgesBetween(ctx, refreshedDocID, refreshedPartID, "HAS", []string{"peer"}, func(edge *Edge) bool {
			return edge.Properties["peer"] == int64(2)
		})
		require.NoError(t, err)
		require.Equal(t, []string{"test:edge", "test:own-edge"}, edgeIDs(matched))
	})

	t.Run("point edge reads agree", func(t *testing.T) {
		edge, err := f.tx.GetEdge(ctx, refreshedEdgeID)
		require.NoError(t, err)
		require.Equal(t, int64(2), edge.Properties["peer"])
		require.Equal(t, int64(7), edge.Properties["fresh"])
		stored, err := f.tx.GetStoredEdge(ctx, refreshedEdgeID)
		require.NoError(t, err)
		require.Equal(t, int64(2), stored.Properties["peer"])
		pinned, err := f.tx.GetEdge(ctx, refreshedLinkID)
		require.NoError(t, err)
		require.Equal(t, int64(1), pinned.Properties["peer"], "an unlocked edge commit stays outside the snapshot")
	})

	t.Run("caller mutation cannot change a later read", func(t *testing.T) {
		nodes, err := f.tx.GetNodesByLabel(ctx, "Doc")
		require.NoError(t, err)
		doc := nodeWithID(t, nodes, refreshedDocID)
		doc.Properties["peer"] = int64(99)
		doc.Labels = append(doc.Labels, "Mutated")
		edges, err := f.tx.AllEdges(ctx)
		require.NoError(t, err)
		edgeWithID(t, edges, refreshedEdgeID).Properties["peer"] = int64(99)
		candidates, complete, err := f.tx.LookupNodesByEquality(ctx, "Doc", map[string]interface{}{"peer": int64(2)})
		require.NoError(t, err)
		require.True(t, complete)
		nodeWithID(t, candidates, refreshedDocID).Properties["peer"] = int64(99)

		again, err := f.tx.GetNodesByLabel(ctx, "Doc")
		require.NoError(t, err)
		require.Equal(t, int64(2), nodeWithID(t, again, refreshedDocID).Properties["peer"])
		require.NotContains(t, nodeWithID(t, again, refreshedDocID).Labels, "Mutated")
		point, err := f.tx.GetNode(ctx, refreshedDocID)
		require.NoError(t, err)
		require.Equal(t, int64(2), point.Properties["peer"])
		edge, err := f.tx.GetEdge(ctx, refreshedEdgeID)
		require.NoError(t, err)
		require.Equal(t, int64(2), edge.Properties["peer"])
	})

	t.Run("read composition stages nothing", func(t *testing.T) {
		require.Zero(t, f.tx.OperationCount(), "refreshing a no-op and composing reads must not manufacture writes")
		require.Empty(t, f.tx.pendingNodes)
		require.Empty(t, f.tx.pendingEdges)
		require.Empty(t, f.tx.deletedNodes)
		require.Empty(t, f.tx.deletedEdges)
		require.Empty(t, f.tx.pendingWrites)
		require.Empty(t, f.tx.pendingDeletes)
		require.Nil(t, f.tx.pendingAdjacency, "read-only adjacency composition must not enter the staged index")
	})
}

func TestWriteOwnedReadImagesYieldToOwnStagedWritesAndDeletes(t *testing.T) {
	f := newRefreshedReadFixture(t)
	warmRefreshedReadRoutes(t, f)
	commitPeerUpdates(t, f)
	ctx := t.Context()

	f.lockNode(t, refreshedDocID)
	f.lockNode(t, refreshedRenewedID)
	f.lockNode(t, refreshedDoomedID)
	f.lockEdge(t, refreshedEdgeID)
	f.lockEdge(t, refreshedOwnEdgeID)
	f.lockEdge(t, refreshedDoomedEdgeID)

	renewed, err := f.tx.GetNode(ctx, refreshedRenewedID)
	require.NoError(t, err)
	renewed.Properties["peer"] = int64(3)
	require.NoError(t, f.tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: renewed, Policy: ClientNodeEdits}))

	ownEdge, err := f.tx.GetEdge(ctx, refreshedOwnEdgeID)
	require.NoError(t, err)
	ownEdge.Properties["peer"] = int64(4)
	require.NoError(t, f.tx.UpdateEdge(ctx, EdgeUpdateRequest{After: ownEdge}))

	require.NoError(t, f.tx.DeleteGraphNode(ctx, refreshedDoomedID, CascadeEdges))
	require.NoError(t, f.tx.DeleteEdge(ctx, refreshedDoomedEdgeID))

	nodes, err := f.tx.GetNodesByLabel(ctx, "Doc")
	require.NoError(t, err)
	require.Equal(t, []string{"test:bystander", "test:doc", "test:renewed"}, nodeIDs(nodes))
	requireNodePeers(t, nodes, map[NodeID]interface{}{
		refreshedDocID:       int64(2),
		refreshedRenewedID:   int64(3),
		refreshedBystanderID: int64(1),
	}, "staged change and delete override refreshed images")

	requireNodePeers(t, collectStreamNodes(t, func(visit func(*Node) error) error {
		return f.tx.StreamNodesByLabelProjected(ctx, "Doc", nil, visit)
	}), map[NodeID]interface{}{
		refreshedDocID:       int64(2),
		refreshedRenewedID:   int64(3),
		refreshedBystanderID: int64(1),
	}, "staged change in the label stream")

	all, err := f.tx.AllNodes(ctx)
	require.NoError(t, err)
	requireNodePeers(t, all, map[NodeID]interface{}{
		refreshedDocID:       int64(2),
		refreshedRenewedID:   int64(3),
		refreshedBystanderID: int64(1),
		refreshedPartID:      nil,
		refreshedPartTwoID:   nil,
	}, "staged change and delete in AllNodes")

	point, err := f.tx.GetNode(ctx, refreshedRenewedID)
	require.NoError(t, err)
	require.Equal(t, int64(3), point.Properties["peer"])
	_, err = f.tx.GetNode(ctx, refreshedDoomedID)
	require.ErrorIs(t, err, ErrNotFound)

	changed, complete, err := f.tx.LookupNodesByEquality(ctx, "Doc", map[string]interface{}{"peer": int64(3)})
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, []string{"test:renewed"}, nodeIDs(changed))
	latestOnly, complete, err := f.tx.LookupNodesByEquality(ctx, "Doc", map[string]interface{}{"peer": int64(2)})
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, []string{"test:doc"}, nodeIDs(latestOnly), "a staged image is not also answered by its latest committed image")
	deleted, complete, err := f.tx.LookupNodesByEquality(ctx, "Doc", map[string]interface{}{"peer": int64(8)})
	require.NoError(t, err)
	require.True(t, complete)
	require.Empty(t, deleted, "a staged delete removes the refreshed image")

	edges, err := f.tx.GetEdgesByType(ctx, "HAS")
	require.NoError(t, err)
	require.Equal(t, []string{"test:edge", "test:own-edge"}, edgeIDs(edges))
	requireEdgePeers(t, edges, map[EdgeID]interface{}{
		refreshedEdgeID:    int64(2),
		refreshedOwnEdgeID: int64(4),
	}, "staged edge change and delete override refreshed images")

	allEdges, err := f.tx.AllEdges(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"test:edge", "test:link", "test:own-edge"}, edgeIDs(allEdges))
	requireEdgePeers(t, allEdges, map[EdgeID]interface{}{
		refreshedEdgeID:    int64(2),
		refreshedOwnEdgeID: int64(4),
		refreshedLinkID:    int64(1),
	}, "AllEdges with a staged delete")

	outgoing, err := f.tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: refreshedDocID})
	require.NoError(t, err)
	require.Equal(t, []string{"test:edge", "test:link", "test:own-edge"}, edgeIDs(outgoing))
	requireEdgePeers(t, outgoing, map[EdgeID]interface{}{
		refreshedEdgeID:    int64(2),
		refreshedOwnEdgeID: int64(4),
		refreshedLinkID:    int64(1),
	}, "outgoing adjacency with a staged delete")

	_, err = f.tx.GetEdge(ctx, refreshedDoomedEdgeID)
	require.ErrorIs(t, err, ErrNotFound)

	require.NotContains(t, f.tx.pendingNodes, refreshedDocID, "a lock-only refresh is a read image, not a staged write")
	require.Contains(t, f.tx.pendingNodes, refreshedRenewedID)
	require.Contains(t, f.tx.deletedNodes, refreshedDoomedID)
	require.Contains(t, f.tx.deletedEdges, refreshedDoomedEdgeID)
}

func TestWriteOwnedReadImagesPropagateCancellation(t *testing.T) {
	f := newRefreshedReadFixture(t)
	ctx := t.Context()
	doc, err := f.tx.GetNode(ctx, refreshedDocID)
	require.NoError(t, err)
	require.Equal(t, int64(1), doc.Properties["peer"])

	commitPeerUpdates(t, f)
	f.lockNode(t, refreshedDocID)

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	readCalls := 0
	images, err := writeOwnedReadImagesLocked(canceled, f.tx.pendingNodes, f.tx.deletedNodes, &f.tx.entityLocks, nodeWriteLockKind, func(context.Context, NodeID) (*Node, error) {
		readCalls++
		return nil, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, images)
	require.Zero(t, readCalls, "the composed read must observe cancellation before reading an image")

	_, err = f.tx.GetNodesByLabel(canceled, "Doc")
	require.ErrorIs(t, err, context.Canceled)
	_, err = f.tx.AllNodes(canceled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = f.tx.GetEdgesByType(canceled, "HAS")
	require.ErrorIs(t, err, context.Canceled)

	require.Zero(t, f.tx.OperationCount())
	live, err := f.tx.GetNode(ctx, refreshedDocID)
	require.NoError(t, err)
	require.Equal(t, int64(2), live.Properties["peer"], "a canceled read leaves the transaction usable")
}

func collectStreamNodes(t *testing.T, stream func(func(*Node) error) error) []*Node {
	t.Helper()
	var nodes []*Node
	require.NoError(t, stream(func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	}))
	return nodes
}

func collectStreamEdges(t *testing.T, stream func(func(*Edge) error) error) []*Edge {
	t.Helper()
	var edges []*Edge
	require.NoError(t, stream(func(edge *Edge) error {
		edges = append(edges, edge)
		return nil
	}))
	return edges
}

// refreshedEdgesOfType narrows an expected edge-peer map to one streamed type.
func refreshedEdgesOfType(want map[EdgeID]interface{}, edgeType string) map[EdgeID]interface{} {
	if edgeType == "" {
		return want
	}
	types := map[EdgeID]string{
		refreshedEdgeID:       "HAS",
		refreshedOwnEdgeID:    "HAS",
		refreshedDoomedEdgeID: "HAS",
		refreshedLinkID:       "LINK",
	}
	filtered := make(map[EdgeID]interface{}, len(want))
	for id, peer := range want {
		if types[id] == edgeType {
			filtered[id] = peer
		}
	}
	return filtered
}

func requireNodePeers(t *testing.T, nodes []*Node, want map[NodeID]interface{}, what string) {
	t.Helper()
	require.Len(t, nodes, len(want), what)
	for _, node := range nodes {
		expected, ok := want[node.ID]
		require.True(t, ok, "%s: unexpected node %s", what, node.ID)
		require.Equal(t, expected, node.Properties["peer"], "%s: node %s", what, node.ID)
	}
}

func requireEdgePeers(t *testing.T, edges []*Edge, want map[EdgeID]interface{}, what string) {
	t.Helper()
	require.Len(t, edges, len(want), what)
	for _, edge := range edges {
		expected, ok := want[edge.ID]
		require.True(t, ok, "%s: unexpected edge %s", what, edge.ID)
		require.Equal(t, expected, edge.Properties["peer"], "%s: edge %s", what, edge.ID)
	}
}

func nodeWithID(t *testing.T, nodes []*Node, id NodeID) *Node {
	t.Helper()
	for _, node := range nodes {
		if node != nil && node.ID == id {
			return node
		}
	}
	t.Fatalf("node %s not found in %v", id, nodeIDs(nodes))
	return nil
}

func edgeWithID(t *testing.T, edges []*Edge, id EdgeID) *Edge {
	t.Helper()
	for _, edge := range edges {
		if edge != nil && edge.ID == id {
			return edge
		}
	}
	t.Fatalf("edge %s not found in %v", id, edgeIDs(edges))
	return nil
}

func TestWriteOwnedReadImagesApplyLiveVisibility(t *testing.T) {
	f := newRefreshedReadFixture(t)
	warmRefreshedReadRoutes(t, f)
	commitPeerUpdates(t, f)
	f.lockNode(t, refreshedDocID)
	f.lockEdge(t, refreshedEdgeID)
	ctx := t.Context()
	require.NoError(t, f.engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(refreshedDocID), true))
	require.NoError(t, f.engine.SetManualSuppression(ctx, StoredEntityEdgeKind, string(refreshedEdgeID), true))
	for name, read := range map[string]func() ([]*Node, error){
		"all":   func() ([]*Node, error) { return f.tx.AllNodes(ctx) },
		"label": func() ([]*Node, error) { return f.tx.GetNodesByLabel(ctx, "Doc") },
		"stream": func() ([]*Node, error) {
			var rows []*Node
			err := f.tx.StreamNodesWithOptions(ctx, StreamNodesOptions{}, func(n *Node) error { rows = append(rows, n); return nil })
			return rows, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			rows, err := read()
			require.NoError(t, err)
			for _, n := range rows {
				require.NotEqual(t, refreshedDocID, n.ID)
			}
		})
	}
	for name, read := range map[string]func() ([]*Edge, error){
		"all_edges": func() ([]*Edge, error) { return f.tx.AllEdges(ctx) },
		"matcher": func() ([]*Edge, error) {
			return f.tx.MatchEdgesBetween(ctx, refreshedDocID, refreshedPartID, "HAS", nil, func(*Edge) bool { return true })
		},
		"type": func() ([]*Edge, error) { return f.tx.GetEdgesByType(ctx, "HAS") },
		"outgoing": func() ([]*Edge, error) {
			return f.tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: refreshedDocID})
		},
	} {
		t.Run(name, func(t *testing.T) {
			rows, err := read()
			require.NoError(t, err)
			for _, e := range rows {
				require.NotEqual(t, refreshedEdgeID, e.ID)
			}
		})
	}
	stored, err := f.tx.GetStoredNode(ctx, refreshedDocID)
	require.NoError(t, err)
	require.Equal(t, int64(2), stored.Properties["peer"])
	storedEdge, err := f.tx.GetStoredEdge(ctx, refreshedEdgeID)
	require.NoError(t, err)
	require.Equal(t, int64(2), storedEdge.Properties["peer"])
}
