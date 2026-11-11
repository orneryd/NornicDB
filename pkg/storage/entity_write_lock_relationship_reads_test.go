package storage

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR-213: a transaction that holds a node's write lock reads that node's
// relationships as other transactions committed them, as it reads the node's
// properties. A peer that creates or deletes a relationship on the node does
// not move the node's own head, so the lock's property refresh does not see
// it; the relationship reads must not depend on that refresh.

const (
	lockedAdjacencyNode  = NodeID("test:a")
	lockedAdjacencyOther = NodeID("test:b")
	lockedAdjacencyThird = NodeID("test:c")
	lockedAdjacencyEdge  = EdgeID("test:a-b")
)

func newLockedAdjacencyEngine(t *testing.T, seedEdge bool) *MemoryEngine {
	t.Helper()
	ctx := t.Context()
	engine := newTestMemoryEngine(t)
	for _, id := range []NodeID{lockedAdjacencyNode, lockedAdjacencyOther, lockedAdjacencyThird} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Probe"}, Properties: map[string]interface{}{"id": string(id)}})
		require.NoError(t, err)
	}
	if seedEdge {
		require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: lockedAdjacencyEdge, StartNode: lockedAdjacencyNode, EndNode: lockedAdjacencyOther, Type: "LINK"}))
	}
	return engine
}

// beginPinnedAdjacencyReader opens a transaction and pins its snapshot with a
// read of the node's relationships.
func beginPinnedAdjacencyReader(t *testing.T, engine *MemoryEngine, wantBefore int) *BadgerTransaction {
	t.Helper()
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	require.Equal(t, wantBefore, len(lockedAdjacencyReads(t, tx)["outgoing"]))
	return tx
}

// lockedAdjacencyReads reads the node's relationships through every
// transaction adjacency route: full outgoing records, outgoing headers, the
// other end's incoming records, and the both-ends lookup.
func lockedAdjacencyReads(t *testing.T, tx *BadgerTransaction) map[string][]string {
	t.Helper()
	ctx := t.Context()
	out, err := tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadOutgoing, Node: lockedAdjacencyNode})
	require.NoError(t, err)
	headers, answered, err := tx.OutgoingEdgeHeaders(ctx, lockedAdjacencyNode)
	require.NoError(t, err)
	require.True(t, answered)
	between, err := tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadBetween, Node: lockedAdjacencyNode, Other: lockedAdjacencyOther})
	require.NoError(t, err)
	incoming, err := tx.ReadEdges(ctx, TransactionEdgeRead{Kind: TransactionReadIncoming, Node: lockedAdjacencyOther})
	require.NoError(t, err)
	return map[string][]string{
		"outgoing":       sortedEdgeIDs(out),
		"headers":        sortedEdgeIDs(headers),
		"between":        sortedEdgeIDs(between),
		"other-incoming": sortedEdgeIDs(incoming),
	}
}

func sortedEdgeIDs(edges []*Edge) []string {
	ids := make([]string, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, string(edge.ID))
	}
	sort.Strings(ids)
	return ids
}

func TestLockedNodeReadsRelationshipsAPeerCreated(t *testing.T) {
	ctx := t.Context()
	engine := newLockedAdjacencyEngine(t, false)
	tx := beginPinnedAdjacencyReader(t, engine, 0)
	unlocked := beginPinnedAdjacencyReader(t, engine, 0)

	// The peer creates a relationship on the node without writing the node.
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: lockedAdjacencyEdge, StartNode: lockedAdjacencyNode, EndNode: lockedAdjacencyOther, Type: "LINK"}))

	latest, err := tx.LockNodeForWrite(ctx, lockedAdjacencyNode)
	require.NoError(t, err)
	require.Nil(t, latest, "a relationship commit does not move the node's own head, so the lock refreshes no property")

	reads := lockedAdjacencyReads(t, tx)
	want := []string{string(lockedAdjacencyEdge)}
	require.Equal(t, want, reads["outgoing"], "full adjacency of the locked node")
	require.Equal(t, want, reads["headers"], "header adjacency of the locked node")
	require.Equal(t, want, reads["between"], "both-ends lookup")
	require.Empty(t, reads["other-incoming"], "the unlocked other end keeps the snapshot")

	stale := lockedAdjacencyReads(t, unlocked)
	require.Empty(t, stale["outgoing"], "a transaction without the lock keeps the snapshot")
	require.Empty(t, stale["headers"], "a transaction without the lock keeps the snapshot")
}

func TestLockedNodeReadsRelationshipsAPeerDeleted(t *testing.T) {
	ctx := t.Context()
	engine := newLockedAdjacencyEngine(t, true)
	tx := beginPinnedAdjacencyReader(t, engine, 1)
	unlocked := beginPinnedAdjacencyReader(t, engine, 1)

	require.NoError(t, engine.DeleteEdge(ctx, lockedAdjacencyEdge))

	latest, err := tx.LockNodeForWrite(ctx, lockedAdjacencyNode)
	require.NoError(t, err)
	require.Nil(t, latest, "a relationship delete does not move the node's own head")

	reads := lockedAdjacencyReads(t, tx)
	require.Empty(t, reads["outgoing"], "the peer's delete is seen")
	require.Empty(t, reads["headers"], "the peer's delete is seen")
	require.Equal(t, []string{string(lockedAdjacencyEdge)}, lockedAdjacencyReads(t, unlocked)["outgoing"], "a transaction without the lock keeps the snapshot")
}

func TestLockedNodeRelationshipReadsKeepTheTransactionsOwnWrites(t *testing.T) {
	ctx := t.Context()
	engine := newLockedAdjacencyEngine(t, true)
	tx := beginPinnedAdjacencyReader(t, engine, 1)

	peerEdge := EdgeID("test:a-c-peer")
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: peerEdge, StartNode: lockedAdjacencyNode, EndNode: lockedAdjacencyThird, Type: "LINK"}))
	_, err := tx.LockNodeForWrite(ctx, lockedAdjacencyNode)
	require.NoError(t, err)

	ownEdge := EdgeID("test:a-c-own")
	require.NoError(t, tx.CreateEdge(ctx, &Edge{ID: ownEdge, StartNode: lockedAdjacencyNode, EndNode: lockedAdjacencyThird, Type: "LINK"}))
	require.NoError(t, tx.DeleteEdge(ctx, lockedAdjacencyEdge))

	reads := lockedAdjacencyReads(t, tx)
	want := []string{string(ownEdge), string(peerEdge)}
	sort.Strings(want)
	require.Equal(t, want, reads["outgoing"], "own create and own delete over the committed relationships")
	require.Equal(t, want, reads["headers"], "own create and own delete over the committed relationships")
}

// TestLockedNodeRelationshipReadsAreTheSnapshotWithoutALaterCommit: with no
// commit after the snapshot, the lock changes nothing.
func TestLockedNodeRelationshipReadsAreTheSnapshotWithoutALaterCommit(t *testing.T) {
	engine := newLockedAdjacencyEngine(t, true)
	tx := beginPinnedAdjacencyReader(t, engine, 1)
	_, err := tx.LockNodeForWrite(context.Background(), lockedAdjacencyNode)
	require.NoError(t, err)
	require.Equal(t, []string{string(lockedAdjacencyEdge)}, lockedAdjacencyReads(t, tx)["outgoing"])
}

// TestLockedNodeReadsTheOtherEndOfANewRelationship: a peer creates node y and
// the locked node's relationship to it after the snapshot. The relationship
// read returns it, and the transaction reads y as committed; a transaction
// without the lock does neither.
func TestLockedNodeReadsTheOtherEndOfANewRelationship(t *testing.T) {
	ctx := t.Context()
	engine := newLockedAdjacencyEngine(t, false)
	tx := beginPinnedAdjacencyReader(t, engine, 0)
	unlocked := beginPinnedAdjacencyReader(t, engine, 0)

	newEnd := NodeID("test:y")
	_, err := engine.CreateNode(ctx, &Node{ID: newEnd, Labels: []string{"Held"}, Properties: map[string]interface{}{"key": "y"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:a-y", StartNode: lockedAdjacencyNode, EndNode: newEnd, Type: "HAS"}))

	_, err = tx.GetNode(ctx, newEnd)
	require.ErrorIs(t, err, ErrNotFound, "before the lock the new node is outside the snapshot")
	_, err = tx.LockNodeForWrite(ctx, lockedAdjacencyNode)
	require.NoError(t, err)
	require.Equal(t, []string{"test:a-y"}, lockedAdjacencyReads(t, tx)["outgoing"])

	for name, read := range map[string]func(context.Context, NodeID) (*Node, error){
		"GetNode":      tx.GetNode,
		"GetQueryNode": tx.GetQueryNode,
		"GetNodeProjected": func(ctx context.Context, id NodeID) (*Node, error) {
			return tx.GetNodeProjected(ctx, id, []string{"key"})
		},
	} {
		node, err := read(ctx, newEnd)
		require.NoError(t, err, name)
		require.Equal(t, []string{"Held"}, node.Labels, name)
		require.Equal(t, "y", node.Properties["key"], name)
	}

	_, err = unlocked.GetNode(ctx, newEnd)
	require.ErrorIs(t, err, ErrNotFound, "a transaction without the lock keeps the snapshot")
}
