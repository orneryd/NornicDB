package storage

// NOR-136 explicit-ID create/create regression against write-behind.
//
// The authorised reproduction is the unmodified b098 baseline order:
//
//  1. an implicit (autocommit) transaction begins and stages CreateNode (or
//     CreateEdge) for an explicit ID while nothing holds that ID yet, so
//     staging legitimately finds no conflict;
//  2. a peer implicit transaction creates the same explicit ID and is
//     acknowledged by the write-behind buffer (never drained: the fixture's
//     background flusher is stopped, so the ACK is genuinely buffered);
//  3. the first transaction commits.
//
// Exactly one of the two creates may be acknowledged. The second one must
// fail with ErrConflict or ErrAlreadyExists; the peer's buffered create owns
// the ID, and after an explicit flush and a restart the entity body, the
// stored counters, the stored indexes and the adjacency must all describe
// that single entity with no duplicated or lost count.
//
// Both tests drive the canonical storage producers only:
// newWriteBehindTestEngine (manualDrain), bufferedWrite,
// BadgerTransaction.CreateNode/CreateEdge, FlushWriteBehind. No Cypher,
// parser, hook or checker is involved, and no production file is changed for
// these tests.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNOR136ExplicitIDCreateConflictWithBufferedPeerNode reproduces the node
// shape: a staged explicit-ID CREATE is acknowledged although a peer's
// buffered CREATE of the same ID was acknowledged first.
func TestNOR136ExplicitIDCreateConflictWithBufferedPeerNode(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, manualDrain: true})
	require.Zero(t, engine.writeBehind.PendingOps(), "the fixture starts with an empty buffer")

	const id = NodeID("test:same")
	writerNode := &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"owner": "writer"}}
	peerNode := &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"owner": "peer"}}

	// 1. Begin and stage the explicit-ID create while the ID is still free.
	writer, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	writerOpen := true
	t.Cleanup(func() {
		if writerOpen {
			_ = writer.Rollback()
		}
	})
	require.NoError(t, writer.SetImplicit(true))
	require.NoError(t, writer.SetNamespace("test"))
	_, err = writer.CreateNode(ctx, writerNode)
	require.NoError(t, err, "staging a create for a free explicit ID must not conflict")

	// 2. The peer creates the same ID and is acknowledged through the buffer.
	pendingBeforePeer := engine.writeBehind.PendingOps()
	peer := bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		_, err := tx.CreateNode(ctx, peerNode)
		return err
	})
	require.True(t, peer.AcknowledgedBuffered(), "the peer create must be acknowledged by the write-behind buffer")
	require.Greater(t, engine.writeBehind.PendingOps(), pendingBeforePeer,
		"the acknowledged peer create must still be undrained")
	pendingAfterPeer := engine.writeBehind.PendingOps()

	// 3. The first create must not also be acknowledged.
	err = writer.Commit()
	writerOpen = false
	require.Error(t, err, "the second explicit-ID create was acknowledged after the peer's buffered create")
	require.True(t, errors.Is(err, ErrConflict) || errors.Is(err, ErrAlreadyExists),
		"a create for an ID a peer already acknowledged must fail with ErrConflict or ErrAlreadyExists, got: %v", err)
	require.False(t, writer.AcknowledgedBuffered(), "a rejected create must not report a buffered acknowledgement")
	require.Equal(t, pendingAfterPeer, engine.writeBehind.PendingOps(),
		"the rejected create must not enqueue another buffered commit")

	// The peer's acknowledged image is the only one visible, before and after
	// the flush.
	visible, err := engine.GetNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "peer", visible.Properties["owner"], "the peer's buffered image must own the ID")

	require.NoError(t, engine.FlushWriteBehind(ctx))
	require.Zero(t, engine.writeBehind.PendingOps())

	assertNodeState := func(t *testing.T, engine *BadgerEngine) {
		t.Helper()
		visible, err := engine.GetNode(ctx, id)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"Doc"}, visible.Labels)
		require.Equal(t, "peer", visible.Properties["owner"], "the acknowledged peer create owns the ID")

		stored, err := engine.GetStoredNode(ctx, id)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"Doc"}, stored.Labels)
		require.Equal(t, "peer", stored.Properties["owner"], "the stored body must be the peer's acknowledged create")

		total, err := engine.NodeCount(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 1, total, "exactly one node must exist")

		count, err := engine.NodeCountByLabelInNamespace(ctx, "test", "Doc")
		require.NoError(t, err)
		require.EqualValues(t, 1, count, "exactly one Doc node must be counted")

		indexed, err := engine.GetStoredNodesByLabel(ctx, "Doc")
		require.NoError(t, err)
		require.Len(t, indexed, 1, "the stored label index must hold exactly one Doc node")
		require.Equal(t, id, indexed[0].ID)

		// Independent full recount: decode every raw node body under the test
		// namespace instead of trusting the derived counters.
		bodyTotal, bodyCounts, err := engine.collectNodeLabelCountsByPrefix(append([]byte{prefixNode}, []byte("test:")...))
		require.NoError(t, err)
		require.EqualValues(t, 1, bodyTotal, "primary node bodies must hold exactly one node")
		require.Equal(t, map[namespaceLabel]int64{{namespace: "test", label: "Doc"}: 1}, bodyCounts,
			"primary-body recount must agree with the persisted label counter")
	}
	assertNodeState(t, engine)

	require.NoError(t, engine.Close())
	reopened := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, manualDrain: true})
	require.Zero(t, reopened.writeBehind.PendingOps(), "a flushed and closed generation must not replay")
	assertNodeState(t, reopened)
}

// TestNOR136ExplicitIDCreateConflictWithBufferedPeerEdge reproduces the
// relationship shape: a staged explicit-ID edge CREATE with distinct
// endpoints must be rejected once a peer's buffered CREATE of the same edge
// ID between other endpoints is acknowledged, leaving one edge and no
// duplicated type, label or adjacency entry.
func TestNOR136ExplicitIDCreateConflictWithBufferedPeerEdge(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, manualDrain: true})
	require.Zero(t, engine.writeBehind.PendingOps(), "the fixture starts with an empty buffer")

	const edgeID = EdgeID("test:rel-same")
	endpoints := []NodeID{"test:node-a", "test:node-b", "test:node-c", "test:node-d"}
	for _, id := range endpoints {
		bufferedCreate(t, engine, &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"name": string(id)}})
	}
	require.NoError(t, engine.FlushWriteBehind(ctx))
	require.Zero(t, engine.writeBehind.PendingOps(), "the seeded endpoints must be durable before the create race")

	peerEdge := &Edge{ID: edgeID, StartNode: "test:node-a", EndNode: "test:node-b", Type: "LINK",
		Properties: map[string]any{"owner": "peer"}}
	writerEdge := &Edge{ID: edgeID, StartNode: "test:node-c", EndNode: "test:node-d", Type: "LINK",
		Properties: map[string]any{"owner": "writer"}}

	// 1. Begin and stage the explicit-ID edge create while the ID is free.
	writer, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	writerOpen := true
	t.Cleanup(func() {
		if writerOpen {
			_ = writer.Rollback()
		}
	})
	require.NoError(t, writer.SetImplicit(true))
	require.NoError(t, writer.SetNamespace("test"))
	require.NoError(t, writer.CreateEdge(ctx, writerEdge),
		"staging an edge create for a free explicit ID must not conflict")

	// 2. The peer creates the same edge ID between different endpoints and is
	// acknowledged through the buffer.
	pendingBeforePeer := engine.writeBehind.PendingOps()
	peer := bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		return tx.CreateEdge(ctx, peerEdge)
	})
	require.True(t, peer.AcknowledgedBuffered(), "the peer edge create must be acknowledged by the write-behind buffer")
	require.Greater(t, engine.writeBehind.PendingOps(), pendingBeforePeer,
		"the acknowledged peer edge create must still be undrained")
	pendingAfterPeer := engine.writeBehind.PendingOps()

	// 3. The first edge create must not also be acknowledged.
	err = writer.Commit()
	writerOpen = false
	require.Error(t, err, "the second explicit-ID edge create was acknowledged after the peer's buffered create")
	require.True(t, errors.Is(err, ErrConflict) || errors.Is(err, ErrAlreadyExists),
		"an edge create for an ID a peer already acknowledged must fail with ErrConflict or ErrAlreadyExists, got: %v", err)
	require.False(t, writer.AcknowledgedBuffered(), "a rejected edge create must not report a buffered acknowledgement")
	require.Equal(t, pendingAfterPeer, engine.writeBehind.PendingOps(),
		"the rejected edge create must not enqueue another buffered commit")

	require.NoError(t, engine.FlushWriteBehind(ctx))
	require.Zero(t, engine.writeBehind.PendingOps())

	assertEdgeState := func(t *testing.T, engine *BadgerEngine) {
		t.Helper()
		visible, err := engine.GetEdge(ctx, edgeID)
		require.NoError(t, err)
		require.Equal(t, NodeID("test:node-a"), visible.StartNode, "the peer's acknowledged edge must own the ID")
		require.Equal(t, NodeID("test:node-b"), visible.EndNode, "the peer's acknowledged edge must own the ID")
		require.Equal(t, "LINK", visible.Type)
		require.Equal(t, "peer", visible.Properties["owner"])

		stored, err := engine.GetStoredEdge(ctx, edgeID)
		require.NoError(t, err)
		require.Equal(t, NodeID("test:node-a"), stored.StartNode)
		require.Equal(t, NodeID("test:node-b"), stored.EndNode)
		require.Equal(t, "LINK", stored.Type)
		require.Equal(t, "peer", stored.Properties["owner"], "the stored body must be the peer's acknowledged create")

		total, err := engine.EdgeCount(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 1, total, "exactly one relationship must exist")

		typed, err := engine.EdgeCountByTypeInNamespace(ctx, "test", "LINK")
		require.NoError(t, err)
		require.EqualValues(t, 1, typed, "exactly one LINK relationship must be counted")

		startLabeled, err := engine.EdgeCountByStartLabelInNamespace(ctx, "test", "Doc", "LINK")
		require.NoError(t, err)
		require.EqualValues(t, 1, startLabeled, "exactly one LINK relationship starts at a Doc node")

		endLabeled, err := engine.EdgeCountByEndLabelInNamespace(ctx, "test", "Doc", "LINK")
		require.NoError(t, err)
		require.EqualValues(t, 1, endLabeled, "exactly one LINK relationship ends at a Doc node")

		out, err := engine.ReadStoredAdjacentEdges(ctx, AdjacentEdgeRead{Node: "test:node-a"})
		require.NoError(t, err)
		require.Len(t, out, 1, "the acknowledged edge must be adjacent to its start node")
		require.Equal(t, edgeID, out[0].ID)

		in, err := engine.ReadStoredAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "test:node-b"})
		require.NoError(t, err)
		require.Len(t, in, 1, "the acknowledged edge must be adjacent to its end node")
		require.Equal(t, edgeID, in[0].ID)

		out, err = engine.ReadStoredAdjacentEdges(ctx, AdjacentEdgeRead{Node: "test:node-c"})
		require.NoError(t, err)
		require.Empty(t, out, "the rejected create must not publish adjacency for its start node")

		in, err = engine.ReadStoredAdjacentEdges(ctx, AdjacentEdgeRead{Direction: AdjacentIncoming, Node: "test:node-d"})
		require.NoError(t, err)
		require.Empty(t, in, "the rejected create must not publish adjacency for its end node")

		nodes, err := engine.NodeCount(ctx)
		require.NoError(t, err)
		require.EqualValues(t, len(endpoints), nodes, "the rejected edge create must not disturb the node total")

		docs, err := engine.NodeCountByLabelInNamespace(ctx, "test", "Doc")
		require.NoError(t, err)
		require.EqualValues(t, len(endpoints), docs, "the rejected edge create must not disturb the Doc count")
	}
	assertEdgeState(t, engine)

	require.NoError(t, engine.Close())
	reopened := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{dataDir: dir, manualDrain: true})
	require.Zero(t, reopened.writeBehind.PendingOps(), "a flushed and closed generation must not replay")
	assertEdgeState(t, reopened)
}
