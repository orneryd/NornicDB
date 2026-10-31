// Author: NOR-288/NOR-292 implementation lane (Codex), 2026-10-27.
package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcceptedReplayMaterializesDecodedTimestampImages(t *testing.T) {
	base, wal, owner, dir := acceptedCloseFixture(t)
	tx, err := BeginEntityTransaction(base, CommitModeExplicit)
	require.NoError(t, err)
	seed := time.Unix(1, 0).UTC()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err = tx.CreateNode(&Node{ID: id, CreatedAt: seed, UpdatedAt: seed})
		require.NoError(t, err)
		require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: id}, Policy: StoredNodeImages}))
	}
	require.NoError(t, tx.CreateEdge(&Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK", CreatedAt: seed, UpdatedAt: seed}))
	require.NoError(t, tx.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: "test:e", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}}))
	prepared, err := tx.PrepareEntityCommit(t.Context(), EntityCommitRequest{LogicalID: "accepted-zero-time"})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
	require.NoError(t, err)
	prepared.Abort()
	_, accepted, err := DecodeEntityTransaction(payload)
	require.NoError(t, err)
	// The merged semantic codec's zero/epoch distinctions have a separate
	// owner. This test asserts the native application contract on the actual
	// decoded image, not lossless encoding of the original unset input.
	acceptedNodes := map[NodeID]*Node{}
	for _, op := range accepted {
		if op.Node != nil {
			acceptedNodes[op.Node.ID] = op.Node
		}
	}
	canonicalEdge := accepted[len(accepted)-1].Edge
	// Compact stored edge images use UTC; the semantic decoder returns the
	// same instant in the process's local zone.
	canonicalEdge.CreatedAt = canonicalEdge.CreatedAt.UTC()
	canonicalEdge.UpdatedAt = canonicalEdge.UpdatedAt.UTC()
	require.Equal(t, compactEdgeTime(time.Time{}), canonicalEdge.CreatedAt, "existing compact body limitation is unchanged")
	require.Equal(t, compactEdgeTime(time.Time{}), canonicalEdge.UpdatedAt)
	require.NoError(t, wal.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, "test"))
	assertImages := func(engine *BadgerEngine) {
		t.Helper()
		for _, id := range []NodeID{"test:a", "test:b"} {
			node, err := engine.GetStoredNode(id)
			require.NoError(t, err)
			require.Equal(t, acceptedNodes[id].CreatedAt.UTC(), node.CreatedAt.UTC(), "accepted decoded CreatedAt is data, not a new-create default")
			require.Equal(t, acceptedNodes[id].UpdatedAt.UTC(), node.UpdatedAt.UTC())
		}
		edge, err := engine.GetStoredEdge("test:e")
		require.NoError(t, err)
		require.Equal(t, canonicalEdge.CreatedAt, edge.CreatedAt, "accepted canonical edge CreatedAt remains identical")
		require.Equal(t, canonicalEdge.UpdatedAt, edge.UpdatedAt, "accepted canonical edge UpdatedAt remains identical")
		nodes, err := engine.NodeCount()
		require.NoError(t, err)
		require.EqualValues(t, 2, nodes)
		edges, err := engine.EdgeCount()
		require.NoError(t, err)
		require.EqualValues(t, 1, edges)
	}
	require.NoError(t, ApplyAcceptedEntityTransaction(owner, payload))
	assertImages(base)
	require.EqualValues(t, 1, wal.Sequence(), "accepted application never records a new command")
	require.NoError(t, owner.Close())
	recovered, replay, err := RecoverFromWALWithResult(dir, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = recovered.Close() })
	require.Empty(t, replay.Errors)
	assertImages(recovered.BadgerEngine)
	require.NoError(t, recovered.UpdateNode(NodeUpdateRequest{After: &Node{ID: "test:a", Properties: map[string]any{"v": "writable"}}}))
}

func TestNativeEdgeEpochTimeSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	base, err := NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := base.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	epoch := time.Unix(0, 0).UTC()
	require.NoError(t, base.CreateEdge(&Edge{ID: "test:epoch", StartNode: "test:a", EndNode: "test:b", Type: "LINK", CreatedAt: epoch, UpdatedAt: epoch}))
	require.NoError(t, base.Close())
	base, err = NewBadgerEngine(dir)
	require.NoError(t, err)
	edge, err := base.GetStoredEdge("test:epoch")
	require.NoError(t, err)
	require.Equal(t, epoch, edge.CreatedAt, "old zero-nanosecond compact bodies remain epoch timestamps")
	require.Equal(t, epoch, edge.UpdatedAt)
}
