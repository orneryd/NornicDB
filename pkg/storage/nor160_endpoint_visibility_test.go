package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// NOR160 #1688: RelationshipEndpointVisible documents that it reports whether
// GetNode would return a node. GetNode's unified completion (readTransactionEntity)
// applies the live visibility layer to every image it can return — a connected
// delete, a pending write and a cached snapshot image included — while the
// transaction's check answered those overlay images before consulting the
// filter. These tests hold the check to the point read it promises to agree
// with, with hidden/visible results across suppression, reveal, and rollback.

// endpointCheckMatchesGetNode states the contract as an assertion: an answered
// check must carry GetNode's answer for the same ID in the same transaction,
// and a node GetNode hides must never be answered as visible.
func endpointCheckMatchesGetNode(t *testing.T, tx *BadgerTransaction, id NodeID) (visible, answered bool, getErr error) {
	t.Helper()
	visible, answered = tx.RelationshipEndpointVisible(id)
	node, getErr := tx.GetNode(t.Context(), id)
	if !answered {
		return visible, answered, getErr
	}
	require.Equal(t, getErr == nil, visible, "%s: an answered check must carry GetNode's answer", id)
	if visible {
		require.NotNil(t, node)
		require.Equal(t, id, node.ID)
	}
	return visible, answered, getErr
}

// createEndpointFixture commits a start node, an endpoint node and the
// relationship between them.
func createEndpointFixture(t *testing.T, engine *MemoryEngine, ids ...NodeID) {
	t.Helper()
	for _, id := range ids {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Held"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:r", StartNode: ids[0], EndNode: ids[1], Type: "R"}))
}

// Pending writes and connected-delete images must obey the same point-read
// visibility contract. Unknown answers may defer; answered results must agree.
func TestNOR160TransactionEndpointVisibilityForOverlayImages(t *testing.T) {
	for _, test := range []struct {
		name      string
		connected bool
	}{{"pending write", false}, {"connected delete", true}} {
		t.Run(test.name, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			ctx := t.Context()
			createEndpointFixture(t, engine, "test:start", "test:end")
			tx, err := engine.BeginTransaction(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			id := NodeID("test:end")
			if test.connected {
				id = "test:start"
				require.NoError(t, tx.DeleteGraphNode(ctx, id, RejectConnected))
				require.Contains(t, tx.connectedDeletes, id)
			} else {
				committed, err := tx.GetNode(ctx, id)
				require.NoError(t, err)
				updated := CopyNode(committed)
				updated.Properties = map[string]any{"n": int64(2)}
				require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: committed, After: updated}))
				require.Contains(t, tx.pendingNodes, id)
			}
			// No read-time filter is active: the ordinary fast answer stands.
			visible, answered, getErr := endpointCheckMatchesGetNode(t, tx, id)
			require.True(t, answered)
			require.True(t, visible)
			require.NoError(t, getErr)

			// Suppressed on demand: the pending image is hidden from the point read.
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), true))
			require.True(t, engine.visibilityFiltered())
			_, getErr = tx.GetNode(ctx, id)
			require.ErrorIs(t, getErr, ErrNotFound, "the pending image is hidden")
			visible, answered, getErr = endpointCheckMatchesGetNode(t, tx, id)
			require.ErrorIs(t, getErr, ErrNotFound)

			// Shown again: the same pending image answers as before.
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), false))
			require.False(t, engine.visibilityFiltered())
			visible, answered, getErr = endpointCheckMatchesGetNode(t, tx, id)
			require.True(t, answered)
			require.True(t, visible)
			require.NoError(t, getErr)

			// An open reveal scope makes the pending image readable again.
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), true))
			end := engine.BeginQueryRevealScope(true)
			require.False(t, engine.visibilityFiltered())
			visible, answered, getErr = endpointCheckMatchesGetNode(t, tx, id)
			require.True(t, answered)
			require.True(t, visible)
			require.NoError(t, getErr)
			end()
			require.True(t, engine.visibilityFiltered())
			require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, string(id), false))

			require.NoError(t, tx.Rollback())
			_, getErr = engine.GetNode(ctx, id)
			require.NoError(t, getErr, "rollback preserves the committed image")
		})
	}
}

// Overlay, cached and committed sources preserve point-read truth while
// visibility policies change. A capability may decline instead of answering;
// ordinary unfiltered sources retain their fast answers.
func TestNOR160TransactionEndpointVisibilityAgreementAcrossSources(t *testing.T) {
	engine := newTestMemoryEngine(t)
	ctx := t.Context()
	createEndpointFixture(t, engine, "test:start", "test:end")
	for _, id := range []NodeID{"test:other", "test:hidden", "test:cached"} {
		_, err := engine.CreateNode(ctx, &Node{ID: id, Labels: []string{"Held"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:r2", StartNode: "test:other", EndNode: "test:hidden", Type: "R"}))

	tx, err := engine.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	// connected delete for test:start
	require.NoError(t, tx.DeleteGraphNode(ctx, "test:start", RejectConnected))
	// pending write for test:end
	committed, err := tx.GetNode(ctx, "test:end")
	require.NoError(t, err)
	updated := CopyNode(committed)
	updated.Properties = map[string]any{"n": int64(2)}
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: committed, After: updated}))
	// The snapshot's prefix-scan cache answers for the nodes it holds.
	cached, err := engine.GetNode(ctx, "test:cached")
	require.NoError(t, err)
	if tx.snapshotPrefixNodeByID == nil {
		tx.snapshotPrefixNodeByID = make(map[NodeID]*Node)
	}
	tx.snapshotPrefixNodeByID["test:cached"] = cached
	// test:other is only committed, so its answer comes from the MVCC head.
	require.NotContains(t, tx.pendingNodes, NodeID("test:other"))
	require.NotContains(t, tx.connectedDeletes, NodeID("test:other"))

	// Nothing is hidden: every ordinary fast answer stands.
	for _, id := range []NodeID{"test:start", "test:end", "test:cached", "test:other"} {
		visible, answered, getErr := endpointCheckMatchesGetNode(t, tx, id)
		require.True(t, answered, id)
		require.True(t, visible, id)
		require.NoError(t, getErr, id)
	}

	// The transaction's own deletion decides before any other image, as it
	// does for GetNode.
	require.NoError(t, tx.DeleteGraphNode(ctx, "test:other", CascadeEdges))
	visible, answered := tx.RelationshipEndpointVisible("test:other")
	require.True(t, answered)
	require.False(t, visible)
	_, getErr := tx.GetNode(ctx, "test:other")
	require.ErrorIs(t, getErr, ErrNotFound)

	// An unrelated node is suppressed, so the layer is active while every
	// node under test is still itself readable.
	require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, "test:hidden", true))
	require.True(t, engine.visibilityFiltered())
	for _, id := range []NodeID{"test:start", "test:end", "test:cached"} {
		_, _, getErr := endpointCheckMatchesGetNode(t, tx, id)
		require.NoError(t, getErr, "%s: the point read still returns the entity", id)
	}
	// An absent image must remain absent, whether answered or deferred.
	_, _, getErr = endpointCheckMatchesGetNode(t, tx, "test:other")
	require.ErrorIs(t, getErr, ErrNotFound)

	// Nothing is hidden any more: the answers return, with the deletion's
	// precedence intact.
	require.NoError(t, engine.SetManualSuppression(ctx, StoredEntityNodeKind, "test:hidden", false))
	require.False(t, engine.visibilityFiltered())
	for _, id := range []NodeID{"test:start", "test:end", "test:cached"} {
		visible, answered, getErr := endpointCheckMatchesGetNode(t, tx, id)
		require.True(t, answered, id)
		require.True(t, visible, id)
		require.NoError(t, getErr, id)
	}
	visible, answered = tx.RelationshipEndpointVisible("test:other")
	require.True(t, answered)
	require.False(t, visible)

	require.NoError(t, tx.Rollback())
	for _, id := range []NodeID{"test:start", "test:other", "test:hidden"} {
		_, getErr = engine.GetNode(ctx, id)
		require.NoError(t, getErr, "%s: rollback restores the committed image", id)
	}
}
