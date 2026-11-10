package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NOR-160 experiment: an actually pinned transaction owns the
// version-qualified body of every node its immutable physical snapshot holds,
// so its point reads may read that body instead of resolving the logical MVCC
// head again. These tests prove the two owners answer the same scenario the
// same way, and that every other transaction keeps the logical owner.

// nor160NodeImage is the image a point read delivers: public graph data,
// embeddings and runtime metadata. Two owners' answers compare through it
// without depending on whether they share one immutable runtime record.
func nor160NodeImage(node *Node) map[string]any {
	if node == nil {
		return nil
	}
	return map[string]any{
		"id":         node.ID,
		"labels":     append([]string(nil), node.Labels...),
		"props":      CopyValue(node.Properties, nil),
		"created":    node.CreatedAt.UTC(),
		"updated":    node.UpdatedAt.UTC(),
		"chunks":     CopyValue(node.ChunkEmbeddings, nil),
		"named":      CopyValue(node.NamedEmbeddings, nil),
		"metadata":   node.RuntimeMetadata(),
		"embed_meta": CopyValue(node.EmbedMeta, nil),
	}
}

// nor160SetManualSuppression hides or reveals a stored node through the
// engine's existing visibility layer.
func nor160SetManualSuppression(t *testing.T, b *BadgerEngine, id NodeID, hidden bool) {
	t.Helper()
	require.NoError(t, b.SetManualSuppression(t.Context(), StoredEntityNodeKind, string(id), hidden))
}

// nor160BeginPinned opens a transaction and pins it to namespace exactly as a
// request's first prefixed read does.
func nor160BeginPinned(t *testing.T, b *BadgerEngine, namespace string) *BadgerTransaction {
	t.Helper()
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace(namespace))
	return tx
}

// nor160Owners is one committed node read answered by both owners.
type nor160Owners struct {
	physical, logical       *Node
	physicalErr, logicalErr error
}

// nor160ReadBothOwners answers one committed node read through the selected
// owner (readCommittedNodeAtSnapshotLocked's pinned branch) and through the
// logical MVCC owner it replaces, under the transaction's lock.
func nor160ReadBothOwners(t *testing.T, ctx context.Context, tx *BadgerTransaction, id NodeID, vectors bool, v readVisibility) nor160Owners {
	t.Helper()
	require.True(t, tx.readsPinnedSnapshotBodyLocked(), "the pinned physical owner must be selected for this scenario")
	tx.mu.Lock()
	defer tx.mu.Unlock()
	var out nor160Owners
	out.physical, out.physicalErr = tx.readPinnedSnapshotNodeLocked(ctx, id, vectors, v)
	out.logical, out.logicalErr = readEntityAtWithView(ctx, tx.engine, id, tx.readTS, mvccEntityRead{vectors: vectors, visibility: v}, tx.withSnapshotViewLocked, (*BadgerEngine).getNodeAtInTxn)
	return out
}

// requireOwnerAgreement asserts both owners returned the same value/error pair.
func requireOwnerAgreement(t *testing.T, got nor160Owners) {
	t.Helper()
	switch {
	case got.logicalErr == nil:
		require.NoError(t, got.physicalErr)
		require.Equal(t, nor160NodeImage(got.logical), nor160NodeImage(got.physical),
			"the pinned body owner must deliver the logical owner's image")
	case errors.Is(got.logicalErr, ErrNotFound):
		require.ErrorIs(t, got.physicalErr, ErrNotFound,
			"the pinned body owner must report the logical owner's miss")
	default:
		require.Failf(t, "unexpected logical owner answer", "err = %v", got.logicalErr)
	}
}

// nor160SeededNode commits a node with one chunk embedding in namespace test.
func nor160SeededNode(t *testing.T, b *BadgerEngine) NodeID {
	t.Helper()
	id, err := b.CreateNode(t.Context(), &Node{
		ID:              "test:pinned-point",
		Labels:          []string{"Held"},
		Properties:      map[string]any{"n": int64(1)},
		ChunkEmbeddings: [][]float32{{0.25, 0.5}},
	})
	require.NoError(t, err)
	return id
}

func TestNOR160PinnedSnapshotPointReadMatchesLogicalOwner(t *testing.T) {
	type action func(t *testing.T, b *BadgerEngine, id NodeID)
	peerUpdate := func(t *testing.T, b *BadgerEngine, id NodeID) {
		t.Helper()
		require.NoError(t, b.UpdateNode(t.Context(), NodeUpdateRequest{After: &Node{
			ID: id, Labels: []string{"Held"}, Properties: map[string]any{"n": int64(2)},
		}}))
	}
	for _, scenario := range []struct {
		name   string
		action action
	}{
		{name: "committed current body"},
		{name: "peer update after begin", action: peerUpdate},
		{name: "peer delete after begin", action: func(t *testing.T, b *BadgerEngine, id NodeID) {
			t.Helper()
			require.NoError(t, b.DeleteNode(t.Context(), id))
		}},
		{name: "peer delete and recreate after begin", action: func(t *testing.T, b *BadgerEngine, id NodeID) {
			t.Helper()
			require.NoError(t, b.DeleteNode(t.Context(), id))
			_, err := b.CreateNode(t.Context(), &Node{
				ID: id, Labels: []string{"Held"}, Properties: map[string]any{"n": int64(3)},
			})
			require.NoError(t, err)
		}},
		{name: "peer create after begin makes the snapshot miss", action: func(t *testing.T, b *BadgerEngine, id NodeID) {
			t.Helper()
			_, err := b.CreateNode(t.Context(), &Node{
				ID: "test:pinned-point-late", Labels: []string{"Held"}, Properties: map[string]any{"n": int64(4)},
			})
			require.NoError(t, err)
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			b := newTestEngine(t)
			id := nor160SeededNode(t, b)
			tx := nor160BeginPinned(t, b, "test")
			if scenario.action != nil {
				scenario.action(t, b, id)
			}
			for _, vectors := range []bool{false, true} {
				requireOwnerAgreement(t, nor160ReadBothOwners(t, t.Context(), tx, id, vectors, visibleRead))
				requireOwnerAgreement(t, nor160ReadBothOwners(t, t.Context(), tx, id, vectors, storedRead))
			}
			// A node the peer created after this transaction's cut stays
			// invisible to both owners.
			requireOwnerAgreement(t, nor160ReadBothOwners(t, t.Context(), tx, "test:pinned-point-late", true, visibleRead))
			requireOwnerAgreement(t, nor160ReadBothOwners(t, t.Context(), tx, "test:absent", true, visibleRead))
		})
	}
}

func TestNOR160PinnedSnapshotPointReadMatchesLogicalOwnerForSidecarsAndSuppression(t *testing.T) {
	b := newTestEngine(t)
	id := nor160SeededNode(t, b)
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(t.Context(),
		embeddingWriteback(t, b, id, [][]float32{{0.1, 0.2}}, map[string]any{"has_embedding": true, "chunk_count": 1}, time.Time{})))

	tx := nor160BeginPinned(t, b, "test")
	for _, vectors := range []bool{false, true} {
		requireOwnerAgreement(t, nor160ReadBothOwners(t, t.Context(), tx, id, vectors, visibleRead))
	}

	nor160SetManualSuppression(t, b, id, true)
	requireOwnerAgreement(t, nor160ReadBothOwners(t, t.Context(), tx, id, true, visibleRead))
	requireOwnerAgreement(t, nor160ReadBothOwners(t, t.Context(), tx, id, false, visibleRead))
	requireOwnerAgreement(t, nor160ReadBothOwners(t, t.Context(), tx, id, true, storedRead))
	nor160SetManualSuppression(t, b, id, false)
	requireOwnerAgreement(t, nor160ReadBothOwners(t, t.Context(), tx, id, true, visibleRead))
}

func TestNOR160PinnedSnapshotPointReadKeepsTransactionContracts(t *testing.T) {
	b := newTestMemoryEngine(t)
	ctx := t.Context()
	// No stored vectors: the bounded snapshot owner admits small images only.
	id, err := b.CreateNode(ctx, &Node{ID: "test:pinned-point", Labels: []string{"Held"}, Properties: map[string]any{"n": int64(1)}})
	require.NoError(t, err)
	// A worker sidecar records embeddings separately from the node body.
	sidecarID, err := b.CreateNode(ctx, &Node{ID: "test:pinned-sidecar", Labels: []string{"Held"}, Properties: map[string]any{"n": int64(9)}})
	require.NoError(t, err)
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(ctx,
		embeddingWriteback(t, b.BadgerEngine, sidecarID, [][]float32{{0.5, 0.75}}, map[string]any{"has_embedding": true, "chunk_count": 1}, time.Time{})))
	tx := nor160BeginPinned(t, b.BadgerEngine, "test")
	require.True(t, tx.readsPinnedSnapshotBodyLocked(), "an ordinary pinned transaction takes the physical body owner")

	// GetNode still hydrates embeddings the body does not hold and binds the
	// read source the same way a scan read does.
	full, err := tx.GetNode(ctx, sidecarID)
	require.NoError(t, err)
	require.Equal(t, [][]float32{{0.5, 0.75}}, full.ChunkEmbeddings)
	require.Same(t, b.BadgerEngine, full.runtime.embeddingOwner)
	require.NotZero(t, full.runtime.embeddingRevision.physical)

	// GetQueryNode still delivers settled metadata without separately stored
	// vectors.
	query, err := tx.GetQueryNode(ctx, sidecarID)
	require.NoError(t, err)
	require.Empty(t, query.ChunkEmbeddings)
	require.Equal(t, int64(9), query.Properties["n"])

	// Cache seeding and admission after a previous read are unchanged.
	seeded, err := tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(1), seeded.Properties["n"])
	_, err = tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	require.Contains(t, tx.snapshotPrefixNodeByID, id, "a repeated point read still admits the bounded snapshot owner")

	// The admitted image keeps the pinned snapshot: a peer's later update
	// stays invisible.
	require.NoError(t, b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{
		ID: id, Labels: []string{"Held"}, Properties: map[string]any{"n": int64(2)},
	}}))
	again, err := tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(1), again.Properties["n"], "peer update must not replace the pinned body")

	// Pending writes keep their precedence over the committed body.
	before, err := tx.GetStoredNode(ctx, id)
	require.NoError(t, err)
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: before, After: &Node{
		ID: id, Labels: []string{"Held"}, Properties: map[string]any{"n": int64(3)},
	}}))
	own, err := tx.GetQueryNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(3), own.Properties["n"])

	// Live visibility still completes the read: a hidden node is not
	// visible, while the stored read keeps finding it.
	nor160SetManualSuppression(t, b.BadgerEngine, id, true)
	_, err = tx.GetQueryNode(ctx, id)
	require.ErrorIs(t, err, ErrNotFound)
	stored, err := tx.GetStoredNode(ctx, id)
	require.NoError(t, err)
	require.Equal(t, int64(3), stored.Properties["n"], "stored reads bypass live suppression")
	_, err = tx.GetNodeProjected(ctx, id, []string{"n"})
	require.ErrorIs(t, err, ErrNotFound, "the projected owner keeps its own live visibility")
	nor160SetManualSuppression(t, b.BadgerEngine, id, false)

	// Cancellation still retires the read with the caller's error.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	tx.mu.Lock()
	_, err = tx.getCommittedNodeReadLocked(canceled, id, false, visibleRead)
	tx.mu.Unlock()
	require.ErrorIs(t, err, context.Canceled)

	// The transaction's own delete precedes the pinned body.
	require.NoError(t, tx.DeleteGraphNode(ctx, id, CascadeEdges))
	_, err = tx.GetQueryNode(ctx, id)
	require.ErrorIs(t, err, ErrNotFound, "own deletion precedes the pinned body")
}

func TestNOR160PinnedSnapshotPointReadKeepsLogicalOwnerElsewhere(t *testing.T) {
	t.Run("unpinned transaction", func(t *testing.T) {
		b := newTestEngine(t)
		id := nor160SeededNode(t, b)
		tx, err := b.BeginTransaction(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		require.False(t, tx.readsPinnedSnapshotBodyLocked(), "a transaction without a namespace has no pinned body owner")
		node, err := tx.GetQueryNode(t.Context(), id)
		require.NoError(t, err)
		require.Equal(t, int64(1), node.Properties["n"])
	})

	t.Run("late namespace pin", func(t *testing.T) {
		b := newTestEngine(t)
		tx, err := b.BeginTransaction(t.Context())
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		// The namespace and its node are committed after this transaction's
		// cut, so its begin-time boundary does not contain the namespace and
		// its physical contents must stay post-begin.
		_, err = b.CreateNode(t.Context(), &Node{ID: "late:node", Labels: []string{"Held"}, Properties: map[string]any{"n": int64(5)}})
		require.NoError(t, err)
		require.NoError(t, tx.SetNamespace("late"))
		require.False(t, tx.readsPinnedSnapshotBodyLocked(), "a late namespace pin keeps the logical owner")
		_, err = tx.GetQueryNode(t.Context(), "late:node")
		require.ErrorIs(t, err, ErrNotFound, "post-begin commits stay invisible")
		current, err := b.GetNode(t.Context(), "late:node")
		require.NoError(t, err)
		require.Equal(t, int64(5), current.Properties["n"], "the node is committed; only the snapshot hides it")
	})

	t.Run("read version behind the namespace boundary", func(t *testing.T) {
		b := newTestEngine(t)
		id := nor160SeededNode(t, b)
		tx := nor160BeginPinned(t, b, "test")
		require.True(t, tx.readsPinnedSnapshotBodyLocked())
		tx.readTS = MVCCVersion{CommitTimestamp: time.Unix(1, 0).UTC()}
		require.False(t, tx.readsPinnedSnapshotBodyLocked(), "a read version behind the boundary keeps the logical owner")
		_, err := tx.GetNode(t.Context(), id)
		require.ErrorIs(t, err, ErrNotFound, "a read version older than the node's head stays a snapshot miss")
		tx.readTS = MVCCVersion{}
		require.False(t, tx.readsPinnedSnapshotBodyLocked(), "a zero read version keeps the logical owner")
	})

	t.Run("write-behind", func(t *testing.T) {
		b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
		bufferedCreate(t, b, &Node{ID: "test:buffered", Labels: []string{"Held"}, Properties: map[string]any{"n": int64(1)}})
		tx := nor160BeginPinned(t, b, "test")
		require.False(t, tx.readsPinnedSnapshotBodyLocked(), "write-behind keeps the logical owner")
		node, err := tx.GetQueryNode(t.Context(), "test:buffered")
		require.NoError(t, err)
		require.Equal(t, int64(1), node.Properties["n"])
	})
}
