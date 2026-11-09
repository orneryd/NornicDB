package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A mixed import (one observed node update plus 4,096 creates) on a store
// without write-behind or unique constraints needs no predecessor
// validation, so the commit-time validation phase allocates nothing,
// whatever the frame size (NOR-418).
func TestPlainMixedImportPreimageValidationAllocatesNothing(t *testing.T) {
	b := newTestMemoryEngine(t)
	ctx := t.Context()
	_, err := b.CreateNode(ctx, &Node{ID: "test:existing", Properties: map[string]any{"n": int64(0)}})
	require.NoError(t, err)
	tx, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	before, err := tx.GetNode(ctx, "test:existing")
	require.NoError(t, err)
	after := CopyNode(before)
	after.Properties["n"] = int64(1)
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: before, After: after}))
	for i := range 4096 {
		_, err := tx.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("test:new-%d", i))})
		require.NoError(t, err)
	}
	validate := func() { require.NoError(t, tx.validateMutationPreimagesLocked(ctx)) }
	require.Zero(t, testing.AllocsPerRun(5, validate), "inactive predecessor validation must not fold the frame")
	require.NoError(t, tx.Commit())
}

// With write-behind on, the same mixed import still validates its one
// update, but the fold keeps only identities an UPDATE targets.
func TestWriteBehindMixedImportFoldKeepsOnlyUpdateTargets(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	ctx := t.Context()
	_, err := b.CreateNode(ctx, &Node{ID: "test:existing", Properties: map[string]any{"n": int64(0)}})
	require.NoError(t, err)
	tx, err := b.BeginTransaction(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	before, err := tx.GetNode(ctx, "test:existing")
	require.NoError(t, err)
	after := CopyNode(before)
	after.Properties["n"] = int64(1)
	require.NoError(t, tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: before, After: after}))
	for i := range 4096 {
		_, err := tx.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("test:new-%d", i))})
		require.NoError(t, err)
	}
	folded, distinct, err := mutationPreimagesLocked(ctx, tx.operations, len(tx.pendingNodes), OpUpdateNode, nodeClaimPreimage)
	require.NoError(t, err)
	require.False(t, distinct)
	require.Len(t, folded, 1)
	require.Same(t, before, folded["test:existing"])
	require.NoError(t, tx.Commit())
}

// Restricting CREATE entries to UPDATE targets keeps the incarnation
// semantics of the full fold: an in-frame CREATE leaves no predecessor for
// its later updates, a DELETE starts a new incarnation, and the first update
// of each live incarnation supplies its predecessor.
func TestMutationPreimageFoldTargetsMatchFullFold(t *testing.T) {
	a1, c1, e1, e2 := &Node{ID: "a"}, &Node{ID: "c"}, &Node{ID: "e"}, &Node{ID: "e"}
	core := []Operation{
		{Type: OpUpdateNode, NodeID: "a", OldNode: a1},
		{Type: OpCreateNode, NodeID: "c"},
		{Type: OpUpdateNode, NodeID: "c", OldNode: c1},
		{Type: OpDeleteNode, NodeID: "d"},
		{Type: OpUpdateNode, NodeID: "e", OldNode: e1},
		{Type: OpDeleteNode, NodeID: "e"},
		{Type: OpCreateNode, NodeID: "e"},
		{Type: OpUpdateNode, NodeID: "e", OldNode: e2},
	}
	want := map[NodeID]*Node{"a": a1, "c": nil, "e": nil}
	// Four updates and four CREATE/DELETEs: the full fold.
	full, distinct, err := mutationPreimagesLocked(t.Context(), core, 3, OpUpdateNode, nodeClaimPreimage)
	require.NoError(t, err)
	require.False(t, distinct)
	require.Equal(t, want, full)
	// Padding with unrelated creates selects the target-restricted fold.
	padded := append([]Operation(nil), core...)
	for i := range 16 {
		padded = append(padded, Operation{Type: OpCreateNode, NodeID: NodeID(fmt.Sprintf("x%d", i))})
	}
	restricted, distinct, err := mutationPreimagesLocked(t.Context(), padded, 19, OpUpdateNode, nodeClaimPreimage)
	require.NoError(t, err)
	require.False(t, distinct)
	require.Equal(t, want, restricted)
}
