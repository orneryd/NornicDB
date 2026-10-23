package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPrepareMergeNodeForWriteSerializesWritersOfOneNode (NOR-117): a MERGE
// that will write the node it matches holds the node's key, so a second one
// waits for it, then catches up to its commit and writes on top of it; a
// MERGE that only reads the node matches without waiting.
func TestPrepareMergeNodeForWriteSerializesWritersOfOneNode(t *testing.T) {
	engine, _ := newMergeKeyTestEngine(t)
	ctx := context.Background()
	nodeID := createKeyNode(t, engine, "w1", 1)
	waits := make(chan struct{}, 8)
	restore := SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waits <- struct{}{}:
		default:
		}
	})
	defer restore()
	key := map[string]interface{}{"k": int64(1)}

	first := beginMergeKeyTx(t, engine)
	require.NoError(t, first.PrepareMergeNodeForWrite(ctx, []string{"U"}, key))
	require.Len(t, first.mergeKeyReleases, 1, "the writer holds the existing node's key")
	node, err := first.GetNode(nodeID)
	require.NoError(t, err)
	node.Properties["w"] = int64(1)
	require.NoError(t, first.UpdateNode(node))

	reader := beginMergeKeyTx(t, engine)
	require.NoError(t, reader.PrepareMergeNode(ctx, []string{"U"}, key), "a reader doesn't wait")
	require.Empty(t, reader.mergeKeyReleases)
	require.Empty(t, waits)
	require.NoError(t, reader.Rollback())

	second := beginMergeKeyTx(t, engine)
	done := make(chan error, 1)
	go func() { done <- second.PrepareMergeNodeForWrite(ctx, []string{"U"}, key) }()
	<-waits
	require.NoError(t, first.Commit())
	require.NoError(t, <-done)
	node, err = second.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, int64(1), node.Properties["w"], "the second writer sees the first one's commit")
	node.Properties["w"] = int64(2)
	require.NoError(t, second.UpdateNode(node))
	require.NoError(t, second.Commit())
	stored, err := engine.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, int64(2), stored.Properties["w"])
}

// TestPrepareMergeNodeForWriteLocksUntrustedHolder: a holder the values
// cache knows while it isn't complete is still caught up to, and a writer
// still takes its key.
func TestPrepareMergeNodeForWriteLocksUntrustedHolder(t *testing.T) {
	engine, schema := newMergeKeyTestEngine(t)
	ctx := context.Background()
	nodeID := createKeyNode(t, engine, "w2", 2)
	constraint := schema.uniqueConstraints["U:k"]
	constraint.mu.Lock()
	constraint.valuesCacheComplete = false
	constraint.mu.Unlock()

	writer := beginMergeKeyTx(t, engine)
	require.NoError(t, writer.PrepareMergeNodeForWrite(ctx, []string{"U"}, map[string]interface{}{"k": int64(2)}))
	require.Len(t, writer.mergeKeyReleases, 1)
	node, err := writer.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, int64(2), node.Properties["k"])
	require.NoError(t, writer.Rollback())

	// An absent key with an incomplete cache admits nothing, as before.
	absent := beginMergeKeyTx(t, engine)
	require.NoError(t, absent.PrepareMergeNodeForWrite(ctx, []string{"U"}, map[string]interface{}{"k": int64(3)}))
	require.Empty(t, absent.mergeKeyReleases)
	require.NoError(t, absent.Rollback())
}

func TestSetUniqueKeyLockWaitHookForTestingRestores(t *testing.T) {
	require.Nil(t, uniqueKeyLockWaitHook.Load())
	restoreOuter := SetUniqueKeyLockWaitHookForTesting(func() {})
	outer := uniqueKeyLockWaitHook.Load()
	restoreInner := SetUniqueKeyLockWaitHookForTesting(func() {})
	require.NotSame(t, outer, uniqueKeyLockWaitHook.Load())
	restoreInner()
	require.Same(t, outer, uniqueKeyLockWaitHook.Load())
	restoreOuter()
	require.Nil(t, uniqueKeyLockWaitHook.Load())
}

// TestPrepareMergeNodeForWriteLandsBufferedCommit: with write-behind, an
// auto-commit write of the node is buffered when its key is released. The
// next MERGE writer lands it before catching up, so it reads and writes on
// top of it and commits, instead of losing to the later flush.
func TestPrepareMergeNodeForWriteLandsBufferedCommit(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{Name: "u_k", Type: ConstraintUnique, Label: "U", Properties: []string{"k"}}))
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(engine, "test"), schema))
	ctx := context.Background()
	nodeID := createKeyNode(t, engine, "wb", 1)
	require.NoError(t, engine.FlushWriteBehind())
	key := map[string]interface{}{"k": int64(1)}

	buffered := beginMergeKeyTx(t, engine)
	require.NoError(t, buffered.SetImplicit(true))
	require.NoError(t, buffered.PrepareMergeNodeForWrite(ctx, []string{"U"}, key))
	node, err := buffered.GetNode(nodeID)
	require.NoError(t, err)
	node.Properties["w"] = int64(1)
	require.NoError(t, buffered.UpdateNode(node))
	require.NoError(t, buffered.Commit())
	_, pending, _ := engine.writeBehind.GetNode(nodeID)
	require.True(t, pending, "the auto-commit write is buffered")

	writer := beginMergeKeyTx(t, engine)
	require.NoError(t, writer.PrepareMergeNodeForWrite(ctx, []string{"U"}, key))
	_, pending, _ = engine.writeBehind.GetNode(nodeID)
	require.False(t, pending, "the writer landed the buffered commit")
	node, err = writer.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, int64(1), node.Properties["w"])
	node.Properties["w"] = int64(2)
	require.NoError(t, writer.UpdateNode(node))
	require.NoError(t, writer.Commit())
	stored, err := engine.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, int64(2), stored.Properties["w"])

	// A writer that observed its snapshot before the commit landed can't
	// move it: its MERGE fails with the retryable conflict.
	observed := beginMergeKeyTx(t, engine)
	_, _ = observed.GetNodesByLabel("U")
	again := beginMergeKeyTx(t, engine)
	require.NoError(t, again.SetImplicit(true))
	require.NoError(t, again.PrepareMergeNodeForWrite(ctx, []string{"U"}, key))
	node, err = again.GetNode(nodeID)
	require.NoError(t, err)
	node.Properties["w"] = int64(3)
	require.NoError(t, again.UpdateNode(node))
	require.NoError(t, again.Commit())
	require.ErrorIs(t, observed.PrepareMergeNodeForWrite(ctx, []string{"U"}, key), ErrConflict)
	require.NoError(t, observed.Rollback())
}
