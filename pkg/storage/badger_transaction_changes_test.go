package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPendingChangesHoldsMergeKeyWithPerKeyLocks: PendingChanges reports a
// held MERGE key while the transaction holds one or several per-key locks
// (PrepareMergeKey), and stops once ReleaseMergeKeys releases them, so quota
// admission sees the same MERGE-key state the locks do.
func TestPendingChangesHoldsMergeKeyWithPerKeyLocks(t *testing.T) {
	engine, _ := newMergeKeyTestEngine(t)
	ctx := context.Background()

	tx := beginMergeKeyTx(t, engine)
	require.False(t, tx.PendingChanges(false).HoldsMergeKey)

	require.NoError(t, tx.PrepareMergeKey(ctx, "U", "k", int64(1)))
	require.True(t, tx.PendingChanges(false).HoldsMergeKey)
	require.NoError(t, tx.PrepareMergeKey(ctx, "U", "k", int64(2)))
	require.True(t, tx.PendingChanges(false).HoldsMergeKey)
	require.True(t, tx.PendingChanges(true).HoldsMergeKey)

	tx.ReleaseMergeKeys()
	require.False(t, tx.PendingChanges(false).HoldsMergeKey)
	require.NoError(t, tx.Rollback())
}
