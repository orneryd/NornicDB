package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestPreparedEntityRejectsPriorSnapshotConflictBeforeAdmission(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "initial"}})
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:a", Properties: map[string]any{"value": "prepared"}}, ClientNodeEdits))
	require.NoError(t, b.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "concurrent"}}))
	prepared, err := tx.PrepareEntityCommit(context.Background(), "logical-1")
	require.ErrorIs(t, err, ErrConflict)
	require.Nil(t, prepared)
	got, err := b.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "concurrent", got.Properties["value"])
}
func TestPreparedEntityImmutableChangesCommitAndDiscard(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "discard", true: "commit"}[commit], func(t *testing.T) {
			b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
			require.NoError(t, err)
			_, err = b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "initial"}})
			require.NoError(t, err)
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:a", Properties: map[string]any{"value": "prepared"}}, ClientNodeEdits))
			prepared, err := tx.PrepareEntityCommit(context.Background(), "logical-1")
			require.NoError(t, err)
			require.Equal(t, "test", prepared.Namespace())
			require.Equal(t, "logical-1", prepared.LogicalID())
			changes := prepared.Changes()
			require.Len(t, changes, 1)
			require.Equal(t, "initial", changes[0].BeforeNode.Properties["value"])
			require.Equal(t, "prepared", changes[0].AfterNode.Properties["value"])
			changes[0].AfterNode.Properties["value"] = "caller mutation"
			require.Equal(t, "prepared", prepared.Changes()[0].AfterNode.Properties["value"])
			published := false
			if commit {
				require.NoError(t, prepared.Commit(func() { published = true }))
			} else {
				prepared.Abort()
			}
			require.Equal(t, commit, published)
			got, err := b.GetStoredNode("test:a")
			require.NoError(t, err)
			want := "initial"
			if commit {
				want = "prepared"
			}
			require.Equal(t, want, got.Properties["value"])
		})
	}
}

func TestPreparedEntityRetainsAuthorityAgainstOrdinaryDirectUpdate(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "initial"}})
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.UpdateGraphNode(&Node{ID: "test:a", Properties: map[string]any{"value": "prepared"}}, ClientNodeEdits))
	prepared, err := tx.PrepareEntityCommit(context.Background(), "logical-1")
	require.NoError(t, err)
	defer prepared.Abort()
	waiting := make(chan struct{}, 1)
	restore := SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	})
	defer restore()
	done := make(chan error, 1)
	go func() { done <- b.UpdateNode(&Node{ID: "test:a", Properties: map[string]any{"value": "concurrent"}}) }()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary writer did not join canonical prepared authority")
	}
	select {
	case err := <-done:
		t.Fatalf("writer bypassed prepared authority: %v", err)
	default:
	}
	// Disjoint direct writes still complete while the prepared node is reserved.
	_, err = b.CreateNode(&Node{ID: "test:disjoint", Properties: map[string]any{"value": "other"}})
	require.NoError(t, err)
	require.NoError(t, prepared.Commit(nil))
	require.NoError(t, <-done)
	got, err := b.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "concurrent", got.Properties["value"])
}

func TestPreparedEntityTimeoutCleanupDoesNotWaitOrDiscard(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:late", Properties: map[string]any{"value": "accepted"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), "logical-late")
	require.NoError(t, err)
	defer prepared.Abort()
	receipt, resolve := NewCommitReceipt(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, receipt.Wait(ctx), context.Canceled)
	cleanup := make(chan error, 1)
	go func() { cleanup <- tx.Rollback() }()
	select {
	case err := <-cleanup:
		require.ErrorIs(t, err, ErrTransactionClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("frontend cleanup waited for accepted work")
	}
	require.False(t, tx.IsActive())
	require.ErrorIs(t, tx.UpdateGraphNode(&Node{ID: "test:late"}, ClientNodeEdits), ErrTransactionClosed)
	require.NoError(t, prepared.Commit(nil))
	require.NoError(t, resolve.Resolve(CommitOutcome{State: CommitApplied}))
	require.NoError(t, receipt.Wait(context.Background()))
	got, err := b.GetStoredNode("test:late")
	require.NoError(t, err)
	require.Equal(t, "accepted", got.Properties["value"])
}

func TestNativeSubmitReceiptTimeoutAndPublicCleanup(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:native", Properties: map[string]any{"value": "accepted"}})
	require.NoError(t, err)
	prepared, err := tx.PrepareEntityCommit(context.Background(), "logical-native")
	require.NoError(t, err)
	defer prepared.Abort()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	restore := setCommitTailHook(func() { close(entered); <-release })
	defer restore()
	owner, ok := EntityCommitSubmitterFor(b)
	require.True(t, ok)
	receipt, err := owner.Submitter.SubmitEntityCommit(context.Background(), prepared, nil)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native accepted commit did not reach paused tail")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, receipt.Wait(ctx), context.Canceled)
	cleanup := make(chan error, 1)
	go func() { cleanup <- tx.Rollback() }()
	select {
	case err := <-cleanup:
		require.ErrorIs(t, err, ErrTransactionClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("public cleanup blocked behind native accepted continuation")
	}
	once.Do(func() { close(release) })
	require.NoError(t, receipt.Wait(context.Background()))
	require.Equal(t, CommitApplied, receipt.Outcome().State)
	got, err := b.GetStoredNode("test:native")
	require.NoError(t, err)
	require.Equal(t, "accepted", got.Properties["value"])
}
