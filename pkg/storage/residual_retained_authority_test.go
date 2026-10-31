package storage

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestResidualRetainedAuthorityAdmission(t *testing.T) {
	b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	schema := b.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("retained-key", "Retained", "k"))
	before := &Node{ID: "test:retained", Labels: []string{"Retained"}, Properties: map[string]any{"k": "old"}}
	_, err = b.CreateNode(before)
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: before.ID, Labels: before.Labels, Properties: map[string]any{"k": "new"}}, Policy: ClientNodeEdits}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prepared, err := tx.PrepareEntityCommit(ctx, EntityCommitRequest{LogicalID: "retained-context"})
	require.NoError(t, err)
	t.Cleanup(prepared.Abort)
	keys := schema.appendConstraintValueKeys(nil, []*Node{before}, nil)
	require.NotEmpty(t, keys)
	releasePeer, err := schema.acquireUniqueConstraintCommitLocks(context.Background(), "peer", keys)
	require.NoError(t, err)
	t.Cleanup(releasePeer)
	waited := make(chan struct{}, 1)
	t.Cleanup(SetUniqueKeyLockWaitHookForTesting(func() {
		select {
		case waited <- struct{}{}:
		default:
		}
	}))
	done := make(chan error, 1)
	go func() {
		release, err := prepared.RetainAcceptedAuthority(ctx)
		if release != nil {
			release()
		}
		done <- err
	}()
	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		cancel()
		releasePeer()
		<-done
		t.Fatal("accepted authority did not wait for the old value key")
	}
	cancel()
	var retainErr error
	returnedBeforeRelease := false
	select {
	case retainErr = <-done:
		returnedBeforeRelease = true
	case <-time.After(2 * time.Second):
	}
	releasePeer()
	if !returnedBeforeRelease {
		retainErr = <-done
	}
	require.True(t, returnedBeforeRelease)
	require.ErrorIs(t, retainErr, context.Canceled)
	prepared.Abort()
	stored, err := b.GetStoredNode(before.ID)
	require.NoError(t, err)
	require.Equal(t, "old", stored.Properties["k"])
}
