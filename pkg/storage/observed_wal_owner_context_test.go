package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Author: NOR-288 lane (Claude), 2026-10-31. An observed WAL change (an
// UpdateNode request carrying its Before image) has no caller context: its
// preparation, accepted commit and outcome wait run under the applying
// engine's lifetime, so closing the engine ends a change still preparing, and
// the caller is told the store closed rather than that a lifetime ended.
func TestObservedWALChangeTakesApplyingEngineLifetime(t *testing.T) {
	setup := func(t *testing.T) (*BadgerEngine, *WALEngine, *Node) {
		t.Cleanup(config.WithWALEnabled())
		b, err := NewBadgerEngine(t.TempDir())
		require.NoError(t, err)
		_, err = b.CreateNode(&Node{ID: "test:a", Properties: map[string]any{"v": "before"}})
		require.NoError(t, err)
		before, err := b.GetNode("test:a")
		require.NoError(t, err)
		wal, err := NewWAL(t.TempDir(), nil)
		require.NoError(t, err)
		owner := NewWALEngine(b, wal)
		t.Cleanup(func() { _ = owner.Close() })
		return b, owner, before
	}
	observed := func(owner *WALEngine, before *Node) error {
		return owner.UpdateNode(NodeUpdateRequest{Before: before, After: &Node{ID: before.ID, Properties: map[string]any{"v": "after"}}})
	}

	t.Run("close_during_preparation", func(t *testing.T) {
		b, owner, before := setup(t)
		entered := make(chan context.Context, 1)
		released := make(chan struct{})
		t.Cleanup(func() { close(released) }) // frees a preparer a failing run leaves waiting
		b.RegisterManagedEntityPreparer("test", func(ctx context.Context, _ []Operation, _ EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error) {
			entered <- ctx
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-released:
				return nil, nil, errors.New("released by test cleanup")
			}
		}, nil)
		updated := make(chan error, 1)
		go func() { updated <- observed(owner, before) }()
		var received context.Context
		select {
		case received = <-entered:
		case err := <-updated:
			t.Fatalf("observed change returned before preparation: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("observed change never reached preparation")
		}
		require.NotNil(t, received.Done(), "preparation runs under a lifetime Close can end, not an uncancellable one")
		closed := make(chan error, 1)
		go func() { closed <- owner.Close() }()
		select {
		case err := <-updated:
			require.ErrorIs(t, err, ErrStorageClosed)
			require.False(t, errors.Is(err, context.Canceled), "the closed store, not the ended lifetime, is the cause")
		case <-time.After(10 * time.Second):
			t.Fatal("closing the engine did not end the observed change")
		}
		select {
		case err := <-closed:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("Close did not return")
		}
	})

	t.Run("after_close", func(t *testing.T) {
		_, owner, before := setup(t)
		require.NoError(t, owner.Close())
		err := observed(owner, before)
		require.ErrorIs(t, err, ErrStorageClosed)
		require.False(t, errors.Is(err, context.Canceled), "the closed store, not the ended lifetime, is the cause")
	})
}
