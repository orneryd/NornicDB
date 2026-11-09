package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func observedWALContextFixture(t *testing.T) (*BadgerEngine, *WALEngine, *Node) {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	_, err = b.CreateNode(t.Context(), &Node{ID: "test:a", Properties: map[string]any{"v": "before"}})
	require.NoError(t, err)
	before, err := b.GetNode(t.Context(), "test:a")
	require.NoError(t, err)
	walConfig := DefaultWALConfig()
	walConfig.SnapshotInterval = time.Millisecond
	wal, err := NewWAL(t.TempDir(), walConfig)
	require.NoError(t, err)
	owner := NewWALEngine(b, wal)
	t.Cleanup(func() { require.NoError(t, owner.Close()) })
	return b, owner, before
}

// holdObservedWALPreparation owns the controlled preparer and joins its update
// before fixture cleanup, including when an assertion fails while it is held.
func holdObservedWALPreparation(t *testing.T, b *BadgerEngine, owner *WALEngine, before *Node, ctx context.Context) (context.Context, <-chan error) {
	t.Helper()
	entered := make(chan context.Context, 1)
	released := make(chan struct{})
	finished := make(chan struct{})
	updated := make(chan error, 1)
	b.RegisterManagedEntityPreparer("test", func(ctx context.Context, _ []Operation, _ EntityCommitMode) (*PreparedEntityCommit, func(CommitOutcome), error) {
		entered <- ctx
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-released:
			return nil, nil, errors.New("released by test cleanup")
		}
	}, nil)
	go func() {
		defer close(finished)
		updated <- owner.UpdateNode(ctx, NodeUpdateRequest{Before: before, After: &Node{ID: before.ID, Properties: map[string]any{"v": "after"}}})
	}()
	t.Cleanup(func() {
		close(released)
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("observed preparer did not stop during cleanup")
		}
	})
	select {
	case received := <-entered:
		return received, updated
	case err := <-updated:
		t.Fatalf("observed change returned before preparation: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("observed change never reached preparation")
	}
	return nil, updated
}

// Closing the applying engine must terminate preparation and report the
// closed store rather than the cancellation of its internal lifetime.
func TestObservedWALChangeTakesApplyingEngineLifetime(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		compaction bool
	}{
		{"close_during_preparation", false},
		{"close_during_snapshot_admission", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			b, owner, before := observedWALContextFixture(t)
			var closeFinished chan struct{}
			t.Cleanup(func() {
				if closeFinished != nil {
					select {
					case <-closeFinished:
					case <-time.After(5 * time.Second):
						t.Error("Close goroutine did not stop during cleanup")
					}
				}
			})
			received, updated := holdObservedWALPreparation(t, b, owner, before, context.Background())
			require.NotNil(t, received.Done(), "preparation runs under a lifetime Close can end, not an uncancellable one")
			if scenario.compaction {
				waiting := make(chan struct{}, 1)
				t.Cleanup(setAdmissionWaitObserver(func(lock any, _ context.Context) {
					if lock == &owner.mutationMu {
						select {
						case waiting <- struct{}{}:
						default:
						}
					}
				}))
				require.NoError(t, owner.EnableAutoCompaction(t.TempDir()))
				select {
				case <-waiting:
				case <-time.After(5 * time.Second):
					t.Fatal("snapshot did not wait behind the preparing mutation")
				}
			}
			closed := make(chan error, 1)
			closeFinished = make(chan struct{})
			go func() { defer close(closeFinished); closed <- owner.Close() }()
			select {
			case err := <-updated:
				require.ErrorIs(t, err, ErrStorageClosed)
				require.False(t, errors.Is(err, context.Canceled), "the closed store, not the ended lifetime, is the cause")
			case <-time.After(5 * time.Second):
				t.Fatal("closing the engine did not end the observed change")
			}
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not return")
			}
		})
	}
	t.Run("after_close", func(t *testing.T) {
		_, owner, before := observedWALContextFixture(t)
		require.NoError(t, owner.Close())
		err := owner.UpdateNode(t.Context(), NodeUpdateRequest{Before: before, After: &Node{ID: before.ID, Properties: map[string]any{"v": "after"}}})
		require.ErrorIs(t, err, ErrStorageClosed)
		require.False(t, errors.Is(err, context.Canceled), "the closed store, not the ended lifetime, is the cause")
	})
}

// Caller cancellation is additional coverage, not a substitute for the Close
// assertion above. Before acceptance it must leave both data and WAL untouched.
func TestObservedWALPreparationUsesCallerContext(t *testing.T) {
	b, owner, before := observedWALContextFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	received, updated := holdObservedWALPreparation(t, b, owner, before, ctx)
	require.NotNil(t, received.Done(), "preparation must observe cancellation")
	require.NoError(t, received.Err(), "preparation is live before the caller cancels")
	cancel()
	select {
	case err := <-updated:
		require.ErrorIs(t, err, context.Canceled)
		var accepted *AcceptedCommitError
		require.False(t, errors.As(err, &accepted), "canceled preparation has not been accepted")
	case <-time.After(5 * time.Second):
		t.Fatal("caller cancellation did not end observed preparation")
	}
	stored, err := b.GetStoredNode(t.Context(), before.ID)
	require.NoError(t, err)
	require.Equal(t, "before", stored.Properties["v"])
	entries, err := ReadWALEntriesFromDir(ctx, owner.GetWAL().Config().Dir)
	require.NoError(t, err)
	require.Empty(t, entries, "canceled preparation must not journal an accepted change")
	require.NoError(t, owner.Close())
}
