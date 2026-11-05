package txsession

import (
	"context"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newEnrollmentManager registers retirement before the first assertion so a
// failed control cannot leave an idle timer or an accepted session behind.
func newEnrollmentManager(t *testing.T, parent context.Context) *Manager {
	t.Helper()
	manager := NewManager(time.Hour, newExecutorFactory(t), parent)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := manager.CloseContext(ctx); err != nil {
			t.Errorf("join test transaction manager: %v", err)
		}
	})
	return manager
}

// A caller's cancellation belongs to its own BEGIN statement, which receives
// the caller's context. The manager's lifetime enrollment does not consume the
// caller's context, so a canceled caller observes the BEGIN failure and the
// manager still reaches actual completion without surviving enrollment.
func TestManagerCanceledCallerBeginOutcome(t *testing.T) {
	for _, mode := range []struct {
		name string
		want error
		call func() (context.Context, context.CancelFunc)
	}{
		{
			name: "canceled",
			want: context.Canceled,
			call: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
		},
		{
			name: "deadline exceeded",
			want: context.DeadlineExceeded,
			call: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
				<-ctx.Done()
				return ctx, cancel
			},
		},
	} {
		t.Run(mode.name, func(t *testing.T) {
			manager := newEnrollmentManager(t, t.Context())

			caller, cancelCaller := mode.call()
			defer cancelCaller()

			session, openErr := manager.OpenForOwner(caller, "neo4j", "user:alice")
			require.ErrorIs(t, openErr, mode.want, "a canceled caller's BEGIN must report its own cancellation")
			require.Nil(t, session)
			code, _ := nornicerrors.Neo4jStatus(openErr)
			require.Equal(t, nornicerrors.TransactionStartFailed, code,
				"the failed opening must be classified as a BEGIN failure")

			lease, requestErr := manager.OpenRequestForOwner(caller, "neo4j", "user:alice")
			if lease != nil {
				lease.Release()
			}
			require.ErrorIs(t, requestErr, mode.want)
			require.Nil(t, lease)

			// Neither failed opening may retain enrollment or cleanup work.
			joinCtx, cancelJoin := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelJoin()
			require.NoError(t, manager.CloseContext(joinCtx))
			require.NoError(t, manager.WaitClosed(joinCtx))
		})
	}
}

// The manager's lifetime is created in NewManager from the host owner's
// context. A host whose own context is already canceled does not discharge
// admission or the accepted transaction's actual completion.
func TestManagerHostParentCancelKeepsAdmission(t *testing.T) {
	host, cancelHost := context.WithCancel(context.Background())
	t.Cleanup(cancelHost)
	manager := newEnrollmentManager(t, host)
	cancelHost()

	session, err := manager.OpenForOwner(context.Background(), "neo4j", "user:alice")
	require.NoError(t, err, "host cancellation must not refuse a new transaction")
	require.NotNil(t, session)

	joinCtx, cancelJoin := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelJoin()
	require.NoError(t, manager.CloseContext(joinCtx))
	require.NoError(t, manager.WaitClosed(joinCtx))
	require.False(t, session.Executor.HasActiveTransaction(), "host cancellation must not skip the accepted rollback")
}

// Roots are admitted by the manager's own lifetime. A host request ticket still
// active in the manager's parent context cannot reopen that lifetime after its
// admission is closed.
func TestManagerRefusesRootsAfterClose(t *testing.T) {
	host := lifecycle.NewWorkLifetime(context.Background())
	hostTicket, releaseHost, admitted := host.Acquire(context.Background())
	require.True(t, admitted)
	defer releaseHost()

	manager := newEnrollmentManager(t, hostTicket)
	session, err := manager.OpenForOwner(context.Background(), "neo4j", "user:alice")
	require.NoError(t, err)
	require.NotNil(t, session)
	require.NoError(t, manager.Close())

	// Control: the parent context really does carry an active request ticket
	// that still enrolls work in its own lifetime.
	require.True(t, host.CloseAdmission())
	_, releaseChild, control := host.Acquire(hostTicket)
	require.True(t, control, "control: an active parent ticket enrolls its own lifetime")
	releaseChild()

	store := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = store.Close() })
	executor := cypher.NewStorageExecutor(store)

	for _, open := range []struct {
		name string
		run  func() error
	}{
		{
			name: "open",
			run: func() error {
				_, openErr := manager.Open(context.Background(), "neo4j")
				return openErr
			},
		},
		{
			name: "open for owner",
			run: func() error {
				_, openErr := manager.OpenForOwner(context.Background(), "neo4j", "user:alice")
				return openErr
			},
		},
		{
			name: "open request for owner",
			run: func() error {
				lease, openErr := manager.OpenRequestForOwner(context.Background(), "neo4j", "user:alice")
				if lease != nil {
					lease.Release()
				}
				return openErr
			},
		},
		{
			name: "open with executor for owner",
			run: func() error {
				_, openErr := manager.OpenWithExecutorForOwner(context.Background(), "neo4j", executor, "user:alice")
				return openErr
			},
		},
	} {
		t.Run(open.name, func(t *testing.T) {
			openErr := open.run()
			require.Error(t, openErr)
			require.True(t, strings.Contains(openErr.Error(), "transaction manager is closed"),
				"a closed manager must refuse the root, got %v", openErr)
		})
	}
}

// Releasing a lease or one of its retained shares is idempotent: the request
// slot, its idle renewal and its lifetime credit each end exactly once.
func TestManagerLeaseReleaseIsIdempotent(t *testing.T) {
	manager := newEnrollmentManager(t, t.Context())
	base := time.Unix(1700000000, 0)
	manager.nowFunc = func() time.Time { return base }
	session, err := manager.OpenForOwner(context.Background(), "neo4j", "user:alice")
	require.NoError(t, err)

	lease, accepted := manager.AcquireForOwner(session.ID, "user:alice")
	require.True(t, accepted)
	t.Cleanup(lease.Release)
	share := lease.Retain()
	require.NotNil(t, share)
	t.Cleanup(share.Release)

	// The first completed share does not end the request.
	lease.Release()
	completedAt := base.Add(time.Minute)
	manager.nowFunc = func() time.Time { return completedAt }
	share.Release()
	completed := session.ExpiresAt()
	require.Equal(t, completedAt.Add(manager.ttl), completed)

	// Redundant releases change neither the deadline nor the ownership. The
	// session stays inside its renewed idle interval, before its expiry.
	manager.nowFunc = func() time.Time { return completed.Add(-time.Second) }
	lease.Release()
	share.Release()
	require.Equal(t, completed, session.ExpiresAt())
	require.Nil(t, lease.Retain(), "a completed request cannot retain another share")

	// The request slot was released exactly once, so it is usable again.
	next, accepted := manager.AcquireForOwner(session.ID, "user:alice")
	require.True(t, accepted)
	require.NotNil(t, next)
	t.Cleanup(next.Release)
	next.Release()

	joinCtx, cancelJoin := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelJoin()
	require.NoError(t, manager.CloseContext(joinCtx))
	require.NoError(t, manager.WaitClosed(joinCtx))
	require.False(t, session.Executor.HasActiveTransaction())
}

// A granted request lease is accepted work: close reports its own wait while the
// request still runs, and actual completion is joined only after Release.
func TestManagerCloseJoinsRequestLeaseCompletion(t *testing.T) {
	manager := newEnrollmentManager(t, t.Context())
	lease, err := manager.OpenRequestForOwner(context.Background(), "neo4j", "user:alice")
	if lease != nil {
		t.Cleanup(lease.Release)
	}
	require.NoError(t, err)
	require.NotNil(t, lease)

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelWait()
	require.ErrorIs(t, manager.WaitClosed(waitCtx), context.DeadlineExceeded,
		"the manager reported completion while its request lease was still held")

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelClose()
	require.ErrorIs(t, manager.CloseContext(closeCtx), context.DeadlineExceeded,
		"a bounded close must not join an active request lease")

	lease.Release()
	joinCtx, cancelJoin := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelJoin()
	require.NoError(t, manager.WaitClosed(joinCtx))
	require.NoError(t, manager.Close())
	require.False(t, lease.Session.Executor.HasActiveTransaction(), "the retired request's transaction was never rolled back")
}
