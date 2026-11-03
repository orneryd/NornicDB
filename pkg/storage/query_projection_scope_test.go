package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestQueryProjectionScopeReusesOnlyLivePhysicalOwner(t *testing.T) {
	be, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer be.Close()
	be.SetDecayEnabled(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	owner, release := BeginQueryRevealScopeContext(ctx, be, true)
	require.True(t, be.revealAll.Load())
	nested, view, end := BeginQueryResultProjectionContext(owner, NewNamespacedEngine(be, "tenant"))
	require.Same(t, owner, nested)
	require.Equal(t, "tenant:", view.prefix)
	end()
	require.True(t, be.revealAll.Load(), "a borrowed normal projection must preserve owned REVEAL")
	be2, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer be2.Close()
	other, otherEnd := BeginQueryRevealScopeContext(owner, be2, false)
	require.NotSame(t, owner, other)
	otherEnd()
	release()
	release()
	require.False(t, be.revealAll.Load())
	fresh, _, freshEnd := BeginQueryResultProjectionContext(owner, be)
	require.NotSame(t, owner, fresh, "expired context must acquire a new genuine scope")
	freshEnd()
	// This Engine-only wrapper does not expose a physical Badger owner.
	unowned, noop := BeginQueryRevealScopeContext(ctx, &namespacedPrefixStatsEngine{Engine: newTestMemoryEngine(t)}, false)
	require.Same(t, ctx, unowned)
	noop()
	same := InheritQueryRevealScopes(ctx, RevealScopes{})
	require.Same(t, ctx, same)
	require.Same(t, ctx, InheritQueryRevealScopes(ctx, QueryRevealScopes(owner)), "expired leases are not inherited")
}
func TestQueryProjectionScopeBorrowingAndCancellation(t *testing.T) {
	be, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer be.Close()
	be.SetDecayEnabled(true)
	owner, end := BeginQueryRevealScopeContext(context.Background(), be, false)
	defer end()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	borrowed := InheritQueryRevealScopes(ctx, QueryRevealScopes(owner))
	require.NotSame(t, ctx, borrowed)
	require.Same(t, borrowed, InheritQueryRevealScopes(borrowed, QueryRevealScopes(owner)))
	nested, _, release := BeginQueryResultProjectionContext(borrowed, be)
	require.Same(t, borrowed, nested)
	release()
	cancel()
	require.ErrorIs(t, borrowed.Err(), context.Canceled)
	_, _, releaseDisabled := BeginQueryResultProjectionContext(ctx, newTestMemoryEngine(t))
	releaseDisabled()
}

// TestQueryRevealScopesHandle covers the typed lease handle captured from a
// genuine owning context: the zero handle and a lease-free context inherit
// nothing, a live lease is inherited without the owner's context, the borrowing
// context keeps its own cancellation/deadline/values, distinct engines retain
// distinct leases, and a released lease stops being inherited.
func TestQueryRevealScopesHandle(t *testing.T) {
	be, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer be.Close()
	be.SetDecayEnabled(true)

	t.Run("empty handle and lease-free context inherit nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		require.Same(t, ctx, InheritQueryRevealScopes(ctx, RevealScopes{}), "the zero handle adds no lease")
		require.Same(t, ctx, InheritQueryRevealScopes(ctx, QueryRevealScopes(ctx)), "a lease-free context captures no lease")
		require.Equal(t, RevealScopes{}, QueryRevealScopes(ctx))
		acquired, acquiredEnd := BeginQueryRevealScopeContext(ctx, be, false)
		require.NotSame(t, ctx, acquired, "an unleased context must still acquire a genuine scope")
		acquiredEnd()
	})

	t.Run("live lease is inherited without the owner context", func(t *testing.T) {
		owner, release := BeginQueryRevealScopeContext(context.Background(), be, true)
		defer release()
		handle := QueryRevealScopes(owner)
		require.Equal(t, handle, QueryRevealScopes(owner), "capturing reuses the existing lease link")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		borrowed := InheritQueryRevealScopes(ctx, handle)
		require.NotSame(t, ctx, borrowed)
		require.Same(t, borrowed, InheritQueryRevealScopes(borrowed, handle), "re-inheriting an owned handle is a no-op")
		nested, view, end := BeginQueryResultProjectionContext(borrowed, NewNamespacedEngine(be, "tenant"))
		require.Same(t, borrowed, nested, "the borrowed context joins the owner's genuine live lease")
		require.Equal(t, "tenant:", view.prefix)
		end()
		require.True(t, be.revealAll.Load(), "a borrowed projection must preserve owned REVEAL")
	})

	t.Run("borrowing keeps caller cancellation deadline and values", func(t *testing.T) {
		owner, release := BeginQueryRevealScopeContext(context.Background(), be, false)
		defer release()
		handle := QueryRevealScopes(owner)
		type callerKey struct{}
		deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), time.Minute)
		defer deadlineCancel()
		wantDeadline, ok := deadlineCtx.Deadline()
		require.True(t, ok)
		caller := context.WithValue(deadlineCtx, callerKey{}, "caller")
		borrowed := InheritQueryRevealScopes(caller, handle)
		require.NotSame(t, caller, borrowed)
		gotDeadline, ok := borrowed.Deadline()
		require.True(t, ok)
		require.Equal(t, wantDeadline, gotDeadline, "the borrower keeps the caller deadline")
		require.Equal(t, "caller", borrowed.Value(callerKey{}), "the borrower keeps the caller values")
		require.NoError(t, borrowed.Err())
		canceled, cancel := context.WithCancel(caller)
		cancel()
		canceledBorrowed := InheritQueryRevealScopes(canceled, handle)
		require.ErrorIs(t, canceledBorrowed.Err(), context.Canceled, "the borrower keeps the caller cancellation")
	})

	t.Run("distinct engines keep distinct inherited leases", func(t *testing.T) {
		be2, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
		require.NoError(t, err)
		defer be2.Close()
		be2.SetDecayEnabled(true)
		be3, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
		require.NoError(t, err)
		defer be3.Close()
		be3.SetDecayEnabled(true)
		ownerFirst, releaseFirst := BeginQueryRevealScopeContext(context.Background(), be, true)
		defer releaseFirst()
		ownerBoth, releaseBoth := BeginQueryRevealScopeContext(ownerFirst, be2, true)
		defer releaseBoth()
		require.NotSame(t, ownerFirst, ownerBoth)
		require.True(t, be2.revealAll.Load())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		borrowed := InheritQueryRevealScopes(ctx, QueryRevealScopes(ownerBoth))
		require.NotSame(t, ctx, borrowed)
		first, _, endFirst := BeginQueryResultProjectionContext(borrowed, be)
		require.Same(t, borrowed, first, "the first engine's lease is inherited")
		endFirst()
		second, _, endSecond := BeginQueryResultProjectionContext(borrowed, be2)
		require.Same(t, borrowed, second, "the second engine's lease is inherited into the same borrower")
		endSecond()
		single := InheritQueryRevealScopes(ctx, QueryRevealScopes(ownerFirst))
		require.NotSame(t, ctx, single)
		acquired, acquiredEnd := BeginQueryRevealScopeContext(single, be3, false)
		require.NotSame(t, single, acquired, "an engine without an inherited lease still acquires its own")
		acquiredEnd()
	})

	t.Run("released lease is not inherited", func(t *testing.T) {
		owner, release := BeginQueryRevealScopeContext(context.Background(), be, false)
		handle := QueryRevealScopes(owner)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		require.NotSame(t, ctx, InheritQueryRevealScopes(ctx, handle))
		release()
		require.Same(t, ctx, InheritQueryRevealScopes(ctx, handle), "a released lease is not inherited")
	})
}

func TestQueryProjectionNestedReaderDoesNotDeadlockWaitingRevealWriter(t *testing.T) {
	be, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	defer be.Close()
	be.SetDecayEnabled(true)
	owner, end := BeginQueryRevealScopeContext(context.Background(), be, false)
	writerStarted, writerEntered, writerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		close(writerStarted)
		_, release := BeginQueryRevealScopeContext(context.Background(), be, true)
		close(writerEntered)
		release()
		close(writerDone)
	}()
	<-writerStarted
	select {
	case <-writerEntered:
		end()
		t.Fatal("writer entered active normal scope")
	case <-time.After(20 * time.Millisecond):
	}
	nestedDone := make(chan struct{})
	go func() { _, _, release := BeginQueryResultProjectionContext(owner, be); release(); close(nestedDone) }()
	select {
	case <-nestedDone:
	case <-time.After(time.Second):
		end()
		<-writerDone
		t.Fatal("nested result projection reacquired an owned read lock behind a waiting writer")
	}
	end()
	<-writerDone
}
