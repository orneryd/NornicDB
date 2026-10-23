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
	unowned, noop := BeginQueryRevealScopeContext(ctx, newTestMemoryEngine(t), false)
	require.Same(t, ctx, unowned)
	noop()
	same := InheritQueryRevealScopes(ctx, nil)
	require.Same(t, ctx, same)
	require.Same(t, ctx, InheritQueryRevealScopes(ctx, owner), "expired leases are not inherited")
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
	borrowed := InheritQueryRevealScopes(ctx, owner)
	require.NotSame(t, ctx, borrowed)
	require.Same(t, borrowed, InheritQueryRevealScopes(borrowed, owner))
	nested, _, release := BeginQueryResultProjectionContext(borrowed, be)
	require.Same(t, borrowed, nested)
	release()
	cancel()
	require.ErrorIs(t, borrowed.Err(), context.Canceled)
	_, _, releaseDisabled := BeginQueryResultProjectionContext(ctx, newTestMemoryEngine(t))
	releaseDisabled()
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
