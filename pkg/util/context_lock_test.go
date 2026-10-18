package util

import (
	"context"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestLockContextCancelledHandoffReleasesLock(t *testing.T) {
	var mu sync.Mutex
	mu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	queued := make(chan struct{})
	go func() {
		release, err := LockContext(ctx, func() { close(queued); mu.Lock() }, mu.Unlock)
		if release != nil {
			release()
		}
		done <- err
	}()
	<-queued
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancelled waiter did not return")
	}
	mu.Unlock()
	acquired := make(chan struct{})
	go func() { mu.Lock(); mu.Unlock(); close(acquired) }()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("cancelled handoff retained the lock")
	}
	release, err := LockContext(context.Background(), mu.Lock, mu.Unlock)
	require.NoError(t, err)
	release()
}
