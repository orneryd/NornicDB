package lifecycle

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A stop before the context ends prevents the callback.
func TestAfterFuncJoinedStopPrevents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var ran atomic.Bool
	stop := AfterFuncJoined(ctx, func() { ran.Store(true) })
	stop()
	cancel()
	// A prevented callback can never start, so no wait is needed to observe it.
	require.False(t, ran.Load(), "a stopped callback must not run")
	stop() // repeated stop is safe
}

// A stop after the callback started waits for it to return.
func TestAfterFuncJoinedStopJoinsRunningCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entered, release := make(chan struct{}), make(chan struct{})
	var returned atomic.Bool
	stop := AfterFuncJoined(ctx, func() {
		close(entered)
		<-release
		returned.Store(true)
	})
	cancel()
	<-entered
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("stop returned while the callback was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-stopped
	require.True(t, returned.Load())
	stop()
}

func TestAfterFuncJoinedRejectsNilContext(t *testing.T) {
	require.Panics(t, func() { AfterFuncJoined(nil, func() {}) })
}
