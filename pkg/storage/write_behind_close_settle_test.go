package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCloseWaitsForInFlightListener: Close does not return while a listener
// of an acknowledged write-behind commit is still running, whether the
// background worker is already delivering it when Close starts or Close's
// own final drain delivers it. Close settles every acknowledged generation
// (applied, notifications delivered) before it stops the worker.
func TestCloseWaitsForInFlightListener(t *testing.T) {
	for name, interval := range map[string]time.Duration{
		"worker delivering when Close starts": time.Millisecond,
		"delivered by Close's final drain":    time.Hour,
	} {
		t.Run(name, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, interval)
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			var finished atomic.Bool
			b.OnNodeCreated(func(n *Node) {
				if n.ID != "test:held" {
					return
				}
				once.Do(func() { close(started) })
				<-release
				finished.Store(true)
			})
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetImplicit(true))
			_, err = tx.CreateNode(&Node{ID: "test:held", Properties: map[string]any{"v": int64(1)}})
			require.NoError(t, err)
			require.NoError(t, tx.Commit()) // acknowledged into the buffer

			if interval == time.Millisecond {
				select {
				case <-started:
				case <-time.After(10 * time.Second):
					t.Fatal("the background worker did not deliver the acknowledged commit")
				}
			}
			closed := make(chan error, 1)
			go func() { closed <- b.Close() }()
			if interval == time.Hour {
				select {
				case <-started:
				case <-time.After(10 * time.Second):
					t.Fatal("Close's final drain did not deliver the acknowledged commit")
				}
			}
			select {
			case err := <-closed:
				t.Fatalf("Close returned (%v) while a listener of an acknowledged commit was still running", err)
			case <-time.After(200 * time.Millisecond):
			}
			close(release)
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("Close did not return once the listener finished")
			}
			require.True(t, finished.Load(), "Close returned before the listener finished")
		})
	}
}

// TestFlushReleasesBarrierBeforeWorkerListeners: a flush that waits for a
// generation the background worker claimed holds no write barrier while the
// worker delivers that generation's notifications: its listener runs with
// the barrier free.
func TestFlushReleasesBarrierBeforeWorkerListeners(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	claimed, applied := make(chan struct{}), make(chan struct{})
	apply := b.writeBehind.apply
	var once sync.Once
	b.writeBehind.apply = func(buf *CommitBuffer) error {
		once.Do(func() {
			close(claimed)
			<-applied
		})
		return apply(buf)
	}
	free := make(chan bool, 1)
	b.OnNodeCreated(func(n *Node) {
		if n.ID != "test:probe" {
			return
		}
		ok := b.writeBarrier.TryLock()
		if ok {
			b.writeBarrier.Unlock()
		}
		free <- ok
	})
	bufferedCreate(t, b, &Node{ID: "test:probe"})
	b.writeBehind.signal()
	select {
	case <-claimed:
	case <-time.After(10 * time.Second):
		t.Fatal("the worker never claimed the generation")
	}
	flushed := make(chan error, 1)
	go func() { flushed <- b.FlushWriteBehind() }()
	// The flush waits for the generation the worker is applying.
	time.Sleep(50 * time.Millisecond)
	close(applied)
	select {
	case ok := <-free:
		require.True(t, ok, "a worker listener ran while a waiting flush held the write barrier")
	case <-time.After(10 * time.Second):
		t.Fatal("the listener never ran")
	}
	require.NoError(t, <-flushed)
}

// TestFlushSettlesGenerationAppliedOutsideTheDrain: a generation applied
// outside the drain loop (replayed directly, or claimed, applied and
// released) is settled when the drain retires it, so a flush or Close that
// waits for it returns.
func TestFlushSettlesGenerationAppliedOutsideTheDrain(t *testing.T) {
	for _, claimed := range []bool{false, true} {
		t.Run(fmt.Sprintf("claimed=%v", claimed), func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			bufferedCreate(t, b, &Node{ID: "test:direct"})
			b.writeBehind.rotate()
			b.writeBehind.mu.RLock()
			buf := b.writeBehind.draining[len(b.writeBehind.draining)-1]
			b.writeBehind.mu.RUnlock()
			if claimed {
				require.True(t, buf.claimDrain())
			}
			require.NoError(t, b.replayWriteBehind(buf))
			if claimed {
				buf.releaseDrain()
			}
			flushed := make(chan error, 1)
			go func() { flushed <- b.FlushWriteBehind() }()
			select {
			case err := <-flushed:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("the flush waits forever on a generation applied outside the drain")
			}
		})
	}
}
