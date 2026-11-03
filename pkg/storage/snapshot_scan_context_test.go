package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestReadSnapshotStreamEdgesCanceledScanWait(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("writeBehind=%v", buffered), func(t *testing.T) {
			engine := newReadSnapshotTestEngine(t, buffered)
			t.Logf("badgerReadView state bytes=%d", unsafe.Sizeof(badgerReadView{}))
			for _, id := range []NodeID{"test:scan-a", "test:scan-b"} {
				_, err := engine.CreateNode(t.Context(), &Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "test:scan-edge", StartNode: "test:scan-a", EndNode: "test:scan-b", Type: "SCAN"}))
			readerHolds := func() int {
				engine.db.oracle.mu.Lock()
				defer engine.db.oracle.mu.Unlock()
				total := 0
				for _, count := range engine.db.oracle.readers {
					total += count
				}
				return total
			}
			before := readerHolds()
			snapshot, err := engine.OpenReadSnapshot(t.Context())
			require.NoError(t, err)
			t.Cleanup(snapshot.Close)
			firstEntered := make(chan struct{})
			releaseFirst := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
			firstDone := make(chan error, 1)
			firstJoined := make(chan struct{})
			secondStarted := make(chan struct{})
			secondDone := make(chan error, 1)
			secondJoined := make(chan struct{})
			ctx, cancel := context.WithCancel(t.Context())
			secondLaunched := false
			// Cleanup is installed before either worker launches. A result send
			// is not a join: both actual goroutines must leave before Close.
			t.Cleanup(func() {
				cancel()
				release()
				select {
				case <-firstJoined:
				case <-time.After(5 * time.Second):
					t.Error("first snapshot scan did not join")
				}
				if secondLaunched {
					select {
					case <-secondJoined:
					case <-time.After(5 * time.Second):
						t.Error("canceled snapshot scan did not join")
					}
				}
			})
			callbackCause := errors.New("first snapshot callback cause")
			go func() {
				defer close(firstJoined)
				firstDone <- snapshot.StreamEdges(t.Context(), func(*Edge) error {
					close(firstEntered)
					<-releaseFirst
					return callbackCause
				})
			}()
			select {
			case <-firstEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("first real edge callback did not enter")
			}
			var secondCallbacks atomic.Int64
			secondLaunched = true
			go func() {
				defer close(secondJoined)
				close(secondStarted)
				secondDone <- snapshot.StreamEdges(ctx, func(*Edge) error { secondCallbacks.Add(1); return nil })
			}()
			select {
			case <-secondStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("second snapshot worker did not start")
			}
			cancel()
			// The first callback still owns the public snapshot's scan lock.
			// Whether cancellation wins just before or during admission, the
			// second caller must return without waiting for that callback.
			select {
			case err := <-secondDone:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("canceled snapshot scan waited for another callback to release admission")
			}
			require.Zero(t, secondCallbacks.Load(), "canceled admission must not invoke the visitor")
			release()
			select {
			case err := <-firstDone:
				require.ErrorIs(t, err, callbackCause)
			case <-time.After(5 * time.Second):
				t.Fatal("first callback outcome did not return")
			}
			select {
			case <-firstJoined:
			case <-time.After(5 * time.Second):
				t.Fatal("first snapshot worker did not join")
			}
			select {
			case <-secondJoined:
			case <-time.After(5 * time.Second):
				t.Fatal("second snapshot worker did not join")
			}
			var seen []EdgeID
			require.NoError(t, snapshot.StreamEdges(t.Context(), func(edge *Edge) error { seen = append(seen, edge.ID); return nil }))
			require.Equal(t, []EdgeID{"test:scan-edge"}, seen, "healthy retry retains the same complete snapshot")
			nodes := 0
			require.NoError(t, snapshot.IterateNodes(func(*Node) bool { nodes++; return true }))
			require.Equal(t, 2, nodes, "context-free node iteration retains its own lifetime")
			snapshot.Close()
			require.Equal(t, before, readerHolds(), "canceled scans and Close must release the pinned read timestamp")
			count, err := engine.NodeCount(t.Context())
			require.NoError(t, err)
			require.Equal(t, int64(2), count)
		})
	}
}

func TestReadSnapshotEmptyStreamHonorsPreCanceledContext(t *testing.T) {
	engine := newReadSnapshotTestEngine(t, false)
	snapshot, err := engine.OpenReadSnapshot(t.Context())
	require.NoError(t, err)
	t.Cleanup(snapshot.Close)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	callbacks := 0
	err = snapshot.StreamEdges(ctx, func(*Edge) error { callbacks++; return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, callbacks)
	require.NoError(t, snapshot.StreamEdges(t.Context(), func(*Edge) error { callbacks++; return nil }))
	require.Zero(t, callbacks)
}

func TestReadSnapshotScanAdmissionUsesCallerContext(t *testing.T) {
	engine := newReadSnapshotTestEngine(t, false)
	snapshot, err := engine.OpenReadSnapshot(t.Context())
	require.NoError(t, err)
	t.Cleanup(snapshot.Close)
	view := snapshot.(*badgerReadSnapshot).view
	type scanContextKey struct{}
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), scanContextKey{}, "caller-owned"), time.Minute)
	waited := make(chan context.Context, 1)
	t.Cleanup(setAdmissionWaitObserver(func(owner any, admission context.Context) {
		if owner == &view.mu {
			select {
			case waited <- admission:
			default:
			}
		}
	}))
	view.mu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(view.mu.Unlock) }
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		unlock()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("admission caller did not join")
		}
	})
	var callbacks atomic.Int64
	go func() {
		defer close(joined)
		done <- snapshot.StreamEdges(ctx, func(*Edge) error { callbacks.Add(1); return nil })
	}()
	var admission context.Context
	select {
	case admission = <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("public scan did not reach the held native scan admission")
	}
	require.True(t, admission == ctx, "scan admission must retain the exact caller context")
	require.Equal(t, "caller-owned", admission.Value(scanContextKey{}))
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	actualDeadline, ok := admission.Deadline()
	require.True(t, ok)
	require.Equal(t, deadline, actualDeadline)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled native scan admission stayed blocked")
	}
	require.Zero(t, callbacks.Load())
	unlock()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("admission caller did not join after returning")
	}
	require.NoError(t, snapshot.StreamEdges(t.Context(), func(*Edge) error { callbacks.Add(1); return nil }))
	require.Zero(t, callbacks.Load())
}
