package storage

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func listenerOrderNode(v string) *Node {
	return &Node{ID: "test:n", Labels: []string{"U"}, Properties: map[string]any{"v": v}}
}

func listenerOrderEdge(v string) *Edge {
	return &Edge{ID: "test:r", StartNode: "test:n", EndNode: "test:n", Type: "LINK", Properties: map[string]any{"v": v}}
}

// finishesWithin runs f and reports false, with every goroutine's stack, when
// it has not returned within d; a deadlocked f is left behind.
func finishesWithin(t *testing.T, d time.Duration, f func()) bool {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		buf := make([]byte, 1<<20)
		t.Errorf("did not finish within %v:\n%s", d, buf[:runtime.Stack(buf, true)])
		return false
	}
}

func storedValue(engine *BadgerEngine, kind string) string {
	engine.invalidateCachesAfterRestore()
	var props map[string]any
	var err error
	if kind == "node" {
		var n *Node
		if n, err = engine.GetNode("test:n"); err == nil {
			props = n.Properties
		}
	} else {
		var e *Edge
		if e, err = engine.GetEdge("test:r"); err == nil {
			props = e.Properties
		}
	}
	switch {
	case errors.Is(err, ErrNotFound):
		return "absent"
	case err != nil:
		return err.Error()
	}
	return fmt.Sprint(props["v"])
}

// A listener that writes back to the entity it is told about runs once the
// write that delivers it released every lock it took, also when that write
// first drained acknowledged write-behind commits of the entity: no listener
// runs on a goroutine holding a write-order stripe or the commit gate
// (lockHoldHooks), none deadlocks the engine, and the listener's write is the
// last one.
func TestListenerWriteBackAfterOrderedWrites(t *testing.T) {
	for _, scenario := range []string{
		"node/delete-after-buffered-update",
		"edge/delete-after-buffered-update",
		"node/direct-update-after-buffered-update",
		"node/explicit-update-after-buffered-update",
		"node/direct-update-no-buffer",
		"edge/direct-update-no-buffer",
		"node/explicit-update-no-buffer",
		"node/delete-after-buffered-update/sync",
		"node/direct-update-no-buffer/sync",
		"node/explicit-update-no-buffer/sync",
	} {
		t.Run(scenario, func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{synchronous: strings.HasSuffix(scenario, "/sync"), manualDrain: true})
			_, err := engine.CreateNode(listenerOrderNode("initial"))
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(listenerOrderEdge("initial")))
			require.NoError(t, engine.FlushWriteBehind())

			var called, writing, ranAfterWrite atomic.Bool
			var listenerErr error
			listenerDone := make(chan struct{})
			engine.OnNodeUpdated(func(n *Node) {
				if n.ID == "test:n" && called.CompareAndSwap(false, true) {
					defer close(listenerDone)
					ranAfterWrite.Store(writing.Load())
					listenerErr = engine.UpdateNode(listenerOrderNode("listener"))
				}
			})
			engine.OnEdgeUpdated(func(e *Edge) {
				if e.ID == "test:r" && called.CompareAndSwap(false, true) {
					defer close(listenerDone)
					ranAfterWrite.Store(writing.Load())
					listenerErr = engine.UpdateEdge(listenerOrderEdge("listener"))
				}
			})
			kind, _, _ := strings.Cut(scenario, "/")
			buffered := func() error {
				return runPublicationTransaction(engine, true, func(tx *BadgerTransaction) error {
					if kind == "node" {
						return tx.UpdateGraphNode(listenerOrderNode("buffered"), ClientNodeEdits)
					}
					return tx.UpdateEdge(listenerOrderEdge("buffered"))
				}, nil)
			}
			mark := lockHolds.mark()
			var writeErr error
			finished := finishesWithin(t, time.Minute, func() {
				switch strings.TrimSuffix(scenario, "/sync") {
				case "node/delete-after-buffered-update":
					if writeErr = buffered(); writeErr == nil {
						writing.Store(true)
						writeErr = engine.DeleteNode("test:n")
					}
				case "edge/delete-after-buffered-update":
					if writeErr = buffered(); writeErr == nil {
						writing.Store(true)
						writeErr = engine.DeleteEdge("test:r")
					}
				case "node/direct-update-after-buffered-update":
					if writeErr = buffered(); writeErr == nil {
						writing.Store(true)
						writeErr = engine.UpdateNode(listenerOrderNode("direct"))
					}
				case "node/explicit-update-after-buffered-update":
					if writeErr = buffered(); writeErr == nil {
						writing.Store(true)
						writeErr = runPublicationTransaction(engine, false, func(tx *BadgerTransaction) error {
							return tx.UpdateGraphNode(listenerOrderNode("explicit"), ClientNodeEdits)
						}, nil)
					}
				case "node/direct-update-no-buffer":
					writing.Store(true)
					writeErr = engine.UpdateNode(listenerOrderNode("direct"))
				case "edge/direct-update-no-buffer":
					writing.Store(true)
					writeErr = engine.UpdateEdge(listenerOrderEdge("direct"))
				case "node/explicit-update-no-buffer":
					writing.Store(true)
					writeErr = runPublicationTransaction(engine, false, func(tx *BadgerTransaction) error {
						return tx.UpdateGraphNode(listenerOrderNode("explicit"), ClientNodeEdits)
					}, nil)
				}
				if writeErr == nil {
					writeErr = engine.FlushWriteBehind()
				}
				if writeErr == nil {
					<-listenerDone
				}
			})
			require.Empty(t, lockHolds.since(mark), "a listener ran under a write-order stripe or the commit gate")
			if !finished {
				return // deadlocked: the engine stays behind
			}
			t.Cleanup(func() { _ = engine.Close() })
			require.NoError(t, writeErr)
			require.True(t, called.Load(), "the listener ran")
			if !ranAfterWrite.Load() {
				// Synchronous mode: the listener ran on the first commit, before
				// the ordered write, whose result is the final state.
				require.NoError(t, listenerErr)
				require.True(t, strings.Contains(scenario, "delete"), "only a delete follows the listener")
				require.Equal(t, "absent", storedValue(engine, kind))
				return
			}
			if strings.Contains(scenario, "delete") && listenerErr != nil {
				// The listener ran after the delete; its write found nothing.
				require.ErrorIs(t, listenerErr, ErrNotFound)
				require.Equal(t, "absent", storedValue(engine, kind))
				return
			}
			require.NoError(t, listenerErr)
			require.Equal(t, "listener", storedValue(engine, kind), "the listener's write is the last one")
		})
	}
}

// A listener of a replayed commit runs once the replay retired its
// generation, so a write waiting for that generation is not held up by a
// listener that writes back to the same entity, and the listener's write
// lands after it.
func TestReplayListenerDoesNotHoldUpOrderedWrites(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	_, err := engine.CreateNode(listenerOrderNode("initial"))
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind())
	entered := make(chan struct{})
	written := make(chan struct{})
	var called, waitedForListener atomic.Bool
	var listenerErr error
	listenerDone := make(chan struct{})
	engine.OnNodeUpdated(func(n *Node) {
		if n.ID != "test:n" || !called.CompareAndSwap(false, true) {
			return
		}
		defer close(listenerDone)
		close(entered)
		select {
		case <-written:
		case <-time.After(15 * time.Second):
			waitedForListener.Store(true)
		}
		listenerErr = engine.UpdateNode(listenerOrderNode("listener"))
	})
	require.NoError(t, runPublicationTransaction(engine, true, func(tx *BadgerTransaction) error {
		return tx.UpdateGraphNode(listenerOrderNode("acknowledged"), ClientNodeEdits)
	}, nil))

	mark := lockHolds.mark()
	var writeErr, flushErr error
	finished := finishesWithin(t, time.Minute, func() {
		flushed := make(chan error, 1)
		go func() { flushed <- engine.FlushWriteBehind() }()
		<-entered
		writeErr = engine.UpdateNode(listenerOrderNode("direct"))
		close(written)
		flushErr = <-flushed
		<-listenerDone
	})
	require.Empty(t, lockHolds.since(mark))
	if !finished {
		return
	}
	t.Cleanup(func() { _ = engine.Close() })
	require.False(t, waitedForListener.Load(), "the ordered write waited for a listener of the replay")
	require.NoError(t, writeErr)
	require.NoError(t, flushErr)
	require.NoError(t, listenerErr)
	require.NoError(t, engine.FlushWriteBehind())
	require.Equal(t, "listener", storedValue(engine, "node"))
}

// A write ordered after a generation that another drain is still replaying,
// for longer than a drain's progress window, waits for that replay before it
// commits, so the acknowledged commit cannot land over it.
func TestOrderedWriteWaitsForAnInFlightReplay(t *testing.T) {
	for _, route := range []string{"delete", "direct-update", "explicit-update"} {
		t.Run(route, func(t *testing.T) {
			engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
			t.Cleanup(func() { _ = engine.Close() })
			// apply is wrapped before any commit signals the flusher.
			var slow atomic.Bool
			apply := engine.writeBehind.apply
			engine.writeBehind.apply = func(buf *CommitBuffer) error {
				if slow.CompareAndSwap(true, false) {
					time.Sleep(3 * time.Second)
				}
				return apply(buf)
			}
			_, err := engine.CreateNode(listenerOrderNode("initial"))
			require.NoError(t, err)
			require.NoError(t, engine.FlushWriteBehind())
			require.NoError(t, runPublicationTransaction(engine, true, func(tx *BadgerTransaction) error {
				return tx.UpdateGraphNode(listenerOrderNode("acknowledged"), ClientNodeEdits)
			}, nil))
			slow.Store(true)
			flushed := make(chan error, 1)
			go func() { flushed <- engine.FlushWriteBehind() }()
			require.Eventually(t, func() bool { return !slow.Load() }, 10*time.Second, time.Millisecond, "the replay started")
			switch route {
			case "delete":
				err = engine.DeleteNode("test:n")
			case "direct-update":
				err = engine.UpdateNode(listenerOrderNode("synchronous"))
			case "explicit-update":
				err = runPublicationTransaction(engine, false, func(tx *BadgerTransaction) error {
					return tx.UpdateGraphNode(listenerOrderNode("synchronous"), ClientNodeEdits)
				}, nil)
			}
			require.NoError(t, err)
			require.NoError(t, <-flushed)
			require.NoError(t, engine.FlushWriteBehind())
			if route == "delete" {
				require.Equal(t, "absent", storedValue(engine, "node"), "the acknowledged update replayed over the later delete")
			} else {
				require.Equal(t, "synchronous", storedValue(engine, "node"), "the acknowledged update replayed over the later write")
			}
		})
	}
}

// The decay suppression rewrite delivers the commits its drain replays once
// it released the write order: a listener of the replayed relabel may write
// the node back.
func TestListenerWriteBackAfterSuppressionRewrite(t *testing.T) {
	old := time.Now().Add(-72 * time.Hour)
	node := func(labels []string, v string) *Node {
		return &Node{ID: "test:n", Labels: labels, Properties: map[string]any{"v": v}, CreatedAt: old, UpdatedAt: old}
	}
	engine := newDecayWriteBehindEngine(t, t.TempDir())
	_, err := engine.CreateNode(node([]string{"Untargeted"}, "initial"))
	require.NoError(t, err)
	require.NoError(t, engine.FlushWriteBehind())
	var called atomic.Bool
	var listenerErr error
	listenerDone := make(chan struct{})
	engine.OnNodeUpdated(func(n *Node) {
		if n.ID == "test:n" && called.CompareAndSwap(false, true) {
			defer close(listenerDone)
			listenerErr = engine.UpdateNode(node([]string{"Doc"}, "listener"))
		}
	})
	require.NoError(t, runPublicationTransaction(engine, true, func(tx *BadgerTransaction) error {
		return tx.UpdateGraphNode(node([]string{"Doc"}, "relabelled"), ClientNodeEdits)
	}, nil))

	mark := lockHolds.mark()
	var rewriteErr error
	finished := finishesWithin(t, time.Minute, func() {
		if _, rewriteErr = engine.EnqueueDeindexIfSuppressed("test:n", false); rewriteErr == nil {
			rewriteErr = engine.FlushWriteBehind()
		}
		if rewriteErr == nil {
			<-listenerDone
		}
	})
	require.Empty(t, lockHolds.since(mark), "a listener ran under a write-order stripe or the commit gate")
	if !finished {
		return
	}
	t.Cleanup(func() { _ = engine.Close() })
	require.NoError(t, rewriteErr)
	require.True(t, called.Load())
	require.NoError(t, listenerErr)
	engine.invalidateCachesAfterRestore()
	stored, err := engine.GetStoredNode("test:n")
	require.NoError(t, err)
	require.Equal(t, "listener", stored.Properties["v"])
}
