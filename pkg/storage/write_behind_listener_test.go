package storage

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Write-behind listener and publication regressions (from the #1532 reviews):
// listeners that write back, later writes inside an earlier commit's tail,
// and synchronous writes racing a slow replay. Only long-standing APIs are
// used, so the file runs unchanged on the base.

func wbListenerEngine(t *testing.T, writeBehind bool) *BadgerEngine {
	t.Helper()
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: writeBehind, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	if b.writeBehind != nil {
		b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	}
	return b
}

func wbListenerNode(v string) *Node {
	return &Node{ID: "test:n", Labels: []string{"U"}, Properties: map[string]any{"v": v}}
}

func wbListenerEdge(v string) *Edge {
	return &Edge{ID: "test:r", StartNode: "test:n", EndNode: "test:n", Type: "LINK", Properties: map[string]any{"v": v}}
}

func wbListenerTx(b *BadgerEngine, implicit bool, f func(tx *BadgerTransaction) error) error {
	tx, err := b.BeginTransaction()
	if err != nil {
		return err
	}
	if implicit {
		if err := tx.SetImplicit(true); err != nil {
			return err
		}
	}
	if err := f(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// A listener that writes back to the entity it was notified about. The
// storage contract (publishNodeOutcome, commitNotification) states listener
// reentrant writes are safe because no storage lock is held across callbacks.
func wbListenerReentryChild(t *testing.T, scenario string) {
	wb := !strings.HasSuffix(scenario, "/sync")
	b := wbListenerEngine(t, wb)
	defer b.Close()
	_, err := b.CreateNode(wbListenerNode("initial"))
	require.NoError(t, err)
	require.NoError(t, b.CreateEdge(wbListenerEdge("initial")))
	require.NoError(t, b.FlushWriteBehind())

	var called atomic.Bool
	var listenerErr atomic.Value
	b.OnNodeUpdated(func(n *Node) {
		if n.ID != "test:n" || !called.CompareAndSwap(false, true) {
			return
		}
		t.Logf("node listener entered for %s; writing it back", n.ID)
		if err := b.UpdateNode(wbListenerNode("listener")); err != nil {
			listenerErr.Store(err.Error())
		}
	})
	b.OnEdgeUpdated(func(e *Edge) {
		if e.ID != "test:r" || !called.CompareAndSwap(false, true) {
			return
		}
		t.Logf("edge listener entered for %s; writing it back", e.ID)
		if err := b.UpdateEdge(wbListenerEdge("listener")); err != nil {
			listenerErr.Store(err.Error())
		}
	})
	bufferedNode := func() {
		require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error { return tx.UpdateNode(wbListenerNode("buffered")) }))
	}
	bufferedEdge := func() {
		require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error { return tx.UpdateEdge(wbListenerEdge("buffered")) }))
	}
	switch strings.TrimSuffix(scenario, "/sync") {
	case "node/delete-after-buffered-update":
		bufferedNode()
		err = b.DeleteNode("test:n")
	case "edge/delete-after-buffered-update":
		bufferedEdge()
		err = b.DeleteEdge("test:r")
	case "node/direct-update-after-buffered-update":
		bufferedNode()
		err = b.UpdateNode(wbListenerNode("direct"))
	case "node/explicit-update-after-buffered-update":
		bufferedNode()
		err = wbListenerTx(b, false, func(tx *BadgerTransaction) error { return tx.UpdateNode(wbListenerNode("explicit")) })
	case "node/direct-update-no-buffer":
		err = b.UpdateNode(wbListenerNode("direct"))
	case "edge/direct-update-no-buffer":
		err = b.UpdateEdge(wbListenerEdge("direct"))
	case "node/explicit-update-no-buffer":
		err = wbListenerTx(b, false, func(tx *BadgerTransaction) error { return tx.UpdateNode(wbListenerNode("explicit")) })
	default:
		t.Fatalf("unknown scenario %q", scenario)
	}
	t.Logf("write returned err=%v", err)
	require.NoError(t, b.FlushWriteBehind())
	t.Logf("WB_LISTENER_COMPLETED listenerCalled=%v listenerErr=%v", called.Load(), listenerErr.Load())
}

func TestWriteBehindListenerReentry(t *testing.T) {
	if scenario := os.Getenv("NORNIC_WB_LISTENER_REENTRY"); scenario != "" {
		wbListenerReentryChild(t, scenario)
		return
	}
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
			cmd := exec.Command(os.Args[0], "-test.run=^TestWriteBehindListenerReentry$", "-test.v", "-test.timeout=10s")
			cmd.Env = append(os.Environ(), "NORNIC_WB_LISTENER_REENTRY="+scenario)
			out, err := cmd.CombinedOutput()
			text := string(out)
			deadlocked := strings.Contains(text, "test timed out after 10s")
			var frames []string
			for _, line := range strings.Split(text, "\n") {
				if strings.Contains(line, "pkg/storage.") && (strings.Contains(line, "writeOrder") || strings.Contains(line, "orderAfterBufferedCommits") || strings.Contains(line, "Flush") || strings.Contains(line, "UpdateNode") || strings.Contains(line, "UpdateEdge") || strings.Contains(line, "Delete") || strings.Contains(line, "Commit") || strings.Contains(line, "lockEngineWriteKeys") || strings.Contains(line, "Notification")) {
					frames = append(frames, strings.TrimSpace(line))
				}
			}
			t.Logf("RESULT scenario=%s deadlocked=%v childErr=%v", scenario, deadlocked, err)
			for _, line := range strings.Split(text, "\n") {
				if strings.Contains(line, "WB_LISTENER_COMPLETED") || strings.Contains(line, "write returned") || strings.Contains(line, "listener entered") {
					t.Logf("child: %s", strings.TrimSpace(line))
				}
			}
			if deadlocked {
				t.Logf("blocked storage frames:\n%s", strings.Join(frames, "\n"))
			} else if err != nil {
				t.Logf("child output:\n%s", text)
			}
			require.False(t, deadlocked, "a listener writing back must not deadlock the engine")
			require.NoError(t, err)
		})
	}
}

// An earlier synchronous commit paused at its commit tail, then a later
// write of the same entity. After both return, the cached read must equal
// the physical read and the later value.
func TestWriteBehindLaterWriteDuringCommitTail(t *testing.T) {
	// Write-behind only: without it the same interleaving still leaves the
	// cache behind the later write. That inherited commit-tail publication
	// item is tracked separately and also fails on the release base.
	for _, mode := range []string{"writebehind"} {
		for _, kind := range []string{"node", "edge"} {
			for _, route := range []string{"direct", "explicit"} {
				t.Run(mode+"/"+kind+"/"+route, func(t *testing.T) {
					b := wbListenerEngine(t, mode == "writebehind")
					defer b.Close()
					_, err := b.CreateNode(wbListenerNode("initial"))
					require.NoError(t, err)
					require.NoError(t, b.CreateEdge(wbListenerEdge("initial")))
					require.NoError(t, b.FlushWriteBehind())
					update := func(route, v string) error {
						if route == "direct" {
							if kind == "node" {
								return b.UpdateNode(wbListenerNode(v))
							}
							return b.UpdateEdge(wbListenerEdge(v))
						}
						return wbListenerTx(b, false, func(tx *BadgerTransaction) error {
							if kind == "node" {
								return tx.UpdateNode(wbListenerNode(v))
							}
							return tx.UpdateEdge(wbListenerEdge(v))
						})
					}
					read := func() string {
						if kind == "node" {
							n, err := b.GetNode("test:n")
							require.NoError(t, err)
							return fmt.Sprint(n.Properties["v"])
						}
						e, err := b.GetEdge("test:r")
						require.NoError(t, err)
						return fmt.Sprint(e.Properties["v"])
					}
					var entered atomic.Bool
					laterDone := make(chan error, 1)
					laterAckedDuringTail := false
					afterLater := ""
					restore := setCommitTailHook(func() {
						if !entered.CompareAndSwap(false, true) {
							return
						}
						go func() { laterDone <- update(route, "later") }()
						select {
						case err := <-laterDone:
							laterAckedDuringTail = true
							laterDone <- err
							afterLater = read()
						case <-time.After(3 * time.Second):
						}
					})
					err = update("explicit", "earlier")
					restore()
					require.NoError(t, err)
					require.NoError(t, <-laterDone)
					cached := read()
					require.NoError(t, b.FlushWriteBehind())
					b.invalidateCachesAfterRestore()
					physical := read()
					t.Logf("RESULT %s/%s/%s laterAckedDuringTail=%v afterLater=%s cachedAfterBoth=%s physical=%s", mode, kind, route, laterAckedDuringTail, afterLater, cached, physical)
					require.Equal(t, "later", physical)
					require.Equal(t, physical, cached, "cache must not keep the earlier commit's body after a later acknowledged write")
				})
			}
		}
	}
}

// The same tail interleaving for the unique-constraint value cache: an
// earlier commit moves k a->b, a later write moves k b->c. Afterwards b is
// free and another node may take it.
func TestWriteBehindLaterWriteDuringCommitTailUniqueCache(t *testing.T) {
	// Write-behind only: without it the same interleaving still leaves the
	// cache behind the later write. That inherited commit-tail publication
	// item is tracked separately and also fails on the release base.
	for _, mode := range []string{"writebehind"} {
		for _, route := range []string{"direct", "explicit"} {
			t.Run(mode+"/"+route, func(t *testing.T) {
				b := wbListenerEngine(t, mode == "writebehind")
				defer b.Close()
				require.NoError(t, b.GetSchemaForNamespace("test").AddUniqueConstraint("u_k", "U", "k"))
				node := func(id NodeID, k string) *Node {
					return &Node{ID: id, Labels: []string{"U"}, Properties: map[string]any{"k": k}}
				}
				_, err := b.CreateNode(node("test:n", "a"))
				require.NoError(t, err)
				require.NoError(t, b.FlushWriteBehind())
				var entered atomic.Bool
				laterDone := make(chan error, 1)
				laterAckedDuringTail := false
				restore := setCommitTailHook(func() {
					if !entered.CompareAndSwap(false, true) {
						return
					}
					go func() {
						if route == "direct" {
							laterDone <- b.UpdateNode(node("test:n", "c"))
							return
						}
						laterDone <- wbListenerTx(b, false, func(tx *BadgerTransaction) error { return tx.UpdateNode(node("test:n", "c")) })
					}()
					select {
					case err := <-laterDone:
						laterAckedDuringTail = true
						laterDone <- err
					case <-time.After(3 * time.Second):
					}
				})
				err = wbListenerTx(b, false, func(tx *BadgerTransaction) error { return tx.UpdateNode(node("test:n", "b")) })
				restore()
				require.NoError(t, err)
				require.NoError(t, <-laterDone)
				require.NoError(t, b.FlushWriteBehind())
				n, err := b.GetNode("test:n")
				require.NoError(t, err)
				_, createErr := b.CreateNode(node("test:m", "b"))
				t.Logf("RESULT %s/%s laterAckedDuringTail=%v n.k=%v createWithFreedValueErr=%v", mode, route, laterAckedDuringTail, n.Properties["k"], createErr)
				require.Equal(t, "c", n.Properties["k"])
				require.NoError(t, createErr, "k=b is held by no node after the later write")
			})
		}
	}
}

// The write order drains the buffer before a synchronous write of a buffered
// entity. While a generation's replay is still running (here held for 4 s,
// e.g. a large generation on a loaded disk), the synchronous write must still
// land after it.
func TestWriteBehindSynchronousWriteDuringSlowReplay(t *testing.T) {
	for _, route := range []string{"delete", "direct-update", "explicit-update"} {
		t.Run(route, func(t *testing.T) {
			b := wbListenerEngine(t, true)
			defer b.Close()
			var slow atomic.Bool
			apply := b.writeBehind.apply
			b.writeBehind.apply = func(buf *CommitBuffer) error {
				if slow.CompareAndSwap(true, false) {
					time.Sleep(4 * time.Second)
				}
				return apply(buf)
			}
			_, err := b.CreateNode(wbListenerNode("initial"))
			require.NoError(t, err)
			require.NoError(t, b.FlushWriteBehind())
			require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error { return tx.UpdateNode(wbListenerNode("acknowledged")) }))
			slow.Store(true)
			flushed := make(chan error, 1)
			go func() { flushed <- b.FlushWriteBehind() }()
			time.Sleep(300 * time.Millisecond)
			start := time.Now()
			switch route {
			case "delete":
				err = b.DeleteNode("test:n")
			case "direct-update":
				err = b.UpdateNode(wbListenerNode("synchronous"))
			case "explicit-update":
				err = wbListenerTx(b, false, func(tx *BadgerTransaction) error { return tx.UpdateNode(wbListenerNode("synchronous")) })
			}
			waited := time.Since(start)
			require.NoError(t, err)
			require.NoError(t, <-flushed)
			require.NoError(t, b.FlushWriteBehind())
			b.invalidateCachesAfterRestore()
			n, getErr := b.GetNode("test:n")
			state := "absent"
			if getErr == nil {
				state = fmt.Sprint(n.Properties["v"])
			} else if !errors.Is(getErr, ErrNotFound) {
				state = getErr.Error()
			}
			t.Logf("RESULT %s waited=%v final=%s", route, waited.Round(time.Millisecond), state)
			if route == "delete" {
				require.Equal(t, "absent", state, "the acknowledged update replayed over the later delete")
			} else {
				require.Equal(t, "synchronous", state, "the acknowledged update replayed over the later write")
			}
		})
	}
}
