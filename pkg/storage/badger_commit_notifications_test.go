package storage

// Commit notification ordering and callback re-entry regressions.
import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func r3Implicit(t *testing.T, b *BadgerEngine, edit func(*BadgerTransaction) error) {
	t.Helper()
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, edit(tx))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
}

// A successful entity commit may pause before publishing its caches/events.
// Reserving only at dispatch would still let a later direct or transactional
// delete overtake that create, even with a serialized callback loop.
func TestCommitNotifications_PublicationOrder(t *testing.T) {
	for _, transactional := range []bool{false, true} {
		t.Run(fmt.Sprintf("transaction=%v", transactional), func(t *testing.T) {
			b := newTestEngine(t)
			id := NodeID("test:publication-order")
			started, resume := make(chan struct{}), make(chan struct{})
			var paused atomic.Bool
			var unblock sync.Once
			defer unblock.Do(func() { close(resume) })
			restore := setCommitTailHook(func() {
				if paused.CompareAndSwap(false, true) {
					close(started)
					<-resume
				}
			})
			defer restore()
			var mu sync.Mutex
			var events []string
			b.OnNodeCreated(func(*Node) { mu.Lock(); defer mu.Unlock(); events = append(events, "create") })
			b.OnNodeDeleted(func(NodeID) { mu.Lock(); defer mu.Unlock(); events = append(events, "delete") })
			created := make(chan error, 1)
			go func() {
				if transactional {
					tx, err := b.BeginTransaction()
					if err == nil {
						_, err = tx.CreateNode(&Node{ID: id})
					}
					if err == nil {
						err = tx.Commit()
					}
					created <- err
				} else {
					_, err := b.CreateNode(&Node{ID: id})
					created <- err
				}
			}()
			select {
			case <-started:
			case <-time.After(4 * time.Second):
				t.Fatal("publication not reached")
			}
			require.NoError(t, b.DeleteNode(id))
			mu.Lock()
			require.Empty(t, events, "later event must wait for earlier publication")
			mu.Unlock()
			unblock.Do(func() { close(resume) })
			r3Await(t, created, "create publication")
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []string{"create", "delete"}, events)
		})
	}
}

func TestCommitNotifications_ReentrantOrder(t *testing.T) {
	for _, path := range []string{"direct", "transaction", "buffered", "startup", "entries"} {
		t.Run(path, func(t *testing.T) {
			dir := t.TempDir()
			b, wal := newWriteBehindWALTestEngine(t, dir, time.Hour)
			id := NodeID("test:reentrant-order")
			var entries []WALEntry
			if path == "startup" || path == "entries" {
				r3Implicit(t, b, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(&Node{ID: id}); return err })
				var err error
				entries, err = ReadWALEntriesFromDir(wal.Config().Dir)
				require.NoError(t, err)
				crashClose(t, b, wal)
				b, wal = newWriteBehindWALTestEngine(t, dir, time.Hour)
			}
			defer wal.Close()
			defer b.Close()
			var events []string
			// The first subscriber performs a nested direct write and flush.
			// The second subscriber must still finish the older create first.
			b.OnNodeCreated(func(n *Node) {
				require.NoError(t, b.UpdateNodeEmbeddingSidecar(&Node{ID: n.ID, ChunkEmbeddings: [][]float32{{1, 2}}}))
				require.NoError(t, b.FlushWriteBehind())
			})
			b.OnNodeCreated(func(*Node) { events = append(events, "create") })
			b.OnNodeUpdated(func(*Node) { events = append(events, "sidecar") })
			switch path {
			case "direct":
				_, err := b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			case "transaction", "buffered":
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(path == "buffered"))
				_, err = tx.CreateNode(&Node{ID: id})
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
				require.NoError(t, b.FlushWriteBehind())
			case "startup":
				n, err := RecoverBufferedCommits(b, wal)
				require.NoError(t, err)
				require.Equal(t, 1, n)
			case "entries":
				for _, entry := range entries {
					if entry.Operation == OpBufferedCommit {
						require.NoError(t, ReplayWALEntry(b, entry))
					}
				}
			}
			b.notifyWG.Wait()
			require.Equal(t, []string{"create", "sidecar"}, events)
		})
	}
}

func r3Await(t *testing.T, c <-chan error, name string) {
	t.Helper()
	select {
	case err := <-c:
		require.NoError(t, err, name)
	case <-time.After(4 * time.Second):
		t.Fatalf("%s did not finish", name)
	}
}

func TestCommitNotifications_BufferedOrder(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	id := NodeID("test:r3-order")
	started, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	// The first subscriber is busy with the create while a second flusher
	// applies the acknowledged deletion. The next subscriber must not end
	// with a create after it has already processed that deletion.
	b.OnNodeCreated(func(*Node) { close(started); <-resume })
	var mu sync.Mutex
	var events []string
	b.OnNodeCreated(func(*Node) { mu.Lock(); defer mu.Unlock(); events = append(events, "create") })
	b.OnNodeDeleted(func(NodeID) { mu.Lock(); defer mu.Unlock(); events = append(events, "delete") })
	r3Implicit(t, b, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(&Node{ID: id}); return err })
	flushed := make(chan error, 1)
	go func() { flushed <- b.FlushWriteBehind() }()
	select {
	case <-started:
	case <-time.After(4 * time.Second):
		t.Fatal("create notification not reached")
	}
	r3Implicit(t, b, func(tx *BadgerTransaction) error { return tx.DeleteNode(id) })
	later := make(chan error, 1)
	go func() { later <- b.FlushWriteBehind() }()
	// Do not require later Flush to finish while the earlier listener is
	// blocked: serialization would be a correct outcome.
	select {
	case err := <-later:
		require.NoError(t, err)
		later = nil
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	if later != nil {
		r3Await(t, later, "later flush")
	}
	r3Await(t, flushed, "earlier flush")
	mu.Lock()
	defer mu.Unlock()
	t.Logf("listener observed events: %v", events)
	require.Equal(t, []string{"create", "delete"}, events)
}

func TestCommitNotifications_EdgeReentry(t *testing.T) {
	if action := os.Getenv("NORNIC_R3_EDGE_CHILD"); action != "" {
		b := newTestEngine(t)
		n := &Node{ID: "test:r3-endpoint"}
		_, err := b.CreateNode(n)
		require.NoError(t, err)
		edge := &Edge{ID: "test:r3-edge", StartNode: n.ID, EndNode: n.ID, Type: "LINK"}
		if action != "create" {
			require.NoError(t, b.CreateEdge(edge))
		}
		writeSidecar := func() {
			x := copyNode(n)
			x.ChunkEmbeddings = [][]float32{{1, 2}}
			require.NoError(t, b.UpdateNodeEmbeddingSidecar(x))
		}
		switch action {
		case "create":
			b.OnEdgeCreated(func(*Edge) { writeSidecar() })
			require.NoError(t, b.CreateEdge(edge))
		case "update":
			b.OnEdgeUpdated(func(*Edge) { writeSidecar() })
			require.NoError(t, b.UpdateEdge(edge))
		case "delete":
			b.OnEdgeDeleted(func(EdgeID) { writeSidecar() })
			require.NoError(t, b.DeleteEdge(edge.ID))
		}
		return
	}
	for _, action := range []string{"create", "update", "delete"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCommitNotifications_EdgeReentry$", "-test.timeout=3s", "-test.v")
			cmd.Env = append(os.Environ(), fmt.Sprintf("NORNIC_R3_EDGE_CHILD=%s", action))
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Logf("child output: %s", out)
			}
			require.NoError(t, err, "direct edge callbacks must allow endpoint sidecar writes")
		})
	}
}

func TestCommitNotifications_CloseDuringCallback(t *testing.T) {
	for _, path := range []string{"direct", "transaction", "sidecar", "buffered"} {
		t.Run(path, func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: path == "buffered", WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			id := NodeID("test:r3-close")
			if path == "sidecar" {
				_, err = b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			started, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(resume) })
			reentered := make(chan error, 1)
			callback := func(n *Node) {
				close(started)
				<-resume
				reentered <- b.UpdateNodeEmbeddingSidecar(copyNode(n))
			}
			if path == "sidecar" {
				b.OnNodeUpdated(callback)
			} else {
				b.OnNodeCreated(callback)
			}
			done := make(chan error, 1)
			go func() {
				n := &Node{ID: id}
				if path == "transaction" || path == "buffered" {
					tx, err := b.BeginTransaction()
					if err != nil {
						done <- err
						return
					}
					if err = tx.SetImplicit(path == "buffered"); err == nil {
						_, err = tx.CreateNode(n)
					}
					if err == nil {
						err = tx.Commit()
					}
					if err == nil && path == "buffered" {
						err = b.FlushWriteBehind()
					}
					done <- err
				} else if path == "sidecar" {
					n.ChunkEmbeddings = [][]float32{{1, 2}}
					done <- b.UpdateNodeEmbeddingSidecar(n)
				} else {
					_, err := b.CreateNode(n)
					done <- err
				}
			}()
			select {
			case <-started:
			case <-time.After(4 * time.Second):
				t.Fatal("notification not reached")
			}
			closed := make(chan error, 1)
			go func() { closed <- b.Close() }()
			// Close may wait for a buffered callback before setting closed.
			select {
			case err := <-closed:
				t.Fatalf("Close returned during callback: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			once.Do(func() { close(resume) })
			select {
			case err := <-reentered:
				if path == "buffered" && err == nil {
					t.Log("callback re-entry completed before Close marked closed")
				} else {
					require.ErrorIs(t, err, ErrStorageClosed)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("callback re-entry did not return")
			}
			r3Await(t, done, "write/flush")
			r3Await(t, closed, "Close")
			drained := make(chan error, 1)
			go func() {
				b.notifyWG.Wait()
				if b.writeBehind != nil {
					b.writeBehind.wg.Wait()
				}
				drained <- nil
			}()
			r3Await(t, drained, "notification and worker goroutine drain")
		})
	}
}

func TestCommitNotifications_PanicReleasesLifetime(t *testing.T) {
	for _, path := range []string{"direct", "transaction", "sidecar", "buffered"} {
		t.Run(path, func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: path == "buffered", WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			id := NodeID("test:r3-panic")
			if path == "sidecar" {
				_, err = b.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			callback := func(*Node) { panic("review callback panic") }
			if path == "sidecar" {
				b.OnNodeUpdated(callback)
			} else {
				b.OnNodeCreated(callback)
			}
			var caught any
			func() {
				defer func() { caught = recover() }()
				if path == "direct" {
					_, _ = b.CreateNode(&Node{ID: id})
					return
				}
				if path == "sidecar" {
					_ = b.UpdateNodeEmbeddingSidecar(&Node{ID: id, ChunkEmbeddings: [][]float32{{1}}})
					return
				}
				tx, e := b.BeginTransaction()
				require.NoError(t, e)
				require.NoError(t, tx.SetImplicit(path == "buffered"))
				_, e = tx.CreateNode(&Node{ID: id})
				require.NoError(t, e)
				require.NoError(t, tx.Commit())
				if path == "buffered" {
					// Invoke the real replay callback in this goroutine so a
					// caller-level recovery can inspect the deferred lifetime.
					_ = b.replayWriteBehind(b.writeBehind.active)
				}
			}()
			require.Equal(t, "review callback panic", caught)
			closed := make(chan error, 1)
			go func() { closed <- b.Close() }()
			r3Await(t, closed, "Close after recovered callback panic")
		})
	}
}
