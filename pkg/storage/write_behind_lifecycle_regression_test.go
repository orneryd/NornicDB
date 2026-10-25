package storage

// Write-behind generation lifecycle regressions from the #1676 and #1532
// reviews: flush and Close completion, Close admission, publication before
// listeners, listener panics, partial retirement, value-ordered UNIQUE reuse,
// prefix deletion and listener re-entry while a unique key is held.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// A second public flush must cover an acknowledged generation that another
// drain has retired but whose callback has not finished. Close races both.
func TestWriteBehindLaterFlushWaitsForRetiredListener(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	b.OnNodeCreated(func(n *Node) {
		if n.ID == "test:held" {
			close(entered)
			<-release
		}
	})
	bufferedCreate(t, b, &Node{ID: "test:held"})
	first := make(chan error, 1)
	go func() { first <- b.FlushWriteBehind() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not enter")
	}
	b.writeBehind.mu.RLock()
	remaining := len(b.writeBehind.draining)
	b.writeBehind.mu.RUnlock()
	require.Zero(t, remaining, "schedule reached retirement before delivery")
	second, closed := make(chan error, 1), make(chan error, 1)
	go func() { second <- b.FlushWriteBehind() }()
	go func() { closed <- b.Close() }()
	secondEarly := false
	select {
	case err := <-second:
		require.NoError(t, err)
		secondEarly = true
	case <-time.After(300 * time.Millisecond):
	}
	select {
	case err := <-closed:
		t.Errorf("Close returned while listener was blocked: %v", err)
		closed <- nil
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-first)
	if !secondEarly {
		require.NoError(t, <-second)
	}
	require.NoError(t, <-closed)
	require.False(t, secondEarly, "second flush returned before listeners of a pre-flush acknowledgement completed")
}

// Close has started while a background listener is held. An ordinary
// buffered commit from then on is refused or kept, never acknowledged and lost.
func TestWriteBehindCloseRefusesWritesAfterFinalDrain(t *testing.T) {
	dir := t.TempDir()
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	b.OnNodeCreated(func(n *Node) {
		if n.ID == "test:first-close" {
			close(entered)
			<-release
		}
	})
	bufferedCreate(t, b, &Node{ID: "test:first-close"})
	b.writeBehind.signal()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("background listener never started")
	}
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	// Close closes buffered admission before its final drain, which waits for
	// the held listener: a commit from here on must not be acknowledged and lost.
	require.Eventually(t, func() bool {
		b.writeBehind.mu.RLock()
		defer b.writeBehind.mu.RUnlock()
		return b.writeBehind.admissionClosed
	}, 5*time.Second, time.Millisecond, "Close never closed admission")
	tx, err := b.BeginTransaction()
	commitErr := err
	ack := false
	if err == nil {
		require.NoError(t, tx.SetImplicit(true))
		_, err = tx.CreateNode(&Node{ID: "test:late-close"})
		require.NoError(t, err)
		commitErr = tx.Commit()
		ack = commitErr == nil
	}
	t.Logf("late close commit acknowledged=%v err=%v", ack, commitErr)
	unblock()
	require.NoError(t, <-closed)
	reopened, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
	require.NoError(t, err)
	defer reopened.Close()
	if ack {
		_, err = reopened.GetStoredNode("test:late-close")
		require.NoError(t, err, "Close discarded a commit it acknowledged after stopping its flusher")
	}
}

// The publication callback precedes this commit's own event. Older events
// belong to their own publications and may run while a later writer proceeds.
func TestWriteBehindPublicationBeforeListeners(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	var published atomic.Bool
	var seen atomic.Bool
	b.OnNodeCreated(func(n *Node) {
		if n.ID == "test:publication" {
			seen.Store(published.Load())
		}
	})
	bufferedCreate(t, b, &Node{ID: "test:prior"})
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:publication"})
	require.NoError(t, err)
	// Writing the buffered node orders the commit after its generation, which
	// the commit lands, so its event is delivered by this commit.
	require.NoError(t, tx.UpdateNode(&Node{ID: "test:prior", Properties: map[string]any{"v": int64(1)}}))
	require.NoError(t, tx.CommitContextWithPublication(context.Background(), func() { published.Store(true) }))
	require.True(t, published.Load())
	b.WaitForListeners()
	require.True(t, seen.Load(), "listener ran before CommitContextWithPublication released wrapper admission")
}

// A callback panic is contained by the dispatcher and cannot wedge settlement.
func TestWriteBehindListenerPanicSettlement(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	var calls atomic.Int32
	b.OnNodeCreated(func(n *Node) {
		calls.Add(1)
		if n.ID == "test:panic" {
			panic("review-listener")
		}
	})
	bufferedCreate(t, b, &Node{ID: "test:panic"})
	b.writeBehind.rotate()
	b.writeBehind.mu.RLock()
	g := b.writeBehind.draining[0]
	b.writeBehind.mu.RUnlock()
	var caught any
	func() { defer func() { caught = recover() }(); _ = b.FlushWriteBehind() }()
	require.Nil(t, caught, "listener panic belongs to the dispatcher")
	select {
	case <-g.delivered():
	case <-time.After(3 * time.Second):
		t.Fatal("panic stranded generation completion")
	}
	bufferedCreate(t, b, &Node{ID: "test:after-panic"})
	require.NoError(t, b.FlushWriteBehind())
	require.EqualValues(t, 2, calls.Load())
}

// The data and marker committed, but the process loses the retired generation
// before its held notification is delivered; later ACK remains WAL-only.
func TestWriteBehindRecoveryAfterPartialRetirement(t *testing.T) {
	// The WAL appends only while WAL is enabled process-wide; do not inherit
	// another test's setting.
	t.Cleanup(config.WithWALEnabled())
	dir := t.TempDir()
	open := func() (*BadgerEngine, *WAL) {
		b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour})
		require.NoError(t, err)
		b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
		cfg := DefaultWALConfig()
		cfg.Dir = filepath.Join(dir, "wal")
		cfg.BatchSyncInterval = time.Hour
		w, err := NewWAL(cfg.Dir, cfg)
		require.NoError(t, err)
		b.SetBufferedCommitWAL(w)
		return b, w
	}
	b, w := open()
	b.writeBehind.Close()
	b.OnNodeCreated(func(*Node) {})
	bufferedCreate(t, b, &Node{ID: "test:retired", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}})
	b.writeBehind.rotate()
	g := b.writeBehind.draining[0]
	require.True(t, g.claimDrain())
	require.NoError(t, b.replayWriteBehind(g))
	require.True(t, b.writeBehind.dequeueApplied(g))
	bufferedCreate(t, b, &Node{ID: "test:walonly", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(2)}})
	crashClose(t, b, w)
	recovered, w2 := open()
	defer recovered.Close()
	defer w2.Close()
	count, err := RecoverBufferedCommits(recovered, w2)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	for i, id := range []NodeID{"test:retired", "test:walonly"} {
		n, err := recovered.GetStoredNode(id)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprint(i+1), fmt.Sprint(n.Properties["v"]))
	}
	n, err := recovered.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 2, n)
	labels, err := recovered.NodeCountByLabelInNamespace("test", "Doc")
	require.NoError(t, err)
	require.EqualValues(t, 2, labels)
	count, err = RecoverBufferedCommits(recovered, w2)
	require.NoError(t, err)
	require.Zero(t, count)
}

// Review probe for #1676: a direct write drains only the generations that hold
// a state of the entity it writes. An older buffered commit of ANOTHER node can
// still land after it; on the release base that replay unregisters the unique
// value it releases by value, dropping the newer direct holder from the
// constraint cache.
func TestWriteBehindDirectCreateAfterBufferedUniqueRelease(t *testing.T) {
	for _, release := range []string{"delete", "rekey"} {
		t.Run(fmt.Sprintf("release=%s", release), func(t *testing.T) {
			b := openWriteBehindOrderEngine(t, t.TempDir(), true)
			defer b.Close()
			store := NewNamespacedEngine(b, "test")
			schema := store.GetSchema()
			require.NoError(t, schema.AddUniqueConstraint("doc_id", "Doc", "id"))
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(store, schema))
			_, err := store.CreateNode(&Node{ID: "a", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}})
			require.NoError(t, err)
			require.NoError(t, b.FlushWriteBehind())

			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, tx.SetImplicit(true))
			if release == "delete" {
				require.NoError(t, tx.DeleteNode("test:a"))
			} else {
				node, err := tx.GetNode("test:a")
				require.NoError(t, err)
				node.Properties["id"] = "other"
				require.NoError(t, tx.UpdateNode(node))
			}
			require.NoError(t, tx.Commit())
			require.True(t, tx.bufferedAck.Load())

			_, err = store.CreateNode(&Node{ID: "b", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}})
			require.NoError(t, err, "the released key is free for a direct create")
			require.NoError(t, b.FlushWriteBehind())

			_, err = store.CreateNode(&Node{ID: "c", Labels: []string{"Doc"}, Properties: map[string]any{"id": "k"}})
			full, indexed := writeBehindOrderHolders(t, store, "Doc", "k")
			t.Logf("second create err=%v; holders full=%d indexed=%d", err, full, indexed)
			require.Error(t, err, "a second holder of the key b holds was accepted")
		})
	}
}

// Review probe for #1676: DeleteByPrefix (DROP DATABASE) writes Badger
// synchronously but is not ordered against the write-behind buffer. An
// acknowledged buffered create in the dropped namespace stays visible and is
// later flushed back into the dropped namespace.
func TestWriteBehindDropPrefixWithBufferedWrites(t *testing.T) {
	b := openWriteBehindOrderEngine(t, t.TempDir(), true)
	defer b.Close()
	seedWriteBehindOrderNode(t, b, "test:seed", []string{"Doc"}, map[string]any{"id": "seed"})

	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&Node{ID: "test:buffered", Labels: []string{"Doc"}, Properties: map[string]any{"id": "buffered"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.True(t, tx.bufferedAck.Load())

	nodes, edges, err := b.DeleteByPrefix("test:")
	require.NoError(t, err)
	t.Logf("DeleteByPrefix reported nodes=%d edges=%d", nodes, edges)
	_, visibleErr := b.GetNode("test:buffered")
	t.Logf("before flush: GetNode(buffered) err=%v", visibleErr)
	if visibleErr == nil {
		t.Error("dropped buffered node remains visible")
	}
	require.NoError(t, b.FlushWriteBehind())

	var survivors []NodeID
	require.NoError(t, StreamStoredNodes(t.Context(), b, StreamNodesOptions{}, func(n *Node) error {
		survivors = append(survivors, n.ID)
		return nil
	}))
	count, err := b.NodeCountByLabelInNamespace("test", "Doc")
	require.NoError(t, err)
	t.Logf("after flush: stored nodes=%v Doc count=%d", survivors, count)
	if count != 0 {
		t.Errorf("dropped namespace label count = %d, want 0", count)
	}
	require.Empty(t, survivors, "a dropped namespace must hold no nodes after the buffer flushes")
}

func TestWriteBehindListenerReentryWhileUniqueKeyHeld(t *testing.T) {
	for _, route := range []string{"direct", "merge-preparation"} {
		t.Run(route, func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			b.writeBehind.Close() // deterministic caller-owned drain before the timer
			schema := b.GetSchemaForNamespace("test")
			require.NoError(t, schema.AddUniqueConstraint("review_key", "U", "k"))
			node := func(v string) *Node {
				return &Node{ID: "test:key", Labels: []string{"U"}, Properties: map[string]any{"k": "held", "v": v}}
			}
			_, err := b.CreateNode(node("seed"))
			require.NoError(t, err)
			prior, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, prior.SetImplicit(true))
			require.NoError(t, prior.UpdateNode(node("buffered")))
			result := make(chan error, 1)
			var once atomic.Bool
			b.OnNodeUpdated(func(n *Node) {
				if n.ID == "test:key" && once.CompareAndSwap(false, true) {
					child, err := b.BeginTransaction()
					if err != nil {
						result <- err
						return
					}
					ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
					defer cancel()
					if err = child.SetNamespace("test"); err == nil {
						err = child.PrepareMergeNodeForWrite(ctx, []string{"U"}, map[string]any{"k": "held"})
					}
					if err == nil {
						err = child.UpdateNode(node("listener"))
					}
					if err != nil {
						_ = child.Rollback()
						result <- err
						return
					}
					result <- child.CommitContextWithPublication(ctx, nil)
				}
			})
			require.NoError(t, prior.Commit())
			if route == "direct" {
				require.NoError(t, b.UpdateNode(node("direct")))
			} else {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetNamespace("test"))
				err = tx.PrepareMergeNodeForWrite(context.Background(), []string{"U"}, map[string]any{"k": "held"})
				_ = tx.Rollback()
				require.NoError(t, err)
			}
			select {
			case err := <-result:
				require.NoError(t, err, "listener waited on a unique key held by the caller delivering it")
			case <-time.After(3 * time.Second):
				t.Fatal("listener never finished")
			}
		})
	}
}

func TestWriteBehindCloseListenerAcknowledgement(t *testing.T) {
	if os.Getenv("NORNIC_WB_CLOSE_CHILD") == "1" {
		dir := t.TempDir()
		b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour})
		require.NoError(t, err)
		b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
		_, err = b.CreateNode(wbListenerNode("initial"))
		require.NoError(t, err)
		var once, acknowledged atomic.Bool
		b.OnNodeUpdated(func(n *Node) {
			if once.CompareAndSwap(false, true) {
				err := wbListenerTx(b, true, func(tx *BadgerTransaction) error { return tx.UpdateNode(wbListenerNode("listener-ack")) })
				if err == nil {
					acknowledged.Store(true)
				} else {
					require.True(t, errors.Is(err, ErrStorageClosed), "rejection must be the closing boundary: %v", err)
				}
				t.Logf("listener write acknowledged=%v err=%v", acknowledged.Load(), err)
			}
		})
		require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error { return tx.UpdateNode(wbListenerNode("first")) }))
		require.NoError(t, b.Close())
		require.True(t, once.Load())
		t.Log("Close returned")
		reopened, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir})
		require.NoError(t, err)
		defer reopened.Close()
		n, err := reopened.GetStoredNode("test:n")
		require.NoError(t, err)
		want := "first"
		if acknowledged.Load() {
			want = "listener-ack"
		}
		require.Equal(t, want, n.Properties["v"], "Close keeps every acknowledged write; a closing engine may reject new writes")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestWriteBehindCloseListenerAcknowledgement$", "-test.v", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "NORNIC_WB_CLOSE_CHILD=1")
	out, err := cmd.CombinedOutput()
	t.Logf("child: %s", out)
	require.False(t, strings.Contains(string(out), "test timed out"), "Close waited for an acknowledged notification whose generation was never replayed")
	require.NoError(t, err)
}

// A schema loaded from disk at open must see buffered nodes: after a restart
// (and a drop of another database) a second buffered CREATE of a UNIQUE
// value was acknowledged, because only lazily created schemas were attached
// to the commit buffer.
func TestWriteBehindUniqueSeesBufferedAfterReopen(t *testing.T) {
	dir := t.TempDir()
	var b *BadgerEngine
	open := func() {
		var err error
		b, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: true, WriteBehindInterval: time.Hour})
		require.NoError(t, err)
		b.writeBehind.SetDelayBounds(time.Hour, time.Hour)
		for _, ns := range []string{"m1", "m2"} {
			schema := b.GetSchemaForNamespace(ns)
			if !schema.valueOrdered(ConstraintEntityNode, []string{"U"}) {
				require.NoError(t, schema.AddUniqueConstraint(ns+"_u_k", "U", "k"))
			}
			require.NoError(t, RefreshUniqueConstraintValuesForEngine(NewNamespacedEngine(b, ns), schema))
		}
	}
	create := func(id NodeID) error {
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		if _, err := tx.CreateNode(&Node{ID: id, Labels: []string{"U"}, Properties: map[string]any{"k": "b"}}); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	open()
	require.NoError(t, create("m2:n4"))
	require.NoError(t, b.Close())
	open()
	defer b.Close()
	_, _, err := b.DeleteByPrefix("m2:")
	require.NoError(t, err)
	require.NoError(t, create("m1:n5"))

	err = create("m1:n1")
	var violation *ConstraintViolationError
	require.ErrorAs(t, err, &violation, "a second buffered holder of U.k=b was acknowledged")
	require.NoError(t, b.FlushWriteBehind())
	_, err = b.GetNode("m1:n1")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = b.GetNode("m1:n5")
	require.NoError(t, err)
}

// A synchronous commit lands only the buffered generations it is ordered
// after: an explicit write of an unrelated node leaves an acknowledged write
// buffered, one of the buffered node lands it first, and one under a
// uniqueness constraint lands everything.
func TestWriteBehindExplicitCommitLandsOnlyWhatItTouches(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	// No background flusher: only the commits under test land the buffer.
	b.writeBehind.Close()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(0)}})
		require.NoError(t, err)
	}
	require.NoError(t, b.FlushWriteBehind())
	write := func(implicit bool, node *Node, create bool) {
		t.Helper()
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(implicit))
		require.NoError(t, tx.SetNamespace("test"))
		if create {
			_, err = tx.CreateNode(node)
		} else {
			err = tx.UpdateNode(node)
		}
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}
	buffered := func() int {
		b.writeBehind.mu.RLock()
		defer b.writeBehind.mu.RUnlock()
		return b.writeBehind.opCount + len(b.writeBehind.draining)
	}

	write(true, &Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}}, false)
	require.NotZero(t, buffered(), "the autocommit write was not buffered")
	write(false, &Node{ID: "test:b", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(1)}}, false)
	require.NotZero(t, buffered(), "an unrelated explicit commit landed the buffer")

	write(false, &Node{ID: "test:a", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(2)}}, false)
	require.Zero(t, buffered(), "an explicit commit of a buffered node did not land it first")
	stored, err := b.GetStoredNode("test:a")
	require.NoError(t, err)
	require.Equal(t, int64(2), stored.Properties["v"])

	require.NoError(t, b.GetSchemaForNamespace("test").AddUniqueConstraint("test_u_k", "U", "k"))
	write(true, &Node{ID: "test:u1", Labels: []string{"U"}, Properties: map[string]any{"k": "x"}}, true)
	require.NotZero(t, buffered())
	write(false, &Node{ID: "test:u2", Labels: []string{"U"}, Properties: map[string]any{"k": "y"}}, true)
	require.Zero(t, buffered(), "a unique claim did not land the buffer first")
}
