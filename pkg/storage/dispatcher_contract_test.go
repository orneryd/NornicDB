package storage

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

// Isolate a deliberately deadlocking baseline in a child test process. The
// same public schedule and assertions run unchanged on the correction.
func dispatcherProbe(t *testing.T, body func()) {
	t.Helper()
	if os.Getenv("NORNIC_DISPATCH_PROBE") == t.Name() {
		body()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$", "-test.timeout=12s", "-test.v")
	cmd.Env = append(os.Environ(), "NORNIC_DISPATCH_PROBE="+t.Name())
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
}

func TestDispatcherOrdinaryWritesDoNotConsumeListeners(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint(buffered), func(t *testing.T) {
			b := wbListenerEngine(t, buffered)
			defer b.Close()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			b.OnNodeCreated(func(n *Node) {
				if n.ID == "test:ordinary" {
					close(entered)
					<-release
				}
			})
			written := make(chan error, 1)
			go func() { _, err := b.CreateNode(&Node{ID: "test:ordinary"}); written <- err }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("listener did not enter")
			}
			early := false
			select {
			case err := <-written:
				require.NoError(t, err)
				early = true
			case <-time.After(300 * time.Millisecond):
			}
			unblock()
			if !early {
				require.NoError(t, <-written)
			}
			require.True(t, early, "ordinary write waited for listener consumption")
		})
	}
}

func TestDispatcherReentrantFlush(t *testing.T) {
	dispatcherProbe(t, func() {
		b := wbListenerEngine(t, true)
		defer b.Close()
		completed := make(chan error, 1)
		b.OnNodeCreated(func(n *Node) {
			if n.ID != "test:outer" {
				return
			}
			err := wbListenerTx(b, true, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(&Node{ID: "test:inner"}); return err })
			if err == nil {
				err = b.FlushWriteBehind()
			}
			completed <- err
		})
		require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(&Node{ID: "test:outer"}); return err }))
		require.NoError(t, b.FlushWriteBehind())
		require.NoError(t, <-completed)
		_, err := b.GetStoredNode("test:inner")
		require.NoError(t, err)
	})
}

func TestDispatcherFirstRegistrationAfterFailedLander(t *testing.T) {
	b := wbListenerEngine(t, true)
	defer b.Close()
	b.writeBehind.Close() // eliminate the unrelated worker that can rescue the old queue
	require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(&Node{ID: "test:registered"}); return err }))
	called := make(chan struct{}, 1)
	apply := b.writeBehind.apply
	b.writeBehind.apply = func(g *CommitBuffer) error {
		b.OnNodeCreated(func(n *Node) {
			if n.ID == "test:registered" || n.ID == "test:registered-after" {
				select {
				case called <- struct{}{}:
				default:
				}
			}
		})
		return apply(g)
	}
	_, err := b.CreateNode(&Node{ID: "test:registered"}) // fails after landing; no own event
	require.Error(t, err)
	flushed := make(chan error, 1)
	go func() { flushed <- b.FlushWriteBehind() }()
	early := false
	select {
	case err := <-flushed:
		require.NoError(t, err)
		early = true
	case <-time.After(500 * time.Millisecond):
	}
	if !early {
		_, err := b.CreateNode(&Node{ID: "test:rescue"})
		require.NoError(t, err)
		require.NoError(t, <-flushed)
	}
	_, err = b.CreateNode(&Node{ID: "test:registered-after"})
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind())
	select {
	case <-called:
	case <-time.After(3 * time.Second):
		t.Fatal("registered listener never ran")
	}
	require.True(t, early, "ready notification needed an unrelated write to wake delivery")
}

func dispatcherWAL(t *testing.T) (*BadgerEngine, *WALEngine) {
	t.Helper()
	t.Cleanup(config.WithWALEnabled())
	b := wbListenerEngine(t, true)
	cfg := DefaultWALConfig()
	cfg.Dir = t.TempDir()
	log, err := NewWAL(cfg.Dir, cfg)
	require.NoError(t, err)
	w := NewWALEngine(b, log)
	w.snapshotDir = t.TempDir()
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	return b, w
}

func TestDispatcherWALCompactionLanding(t *testing.T) {
	dispatcherProbe(t, func() {
		b, w := dispatcherWAL(t)
		written := make(chan error, 1)
		b.OnNodeCreated(func(n *Node) {
			if n.ID == "test:compact" {
				_, err := w.CreateNode(&Node{ID: "test:callback"})
				written <- err
			}
		})
		require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error { _, err := tx.CreateNode(&Node{ID: "test:compact"}); return err }))
		require.NoError(t, w.createSnapshotAndCompact())
		require.NoError(t, <-written)
		_, err := b.GetStoredNode("test:compact")
		require.NoError(t, err, "compaction landed acknowledged data before truncation")
	})
}

func TestDispatcherPanicContinuesCallbacksAndBatch(t *testing.T) {
	b := wbListenerEngine(t, true)
	defer b.Close()
	var delivered atomic.Int32
	b.OnNodeCreated(func(n *Node) {
		if n.ID == "test:panic-first" {
			panic("dispatcher-probe")
		}
	})
	b.OnNodeCreated(func(*Node) { delivered.Add(1) })
	require.NoError(t, wbListenerTx(b, true, func(tx *BadgerTransaction) error {
		for _, id := range []NodeID{"test:panic-first", "test:panic-second"} {
			if _, err := tx.CreateNode(&Node{ID: id}); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, b.FlushWriteBehind())
	require.EqualValues(t, 2, delivered.Load(), "a panic discarded another callback or event")
}

func TestDispatcherNoListenerNotificationAllocations(t *testing.T) {
	b := wbListenerEngine(t, true)
	defer b.Close()
	ops := []Operation{{Type: OpCreateNode, Node: &Node{ID: "test:no-listeners", Properties: map[string]any{"large": make([]byte, 4096)}}}}
	allocs := testing.AllocsPerRun(100, func() { require.Nil(t, b.reserveCommitOperations(ops)) })
	require.Zero(t, allocs, "notification admission copied a listener-free payload")
}

func TestDispatcherWALBoundEmbeddingLease(t *testing.T) {
	dispatcherProbe(t, func() {
		b, w := dispatcherWAL(t)
		_, err := w.CreateNode(&Node{ID: "test:embedding", Properties: map[string]any{"text": "body"}})
		require.NoError(t, err)
		payload, err := b.GetNode("test:embedding")
		require.NoError(t, err)
		payload, err = SnapshotNodeForEmbedding(payload)
		require.NoError(t, err)
		require.NotZero(t, payload.embeddingRevision, "the worker must bind an authoritative body revision")
		payload.EmbedMeta = map[string]any{"embedding_failed": true}
		payload.embeddingAttempt = NewEmbeddingAttempt(1, 1, payload.embeddingRevision, func() (func(), bool) { return func() {}, true }, nil)
		entered, proceed := make(chan struct{}), make(chan struct{})
		b.OnNodeUpdated(func(n *Node) {
			if n.ID != "test:embedding" {
				return
			}
			require.False(t, n.embeddingWaitDeferred, "wrapper wait scope leaked into delivery")
			close(entered)
			<-proceed
			_, writeErr := w.CreateNode(&Node{ID: "test:embedding-callback"})
			require.NoError(t, writeErr)
		})
		updated := make(chan error, 1)
		go func() { updated <- w.UpdateNodeEmbeddingSidecar(payload) }()
		select {
		case <-entered:
		case err := <-updated:
			require.NoError(t, err)
			t.Fatal("outcome returned without entering its callback")
		}
		// Hold an independent read lease, enqueue the compactor's exclusive
		// lease, then observe writer preference before letting the callback write.
		w.mutationMu.RLock()
		compacted := make(chan error, 1)
		go func() { compacted <- w.createSnapshotAndCompact() }()
		for w.mutationMu.TryRLock() {
			w.mutationMu.RUnlock()
			runtime.Gosched()
		}
		close(proceed)
		w.mutationMu.RUnlock()
		require.NoError(t, <-updated)
		require.NoError(t, <-compacted)
	})
}
