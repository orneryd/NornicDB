package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// An external applier claims a generation, applies it, and gives up the claim
// before or after a concurrent flush retires it. Either way the flush returns,
// once the generation's single notification has been delivered, and the node
// is counted once.
func TestWriteBehindExternalApplyRacesFlush(t *testing.T) {
	for _, releaseFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("releaseFirst=%v", releaseFirst), func(t *testing.T) {
			b := newWriteBehindTestEngine(t, time.Hour)
			var notifications atomic.Int32
			b.OnNodeCreated(func(n *Node) {
				if n.ID == "test:external" {
					notifications.Add(1)
				}
			})
			bufferedCreate(t, b, &Node{ID: "test:external"})
			b.writeBehind.rotate()
			b.writeBehind.mu.RLock()
			buf := b.writeBehind.draining[0]
			b.writeBehind.mu.RUnlock()
			require.True(t, buf.claimDrain())
			require.NoError(t, b.replayWriteBehind(buf))
			if releaseFirst {
				buf.releaseDrain()
			}
			done := make(chan error, 1)
			go func() { done <- b.FlushWriteBehind() }()
			require.Eventually(t, func() bool {
				b.writeBehind.mu.RLock()
				defer b.writeBehind.mu.RUnlock()
				return len(b.writeBehind.draining) == 0
			}, 5*time.Second, time.Millisecond)
			if !releaseFirst {
				buf.releaseDrain()
			}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Error("flush never settles a generation retired before its external applier gave up the claim")
				b.writeBehind.settle(buf, true)
				require.NoError(t, <-done)
			}
			require.EqualValues(t, 1, notifications.Load(), "one apply produces one notification")
			count, err := b.NodeCount()
			require.NoError(t, err)
			require.EqualValues(t, 1, count, "external apply and retirement must not count twice")
			require.NoError(t, b.FlushWriteBehind())
		})
	}
}

// A flush returns only once the listeners of the generations it drained have
// run, also while another goroutine is delivering an earlier event.
func TestWriteBehindFlushWaitsForQueuedNotifications(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var secondDelivered atomic.Bool
	b.OnNodeCreated(func(n *Node) {
		if n.ID == "test:first" {
			close(entered)
			<-release
		}
		if n.ID == "test:second" {
			secondDelivered.Store(true)
		}
	})
	first := make(chan error, 1)
	go func() { _, err := b.CreateNode(&Node{ID: "test:first"}); first <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first listener never started")
	}
	bufferedCreate(t, b, &Node{ID: "test:second"})
	flushed := make(chan error, 1)
	go func() { flushed <- b.FlushWriteBehind() }()
	select {
	case err := <-flushed:
		require.NoError(t, err)
		t.Fatal("FlushWriteBehind returned while its generation's notification was queued behind an earlier listener")
	case <-time.After(300 * time.Millisecond):
	}
	unblock()
	require.NoError(t, <-flushed)
	require.True(t, secondDelivered.Load())
	require.NoError(t, <-first)
}

// Two transactions open before either commits create the same stable ID; the
// first is acknowledged into the buffer. The second must fail with a
// conflict, so one node is stored and counted, with the first body.
func TestWriteBehindDuplicateStableIDCreate(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour})
			require.NoError(t, err)
			defer b.Close()
			var txs [2]*BadgerTransaction
			for i := range txs {
				txs[i], err = b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, txs[i].SetImplicit(buffered))
				_, err = txs[i].CreateNode(&Node{ID: "test:shared", Labels: []string{"Doc"}, Properties: map[string]any{"v": int64(i)}})
				require.NoError(t, err)
			}
			require.NoError(t, txs[0].Commit())
			err = txs[1].Commit()
			require.ErrorIs(t, err, ErrConflict, "both stable-ID creates acknowledged for one stored body")
			require.NoError(t, b.FlushWriteBehind())
			count, err := b.NodeCount()
			require.NoError(t, err)
			require.EqualValues(t, 1, count)
			docs, err := b.NodeCountByLabelInNamespace("test", "Doc")
			require.NoError(t, err)
			require.EqualValues(t, 1, docs)
			node, err := b.GetNode("test:shared")
			require.NoError(t, err)
			require.EqualValues(t, 0, node.Properties["v"], "the first successful CREATE keeps the ID")
		})
	}
}
