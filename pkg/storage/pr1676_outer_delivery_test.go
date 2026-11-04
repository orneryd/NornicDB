package storage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUnboundEmbeddingWALListenerOutsideMutationLease(t *testing.T) {
	b := newSidecarTestBadger(t)
	node := publicationProofPayload(t, b)
	log, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	defer log.Close()
	wal := NewWALEngine(b, log)
	var called, unlocked bool
	b.OnNodeUpdated(func(*Node) {
		called = true
		unlocked = wal.mutationMu.TryLock()
		if unlocked {
			wal.mutationMu.Unlock()
		}
	})
	require.NoError(t, wal.UpdateNodeEmbeddingSidecar(context.Background(), node))
	require.True(t, called)
	require.True(t, unlocked, "unbound sidecar delivered under WAL mutation lease")
}

func TestBufferedReplayDeliveryOutsideEmbeddingLease(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	ctx := context.Background()
	_, err = b.CreateNode(ctx, &Node{ID: "test:n", Properties: map[string]any{"text": "before"}})
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind(ctx))
	buffered, err := implicitWrite(b, func(tx *BadgerTransaction) error {
		return tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:n", Properties: map[string]any{"text": "after"}}, Policy: ClientNodeEdits})
	})
	require.NoError(t, err)
	require.True(t, buffered)
	node, err := b.GetNode(ctx, "test:n")
	require.NoError(t, err)
	node, err = SnapshotNodeForEmbedding(ctx, node)
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{1, 2}}
	node.SetEmbeddingAttempt(NewEmbeddingAttempt(1, 1, node.EmbeddingRevision(), func() (func(), bool) { return func() {}, true }, nil))
	var held, replayUnderLease atomic.Bool
	var calls atomic.Int32
	b.OnNodeUpdated(func(*Node) {
		calls.Add(1)
		if held.Load() {
			replayUnderLease.Store(true)
		}
	})
	err = PersistEmbeddingOutcome(ctx, node, func(ctx context.Context) error {
		held.Store(true)
		defer held.Store(false)
		return b.UpdateNodeEmbeddingSidecar(ctx, node)
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, calls.Load(), int32(2))
	require.False(t, replayUnderLease.Load(), "replay or outcome callback ran under outer embedding lease")
}

func TestInterleavedNotificationOutsideEmbeddingLease(t *testing.T) {
	b, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	defer b.Close()
	ctx := context.Background()
	node := publicationProofPayload(t, b)
	_, err = b.CreateNode(ctx, &Node{ID: "test:other", Properties: map[string]any{"v": "before"}})
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind(ctx))
	node.SetEmbeddingAttempt(NewEmbeddingAttempt(1, 1, node.EmbeddingRevision(), func() (func(), bool) { return func() {}, true }, nil))
	var held, callbackUnderLease, first atomic.Bool
	b.OnNodeUpdated(func(*Node) {
		if held.Load() {
			callbackUnderLease.Store(true)
		}
	})
	entered, resume := make(chan struct{}), make(chan struct{})
	restore := setCommitTailHook(func() {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-resume
		}
	})
	defer restore()
	sidecar := make(chan error, 1)
	go func() {
		sidecar <- PersistEmbeddingOutcome(ctx, node, func(ctx context.Context) error {
			held.Store(true)
			defer held.Store(false)
			return b.UpdateNodeEmbeddingSidecar(ctx, node)
		})
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		close(resume)
		t.Fatal("sidecar did not reach the commit tail")
	}
	other := make(chan error, 1)
	go func() {
		other <- b.UpdateNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:other", Properties: map[string]any{"v": "after"}}})
	}()
	queued := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b.notificationMu.Lock()
		count := 0
		for n := b.notificationHead; n != nil; n = n.next {
			count++
		}
		b.notificationMu.Unlock()
		if count >= 3 {
			queued = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	close(resume)
	require.NoError(t, <-sidecar)
	require.NoError(t, <-other)
	require.True(t, queued, "the other update did not reach the notification queue")
	require.False(t, callbackUnderLease.Load(), "an interleaved callback ran under the embedding lease")
}
