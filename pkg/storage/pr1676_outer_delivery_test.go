package storage

import (
	"context"
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
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

// TestBufferedReplayDeliveryOutsideEmbeddingLease pins the replay branch of the
// outer delivery guard: an outcome write ordered after an acknowledged buffered
// commit replays that commit while the caller still holds its outer embedding
// lease, and neither the replayed body's listener delivery nor the accepted
// outcome's may run under that lease.
//
// The pending generation must still be pending when the outcome write runs, or
// the replay this test observes does not exist: the engine's write-behind worker
// is woken by this test's own seed flush, and a wake consumed after the buffered
// ACK (before the listener below is registered) replays the generation with no
// listener attached, so queueCommitOperations reserves no notification and the
// replayed body reaches nobody while the outcome is delivered once. Stopping that
// worker (the manual-drain fixture) leaves the sidecar's ordering flush
// (orderAfterBufferedCommits) as the only replay, which fixes the schedule:
// buffered ACK -> outcome write under the outer lease -> the sidecar's
// publication hold released by PersistEmbeddingOutcome -> one drain delivering
// the replayed body and then the accepted outcome, both after the lease ended.
func TestBufferedReplayDeliveryOutsideEmbeddingLease(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	ctx := context.Background()
	_, err := b.CreateNode(ctx, &Node{ID: "test:n", Properties: map[string]any{"text": "before"}})
	require.NoError(t, err)
	require.NoError(t, b.FlushWriteBehind(ctx))
	buffered, err := implicitWrite(b, func(tx *BadgerTransaction) error {
		return tx.UpdateGraphNode(ctx, NodeUpdateRequest{After: &Node{ID: "test:n", Properties: map[string]any{"text": "after"}}, Policy: ClientNodeEdits})
	})
	require.NoError(t, err)
	require.True(t, buffered, "the update must be acknowledged buffered, so its replay is pending")
	node, err := b.GetNode(ctx, "test:n")
	require.NoError(t, err)
	node, err = SnapshotNodeForEmbedding(ctx, node)
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{{1, 2}}
	node.SetEmbeddingAttempt(NewEmbeddingAttempt(1, 1, node.EmbeddingRevision(), func() (func(), bool) { return func() {}, true }, nil))
	// Snapshot setup must leave the replay pending: the outcome write, not the
	// fixture, is what applies it.
	require.Positive(t, b.writeBehind.PendingOps(), "the buffered commit was replayed before the outcome write")
	var leased, released, unexpected atomic.Bool
	var replays, outcomes atomic.Int64
	var deliverySeq, replaySeq, outcomeSeq atomic.Int64
	var replayUnderLease, outcomeUnderLease atomic.Bool
	var replayBeforeRelease, outcomeBeforeRelease atomic.Bool
	b.OnNodeUpdated(func(updated *Node) {
		// The replayed buffered body carries no embedding payload; the accepted
		// outcome carries it. An ACK alone never reaches a listener.
		switch {
		case len(updated.ChunkEmbeddings) == 1:
			outcomes.Add(1)
			outcomeSeq.Store(deliverySeq.Add(1))
			if leased.Load() {
				outcomeUnderLease.Store(true)
			}
			if !released.Load() {
				outcomeBeforeRelease.Store(true)
			}
		case len(updated.ChunkEmbeddings) == 0 && updated.Properties["text"] == "after":
			replays.Add(1)
			replaySeq.Store(deliverySeq.Add(1))
			if leased.Load() {
				replayUnderLease.Store(true)
			}
			if !released.Load() {
				replayBeforeRelease.Store(true)
			}
		default:
			unexpected.Store(true)
		}
	})
	err = PersistEmbeddingOutcome(ctx, node, func(ctx context.Context) error {
		leased.Store(true)
		defer func() {
			leased.Store(false)
			released.Store(true)
		}()
		return b.UpdateNodeEmbeddingSidecar(ctx, node)
	})
	require.NoError(t, err)
	require.False(t, unexpected.Load(), "a listener delivery matched neither the replayed body nor the accepted outcome")
	require.EqualValues(t, 1, replays.Load(), "the replayed buffered body was not delivered exactly once")
	require.EqualValues(t, 1, outcomes.Load(), "the accepted outcome was not delivered exactly once")
	require.False(t, replayUnderLease.Load(), "replay callback ran under outer embedding lease")
	require.False(t, outcomeUnderLease.Load(), "outcome callback ran under outer embedding lease")
	require.False(t, replayBeforeRelease.Load(), "replay callback was delivered before the outer embedding lease ended")
	require.False(t, outcomeBeforeRelease.Load(), "outcome callback was delivered before the outer embedding lease ended")
	require.Less(t, replaySeq.Load(), outcomeSeq.Load(), "the replayed body must be delivered before the outcome that depends on it")
	stored, err := b.GetNode(ctx, "test:n")
	require.NoError(t, err)
	require.Equal(t, "after", stored.Properties["text"], "the buffered update was never replayed into storage")
}

func TestInterleavedNotificationOutsideEmbeddingLease(t *testing.T) {
	b, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
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
