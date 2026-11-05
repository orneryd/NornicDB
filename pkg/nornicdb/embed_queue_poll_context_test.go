package nornicdb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// pollErrorQueue fails one exact discovery boundary while retaining the real
// existing queue, pending marker, point read and publication implementations.
type pollErrorQueue struct {
	*rejectingSidecarQueue
	calls   int
	failAt  int
	failure error
}

func (q *pollErrorQueue) FindNodeNeedingEmbedding(ctx context.Context) (*storage.Node, error) {
	q.calls++
	if q.calls == q.failAt {
		return nil, q.failure
	}
	return q.rejectingSidecarQueue.FindNodeNeedingEmbedding(ctx)
}

func TestEmbeddingPollErrorIsPausedAndRetryable(t *testing.T) {
	for _, mode := range []string{"whole", "document-batch", "resolved-document-batch"} {
		t.Run(mode, func(t *testing.T) {
			worker, queue, provider := nodeSaveRetryFixture(t, mode, 1, 0)
			failure := errors.New("native discovery read rejected")
			faults := &pollErrorQueue{rejectingSidecarQueue: queue, failAt: 1, failure: failure}
			worker.storage = faults
			empty := 0
			worker.onQueueEmpty = func(int) { empty++ }
			require.Equal(t, embedBatchPaused, worker.processNextBatchResult())
			require.Zero(t, provider.GetEmbedCount())
			require.Zero(t, worker.inFlight.Load())
			require.Empty(t, worker.claimedNodes)
			require.Zero(t, empty)
			require.Equal(t, []storage.NodeID{"node-000"}, queue.pending)
			require.Equal(t, embedBatchWorked, worker.processNextBatchResult())
			require.Positive(t, provider.GetEmbedCount())
			require.Empty(t, worker.claimedNodes)
			require.Zero(t, empty, "only the outer proven-empty owner schedules clustering")
		})
	}
}

func TestEmbeddingLaterBatchPollErrorRestoresEarlierClaim(t *testing.T) {
	worker, queue, provider := nodeSaveRetryFixture(t, "document-batch", 2, 0)
	failure := errors.New("second native discovery read rejected")
	worker.storage = &pollErrorQueue{rejectingSidecarQueue: queue, failAt: 2, failure: failure}
	nodes, _, err := worker.claimNodeBatch(2)
	require.ErrorIs(t, err, failure)
	require.Empty(t, nodes)
	require.Empty(t, worker.claimedNodes)
	require.Zero(t, worker.inFlight.Load())
	require.Zero(t, provider.GetEmbedCount())
	require.ElementsMatch(t, []storage.NodeID{"node-000", "node-001"}, queue.pending)
	require.Equal(t, embedBatchWorked, worker.processNextBatchResult())
	require.Empty(t, worker.claimedNodes)
	require.Equal(t, int64(2), worker.processed.Load())
}

func TestEmbeddingFinderReturnsActualCancellation(t *testing.T) {
	worker, queue, provider := nodeSaveRetryFixture(t, "whole", 1, 0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	node, err := worker.findNodeWithoutEmbedding(ctx)
	require.Nil(t, node)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, provider.GetEmbedCount())
	require.Equal(t, []storage.NodeID{"node-000"}, queue.pending)
	require.Equal(t, embedBatchWorked, worker.processNextBatchResult())
}

func TestEmbeddingFailedPollRetainsProgressUntilProvenEmpty(t *testing.T) {
	worker, queue, provider := nodeSaveRetryFixture(t, "whole", 1, 0)
	faults := &pollErrorQueue{rejectingSidecarQueue: queue, failAt: 2, failure: errors.New("discovery after completed work failed")}
	worker.storage = faults
	worker.config.ClusterDebounceDelay = time.Hour
	worker.config.ClusterMinBatchSize = 1
	delivered := make(chan int, 8)
	worker.onQueueEmpty = func(count int) { delivered <- count }
	require.Equal(t, embedBatchPaused, worker.processUntilEmpty())
	require.Equal(t, int64(1), worker.processed.Load())
	require.Positive(t, provider.GetEmbedCount())
	require.Empty(t, worker.claimedNodes)
	worker.clusterDebounceMu.Lock()
	scheduled, count := worker.clusterDebounceTimer, worker.pendingClusterCount
	worker.clusterDebounceMu.Unlock()
	require.Nil(t, scheduled, "a failed poll cannot schedule an empty event")
	require.Equal(t, 1, count, "completed work must survive a paused discovery pass")
	select {
	case value := <-delivered:
		t.Fatalf("false queue-empty event after failed poll: %d", value)
	default:
	}
	require.Equal(t, embedBatchIdle, worker.processUntilEmpty())
	worker.clusterDebounceMu.Lock()
	first := worker.clusterDebounceTimer
	worker.clusterDebounceMu.Unlock()
	require.NotNil(t, first)
	require.Equal(t, embedBatchIdle, worker.processUntilEmpty())
	worker.clusterDebounceMu.Lock()
	repeated := worker.clusterDebounceTimer
	worker.clusterDebounceMu.Unlock()
	require.Same(t, first, repeated, "an empty poll with no new work must not postpone the active debounce")
	// A later failed poll stops that timer while preserving the same progress.
	faults.failAt = faults.calls + 1
	require.Equal(t, embedBatchPaused, worker.processUntilEmpty())
	worker.clusterDebounceMu.Lock()
	scheduled, count = worker.clusterDebounceTimer, worker.pendingClusterCount
	worker.clusterDebounceMu.Unlock()
	require.Nil(t, scheduled)
	require.Equal(t, 1, count)
	worker.config.ClusterDebounceDelay = time.Millisecond
	require.Equal(t, embedBatchIdle, worker.processUntilEmpty())
	select {
	case value := <-delivered:
		require.Equal(t, 1, value)
	case <-time.After(time.Second):
		t.Fatal("healthy empty retry lost completed clustering progress")
	}
	worker.Close() // joins actual owned timer callbacks before exact-once assertion
	select {
	case value := <-delivered:
		t.Fatalf("duplicate queue-empty event: %d", value)
	default:
	}
	worker.clusterDebounceMu.Lock()
	defer worker.clusterDebounceMu.Unlock()
	require.Zero(t, worker.pendingClusterCount)
	require.Nil(t, worker.clusterDebounceTimer)
}

func TestClusteringClaimedCallbackDoesNotConsumePausedReplacement(t *testing.T) {
	worker, _, _ := nodeSaveRetryFixture(t, "whole", 0, 0)
	worker.config.ClusterDebounceDelay = time.Millisecond
	worker.config.ClusterMinBatchSize = 1
	delivered := make(chan int, 8)
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(releaseFirst) }) })
	worker.onQueueEmpty = func(count int) {
		delivered <- count
		if count == 1 {
			close(firstEntered)
			<-releaseFirst
		}
	}
	worker.reconcileClusteringProgress(1, true)
	select {
	case <-firstEntered:
	case <-time.After(time.Second):
		t.Fatal("first timer callback never claimed its completed work")
	}
	// The first callback already consumed its count and owns its lifetime credit.
	// A later pause may stop a queued replacement, never retract that first event.
	worker.config.ClusterDebounceDelay = time.Hour
	worker.reconcileClusteringProgress(2, true)
	worker.reconcileClusteringProgress(0, false)
	worker.clusterDebounceMu.Lock()
	count, timer := worker.pendingClusterCount, worker.clusterDebounceTimer
	worker.clusterDebounceMu.Unlock()
	require.Equal(t, 2, count)
	require.Nil(t, timer)
	release.Do(func() { close(releaseFirst) })
	worker.config.ClusterDebounceDelay = time.Millisecond
	worker.reconcileClusteringProgress(0, true)
	got := []int{}
	for len(got) < 2 {
		select {
		case count := <-delivered:
			got = append(got, count)
		case <-time.After(time.Second):
			t.Fatal("replacement lost its distinct completed progress")
		}
	}
	worker.Close() // joins both actual callbacks, including the first stopped-too-late owner
	require.Equal(t, []int{1, 2}, got)
	select {
	case count := <-delivered:
		t.Fatalf("duplicate clustering progress %d", count)
	default:
	}
}

func TestEmbeddingWithoutClusteringCallbackRetainsNoTimerWork(t *testing.T) {
	worker, _, provider := nodeSaveRetryFixture(t, "whole", 1, 0)
	require.Nil(t, worker.onQueueEmpty)
	generation := worker.generation()
	require.Zero(t, generation.Active())
	require.Zero(t, worker.work.Active())
	require.Equal(t, embedBatchIdle, worker.processUntilEmpty())
	require.Equal(t, int64(1), worker.processed.Load())
	require.Positive(t, provider.GetEmbedCount())
	require.Empty(t, worker.claimedNodes)
	worker.clusterDebounceMu.Lock()
	timer, count, running := worker.clusterDebounceTimer, worker.pendingClusterCount, worker.clusterDebounceRunning
	worker.clusterDebounceMu.Unlock()
	require.Nil(t, timer, "disabled clustering must not arm a default debounce")
	require.Zero(t, count, "disabled clustering must not retain completed progress")
	require.False(t, running)
	require.Zero(t, generation.Active(), "no timer may hold an admitted generation credit")
	require.Zero(t, worker.work.Active())
}
