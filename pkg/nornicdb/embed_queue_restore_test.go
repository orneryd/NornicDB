package nornicdb

import (
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// pendingReadEngine has one pending node whose read fails with getErr.
type pendingReadEngine struct {
	storage.Engine
	pending storage.NodeID
	getErr  error
	marked  []storage.NodeID
}

func (e *pendingReadEngine) FindNodeNeedingEmbedding() *storage.Node {
	return &storage.Node{ID: e.pending}
}
func (e *pendingReadEngine) GetNode(storage.NodeID) (*storage.Node, error) { return nil, e.getErr }
func (e *pendingReadEngine) MarkNodeEmbedded(id storage.NodeID)            { e.marked = append(e.marked, id) }
func (*pendingReadEngine) RefreshPendingEmbeddingsIndex() int              { return 0 }

func newHeldTestWorker(t *testing.T, engine storage.Engine) *EmbedWorker {
	t.Helper()
	worker := NewEmbedWorker(nil, engine, &EmbedWorkerConfig{NumWorkers: 0, EmbedBatchSize: 1, ChunkSize: 512, MaxRetries: 1, DeferWorkerStart: true})
	t.Cleanup(func() { worker.Close() })
	return worker
}

// A pending node whose read fails stays pending unless it is gone: a
// restore turning reads away must not drop it (#1020).
func TestClaimNextNodeKeepsUnreadablePendingNode(t *testing.T) {
	restoring := &pendingReadEngine{pending: "nornic:a", getErr: storage.ErrStorageRestoring}
	require.Nil(t, newHeldTestWorker(t, restoring).claimNextNode())
	require.Empty(t, restoring.marked, "the node stays pending")

	deleted := &pendingReadEngine{pending: "nornic:b", getErr: storage.ErrNotFound}
	require.Nil(t, newHeldTestWorker(t, deleted).claimNextNode())
	require.Equal(t, []storage.NodeID{"nornic:b"}, deleted.marked, "a deleted node leaves the pending index")
}

// Recording a failure for a node that can't be read leaves it pending the
// same way; a deleted one leaves the pending index.
func TestMarkNodeEmbeddingFailedKeepsUnreadablePendingNode(t *testing.T) {
	restoring := &pendingReadEngine{pending: "nornic:a", getErr: storage.ErrStorageRestoring}
	newHeldTestWorker(t, restoring).markNodeEmbeddingFailed("nornic:a", errors.New("provider failed"))
	require.Empty(t, restoring.marked)

	deleted := &pendingReadEngine{pending: "nornic:b", getErr: storage.ErrNotFound}
	newHeldTestWorker(t, deleted).markNodeEmbeddingFailed("nornic:b", errors.New("provider failed"))
	require.Equal(t, []storage.NodeID{"nornic:b"}, deleted.marked)
}

// While a restore holds the worker it claims and scans nothing; the hold
// waits for in-flight nodes, and the release starts a new pass.
func TestEmbedWorkerHoldForRestore(t *testing.T) {
	engine := &pendingReadEngine{pending: "nornic:a", getErr: storage.ErrNotFound}
	worker := newHeldTestWorker(t, engine)

	worker.inFlight.Store(1)
	held := make(chan func(), 1)
	go func() { held <- worker.holdForRestore(5 * time.Second) }()
	select {
	case <-held:
		t.Fatal("the hold returned with a node in flight")
	case <-time.After(30 * time.Millisecond):
	}
	worker.inFlight.Store(0)
	release := <-held

	require.False(t, worker.processNextBatch())
	worker.processUntilEmpty()
	require.Empty(t, engine.marked, "nothing is read while held")

	release()
	require.Zero(t, worker.restoreHolds.Load())
	require.Len(t, worker.trigger, 1, "the release starts a new pass")
	worker.processNextBatch()
	require.Equal(t, []storage.NodeID{"nornic:a"}, engine.marked, "it reads again once released")
}

// A node still in flight when the wait ends doesn't block the restore.
func TestEmbedWorkerHoldForRestoreStopsWaiting(t *testing.T) {
	worker := newHeldTestWorker(t, &pendingReadEngine{pending: "nornic:a", getErr: storage.ErrNotFound})
	worker.inFlight.Store(1)
	start := time.Now()
	release := worker.holdForRestore(20 * time.Millisecond)
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, int32(1), worker.restoreHolds.Load())
	release()
}
