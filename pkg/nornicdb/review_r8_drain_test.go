package nornicdb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type reviewRejectFirstDisk struct {
	*storage.BadgerEngine
	first atomic.Bool
}

func (e *reviewRejectFirstDisk) UpdateNodeEmbeddingSidecar(node *storage.Node) error {
	if e.first.CompareAndSwap(false, true) {
		return errors.New("review: reject the first sidecar commit")
	}
	return e.BadgerEngine.UpdateNodeEmbeddingSidecar(node)
}

func TestReviewPendingCountIncludesRetainedResults(t *testing.T) {
	disk, err := storage.NewBadgerEngine(t.TempDir())
	require.NoError(t, err)
	defer disk.Close()
	disk.SetEmbeddingsEnabled(true)
	_, err = disk.CreateNode(&storage.Node{ID: "review:doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "waiting for a durable vector"}})
	require.NoError(t, err)
	engine := &reviewRejectFirstDisk{BadgerEngine: disk}
	worker := NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{NumWorkers: 1, DeferWorkerStart: true, MaxRetries: 1, ChunkSize: 8192, EmbedBatchSize: 1, ScanInterval: time.Hour})
	defer worker.Close()
	db := &DB{baseStorage: engine, embedQueue: worker}
	require.True(t, worker.processNextBatch())
	// The next ordinary worker pass seeks fresh work while the retry is in
	// its one-second backoff, consuming the retained ID's durable marker.
	worker.processNextBatch()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	waitErr := db.WaitForEmbeddings(ctx)
	node, err := disk.GetNode("review:doc")
	require.NoError(t, err)
	t.Logf("durable pending=%d public pending=%d processed=%d stored vectors=%d wait error=%v", disk.PendingEmbeddingsCount(), db.PendingEmbeddingsCount(), worker.processed.Load(), len(node.ChunkEmbeddings), waitErr)
	if waitErr == nil {
		require.NotEmpty(t, node.ChunkEmbeddings, "public completion cannot succeed while the paid result remains unsaved in retry backoff")
	} else {
		require.ErrorIs(t, waitErr, context.DeadlineExceeded)
	}
}
