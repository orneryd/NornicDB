package storage

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// DeindexCleanupJob periodically drains pending deindex work items,
// writes tombstones for their secondary-index keys, and marks them completed.
type DeindexCleanupJob struct {
	engine   *BadgerEngine
	interval time.Duration
	mu       sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewDeindexCleanupJob creates a cleanup job. Default interval is 24h.
func NewDeindexCleanupJob(engine *BadgerEngine, interval time.Duration) *DeindexCleanupJob {
	if interval <= 0 {
		interval = 24 * time.Hour
	}
	return &DeindexCleanupJob{
		engine:   engine,
		interval: interval,
	}
}

func (j *DeindexCleanupJob) Start(ctx context.Context) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cancel != nil {
		return
	}
	ctx, j.cancel = context.WithCancel(ctx)
	j.done = make(chan struct{})
	go j.run(ctx)
}

func (j *DeindexCleanupJob) Stop() {
	j.mu.Lock()
	cancel := j.cancel
	done := j.done
	j.cancel = nil
	j.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (j *DeindexCleanupJob) run(ctx context.Context) {
	defer close(j.done)
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := j.RunOnce(ctx); err != nil {
				log.Printf("[deindex] cleanup error: %v", err)
			} else if n > 0 {
				log.Printf("[deindex] cleaned up %d entries", n)
			}
		}
	}
}

// RunOnce processes all pending deindex work items. Returns the number of
// entities successfully deindexed.
func (j *DeindexCleanupJob) RunOnce(ctx context.Context) (int, error) {
	items, err := j.engine.ScanPendingDeindexWorkItems()
	if err != nil {
		return 0, fmt.Errorf("scan pending work items: %w", err)
	}
	if len(items) == 0 {
		return 0, nil
	}

	deindexed := 0
	for _, item := range items {
		if ctx.Err() != nil {
			return deindexed, ctx.Err()
		}

		if err := j.processWorkItem(item); err != nil {
			log.Printf("[deindex] failed to process %s (retry %d): %v", item.TargetID, item.RetryCount, err)
			j.retryWorkItem(item)
			continue
		}
		deindexed++
	}
	return deindexed, nil
}

func (j *DeindexCleanupJob) processWorkItem(item *DeindexWorkItem) error {
	cat, err := j.engine.GetIndexEntryCatalog(item.TargetID)
	if err != nil {
		return fmt.Errorf("get catalog for %s: %w", item.TargetID, err)
	}
	if cat == nil {
		return j.engine.DeleteDeindexWorkItem(item.WorkItemID)
	}
	if cat.Deindexed {
		return j.engine.DeleteDeindexWorkItem(item.WorkItemID)
	}

	if err := j.engine.WriteIndexTombstones(cat.IndexKeys); err != nil {
		return fmt.Errorf("write tombstones for %s: %w", item.TargetID, err)
	}

	cat.Deindexed = true
	if err := j.engine.PutIndexEntryCatalog(item.TargetID, cat); err != nil {
		return fmt.Errorf("mark catalog deindexed for %s: %w", item.TargetID, err)
	}

	return j.engine.DeleteDeindexWorkItem(item.WorkItemID)
}

func (j *DeindexCleanupJob) retryWorkItem(item *DeindexWorkItem) {
	item.RetryCount++
	backoff := time.Duration(1<<uint(item.RetryCount)) * time.Minute
	if backoff > 24*time.Hour {
		backoff = 24 * time.Hour
	}
	item.NextAttemptAt = time.Now().Add(backoff).UnixNano()
	if item.RetryCount > 10 {
		item.Status = "failed"
	}
	data, err := msgpack.Marshal(item)
	if err != nil {
		return
	}
	j.engine.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(deindexWorkItemKey(item.WorkItemID), data)
	})
}
