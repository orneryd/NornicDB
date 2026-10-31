package storage

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DeindexCleanupJob periodically drains pending deindex work items,
// writes tombstones for their secondary-index keys, and marks them completed.
type DeindexCleanupJob struct {
	engine   *BadgerEngine
	interval time.Duration
	mu       sync.Mutex
	run      *engineWorker // the running job, accepted engine work; mu
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

// Start runs the job, as accepted engine work, until Stop, the engine's stop
// or ctx ends.
func (j *DeindexCleanupJob) Start(ctx context.Context) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.run != nil {
		return
	}
	j.run = j.engine.startEngineWorker(ctx, j.loop)
}

// Stop stops the job and waits for its actual return.
func (j *DeindexCleanupJob) Stop() {
	j.mu.Lock()
	run := j.run
	j.run = nil
	j.mu.Unlock()
	run.stopAndJoin()
}

func (j *DeindexCleanupJob) loop(ctx context.Context) {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := j.RunOnce(ctx); err != nil {
				j.engine.log.Warn("deindex cleanup error", "component", "deindex", "error", err)
			} else if n > 0 {
				j.engine.log.Info("deindex cleanup complete", "component", "deindex", "entries", n)
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
			j.engine.log.Warn("deindex work item failed",
				"component", "deindex",
				"target_id", item.TargetID,
				"retry", item.RetryCount,
				"error", err,
			)
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
	_ = j.engine.PutDeindexWorkItem(item)
}
