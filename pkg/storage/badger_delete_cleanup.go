package storage

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Deleting a node or relationship leaves a Badger delete marker on each of
// its keys, including the node records and label index that scans walk.
// Badger removes a marker only when a compaction writes it into a level with
// no older data below. A small or idle database never runs one: its data
// stays in the memtable and level 0 until those fill up. Scans then step over
// every marker, so after a mass delete they stay slower indefinitely (#911).
//
// After deleteCleanupThreshold deletes, once deletes have stopped for
// deleteCleanupQuietPeriod, the engine has Badger flush its memtables and
// compact level 0 by dropping a throw-away marker key. Commits wait for the
// clean-up (managedBadgerDB.DropPrefix holds the commit gate) instead of
// failing.
const (
	// deleteCleanupThreshold is how many node and relationship deletes since
	// the last clean-up make the next one due.
	deleteCleanupThreshold = 50_000
	// deleteCleanupQuietPeriod is how long deletes must have stopped before
	// a due clean-up runs, so it doesn't pause writes during a bulk delete.
	deleteCleanupQuietPeriod = 30 * time.Second
)

// deleteCleanupMarkerKey is written and then dropped by each clean-up:
// Badger's DropPrefix only compacts when the prefix has data.
var deleteCleanupMarkerKey = []byte{prefixMVCCMeta, prefixMVCCMetaDeleteCleanupMarker}

// deleteCleanup counts deletes since the last clean-up and runs the next one
// from a background goroutine. A nil *deleteCleanup ignores deletes.
type deleteCleanup struct {
	threshold int64
	quiet     time.Duration
	deletes   atomic.Int64 // since the last clean-up
	last      atomic.Int64 // UnixNano of the latest delete
	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
}

// recordDelete counts one deleted node or relationship and wakes the
// clean-up goroutine once a clean-up is due.
func (c *deleteCleanup) recordDelete() {
	if c == nil {
		return
	}
	c.last.Store(time.Now().UnixNano())
	if c.deletes.Add(1) >= c.threshold {
		select {
		case c.wake <- struct{}{}:
		default:
		}
	}
}

// waitQuiet waits until no delete has happened for c.quiet. It returns false
// when the engine is closing.
func (c *deleteCleanup) waitQuiet() bool {
	for {
		timer := time.NewTimer(c.quiet)
		select {
		case <-c.stop:
			timer.Stop()
			return false
		case <-timer.C:
		}
		if time.Since(time.Unix(0, c.last.Load())) >= c.quiet {
			return true
		}
	}
}

// startDeleteCleanup starts the clean-up goroutine of an on-disk engine.
func (b *BadgerEngine) startDeleteCleanup() {
	c := &deleteCleanup{
		threshold: deleteCleanupThreshold,
		quiet:     deleteCleanupQuietPeriod,
		wake:      make(chan struct{}, 1),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	b.deleteCleanup = c
	go func() {
		defer close(c.done)
		for {
			select {
			case <-c.stop:
				return
			case <-c.wake:
			}
			if !c.waitQuiet() {
				return
			}
			if counted := c.deletes.Load(); counted >= c.threshold {
				b.runDeleteCleanup(counted)
			}
		}
	}()
}

// stopDeleteCleanup stops the clean-up goroutine and waits for it, including
// a clean-up in progress. Close calls it before taking the write barrier.
func (b *BadgerEngine) stopDeleteCleanup() {
	c := b.deleteCleanup
	if c == nil {
		return
	}
	c.stopOnce.Do(func() { close(c.stop) })
	<-c.done
}

// runDeleteCleanup runs one clean-up for counted deletes and logs it. A
// failed clean-up keeps the count, so the next delete retries it.
func (b *BadgerEngine) runDeleteCleanup(counted int64) {
	start := time.Now()
	if err := b.compactDeletedEntries(); err != nil {
		b.log.Error("deleted-entry clean-up failed", "deletes", counted, slog.Any("error", err))
		return
	}
	b.deleteCleanup.deletes.Add(-counted)
	b.log.Info("deleted-entry clean-up completed", "deletes", counted,
		"duration_ms", time.Since(start).Milliseconds())
}

// compactDeletedEntries has Badger flush its memtables and compact level 0,
// which drops delete markers with no older data below them. It writes the
// throw-away marker key with an ordinary commit and drops it.
func (b *BadgerEngine) compactDeletedEntries() error {
	if err := b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(deleteCleanupMarkerKey, []byte{1})
	}); err != nil {
		return err
	}
	return recoverBadgerClosedPanic(func() error {
		return b.db.DropPrefix(deleteCleanupMarkerKey)
	})
}
