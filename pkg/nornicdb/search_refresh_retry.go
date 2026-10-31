package nornicdb

import (
	"log"
	"time"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/search"
)

// A retry owns its failed reservation until it is due, superseded or retired.
// It is absent from the ordinary pending maps while waiting.
type pendingSearchRetry struct {
	work    search.RefreshRetry
	attempt int
	due     time.Time
	waiting bool
	timer   *lifecycle.OwnedTimer
}

func (db *DB) searchRefreshRetry(entry *dbSearchService) search.RefreshRetryOwner {
	return func(work search.RefreshRetry, err error) {
		if db.shouldIgnoreSearchIndexingError(err) {
			work.Discard()
			return
		}
		entry.pendingMu.Lock()
		defer entry.pendingMu.Unlock()
		if entry.retired || work.Retired() {
			work.Discard()
			return
		}
		key := work.Key()
		previous := entry.refreshRetries[key]
		if previous != nil && previous.work.Retired() {
			entry.forgetRetryLocked(key, previous, false)
			previous = nil
		}
		if previous != nil && previous.work != work && !work.NewerThan(previous.work) {
			work.Discard()
			return
		}
		if previous != nil && previous.work == work && previous.waiting {
			return
		}
		attempt := 1
		if previous != nil {
			if previous.work == work {
				attempt = previous.attempt + 1
			}
			entry.forgetRetryLocked(key, previous, previous.work != work)
		}
		if attempt > searchRefreshRetryLimit {
			work.Discard()
			log.Printf("search refresh of %s in database %s failed %d retries; dropped: %v", key[2:], entry.dbName, searchRefreshRetryLimit, err)
			return
		}
		delay := entry.flushDelay() << attempt
		if delay > searchRefreshRetryMax || delay <= 0 {
			delay = searchRefreshRetryMax
		}
		retry := &pendingSearchRetry{work: work, attempt: attempt, due: time.Now().Add(delay), waiting: true}
		if entry.refreshRetries == nil {
			entry.refreshRetries = make(map[string]*pendingSearchRetry)
		}
		entry.refreshRetries[key] = retry
		retry.timer = entry.svc.AfterFunc(delay, func() { db.dispatchSearchRetry(entry, key, retry) })
	}
}

// Eligibility and queue dispatch are one decision under pendingMu. Both node
// and relationship failures, from synchronous consumers or a drain, use it.
func (db *DB) dispatchSearchRetry(entry *dbSearchService, key string, retry *pendingSearchRetry) {
	entry.pendingMu.Lock()
	if entry.refreshRetries[key] != retry || !retry.waiting {
		entry.pendingMu.Unlock()
		return
	}
	if entry.retired || retry.work.Retired() {
		entry.forgetRetryLocked(key, retry, true)
		entry.pendingMu.Unlock()
		return
	}
	delay := time.Until(retry.due)
	if delay <= 0 && !retry.work.Current() {
		delay = entry.flushDelay()
	}
	if delay > 0 {
		retry.timer = entry.svc.AfterFunc(delay, func() { db.dispatchSearchRetry(entry, key, retry) })
		entry.pendingMu.Unlock()
		return
	}
	retry.waiting = false
	retry.timer = nil
	entry.queueReservedRefreshLocked(key[2:], retry.work)
	entry.pendingMu.Unlock()
	db.ensurePendingFlush(entry)
}

func (e *dbSearchService) forgetRetryLocked(key string, retry *pendingSearchRetry, discard bool) {
	if e.refreshRetries[key] != retry {
		return
	}
	delete(e.refreshRetries, key)
	if retry.timer != nil {
		retry.timer.Stop()
	}
	if discard {
		retry.work.Discard()
	}
}

// Successful preparation may have handed work to a build, or been superseded
// while reading. Keep ownership until its reservation actually retires.
func (db *DB) finishSearchRefresh(e *dbSearchService, work search.RefreshRetry) {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	key := work.Key()
	retry := e.refreshRetries[key]
	if retry == nil {
		return
	}
	if retry.work.Retired() {
		e.forgetRetryLocked(key, retry, false)
		return
	}
	if retry.work == work && !retry.waiting {
		retry.waiting = true
		delay := e.flushDelay()
		retry.due = time.Now().Add(delay)
		retry.timer = e.svc.AfterFunc(delay, func() { db.dispatchSearchRetry(e, key, retry) })
	}
}

// Caller holds pendingMu. Replaced queued work has no remaining dispatcher;
// discard it even when it is a noncurrent predecessor of the new reservation.
func (e *dbSearchService) queueReservedRefreshLocked(id string, work search.RefreshRetry) {
	if work.Node != (search.NodeMutation{}) {
		if e.pendingOps == nil {
			e.pendingOps = make(map[string]search.NodeMutation)
		}
		if old, ok := e.pendingOps[id]; ok && old != work.Node {
			(search.RefreshRetry{Node: old}).Discard()
		}
		e.pendingOps[id] = work.Node
	} else {
		if e.pendingEdgeOps == nil {
			e.pendingEdgeOps = make(map[string]search.EdgeMutation)
		}
		if old, ok := e.pendingEdgeOps[id]; ok && old != work.Edge {
			(search.RefreshRetry{Edge: old}).Discard()
		}
		e.pendingEdgeOps[id] = work.Edge
	}
}
