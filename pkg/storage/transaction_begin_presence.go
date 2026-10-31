package storage

import (
	"context"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// transactionBeginPresence retains immutable commit prefixes, not a copy of
// the graph or an observation map populated by ordinary reads. Retired
// generations remain charged to their existing memory leases until unpinned.
type transactionBeginPresence struct {
	mu          sync.RWMutex
	released    bool
	buffer      *WriteBehindBuffer
	active      *CommitBuffer
	commits     []bufferedCommit
	draining    []*CommitBuffer
	drainingBuf [2]*CommitBuffer
	prev, next  *transactionBeginPresence
	listed      bool
	once        sync.Once
}

// pinTransactionBeginLocked runs under the canonical read-view retirement
// boundary, together with capture of the physical oracle timestamp.
func (w *WriteBehindBuffer) pinTransactionBeginLocked() (*transactionBeginPresence, error) {
	if w == nil {
		return nil, nil
	}
	w.beginPresenceMu.Lock()
	defer w.beginPresenceMu.Unlock()
	if w.beginPresenceClosed {
		return nil, ErrStorageClosed
	}
	presence := &transactionBeginPresence{buffer: w, active: w.active, listed: true}
	presence.commits = w.active.commits[:len(w.active.commits):len(w.active.commits)]
	w.active.beginPins.Add(1)
	presence.draining = presence.drainingBuf[:0]
	for i := len(w.draining) - 1; i >= 0; i-- {
		generation := w.draining[i]
		generation.beginPins.Add(1)
		presence.draining = append(presence.draining, generation)
	}
	presence.next = w.beginPresences
	if w.beginPresences != nil {
		w.beginPresences.prev = presence
	}
	w.beginPresences = presence
	return presence, nil
}

func (presence *transactionBeginPresence) release() {
	if presence == nil {
		return
	}
	presence.once.Do(func() {
		presence.mu.Lock()
		defer presence.mu.Unlock()
		w := presence.buffer
		w.beginPresenceMu.Lock()
		if presence.listed {
			if presence.prev != nil {
				presence.prev.next = presence.next
			} else {
				w.beginPresences = presence.next
			}
			if presence.next != nil {
				presence.next.prev = presence.prev
			}
			presence.prev, presence.next, presence.listed = nil, nil, false
		}
		w.beginPresenceMu.Unlock()
		presence.active.releaseBeginPin()
		for _, generation := range presence.draining {
			generation.releaseBeginPin()
		}
		presence.active, presence.commits, presence.draining, presence.buffer = nil, nil, nil, nil
		clear(presence.drainingBuf[:])
		presence.released = true
	})
}

func (w *WriteBehindBuffer) releaseTransactionBegins() {
	w.beginPresenceMu.Lock()
	w.beginPresenceClosed = true
	var presences []*transactionBeginPresence
	for presence := w.beginPresences; presence != nil; presence = presence.next {
		presences = append(presences, presence)
	}
	w.beginPresenceMu.Unlock()
	for _, presence := range presences {
		presence.release()
	}
}

func (generation *CommitBuffer) releaseBeginPin() {
	if generation.beginPins.Add(-1) == 0 && generation.queueRetired.Load() {
		generation.releaseMemoryLeases()
	}
}

func (generation *CommitBuffer) releaseMemoryLeases() {
	if generation.leasesReleased.CompareAndSwap(false, true) {
		for _, commit := range generation.commits {
			commit.memoryLease.Release()
		}
	}
}

// beginTransactionSnapshot transfers the canonical long-lived read lease to
// the native frame. Both the physical cut and buffered prefix are captured
// under the existing retirement lock; failure releases both resources. BEGIN
// does not read any entity population, so its empty ID scope leaves admission
// to each subsequent bound, prefix-scoped or global operation.
func (b *BadgerEngine) beginTransactionSnapshot(ctx context.Context, db *managedBadgerDB) (*badger.Txn, uint64, *transactionBeginPresence, error) {
	var presence *transactionBeginPresence
	var captureErr error
	view, err := b.captureReadView(ctx, func(w *WriteBehindBuffer) error {
		presence, captureErr = w.pinTransactionBeginLocked()
		return nil
	}, readViewOptions{longLived: true, scopeKind: readNodeIDsScope})
	if err == nil {
		err = captureErr
	}
	if err == nil && view.db != db {
		err = ErrStorageClosed
	}
	if err != nil {
		if view != nil {
			view.Close()
		}
		presence.release()
		return nil, 0, nil, err
	}
	return db.newTxnAt(view.readTs, false), view.readTs, presence, nil
}

// missingWriteTargetLocked distinguishes an observed target deleted by a
// peer from a target absent at begin or deleted by this frame itself.
func (tx *BadgerTransaction) missingWriteTargetLocked(kind bufferedEntityKind, id string) error {
	var key []byte
	if kind == bufferedNodeEntity {
		if _, ownDelete := tx.deletedNodes[NodeID(id)]; ownDelete {
			return ErrNotFound
		}
		key = nodeKey(NodeID(id))
	} else {
		if _, ownDelete := tx.deletedEdges[EdgeID(id)]; ownDelete {
			return ErrNotFound
		}
		key = edgeKey(EdgeID(id))
	}
	found, present, err := tx.beginPresence.readBufferedBeginPresence(kind, id)
	if err != nil {
		return err
	}
	if !found && tx.snapshotTx != nil {
		_, err := tx.snapshotTx.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		present = err == nil
	}
	if !present {
		return ErrNotFound
	}
	if kind == bufferedNodeEntity {
		return localizedError(localization.StorageTransactionNodeChanged(id), ErrConflict)
	}
	return localizedError(localization.StorageTransactionEdgeChanged(id), ErrConflict)
}
