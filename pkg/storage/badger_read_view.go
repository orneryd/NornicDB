package storage

// Context-owner and envelope-metadata convergence author: Claude (storage-format-owners lane), #1693, 30 October 2026.

import (
	"context"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

type readScopeKind uint8

const (
	readNamespaceScope readScopeKind = iota
	readPrefixScope
	readNodeIDsScope
)

// readViewOptions is private to the storage owner. cached must have no externally
// visible effects: a concurrent publication can require a retry. current is only
// for materializers with no user callbacks; it permits current body-cache reads,
// validated as one group, with a strict snapshot retry if a cache changes.
type readViewOptions struct {
	scopeKind readScopeKind
	nodeIDs   []NodeID
	longLived bool
	scope     string
	cached    func(*WriteBehindBuffer) (bool, error)
	current   *bool
}

type badgerReadView struct {
	scope     string
	scopeKind readScopeKind
	txn       *badger.Txn
	db        *managedBadgerDB
	engine    *BadgerEngine
	readTs    uint64
	longLived bool
	mu        sync.Mutex
	closed    bool
}

// captureReadView owns every committed/overlay boundary. A nil successful view
// means cached completed the read. The idle case uses two epoch loads, no
// transaction, overlay copy or view allocation. Busy cache reads and transaction
// capture share the retirement lock. Default callers always get a strict view.
//
// Whenever a view is opened, capture always runs under the retirement boundary,
// including when write-behind is disabled or idle. Its argument is the selected
// overlay source: nil means empty, and the overlay selectors accept nil without
// allocating. Committed index/cache reads belong outside any nil-overlay guard.
// Only a complete cached result can return without opening a view or capturing.
//
// ctx is the request: admission and capture observe it, and a capture error
// (including the request's cancellation) closes the view before returning.
// Keep ctx separate from the borrowed callbacks in options: neither the request
// nor the callbacks belong to the returned view's lifetime. A read with no
// request passes context.Background().
func (b *BadgerEngine) captureReadView(ctx context.Context, capture func(*WriteBehindBuffer) error, options ...readViewOptions) (*badgerReadView, error) {
	var opts readViewOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if err := b.pendingReadConflict(ctx, opts); err != nil {
		return nil, err
	}
	w := b.writeBehind
	if opts.cached != nil {
		epoch, idle := w.idleReadEpoch(opts.scope)
		if idle {
			done, err := opts.cached(nil)
			if after, stillIdle := w.idleReadEpoch(opts.scope); stillIdle && after == epoch && (done || err != nil) {
				return nil, err
			}
		}
	}
	// Only the slow capture branch acquires a transaction and retirement lease.
	if w != nil {
		w.mu.RLock()
		defer w.mu.RUnlock()
		if opts.cached != nil {
			if done, err := opts.cached(w); done || err != nil {
				return nil, err
			}
		}
	}
	db, err := b.beginHelperTxn()
	if err != nil {
		return nil, err
	}
	var txn *badger.Txn
	var readTs uint64
	if opts.longLived {
		readTs, err = db.oracle.beginRead()
	} else {
		txn, readTs, err = db.beginTxn(false)
	}
	if err != nil {
		b.txnWG.Done()
		return nil, err
	}
	view := &badgerReadView{scope: strings.Clone(opts.scope), scopeKind: opts.scopeKind, txn: txn, db: db, engine: b, readTs: readTs, longLived: opts.longLived}
	if capture != nil {
		overlay := w
		if _, idle := w.idleReadEpoch(opts.scope); idle {
			overlay = nil
		}
		err = b.runCapture(view, capture, overlay)
	}
	if opts.longLived {
		b.txnWG.Done()
	}
	if err != nil {
		view.Close()
		return nil, err
	}
	return view, nil
}

// runCapture runs capture on a just-opened view. A panic closes the view, and
// ends a long-lived view's capture lease, before it continues: a crash inside a
// capture surfaces as that panic and never leaves Close waiting for the view.
func (b *BadgerEngine) runCapture(view *badgerReadView, capture func(*WriteBehindBuffer) error, overlay *WriteBehindBuffer) error {
	defer func() {
		if recovered := recover(); recovered != nil {
			if view.longLived {
				b.txnWG.Done()
			}
			view.Close()
			panic(recovered)
		}
	}()
	return capture(overlay)
}

func (v *badgerReadView) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return
	}
	v.closed = true
	if v.txn != nil {
		v.txn.Discard()
		v.txn = nil
	}
	v.db.endRead(v.readTs)
	if !v.longLived {
		v.engine.txnWG.Done()
	}
}

// Long-lived views hold only the oracle timestamp between scans. Engine Close
// waits for an active scan, never for the entire build lifetime.
func (v *badgerReadView) withView(read func(*badger.Txn) error) error {
	if !v.longLived {
		return read(v.txn)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return ErrStorageClosed
	}
	// A later scan of a long-lived view re-admits its scope only; it carries no
	// node IDs and is not part of the capturing request.
	if err := v.engine.pendingScopeConflict(v.scopeKind, v.scope); err != nil {
		return err
	}

	db, err := v.engine.beginHelperTxn()
	if err != nil {
		return err
	}
	defer v.engine.txnWG.Done()
	if db != v.db {
		return ErrStorageClosed
	}
	return recoverBadgerClosedPanic(func() error {
		txn := db.newTxnAt(v.readTs, false)
		defer txn.Discard()
		return read(txn)
	})
}

func (b *BadgerEngine) withReadView(ctx context.Context, capture func(*WriteBehindBuffer) error, read func(*badger.Txn) error, options ...readViewOptions) error {
	if len(options) != 0 && options[0].current != nil {
		return b.withCurrentReadView(ctx, capture, read, options[0])
	}
	view, err := b.captureReadView(ctx, capture, options...)
	if err != nil || view == nil {
		return err
	}
	return view.readAndClose(read)
}

func (v *badgerReadView) readAndClose(read func(*badger.Txn) error) error {
	defer v.Close()
	return recoverBadgerClosedPanic(func() error { return v.withView(read) })
}

// Only materializers opting into current body caches need group validation.
func (b *BadgerEngine) withCurrentReadView(ctx context.Context, capture func(*WriteBehindBuffer) error, read func(*badger.Txn) error, opts readViewOptions) error {
	nodeGen, edgeGen := b.nodeCacheGen.current(), b.edgeCacheGen.current()
	*opts.current = true
	view, err := b.captureReadView(ctx, capture, opts)
	if err != nil || view == nil {
		return err
	}
	defer view.Close()

	err = recoverBadgerClosedPanic(func() error { return view.withView(read) })
	if nodeGen != b.nodeCacheGen.current() || edgeGen != b.edgeCacheGen.current() {
		// Re-evaluate only internal materialization, never a user callback.
		*opts.current = false
		return recoverBadgerClosedPanic(func() error { return view.withView(read) })
	}
	return err
}

// pendingReadConflict admits exactly the graph population the capture reads.
// Bounded ID requests only consult their namespaces when any DROP is pending;
// the healthy batch path retains one atomic admission check rather than N. Only
// that per-ID admission observes the request; scope admission is pendingScopeConflict.
func (b *BadgerEngine) pendingReadConflict(ctx context.Context, opts readViewOptions) error {
	if opts.scopeKind != readNodeIDsScope {
		return b.pendingScopeConflict(opts.scopeKind, opts.scope)
	}
	if b.pendingNamespaceConflict("") == nil {
		return nil
	}
	// The retained owner decides whether any DROP is pending. Do not
	// interpret its publication counter at a second read boundary.
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, id := range opts.nodeIDs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if id != "" {
			if err := b.pendingNamespaceConflict(namespaceForNodeID(id)); err != nil {
				return err
			}
		}
	}
	return nil
}

// pendingScopeConflict admits a prefix or namespace scope. A node-ID scope with
// no IDs (a long-lived view's later scan) admits against any pending DROP.
func (b *BadgerEngine) pendingScopeConflict(kind readScopeKind, scope string) error {
	switch kind {
	case readPrefixScope:
		return b.pendingPrefixConflict(scope)
	case readNodeIDsScope:
		return b.pendingNamespaceConflict("")
	default:
		return b.pendingNamespaceConflict(scope)
	}
}
